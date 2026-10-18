"""Read-only HTTP client for the Supabase PostgREST API.

Only GET requests are constructed. The client has no code path that can issue
a write, so an extraction run cannot mutate the source database even when it
holds a service role credential.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class SupabaseError(RuntimeError):
    def __init__(self, message: str, code: str | None = None, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


def _postgrest_error(detail: str) -> tuple[str | None, str | None]:
    try:
        payload = json.loads(detail)
    except (json.JSONDecodeError, TypeError):
        return None, None
    if not isinstance(payload, Mapping):
        return None, None
    code = payload.get("code")
    message = payload.get("message") or payload.get("hint")
    return (str(code) if code else None), (str(message) if message else None)


def _content_range_total(header: str | None) -> int | None:
    if not header or "/" not in header:
        return None
    reported = header.rsplit("/", 1)[1].strip()
    if not reported.isdigit():
        return None
    return int(reported)


class SupabaseReadClient:
    def __init__(
        self,
        rest_base: str,
        api_key: str,
        page_size: int = 1000,
        retry_limit: int = 3,
        retry_backoff_seconds: float = 2.0,
        timeout_seconds: float = 60.0,
    ) -> None:
        self._rest_base = rest_base.rstrip("/")
        self._api_key = api_key
        self._page_size = page_size
        self._retry_limit = retry_limit
        self._retry_backoff_seconds = retry_backoff_seconds
        self._timeout = timeout_seconds

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {
            "apikey": self._api_key,
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }
        if extra:
            headers.update(extra)
        return headers

    def _get(
        self,
        path: str,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> tuple[Any, Mapping[str, str]]:
        url = f"{self._rest_base}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        last_error: Exception | None = None
        for attempt in range(1, self._retry_limit + 1):
            request = urllib.request.Request(url, method="GET", headers=self._headers(headers))
            try:
                with urllib.request.urlopen(request, timeout=self._timeout) as response:
                    payload = response.read().decode("utf-8")
                    decoded = json.loads(payload) if payload.strip() else None
                    return decoded, dict(response.headers)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", "replace")
                code, message = _postgrest_error(detail)
                last_error = SupabaseError(
                    f"GET {path} failed with HTTP {exc.code}" + (f": {message}" if message else ""),
                    code=code,
                    status=exc.code,
                )
                if exc.code not in RETRYABLE_STATUS:
                    raise last_error from None
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = SupabaseError(f"GET {path} failed: {exc}")
            if attempt < self._retry_limit:
                time.sleep(self._retry_backoff_seconds * (2 ** (attempt - 1)))
        raise last_error if last_error else SupabaseError(f"GET {path} failed")

    def openapi(self) -> Mapping[str, Any]:
        document, _ = self._get("/")
        if not isinstance(document, Mapping):
            raise SupabaseError("schema root did not return an object", code="unexpected_schema_root")
        return document

    def count_total(self, table: str, column: str = "id") -> int | None:
        """Return the exact source row count for a table without fetching rows."""
        headers = {"Prefer": "count=exact", "Range": "0-0", "Range-Unit": "rows"}
        _, response_headers = self._get(
            f"/{table}",
            {"select": column, "limit": "0"},
            headers,
        )
        return _content_range_total(
            response_headers.get("Content-Range") or response_headers.get("content-range")
        )

    def paginate(self, table: str, columns: Sequence[str]) -> Iterator[dict[str, Any]]:
        select = ",".join(columns)
        start = 0
        total: int | None = None

        while True:
            end = start + self._page_size - 1
            headers = {
                "Range": f"{start}-{end}",
                "Range-Unit": "rows",
                "Prefer": "count=exact",
            }
            rows, response_headers = self._get(f"/{table}", {"select": select, "order": "id"}, headers)
            if not isinstance(rows, list):
                raise SupabaseError(
                    f"GET /{table} returned a non-array payload", code="unexpected_payload"
                )

            reported = _content_range_total(
                response_headers.get("Content-Range") or response_headers.get("content-range")
            )
            if reported is not None:
                total = reported

            for row in rows:
                yield row

            count = len(rows)
            if count == 0:
                return
            start += count
            if total is not None and start >= total:
                return
            if count < self._page_size:
                return