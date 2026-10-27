"""Type conversion, formatting, and deterministic key helpers."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from spartment.stage2_transform.contract import PHT

CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
SPACE_RE = re.compile(r"\s+")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONEY_STRIP_RE = re.compile(r"[\s,₱]")


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = CONTROL_RE.sub("", str(value))
    text = SPACE_RE.sub(" ", text).strip()
    return text or None


def lower_text(value: object) -> str | None:
    text = clean_text(value)
    return text.lower() if text else None


def parse_date(value: object) -> date | None:
    text = clean_text(value)
    if not text or not DATE_RE.fullmatch(text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_timestamp(value: object) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return None
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError:
            return None
        parsed = datetime.combine(parsed_date, time.min)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PHT)
    return parsed.astimezone(PHT)


def parse_decimal(value: object) -> Decimal | None:
    text = clean_text(value)
    if not text:
        return None
    text = MONEY_STRIP_RE.sub("", text)
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def money(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def area(value: Decimal | None) -> str | None:
    return money(value)


def rate(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


def parse_bool(value: object) -> bool | None:
    text = clean_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "t", "1"}:
        return True
    if lowered in {"false", "f", "0"}:
        return False
    return None


def bool_text(value: bool | None) -> str:
    return "TRUE" if value else "FALSE"


def canonical_json(value: object) -> tuple[str | None, bool]:
    if value is None:
        return None, True
    text = str(value)
    if not text.strip():
        return None, True
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return text, False
    if not isinstance(parsed, dict):
        return text, False
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False), True


def normalize_uuid(value: object) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return str(uuid.UUID(text))
    except (ValueError, AttributeError, TypeError):
        return None


def surrogate(entity: str, source_id: str) -> str:
    return hashlib.md5(f"{entity}:{source_id}".encode("utf-8")).hexdigest()[:16]


def format_date(value: date | None) -> str | None:
    return value.isoformat() if value else None


def format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(PHT).isoformat(timespec="seconds")


def format_period(value: date | None) -> str | None:
    return f"{value.year:04d}-{value.month:02d}" if value else None


def days_between(start: date | None, end: date | None) -> int | None:
    if start is None or end is None:
        return None
    return (end - start).days


def mask_phone(value: object) -> str:
    text = clean_text(value) or ""
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) < 7:
        return "***"
    return f"{digits[:2]}***{digits[-4:]}"
