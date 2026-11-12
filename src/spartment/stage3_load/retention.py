"""Age-based local artifact retention with explicit incident holds and audit."""
from __future__ import annotations

import argparse
import gzip
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spartment.common.config import REPO_ROOT

from .models import atomic_json
from .package import read_json


def retain(root: Path, *, apply: bool = False, now: datetime | None = None,
           delete_expired: bool = False, approved_by: str | None = None) -> dict:
    root = root.resolve()
    moment = now or datetime.now(timezone.utc)
    held = set()
    holds = root / "loaded" / "_holds"
    if holds.exists():
        held = {p.stem for p in holds.glob("*.json")}
    result = {"apply": apply, "archived": [], "deleted": [], "protected": [], "approved_by": approved_by}
    log = root / "logs" / "spartment-load"
    records = {}
    for path in log.glob("*/*.json"):
        if path.parent.name in {"_archive", "_retention"}:
            continue
        try:
            data = read_json(path)
        except Exception:
            result["protected"].append(str(path))
            continue
        run = data.get("run_id")
        age = (moment - datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)).days
        # Conservatively retain all audit summaries and their packages. The registry
        # is not pruned automatically; this avoids breaking retained lineage.
        if "table_name" not in data:
            records[run] = data
            continue
        if run in held:
            result["protected"].append(str(path))
        elif age >= 90:
            destination = log / "_archive" / path.parent.name / (path.name + ".gz")
            result["archived"].append(str(path))
            if apply:
                destination.parent.mkdir(parents=True, exist_ok=True)
                # Preserve original evidence age for subsequent expiry checks.
                stat = path.stat()
                with path.open("rb") as source, gzip.open(destination, "wb") as target:
                    import shutil
                    shutil.copyfileobj(source, target)
                os.utime(destination, (stat.st_atime, stat.st_mtime))
                path.unlink()
    if delete_expired and not approved_by:
        raise ValueError("Expired archive deletion requires --approved-by")
    for path in (log / "_archive").glob("*/*.json.gz"):
        age = (moment - datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)).days
        # Match run IDs using archive contents, not underscore splitting.
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            data = json.load(stream)
        run = data.get("run_id")
        if run in held or records.get(run, {}).get("status") in {"HELD", "COMMIT_UNKNOWN", "RUNNING"}:
            result["protected"].append(str(path))
        elif age >= 365 and delete_expired:
            if not path.resolve().is_relative_to((log / "_archive").resolve()):
                raise ValueError("Archive deletion escaped intended root")
            result["deleted"].append(str(path))
            if apply:
                path.unlink()
    if apply:
        stamp = moment.strftime("%Y%m%dT%H%M%S%f")
        atomic_json(log / "_retention" / f"{stamp}.json", result)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Archive expired load table logs; dry run by default")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--delete-expired-archives", action="store_true")
    parser.add_argument("--approved-by")
    parser.add_argument("--hold-run", help="Protect an incident's artifacts from expiry")
    parser.add_argument("--reason")
    args = parser.parse_args(argv)
    if args.hold_run:
        import re
        if not re.fullmatch(r"spartment-load-[\w-]+", args.hold_run) or not args.reason:
            parser.error("A valid load run and --reason are required")
        atomic_json(REPO_ROOT / "loaded" / "_holds" / (args.hold_run + ".json"),
                    {"run_id": args.hold_run, "reason": args.reason})
    try:
        print(json.dumps(retain(REPO_ROOT, apply=args.apply, delete_expired=args.delete_expired_archives,
                                approved_by=args.approved_by)))
        return 0
    except ValueError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
