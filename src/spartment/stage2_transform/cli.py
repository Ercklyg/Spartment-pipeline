"""Command line entry point for Stage 2."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from spartment.stage2_transform.models import (
    TransformConfigError,
    TransformError,
    TransformInputError,
    TransformRetryableError,
    TransformSchemaError,
)
from spartment.stage2_transform.pipeline import transform
from spartment.stage2_transform.settings import build_settings


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transform one validated Spartment Stage 1 batch."
    )
    parser.add_argument("--parent-run-id", help="Stage 1 pipeline_run_id to transform")
    parser.add_argument("--run-id", help="Stage 2 run id (default: generated)")
    parser.add_argument("--reference-date", type=date.fromisoformat, help="Today for date-relative rules")
    parser.add_argument("--raw-root", type=Path, help="Raw data root (default: repo rawdata)")
    parser.add_argument("--output-root", type=Path, help="Transformed output root")
    parser.add_argument("--log-root", type=Path, help="Transformation log root")
    parser.add_argument("--quarantine-root", type=Path, help="Quarantine root")
    parser.add_argument(
        "--allow-manual-review",
        action="store_true",
        help="Release a MANUAL REVIEW batch and accept a MANUAL REVIEW parent",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing files")
    return parser


def main(argv: list[str] | None = None, *, raise_errors: bool = False) -> int:
    args = _parser().parse_args(argv)
    try:
        settings, run_id = build_settings(
            raw_root=args.raw_root,
            output_root=args.output_root,
            log_root=args.log_root,
            quarantine_root=args.quarantine_root,
            reference_date=args.reference_date,
            run_id=args.run_id,
            allow_manual_review=args.allow_manual_review,
            dry_run=args.dry_run,
        )
        result = transform(settings, run_id, args.parent_run_id)
    except (TransformConfigError,) as exc:
        if raise_errors:
            raise
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    except TransformRetryableError as exc:
        if raise_errors:
            raise
        print(f"retryable input error: {exc}", file=sys.stderr)
        return 3
    except (TransformSchemaError, TransformInputError) as exc:
        if raise_errors:
            raise
        print(f"input or schema error: {exc}", file=sys.stderr)
        return 3
    except TransformError as exc:
        if raise_errors:
            raise
        print(f"transformation failure: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        if raise_errors:
            raise
        print(f"transformation failure: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    state = "released" if result.released else "held"
    print(f"{result.run_id}: {result.decision} ({state})")
    print(f"parent={result.parent_run_id} run_date={result.run_date}")
    if result.handover_path:
        print(f"handover={result.handover_path}")
    if result.validation_path:
        print(f"validation={result.validation_path}")
    if result.summary_path:
        print(f"summary={result.summary_path}")
    if settings.dry_run:
        print("dry run: no transformed, log, or quarantine files were written")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
