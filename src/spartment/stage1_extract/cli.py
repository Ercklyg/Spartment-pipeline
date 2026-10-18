"""Command line entrypoint for the Spartment Stage 1 extraction.

    python -m pipeline.stage --run-date 2026-10-18

Runs extraction, then extraction level validation, then the section 7
acceptance gate.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Sequence
from datetime import date

from spartment.common.config import ConfigError, Settings, load_settings
from spartment.common.io import write_json
from spartment.common.models import (
    DECISION_MANUAL_REVIEW,
    DECISION_REJECTED,
    STATUS_FAILED,
    STATUS_SUCCESS,
    STATUS_WARNING,
    TableRun,
)
from spartment.stage1_extract.contract import TABLE_NAMES
from spartment.stage1_extract.extract import extract_table, quarantined_run
from spartment.stage1_extract.guard import TablePlan, build_plans
from spartment.stage1_extract.handover import decide, write_handover
from spartment.stage1_extract.source import SupabaseError, SupabaseReadClient
from spartment.stage1_extract.validate import validate

EXIT_APPROVED = 0
EXIT_EXTRACTION_FAILURE = 1
EXIT_CONFIG = 2
EXIT_SCHEMA_UNREACHABLE = 3
EXIT_REJECTED = 4
EXIT_MANUAL_REVIEW = 5


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="spartment-extract",
        description="Extract Spartment operational tables into rawdata/.",
    )
    parser.add_argument("--run-date", type=date.fromisoformat, default=None)
    parser.add_argument(
        "--tables",
        default=",".join(TABLE_NAMES),
        help="comma separated subset of tables to extract",
    )
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--retry-limit", type=int, default=3)
    parser.add_argument(
        "--strict-contract",
        action="store_true",
        help="stop the run if any required table or column is missing",
    )
    parser.add_argument(
        "--no-extra-columns",
        action="store_true",
        help="omit the additional columns that Docs/extraction.md omits",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="debugging only: do not run validation or the acceptance gate",
    )
    parser.add_argument("--max-rows", type=int, default=None)
    return parser.parse_args(argv)


def _print_drift(plans: list[TablePlan], include_extra: bool) -> None:
    print(f"schema guard (extra columns {'included' if include_extra else 'excluded'})")
    print(f"{'table':<26}{'state':<14}{'columns':<10}detail")
    for plan in plans:
        state = "OK" if plan.ok else "QUARANTINED"
        detail = plan.reason or f"{len(plan.selected)} columns resolved"
        print(f"{plan.spec.name:<26}{state:<14}{len(plan.selected):<10}{detail}")
    print()


def _print_validation(report: object) -> None:
    print("validation")
    print(f"{'check':<38}{'result':<16}{'critical':<10}detail")
    for check in report.checks:
        print(
            f"{check.check_name:<38}{check.result:<16}"
            f"{'yes' if check.critical else 'no':<10}{check.detail}"
        )
    print()


def _requested_tables(raw: str) -> list[str]:
    return [name.strip() for name in raw.split(",") if name.strip()]


def _extraction_payload(
    settings: Settings,
    runs: list[TableRun],
    plans: list[TablePlan],
    started_perf: float,
    report: object | None,
    decision: object | None,
) -> dict[str, object]:
    if any(run.extraction_status == STATUS_FAILED for run in runs):
        status, validation = STATUS_FAILED, "FAIL"
    elif any(run.extraction_status == STATUS_WARNING for run in runs):
        status, validation = STATUS_WARNING, "WARNING"
    else:
        status, validation = STATUS_SUCCESS, "PASS"

    if report is not None:
        validation = report.validation_result

    return {
        "pipeline_run_id": settings.run_id,
        "extraction_mode": "full",
        "extraction_started_at": settings.run_started_at.isoformat(),
        "extraction_ended_at": settings.now().isoformat(),
        "extraction_duration_ms": int((time.perf_counter() - started_perf) * 1000),
        "extraction_status": status,
        "validation_result": validation,
        "acceptance_decision": decision.decision if decision else None,
        "records_extracted": sum(run.records_extracted for run in runs),
        "records_rejected": report.records_rejected if report else 0,
        "error_message": None if status == STATUS_SUCCESS else "one or more tables did not extract",
        "error_code": None if status == STATUS_SUCCESS else "PARTIAL_OR_FAILED_RUN",
        "tables": [run.to_dict() for run in runs],
        "manifest": {plan.spec.name: plan.manifest() for plan in plans if plan.ok},
        "schema_drift": [
            {"table_name": plan.spec.name, "detail": plan.reason}
            for plan in plans
            if plan.reason
        ],
    }


def _exit_code(payload: dict[str, object], decision: object | None) -> int:
    if payload["extraction_status"] == STATUS_FAILED:
        return EXIT_EXTRACTION_FAILURE
    if decision is None:
        return EXIT_APPROVED
    if decision.decision == DECISION_REJECTED:
        return EXIT_REJECTED
    if decision.decision == DECISION_MANUAL_REVIEW:
        return EXIT_MANUAL_REVIEW
    return EXIT_APPROVED


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    started_perf = time.perf_counter()

    try:
        settings = load_settings(
            run_date=args.run_date,
            page_size=args.page_size,
            retry_limit=args.retry_limit,
        )
    except (ConfigError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_CONFIG

    requested = _requested_tables(args.tables)
    unknown = [name for name in requested if name not in TABLE_NAMES]
    if unknown:
        print(f"unknown tables requested: {', '.join(unknown)}", file=sys.stderr)
        return EXIT_CONFIG
    if not requested:
        print("no tables requested", file=sys.stderr)
        return EXIT_CONFIG

    client = SupabaseReadClient(
        settings.rest_base,
        settings.service_role_key,
        page_size=settings.page_size,
        retry_limit=settings.retry_limit,
        retry_backoff_seconds=settings.retry_backoff_seconds,
        timeout_seconds=settings.request_timeout_seconds,
    )

    try:
        document = client.openapi()
    except SupabaseError as exc:
        print(f"schema discovery failed: {exc}", file=sys.stderr)
        return EXIT_SCHEMA_UNREACHABLE

    include_extra = not args.no_extra_columns
    plans = [
        plan
        for plan in build_plans(document, include_extra)
        if plan.spec.name in requested
    ]

    _print_drift(plans, include_extra)

    if args.strict_contract:
        blocked = [plan for plan in plans if not plan.ok]
        if blocked:
            print(
                "strict contract: refusing to continue with "
                f"{len(blocked)} non conforming table(s)",
                file=sys.stderr,
            )
            return EXIT_REJECTED

    runs: list[TableRun] = []
    for plan in plans:
        if not plan.ok:
            print(f"QUARANTINED  {plan.spec.name}: {plan.reason}", file=sys.stderr)
            runs.append(quarantined_run(settings, plan))
            continue
        run = extract_table(client, plan, settings, max_rows=args.max_rows)
        runs.append(run)
        print(
            f"{run.extraction_status:<12}{plan.spec.name:<26}"
            f"{run.records_extracted:>8} rows  {run.extraction_duration_ms:>7} ms"
        )

    report = None
    decision = None
    if not args.skip_validation:
        report = validate(settings, runs, plans)
        _print_validation(report)
        decision = decide(report)
        validation_path, handover_path = write_handover(settings, runs, plans, report, decision)

    payload = _extraction_payload(settings, runs, plans, started_perf, report, decision)
    metadata_path = settings.metadata_path()
    write_json(metadata_path, payload)

    print()
    print(f"run id        {settings.run_id}")
    print(f"rows          {payload['records_extracted']} across {len(runs)} table(s)")
    print(f"extraction    {payload['extraction_status']}")
    if decision is not None:
        print(f"validation    {report.validation_result}")
        print(f"acceptance    {decision.decision}")
        print(f"responsible   {decision.responsible_party.split('.')[0]}")
        print(f"handover      {handover_path}")
    else:
        print("validation    skipped")
    print(f"metadata      {metadata_path}")

    return _exit_code(payload, decision)


if __name__ == "__main__":
    raise SystemExit(main())