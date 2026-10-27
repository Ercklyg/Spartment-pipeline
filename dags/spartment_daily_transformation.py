"""Daily Spartment transformation DAG.

Transforms the validated Stage 1 batch for the current processing date and
holds promotion when the batch requires manual review. Run ``pip install -e .``
so the ``spartment`` package is importable from the Airflow environment.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator

from spartment.common.config import REPO_ROOT, load_retention_days
from spartment.common.models import DECISION_MANUAL_REVIEW, DECISION_REJECTED
from spartment.stage2_transform.cli import main as transform_main
from spartment.stage2_transform.models import TransformRetryableError
from spartment.stage2_transform.retention import apply_retention

MANILA = "Asia/Manila"
TASK_TRANSFORM = "transform_and_validate"
TASK_GATE = "acceptance_gate"
TASK_RETAIN = "retain"

logger = logging.getLogger(__name__)


def _handover_for_date(run_date: str) -> Path:
    directory = REPO_ROOT / "rawdata" / "_handover"
    matches: list[Path] = []
    for path in directory.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(payload.get("processing_period", "")) == run_date:
            matches.append(path)
    if not matches:
        raise AirflowException(f"no Stage 1 handover found for processing date {run_date}")
    return max(matches, key=lambda item: item.stat().st_mtime)


def _latest_transformed_handover(parent_run_id: str, since: float) -> Path:
    directory = REPO_ROOT / "transformed" / "_handover"
    files = sorted(directory.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    for path in files:
        if path.stat().st_mtime < since - 1:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("parent_run_id") == parent_run_id:
            return path
    raise AirflowException(
        f"no transformed handover was produced for parent run {parent_run_id}"
    )


def _logical_run_date(context: dict[str, Any]) -> str:
    logical = context.get("logical_date") or context.get("ds")
    if logical:
        if isinstance(logical, str):
            return logical[:10]
        return logical.date().isoformat()
    return pendulum.now(MANILA).date().isoformat()


def run_transformation(**context: Any) -> dict[str, Any]:
    run_date = _logical_run_date(context)
    parent_path = _handover_for_date(run_date)
    parent_run_id = parent_path.stem
    started = time.time()
    try:
        exit_code = transform_main(
            [
                "--parent-run-id",
                parent_run_id,
                "--reference-date",
                run_date,
            ],
            raise_errors=True,
        )
    except TransformRetryableError as exc:
        raise AirflowException(f"retryable read/parse failure: {exc}") from exc
    except Exception as exc:
        raise AirflowFailException(f"transformation failed: {exc}") from exc
    if exit_code not in (0, 1, 5):
        raise AirflowFailException(
            f"transformation failed for parent run {parent_run_id} with exit code {exit_code}"
        )

    path = _latest_transformed_handover(parent_run_id, started)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("parent_run_id") != parent_run_id:
        raise AirflowException(
            f"transformed handover {path} does not match parent run {parent_run_id}"
        )
    return {
        "run_id": payload["run_id"],
        "parent_run_id": parent_run_id,
        "decision": payload["acceptance_decision"]["decision"],
        "released": bool(payload.get("released_for_load")),
        "handover_path": str(path),
    }


def evaluate_acceptance(**context: Any) -> str:
    info = context["ti"].xcom_pull(task_ids=TASK_TRANSFORM) or {}
    decision = info.get("decision", DECISION_MANUAL_REVIEW)

    if decision == DECISION_REJECTED:
        raise AirflowException(
            f"transformation was REJECTED for run {info.get('run_id')}. "
            f"See {info.get('handover_path')}"
        )
    if decision == DECISION_MANUAL_REVIEW and not info.get("released"):
        logger.warning(
            "transformation %s requires MANUAL REVIEW. Promotion is held pending review.",
            info.get("run_id"),
        )
    return decision


def run_retention(**context: Any) -> int:
    operational_days, audit_days = load_retention_days()
    result = apply_retention(
        log_root=REPO_ROOT / "logs" / "spartment-transformation",
        quarantine_root=REPO_ROOT / "quarantine" / "transformation",
        operational_days=operational_days,
        audit_days=audit_days,
        apply=True,
        include_summaries=False,
        include_quarantine=False,
    )
    logger.info(
        "transformation retention archived %d file(s) and removed %d file(s)",
        len(result.archived),
        len(result.deleted),
    )
    return len(result.deleted)


with DAG(
    dag_id="spartment_daily_transformation",
    description="Daily transformation of the validated Spartment extraction batch",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 18, tz=MANILA),
    catchup=False,
    max_active_runs=1,
    retries=0,
    default_args={"owner": "data-pipeline-maintainer", "depends_on_past": False},
    tags=["spartment", "transformation", "stage-2"],
) as dag:
    transform_and_validate = PythonOperator(
        task_id=TASK_TRANSFORM,
        python_callable=run_transformation,
        retries=3,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(hours=1),
    )

    acceptance_gate = PythonOperator(
        task_id=TASK_GATE,
        python_callable=evaluate_acceptance,
        retries=0,
    )

    retain = PythonOperator(
        task_id=TASK_RETAIN,
        python_callable=run_retention,
        retries=0,
        trigger_rule="all_done",
    )

    transform_and_validate >> acceptance_gate >> retain
