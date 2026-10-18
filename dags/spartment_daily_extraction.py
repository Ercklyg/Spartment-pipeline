"""Daily Spartment extraction DAG.

Credentials come from the Airflow connection ``spartment_postgres``. Never
place the service role key in this file, in the DAG run configuration, or in an
Airflow Variable.

Run ``pip install -e .`` so the ``spartment`` package is importable from the
Airflow environment. Airflow places only its own ``dags_folder`` on
``sys.path``, so a repository relative import would not resolve.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from spartment.common.config import REPO_ROOT, load_retention_days
from spartment.common.models import DECISION_MANUAL_REVIEW, DECISION_REJECTED
from spartment.common.retention import apply_retention
from spartment.stage1_extract.cli import main as extract_main

MANILA = "Asia/Manila"
TASK_EXTRACT = "extract_and_validate"
TASK_GATE = "acceptance_gate"
TASK_RETAIN = "retain"

logger = logging.getLogger(__name__)


def _latest_handover() -> Path:
    directory = REPO_ROOT / "rawdata" / "_handover"
    files = sorted(directory.glob("*.json"), key=lambda item: item.stat().st_mtime)
    if not files:
        raise AirflowException("no handover record was produced by the extraction task")
    return files[-1]


def run_extraction_and_validation(**context: Any) -> dict[str, str]:
    extract_main([])
    path = _latest_handover()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        "run_id": payload["pipeline_run_id"],
        "decision": payload["acceptance_decision"]["decision"],
        "handover_path": str(path),
    }


def evaluate_acceptance(**context: Any) -> str:
    info = context["ti"].xcom_pull(task_ids=TASK_EXTRACT) or {}
    decision = info.get("decision", DECISION_MANUAL_REVIEW)

    if decision == DECISION_REJECTED:
        raise AirflowException(
            f"extraction was REJECTED for run {info.get('run_id')}. "
            f"See {info.get('handover_path')}"
        )
    if decision == DECISION_MANUAL_REVIEW:
        logger.warning(
            "extraction %s requires MANUAL REVIEW. Promotion is held pending review.",
            info.get("run_id"),
        )
    return decision


def run_retention(**context: Any) -> int:
    operational_days, audit_days = load_retention_days()
    result = apply_retention(
        raw_root=REPO_ROOT / "rawdata",
        operational_days=operational_days,
        audit_days=audit_days,
        apply=True,
    )
    logger.info("retention removed %d file(s)", len(result.deleted))
    return len(result.deleted)


with DAG(
    dag_id="spartment_daily_extraction",
    description="Daily full extraction of Spartment operational tables with validation and handover",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 10, 18, tz=MANILA),
    catchup=False,
    max_active_runs=1,
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(hours=1),
    default_args={"owner": "data-pipeline-maintainer", "depends_on_past": False},
    tags=["spartment", "extraction", "stage-1"],
) as dag:
    extract_and_validate = PythonOperator(
        task_id=TASK_EXTRACT,
        python_callable=run_extraction_and_validation,
    )

    acceptance_gate = PythonOperator(
        task_id=TASK_GATE,
        python_callable=evaluate_acceptance,
    )

    retain = PythonOperator(
        task_id=TASK_RETAIN,
        python_callable=run_retention,
    )

    extract_and_validate >> acceptance_gate >> retain