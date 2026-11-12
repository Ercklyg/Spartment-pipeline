"""Daily local load: wait for approved Stage 2 evidence for the logical date."""
from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

from spartment.common.config import REPO_ROOT
from spartment.stage3_load.cli import main
from spartment.stage3_load.retention import retain


def selected_handover(context):
    logical = context.get("logical_date")
    run_date = logical.date().isoformat() if logical else context["ds"]
    candidates = []
    for path in (REPO_ROOT / "transformed" / "_handover").glob("*.json"):
        try:
            h = json.loads(path.read_text(encoding="utf-8"))
            if h.get("processing_period") == run_date:
                candidates.append((path.stat().st_mtime_ns, path, h))
        except (OSError, ValueError):
            continue
    if not candidates:
        return None
    _, path, h = max(candidates)
    # A newer held/rejected batch must not silently fall back to an older success.
    if h.get("acceptance_decision", {}).get("decision") != "APPROVED" or not h.get("released_for_load"):
        raise AirflowFailException("Stage 2 batch is not APPROVED and released")
    return path


def ready(**context):
    return selected_handover(context) is not None


def run_load(**context):
    path = selected_handover(context)
    if path is None:
        raise AirflowFailException("No approved transformation handover")
    # Airflow interval end is the actual 03:00 scheduled boundary, unlike the
    # logical date which normally denotes the previous interval's start.
    start = context.get("data_interval_end") or pendulum.now("Asia/Manila")
    deadline = start + timedelta(minutes=30)
    code = main(["run", "--handover", str(path), "--retry-transient", "--deadline", deadline.isoformat()])
    if code:
        raise AirflowFailException(f"Load held or failed (exit {code}); inspect logs/spartment-load")


def retain_logs(**context):
    return retain(REPO_ROOT, apply=True)


with DAG(
    dag_id="spartment_daily_load", schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 11, 12, tz="Asia/Manila"),
    catchup=False, max_active_runs=1,
    default_args={"owner": "data-pipeline-maintainer", "retries": 0},
    tags=["spartment", "load", "stage-3"],
) as dag:
    wait_for_approved_transform = PythonSensor(
        task_id="wait_for_approved_transform", python_callable=ready,
        mode="reschedule", poke_interval=30, timeout=15 * 60,
    )
    load_and_validate = PythonOperator(task_id="load_and_validate", python_callable=run_load)
    retention = PythonOperator(task_id="retain", python_callable=retain_logs, trigger_rule="all_done")
    wait_for_approved_transform >> load_and_validate >> retention
