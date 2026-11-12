"""Operational guards tested without changing the user's scheduler or credentials."""
from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from spartment.common.config import _parse_env_file
from spartment.stage3_load import cli, daily
from spartment.stage3_load.models import Settings


class OperationalTests(unittest.TestCase):
    def test_dotenv_preserves_quoted_password_inside_dsn(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {}, clear=True):
            path = Path(temp) / ".env"
            path.write_text("CONNECTION=host=localhost password='a b'\nWRAPPED=\"hello\"\n")
            _parse_env_file(path)
            self.assertEqual(os.environ["CONNECTION"], "host=localhost password='a b'")
            self.assertEqual(os.environ["WRAPPED"], "hello")

    def test_transient_retries_are_bounded_and_linked(self):
        failures = [dict(run_id=f"attempt-{i}", status="FAILED", retryable=True) for i in range(4)]
        with patch.object(cli, "environment_settings", return_value=Settings(dsn="")), \
             patch("spartment.stage3_load.pipeline.load", side_effect=failures) as loader, \
             patch.object(cli.time, "sleep") as sleeper, contextlib.redirect_stdout(io.StringIO()):
            code = cli.main(["run", "--handover", "unused.json", "--retry-transient"])
        self.assertEqual(code, 1)
        self.assertEqual(loader.call_count, 4)
        self.assertEqual(sleeper.call_count, 300 + 600 + 1200)
        self.assertEqual(loader.call_args.kwargs["retry_of"], "attempt-2")

    def test_critical_failure_is_not_retried(self):
        with patch.object(cli, "environment_settings", return_value=Settings(dsn="")), \
             patch("spartment.stage3_load.pipeline.load", return_value=dict(run_id="x", status="FAILED", retryable=False)) as loader, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["run", "--handover", "unused.json", "--retry-transient"]), 1)
        self.assertEqual(loader.call_count, 1)

    def test_daily_chain_stops_on_upstream_rejection(self):
        with tempfile.TemporaryDirectory() as temp, \
             patch.dict(os.environ, {"SUPABASE_URL": "https://test.invalid", "SUPABASE_SERVICE_ROLE_KEY": "fixture", "SPARTMENT_LOAD_DSN": "fixture"}), \
             patch.object(daily, "REPO_ROOT", Path(temp)), \
             patch("spartment.stage1_extract.cli.main", return_value=4), \
             patch("spartment.stage2_transform.cli.main") as transformer, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(daily.main([]), 1)
            transformer.assert_not_called()
            summary = json.loads(next((Path(temp) / "logs").rglob("*.json")).read_text())
            self.assertEqual(summary["status"], "FAILED")

    def test_airflow_definition_and_approval_sensor(self):
        # This validates callback logic and wiring, not an installed Airflow scheduler.
        class Task:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
            def __rshift__(self, other):
                return other
        class Dag:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
        modules = {name: types.ModuleType(name) for name in (
            "airflow", "airflow.exceptions", "airflow.operators", "airflow.operators.python",
            "airflow.sensors", "airflow.sensors.python", "pendulum")}
        modules["airflow"].DAG = Dag
        modules["airflow.exceptions"].AirflowException = RuntimeError
        modules["airflow.exceptions"].AirflowFailException = RuntimeError
        modules["airflow.operators.python"].PythonOperator = Task
        modules["airflow.sensors.python"].PythonSensor = Task
        modules["pendulum"].datetime = lambda *args, **kw: datetime(*args, tzinfo=timezone.utc)
        modules["pendulum"].now = lambda *args: datetime.now(timezone.utc)
        path = Path(__file__).resolve().parents[1] / "dags" / "spartment_daily_load.py"
        with patch.dict(sys.modules, modules), tempfile.TemporaryDirectory() as temp:
            spec = importlib.util.spec_from_file_location("tested_load_dag", path)
            dag = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(dag)
            dag.REPO_ROOT = Path(temp)
            self.assertEqual(dag.dag.kwargs["schedule"], "0 3 * * *")
            self.assertIsNone(dag.selected_handover({"ds": "2026-11-12"}))
            folder = Path(temp) / "transformed" / "_handover"
            folder.mkdir(parents=True)
            handover = folder / "run.json"
            data = {"processing_period": "2026-11-12", "run_id": "fixture",
                    "released_for_load": True, "acceptance_decision": {"decision": "MANUAL REVIEW"}}
            handover.write_text(json.dumps(data))
            with self.assertRaises(RuntimeError):
                dag.selected_handover({"ds": "2026-11-12"})
            data["acceptance_decision"]["decision"] = "APPROVED"
            handover.write_text(json.dumps(data))
            self.assertEqual(dag.selected_handover({"ds": "2026-11-12"}), handover)


if __name__ == "__main__":
    unittest.main()
