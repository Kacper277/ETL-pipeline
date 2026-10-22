"""DAG integrity tests for the Airflow orchestration layer.

These tests need the Airflow dependency set (see
docker/requirements-airflow.txt) but NO live Airflow infrastructure -
no scheduler, webserver or containers. They run in the default fast
CI job. On a machine without Airflow installed they skip with a
coherent message instead of erroring.

Run with: pytest tests/test_dag_integrity.py
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest

# NOTE: the repository contains an ``airflow/`` directory (the DAG
# folder). On machines WITHOUT Apache Airflow installed, that directory
# is importable as an empty namespace package and shadows the real
# library - hence the explicit ImportError check instead of a plain
# importorskip("airflow"), which would be fooled by it.
try:
    from airflow.models.dagbag import DagBag  # noqa: E402
except ImportError as exc:  # pragma: no cover - depends on environment
    pytest.skip(
        f"Apache Airflow is not installed ({exc}); install "
        "docker/requirements-airflow.txt to run DAG integrity tests",
        allow_module_level=True,
    )

REPO_ROOT = Path(__file__).resolve().parent.parent
DAGS_DIR = Path(os.getenv("AIRFLOW_DAGS_DIR", str(REPO_ROOT / "airflow" / "dags")))

EXPECTED_DAG_ID = "weather_batch_etl_pipeline"
EXPECTED_CHAIN = ["extract", "transform", "validate", "load", "summary"]


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    """Parse the DAG folder exactly like the scheduler would.

    Returns:
        DagBag over ``airflow/dags`` with example DAGs disabled.
    """
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag: DagBag):
    """Return the single weather DAG.

    Args:
        dagbag: Parsed DagBag fixture.

    Returns:
        The DAG instance.
    """
    return dagbag.get_dag(EXPECTED_DAG_ID)


class TestDiscovery:
    def test_no_import_errors(self, dagbag: DagBag) -> None:
        assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"

    def test_exactly_one_dag(self, dagbag: DagBag) -> None:
        assert list(dagbag.dags) == [EXPECTED_DAG_ID]

    def test_dag_found(self, dag) -> None:
        assert dag is not None


class TestSchedule:
    def test_catchup_disabled(self, dag) -> None:
        assert dag.catchup is False

    def test_schedule_interval_present(self, dag) -> None:
        assert dag.schedule_interval is not None
        assert dag.schedule_interval == "0 0 * * *"


class TestTasks:
    def test_expected_task_set(self, dag) -> None:
        assert sorted(t.task_id for t in dag.tasks) == sorted(EXPECTED_CHAIN)

    def test_linear_dependency_chain(self, dag) -> None:
        for upstream, downstream in zip(EXPECTED_CHAIN, EXPECTED_CHAIN[1:]):
            task = dag.get_task(upstream)
            assert task.downstream_task_ids == {downstream}, (
                f"{upstream} must flow exactly into {downstream}, "
                f"got {task.downstream_task_ids}"
            )
        assert dag.get_task(EXPECTED_CHAIN[-1]).downstream_task_ids == set()

    def test_retries_and_timeout_on_every_task(self, dag) -> None:
        for task in dag.tasks:
            assert task.retries >= 1, f"{task.task_id}: retries < 1"
            assert task.execution_timeout is not None, (
                f"{task.task_id}: missing execution_timeout"
            )


class TestXComSafety:
    """Source-level guard against oversized XCom payloads.

    No task may return a bare DataFrame or raw API payload; only
    compact dicts (paths, counts, small stats) may cross task
    boundaries. This is a static check over the task callables in the
    DAG file(s): any ``return df`` / ``return raw`` / ``return payload``
    style statement in a ``*_task`` function fails the suite.
    """

    UNSAFE_RETURN_NAMES = {"df", "raw", "data", "payload", "response"}

    def test_no_unsafe_returns_in_task_functions(self) -> None:
        violations: list[str] = []
        for dag_file in sorted(DAGS_DIR.glob("*.py")):
            tree = ast.parse(dag_file.read_text(encoding="utf-8-sig"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef) or not node.name.endswith("_task"):
                    continue
                for sub in ast.walk(node):
                    if not isinstance(sub, ast.Return) or sub.value is None:
                        continue
                    value = sub.value
                    if isinstance(value, ast.Name) and value.id in self.UNSAFE_RETURN_NAMES:
                        violations.append(f"{dag_file.name}:{sub.lineno} returns '{value.id}'")
        assert violations == [], "Oversized XCom payload suspects: " + "; ".join(violations)
