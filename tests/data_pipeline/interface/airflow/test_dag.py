"""Airflow DAG 구조 검증 테스트.

Airflow가 설치되지 않은 환경에서는 모든 테스트를 skip한다.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from airflow.models import DagBag

    AIRFLOW_AVAILABLE = True
except ImportError:
    DagBag = None  # type: ignore[assignment,misc]
    AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not installed")

DAGS_DIR = str(
    Path(__file__).resolve().parents[4]
    / "services"
    / "data_pipeline"
    / "interface"
    / "airflow"
    / "dags"
)


@pytest.fixture(scope="module")
def dagbag():
    """DAG 파일을 로드하는 DagBag fixture."""
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_dag_loads_without_errors(dagbag):
    """DAG 파일이 import 에러 없이 로드되는지 확인."""
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_dag_exists(dagbag):
    """fraud_detection_pipeline DAG가 존재하는지 확인."""
    assert "fraud_detection_pipeline" in dagbag.dags


def test_dag_has_correct_task_count(dagbag):
    """DAG에 4개의 task가 정의되어 있는지 확인."""
    dag = dagbag.dags["fraud_detection_pipeline"]
    assert len(dag.tasks) == 4


def test_dag_task_dependencies(dagbag):
    """load→validate→engineer→save 의존성 체인을 확인."""
    dag = dagbag.dags["fraud_detection_pipeline"]

    load_task = dag.get_task("load_data")
    validate_task = dag.get_task("validate_data")
    engineer_task = dag.get_task("engineer_features")
    save_task = dag.get_task("save_features")

    assert "validate_data" in {t.task_id for t in load_task.downstream_list}
    assert "engineer_features" in {t.task_id for t in validate_task.downstream_list}
    assert "save_features" in {t.task_id for t in engineer_task.downstream_list}
    assert save_task.downstream_list == []


def test_dag_schedule_is_none(dagbag):
    """DAG 스케줄이 None(수동 트리거)인지 확인."""
    dag = dagbag.dags["fraud_detection_pipeline"]
    assert dag.schedule_interval is None


def test_dag_tags(dagbag):
    """DAG에 'fraud-detection' 태그가 포함되어 있는지 확인."""
    dag = dagbag.dags["fraud_detection_pipeline"]
    assert "fraud-detection" in dag.tags
