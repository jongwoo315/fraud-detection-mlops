"""Fraud Detection Pipeline Airflow DAG.

Kaggle Credit Card Fraud Detection 데이터를 처리하는 파이프라인.
load → validate → engineer_features → save_features 순서로 실행.
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from services.data_pipeline.interface.airflow.tasks import (
    engineer_features,
    load_data,
    save_features,
    validate_data,
)

DATA_DIR = "/data"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud-detection", "data-pipeline"],
) as dag:

    def _load_data(**context):
        conf = context["dag_run"].conf or {}
        source_path = conf.get("source_path", f"{DATA_DIR}/raw/creditcard.csv")
        result = load_data(source_path=source_path, intermediate_dir=f"{DATA_DIR}/intermediate")
        return result

    def _validate_data(**context):
        ti = context["ti"]
        load_result = ti.xcom_pull(task_ids="load_data")
        result = validate_data(
            input_path=load_result["output_path"],
            report_dir=f"{DATA_DIR}/reports",
            intermediate_dir=f"{DATA_DIR}/intermediate",
        )
        if result["should_skip"]:
            from airflow.exceptions import AirflowSkipException

            raise AirflowSkipException(
                f"Validation error rate exceeded threshold: {result['invalid_count']}"
                " invalid records"
            )
        return result

    def _engineer_features(**context):
        ti = context["ti"]
        validate_result = ti.xcom_pull(task_ids="validate_data")
        result = engineer_features(
            input_path=validate_result["output_path"],
            intermediate_dir=f"{DATA_DIR}/intermediate",
        )
        return result

    def _save_features(**context):
        ti = context["ti"]
        eng_result = ti.xcom_pull(task_ids="engineer_features")
        result = save_features(
            input_path=eng_result["output_path"],
            features_dir=f"{DATA_DIR}/features",
        )
        return result

    t_load = PythonOperator(
        task_id="load_data",
        python_callable=_load_data,
    )

    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=_validate_data,
    )

    t_engineer = PythonOperator(
        task_id="engineer_features",
        python_callable=_engineer_features,
    )

    t_save = PythonOperator(
        task_id="save_features",
        python_callable=_save_features,
    )

    t_load >> t_validate >> t_engineer >> t_save
