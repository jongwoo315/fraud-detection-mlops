"""Airflow DAG task callable 함수.

각 함수는 PythonOperator의 python_callable로 사용된다.
Airflow에 의존하지 않는 순수 Python 함수이므로 단독 테스트 가능.
"""

from __future__ import annotations

import csv
from pathlib import Path

from services.data_pipeline.infrastructure.local_repository import (
    LocalFileTransactionRepository,
    LocalFileValidationReportRepository,
)
from services.data_pipeline.infrastructure.validators import TransactionValidator

VALIDATION_ERROR_THRESHOLD = 0.1


def load_data(
    *,
    source_path: str,
    intermediate_dir: str,
) -> dict:
    """CSV 파일에서 원시 트랜잭션을 로드하고 중간 파일로 저장."""
    output_path = Path(intermediate_dir) / "raw_transactions.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    record_count = 0
    with open(source_path, newline="") as src, open(output_path, "w", newline="") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow(row)
            record_count += 1

    return {
        "record_count": record_count,
        "source_path": source_path,
        "output_path": str(output_path),
    }


def validate_data(
    *,
    input_path: str,
    report_dir: str,
    intermediate_dir: str,
) -> dict:
    """트랜잭션 데이터를 검증하고 유효한 행만 저장."""
    # CSV 전체 행 수 카운트
    with open(input_path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        total_rows = sum(1 for _ in reader)

    # 트랜잭션 로드 (파싱 오류 시 빈 리스트)
    repo = LocalFileTransactionRepository()
    try:
        transactions = list(repo.load_raw_transactions(input_path))
    except ValueError:
        transactions = []

    # 검증
    validator = TransactionValidator()
    valid_transactions, report = validator.validate(transactions)

    # 리포트 저장
    report_path = Path(report_dir) / "validation_report.json"
    report_repo = LocalFileValidationReportRepository()
    report_repo.save_report(report, str(report_path))

    # 유효한 트랜잭션만 CSV로 저장
    output_path = Path(intermediate_dir) / "validated_transactions.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid_ids = {t.transaction_id for t in valid_transactions}

    with open(input_path, newline="") as src, open(output_path, "w", newline="") as dst:
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row_index, row in enumerate(reader):
            if f"txn_{row_index:06d}" in valid_ids:
                writer.writerow(row)

    valid_count = len(valid_transactions)
    invalid_count = total_rows - valid_count
    error_rate = invalid_count / total_rows if total_rows > 0 else 0.0

    return {
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "report_path": str(report_path),
        "output_path": str(output_path),
        "should_skip": error_rate > VALIDATION_ERROR_THRESHOLD,
    }
