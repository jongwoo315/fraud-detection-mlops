"""Airflow DAG task callable 함수.

각 함수는 PythonOperator의 python_callable로 사용된다.
Airflow에 의존하지 않는 순수 Python 함수이므로 단독 테스트 가능.
"""

from __future__ import annotations

import csv
from pathlib import Path

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
