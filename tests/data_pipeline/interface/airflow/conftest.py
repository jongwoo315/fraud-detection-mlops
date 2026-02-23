"""data_pipeline interface/airflow 테스트 공통 fixture."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

KAGGLE_FIELDNAMES = ["Time", *(f"V{i}" for i in range(1, 29)), "Amount", "Class"]


def make_kaggle_row(**overrides: str) -> dict[str, str]:
    """Kaggle CSV 형식의 row를 생성하는 헬퍼."""
    row: dict[str, str] = {"Time": "0.0", "Amount": "149.62", "Class": "0"}
    for i in range(1, 29):
        row[f"V{i}"] = "0.0"
    row.update(overrides)
    return row


@pytest.fixture()
def kaggle_csv(tmp_path: Path):
    """tmp_path에 Kaggle CSV를 생성하는 팩토리 fixture."""

    def _create(rows: list[dict[str, str]]) -> Path:
        csv_path = tmp_path / "creditcard.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=KAGGLE_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        return csv_path

    return _create


@pytest.fixture()
def data_dirs(tmp_path: Path) -> dict[str, Path]:
    """테스트용 데이터 디렉토리 구조를 생성하는 fixture."""
    dirs = {
        "raw": tmp_path / "raw",
        "intermediate": tmp_path / "intermediate",
        "features": tmp_path / "features",
        "reports": tmp_path / "reports",
    }
    for d in dirs.values():
        d.mkdir(parents=True)
    return dirs
