import csv
from pathlib import Path

import pytest

from services.data_pipeline.infrastructure.local_repository import (
    LocalFileTransactionRepository,
)

_KAGGLE_FIELDNAMES = ["Time", *(f"V{i}" for i in range(1, 29)), "Amount", "Class"]


class TestLocalFileTransactionRepository:
    def _create_kaggle_csv(self, tmp_path: Path, rows: list[dict]) -> Path:
        csv_path = tmp_path / "creditcard.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_KAGGLE_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        return csv_path

    def _make_kaggle_row(self, **overrides) -> dict:
        row = {"Time": "0.0", "Amount": "149.62", "Class": "0"}
        for i in range(1, 29):
            row[f"V{i}"] = "0.0"
        row.update(overrides)
        return row

    def test_load_raw_transactions(self, tmp_path):
        csv_path = self._create_kaggle_csv(tmp_path, [
            self._make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            self._make_kaggle_row(Time="1.0", Amount="2.69", Class="0"),
            self._make_kaggle_row(Time="2.0", Amount="378.66", Class="1"),
        ])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 3
        assert transactions[0].transaction_id == "txn_000000"
        assert transactions[2].is_fraud is True

    def test_load_empty_csv(self, tmp_path):
        csv_path = self._create_kaggle_csv(tmp_path, [])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 0

    def test_load_nonexistent_file_raises(self):
        repo = LocalFileTransactionRepository()
        with pytest.raises(FileNotFoundError):
            list(repo.load_raw_transactions("/nonexistent/path.csv"))
