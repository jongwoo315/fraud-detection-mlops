import pytest

from services.data_pipeline.infrastructure.local_repository import (
    LocalFileTransactionRepository,
)
from tests.data_pipeline.infrastructure.conftest import make_kaggle_row


class TestLocalFileTransactionRepository:
    def test_load_raw_transactions(self, kaggle_csv):
        csv_path = kaggle_csv([
            make_kaggle_row(Time="0.0", Amount="149.62", Class="0"),
            make_kaggle_row(Time="1.0", Amount="2.69", Class="0"),
            make_kaggle_row(Time="2.0", Amount="378.66", Class="1"),
        ])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 3
        assert transactions[0].transaction_id == "txn_000000"
        assert transactions[2].is_fraud is True

    def test_load_empty_csv(self, kaggle_csv):
        csv_path = kaggle_csv([])
        repo = LocalFileTransactionRepository()
        transactions = list(repo.load_raw_transactions(str(csv_path)))
        assert len(transactions) == 0

    def test_load_nonexistent_file_raises(self):
        repo = LocalFileTransactionRepository()
        with pytest.raises(FileNotFoundError):
            list(repo.load_raw_transactions("/nonexistent/path.csv"))
