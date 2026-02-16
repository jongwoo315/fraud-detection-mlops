from decimal import Decimal

import pytest

from services.data_pipeline.infrastructure.csv_parser import KaggleCsvParser


class TestKaggleCsvParser:
    def _make_kaggle_row(self, **overrides) -> dict[str, str]:
        """Kaggle CSV 형식의 row를 생성하는 헬퍼."""
        row = {"Time": "0.0", "Amount": "149.62", "Class": "0"}
        for i in range(1, 29):
            row[f"V{i}"] = str(float(i) * 0.01)
        row.update(overrides)
        return row

    def test_parse_valid_row(self):
        parser = KaggleCsvParser()
        row = self._make_kaggle_row(
            Time="1.0", Amount="25.50", Class="0"
        )
        tx = parser.parse_row(row, row_index=0)
        assert tx.transaction_id == "txn_000000"
        assert tx.time_seconds == 1.0
        assert tx.amount == Decimal("25.50")
        assert tx.is_fraud is False
        assert len(tx.pca_features) == 28

    def test_parse_fraud_row(self):
        parser = KaggleCsvParser()
        row = self._make_kaggle_row(Class="1")
        tx = parser.parse_row(row, row_index=5)
        assert tx.transaction_id == "txn_000005"
        assert tx.is_fraud is True

    def test_parse_pca_features_order(self):
        parser = KaggleCsvParser()
        row = self._make_kaggle_row()
        for i in range(1, 29):
            row[f"V{i}"] = str(float(i))
        tx = parser.parse_row(row, row_index=0)
        assert tx.pca_features[0] == 1.0   # V1
        assert tx.pca_features[27] == 28.0  # V28

    def test_parse_missing_column_raises(self):
        parser = KaggleCsvParser()
        row = {"Time": "0.0", "Amount": "100.00"}  # Missing Class, V1-V28
        with pytest.raises(KeyError):
            parser.parse_row(row, row_index=0)

    def test_parse_invalid_amount_raises(self):
        parser = KaggleCsvParser()
        row = self._make_kaggle_row(Amount="not_a_number")
        with pytest.raises(ValueError):
            parser.parse_row(row, row_index=0)
