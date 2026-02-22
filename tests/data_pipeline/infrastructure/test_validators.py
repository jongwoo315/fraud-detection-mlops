"""TransactionValidator 테스트."""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from services.data_pipeline.domain.models import RawTransaction
from services.data_pipeline.infrastructure.validators import TransactionValidator


def _make_txn(**overrides: object) -> RawTransaction:
    defaults: dict[str, object] = {
        "transaction_id": "txn-1",
        "time_seconds": 0.0,
        "amount": Decimal("100.00"),
        "is_fraud": False,
        "pca_features": tuple(0.0 for _ in range(28)),
    }
    defaults.update(overrides)
    return RawTransaction(**defaults)  # type: ignore[arg-type]


@pytest.fixture()
def validator() -> TransactionValidator:
    return TransactionValidator()


class TestValidTransactions:
    def test_all_valid_pass_through(self, validator: TransactionValidator) -> None:
        txns = [_make_txn(transaction_id=f"t-{i}") for i in range(3)]
        valid, report = validator.validate(txns)

        assert len(valid) == 3
        assert report.total_records == 3
        assert report.valid_records == 3
        assert report.is_valid
        assert len(report.errors) == 0

    def test_empty_list_returns_empty_result(
        self, validator: TransactionValidator
    ) -> None:
        valid, report = validator.validate([])

        assert valid == []
        assert report.total_records == 0
        assert report.valid_records == 0
        assert report.is_valid


class TestInvalidTransactions:
    def test_empty_transaction_id_collected(
        self, validator: TransactionValidator
    ) -> None:
        txns = [_make_txn(transaction_id="")]
        valid, report = validator.validate(txns)

        assert len(valid) == 0
        assert len(report.errors) == 1
        assert report.errors[0].field == "transaction_id"
        assert report.errors[0].message == "empty transaction_id"

    def test_negative_amount_collected(
        self, validator: TransactionValidator
    ) -> None:
        txns = [_make_txn(amount=Decimal("-1.00"))]
        valid, report = validator.validate(txns)

        assert len(valid) == 0
        assert len(report.errors) == 1
        assert report.errors[0].field == "amount"
        assert report.errors[0].message == "invalid amount"

    def test_nan_in_pca_features_collected(
        self, validator: TransactionValidator
    ) -> None:
        features = list(0.0 for _ in range(28))
        features[5] = math.nan
        txns = [_make_txn(pca_features=tuple(features))]
        valid, report = validator.validate(txns)

        assert len(valid) == 0
        assert len(report.errors) == 1
        assert report.errors[0].field == "pca_features"
        assert report.errors[0].message == "pca_features contains NaN or Inf"

    def test_inf_in_pca_features_collected(
        self, validator: TransactionValidator
    ) -> None:
        features = list(0.0 for _ in range(28))
        features[10] = math.inf
        txns = [_make_txn(pca_features=tuple(features))]
        valid, report = validator.validate(txns)

        assert len(valid) == 0
        assert len(report.errors) == 1
        assert report.errors[0].field == "pca_features"
        assert report.errors[0].message == "pca_features contains NaN or Inf"


class TestMixedTransactions:
    def test_valid_and_invalid_mixed(
        self, validator: TransactionValidator
    ) -> None:
        txns = [
            _make_txn(transaction_id="good-1"),
            _make_txn(transaction_id=""),  # invalid
            _make_txn(transaction_id="good-2"),
        ]
        valid, report = validator.validate(txns)

        assert len(valid) == 2
        assert report.total_records == 3
        assert report.valid_records == 2
        assert not report.is_valid
        assert valid[0].transaction_id == "good-1"
        assert valid[1].transaction_id == "good-2"
