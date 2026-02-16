from datetime import datetime
from decimal import Decimal

import pytest

from services.data_pipeline.domain.models import (
    Feature,
    RawTransaction,
    ValidationError,
    ValidationReport,
)


class TestRawTransaction:
    def test_create_raw_transaction(self):
        tx = RawTransaction(
            transaction_id="tx_001",
            amount=Decimal("150.00"),
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            merchant_id="merchant_001",
            customer_id="customer_001",
            is_fraud=False,
        )
        assert tx.transaction_id == "tx_001"
        assert tx.amount == Decimal("150.00")
        assert tx.is_fraud is False

    def test_raw_transaction_immutable(self):
        tx = RawTransaction(
            transaction_id="tx_001",
            amount=Decimal("100.00"),
            timestamp=datetime(2024, 1, 15, 10, 30, 0),
            merchant_id="m1",
            customer_id="c1",
            is_fraud=False,
        )
        with pytest.raises(AttributeError):
            tx.amount = Decimal("999.99")


class TestFeature:
    def test_create_feature(self):
        feature = Feature(
            transaction_id="tx_001",
            amount=Decimal("150.00"),
            hour_of_day=10,
            day_of_week=0,
            is_weekend=False,
            amount_bin="medium",
            is_fraud=False,
        )
        assert feature.transaction_id == "tx_001"
        assert feature.hour_of_day == 10
        assert feature.amount_bin == "medium"

    def test_feature_immutable(self):
        feature = Feature(
            transaction_id="tx_001",
            amount=Decimal("100.00"),
            hour_of_day=14,
            day_of_week=5,
            is_weekend=True,
            amount_bin="low",
            is_fraud=True,
        )
        try:
            feature.hour_of_day = 99
            assert False, "Should be frozen"
        except AttributeError:
            pass


class TestValidationReport:
    def test_valid_report(self):
        report = ValidationReport(
            total_records=1000,
            valid_records=998,
            errors=[],
        )
        assert report.is_valid
        assert report.error_rate == 0.0

    def test_invalid_report(self):
        errors = [
            ValidationError(field="amount", message="negative value", record_index=5),
            ValidationError(field="timestamp", message="null value", record_index=42),
        ]
        report = ValidationReport(
            total_records=1000,
            valid_records=998,
            errors=errors,
        )
        assert not report.is_valid
        assert report.error_rate == 0.002
