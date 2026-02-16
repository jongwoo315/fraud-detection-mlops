from datetime import datetime
from decimal import Decimal

import pytest

from services.data_pipeline.domain.models import (
    Feature,
    ProcessDataResult,
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
            amount_bin="medium",
            is_fraud=False,
        )
        assert feature.transaction_id == "tx_001"
        assert feature.hour_of_day == 10
        assert feature.amount_bin == "medium"

    def test_is_weekend_derived_from_day_of_week(self):
        weekday = Feature(
            transaction_id="tx_001",
            amount=Decimal("100.00"),
            hour_of_day=10,
            day_of_week=0,
            amount_bin="low",
            is_fraud=False,
        )
        assert weekday.is_weekend is False

        saturday = Feature(
            transaction_id="tx_002",
            amount=Decimal("100.00"),
            hour_of_day=10,
            day_of_week=5,
            amount_bin="low",
            is_fraud=False,
        )
        assert saturday.is_weekend is True

        sunday = Feature(
            transaction_id="tx_003",
            amount=Decimal("100.00"),
            hour_of_day=10,
            day_of_week=6,
            amount_bin="low",
            is_fraud=False,
        )
        assert sunday.is_weekend is True

    def test_feature_immutable(self):
        feature = Feature(
            transaction_id="tx_001",
            amount=Decimal("100.00"),
            hour_of_day=14,
            day_of_week=5,
            amount_bin="low",
            is_fraud=True,
        )
        with pytest.raises(AttributeError):
            feature.hour_of_day = 99

    def test_hour_of_day_range_validation(self):
        with pytest.raises(ValueError, match="hour_of_day must be 0-23"):
            Feature(
                transaction_id="tx_001",
                amount=Decimal("100.00"),
                hour_of_day=24,
                day_of_week=0,
                amount_bin="low",
                is_fraud=False,
            )
        with pytest.raises(ValueError, match="hour_of_day must be 0-23"):
            Feature(
                transaction_id="tx_001",
                amount=Decimal("100.00"),
                hour_of_day=-1,
                day_of_week=0,
                amount_bin="low",
                is_fraud=False,
            )

    def test_day_of_week_range_validation(self):
        with pytest.raises(ValueError, match="day_of_week must be 0-6"):
            Feature(
                transaction_id="tx_001",
                amount=Decimal("100.00"),
                hour_of_day=10,
                day_of_week=7,
                amount_bin="low",
                is_fraud=False,
            )
        with pytest.raises(ValueError, match="day_of_week must be 0-6"):
            Feature(
                transaction_id="tx_001",
                amount=Decimal("100.00"),
                hour_of_day=10,
                day_of_week=-1,
                amount_bin="low",
                is_fraud=False,
            )


class TestProcessDataResult:
    def test_create_process_data_result(self):
        result = ProcessDataResult(
            total_records=1000,
            valid_records=998,
            features_path="/data/features.parquet",
            validation_report_path="/data/report.json",
        )
        assert result.total_records == 1000
        assert result.valid_records == 998
        assert result.features_path == "/data/features.parquet"

    def test_process_data_result_immutable(self):
        result = ProcessDataResult(
            total_records=100,
            valid_records=95,
            features_path="/a",
            validation_report_path="/b",
        )
        with pytest.raises(AttributeError):
            result.total_records = 999


class TestValidationReport:
    def test_valid_report(self):
        report = ValidationReport(
            total_records=1000,
            errors=(),
        )
        assert report.is_valid
        assert report.valid_records == 1000
        assert report.error_rate == 0.0

    def test_invalid_report(self):
        errors = (
            ValidationError(field="amount", message="negative value", record_index=5),
            ValidationError(field="timestamp", message="null value", record_index=42),
        )
        report = ValidationReport(
            total_records=1000,
            errors=errors,
        )
        assert not report.is_valid
        assert report.valid_records == 998
        assert report.error_rate == 0.002

    def test_valid_records_derived_from_errors(self):
        errors = (
            ValidationError(field="amount", message="negative", record_index=0),
        )
        report = ValidationReport(total_records=10, errors=errors)
        assert report.valid_records == 9

    def test_negative_total_records_rejected(self):
        with pytest.raises(ValueError, match="total_records must be non-negative"):
            ValidationReport(total_records=-1, errors=())

    def test_errors_with_zero_total_rejected(self):
        errors = (
            ValidationError(field="amount", message="bad", record_index=0),
        )
        with pytest.raises(ValueError, match="errors cannot exist when total_records is 0"):
            ValidationReport(total_records=0, errors=errors)

