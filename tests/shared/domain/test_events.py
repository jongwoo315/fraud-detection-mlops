from datetime import timezone

import pytest

from shared.domain.events import DataProcessingCompleted, DomainEvent


class TestDomainEvent:
    def test_occurred_at_auto_set(self):
        event = DomainEvent()
        assert event.occurred_at.tzinfo == timezone.utc


class TestDataProcessingCompleted:
    def test_create_valid_event(self):
        event = DataProcessingCompleted(
            total_records=100,
            valid_records=95,
            features_path="/data/features.parquet",
        )
        assert event.total_records == 100
        assert event.valid_records == 95
        assert event.features_path == "/data/features.parquet"

    def test_empty_features_path_rejected(self):
        with pytest.raises(ValueError, match="features_path must not be empty"):
            DataProcessingCompleted(
                total_records=100,
                valid_records=95,
                features_path="",
            )

    def test_default_features_path_rejected(self):
        with pytest.raises(ValueError, match="features_path must not be empty"):
            DataProcessingCompleted()

    def test_negative_total_records_rejected(self):
        with pytest.raises(ValueError, match="total_records must be non-negative"):
            DataProcessingCompleted(
                total_records=-1,
                valid_records=0,
                features_path="/data/features.parquet",
            )

    def test_negative_valid_records_rejected(self):
        with pytest.raises(ValueError, match="valid_records must be non-negative"):
            DataProcessingCompleted(
                total_records=10,
                valid_records=-1,
                features_path="/data/features.parquet",
            )

    def test_valid_records_exceeding_total_rejected(self):
        with pytest.raises(ValueError, match="valid_records cannot exceed total_records"):
            DataProcessingCompleted(
                total_records=5,
                valid_records=10,
                features_path="/data/features.parquet",
            )
