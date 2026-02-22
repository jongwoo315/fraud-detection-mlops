"""KaggleFeatureEngineeringService 테스트."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import pytest

from services.data_pipeline.domain.models import Feature, RawTransaction
from services.data_pipeline.infrastructure.feature_engineering import (
    KaggleFeatureEngineeringService,
)


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
def service() -> KaggleFeatureEngineeringService:
    return KaggleFeatureEngineeringService()


class TestTimeConversion:
    """hour_of_day, day_of_week 변환 테스트."""

    def test_time_zero(self, service: KaggleFeatureEngineeringService) -> None:
        """time_seconds=0 → hour=0, dow=0."""
        txn = _make_txn(time_seconds=0.0)
        feature = service._to_feature(txn)

        assert feature.hour_of_day == 0
        assert feature.day_of_week == 0

    def test_one_hour_one_minute_one_second(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        """time_seconds=3661 (1h 1m 1s) → hour=1, dow=0."""
        txn = _make_txn(time_seconds=3661.0)
        feature = service._to_feature(txn)

        assert feature.hour_of_day == 1
        assert feature.day_of_week == 0

    def test_twenty_five_hours(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        """time_seconds=90000 (25h) → hour=1, dow=1."""
        txn = _make_txn(time_seconds=90000.0)
        feature = service._to_feature(txn)

        assert feature.hour_of_day == 1
        assert feature.day_of_week == 1


class TestAmountBin:
    """amount_bin 경계값 테스트."""

    def test_below_10_is_low(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("9.99"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "low"

    def test_exactly_10_is_medium(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("10.00"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "medium"

    def test_below_100_is_medium(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("99.99"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "medium"

    def test_exactly_100_is_high(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("100.00"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "high"

    def test_below_500_is_high(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("499.99"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "high"

    def test_exactly_500_is_very_high(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(amount=Decimal("500.00"))
        feature = service._to_feature(txn)
        assert feature.amount_bin == "very_high"


class TestStreaming:
    """스트리밍(Iterator) 동작 테스트."""

    def test_multiple_transactions_returns_iterator(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txns = [_make_txn(transaction_id=f"txn-{i}") for i in range(3)]
        result = service.extract_features(txns)

        assert isinstance(result, Iterator)
        features = list(result)
        assert len(features) == 3
        assert all(isinstance(f, Feature) for f in features)
        assert [f.transaction_id for f in features] == [
            "txn-0",
            "txn-1",
            "txn-2",
        ]

    def test_empty_input_returns_empty_iterator(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        result = service.extract_features([])

        assert isinstance(result, Iterator)
        assert list(result) == []


class TestPassThrough:
    """transaction_id, amount, is_fraud pass-through 테스트."""

    def test_fields_passed_through(
        self, service: KaggleFeatureEngineeringService
    ) -> None:
        txn = _make_txn(
            transaction_id="txn-pass",
            amount=Decimal("42.50"),
            is_fraud=True,
        )
        feature = service._to_feature(txn)

        assert feature.transaction_id == "txn-pass"
        assert feature.amount == Decimal("42.50")
        assert feature.is_fraud is True
