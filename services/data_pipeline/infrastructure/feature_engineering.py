"""Kaggle 데이터셋 피처 엔지니어링 구현."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from decimal import Decimal

from services.data_pipeline.domain.models import Feature, RawTransaction
from services.data_pipeline.domain.services import FeatureEngineeringService

SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600
DAYS_PER_WEEK = 7

AMOUNT_LOW_UPPER = Decimal("10")
AMOUNT_MEDIUM_UPPER = Decimal("100")
AMOUNT_HIGH_UPPER = Decimal("500")


class KaggleFeatureEngineeringService(FeatureEngineeringService):
    """Kaggle Credit Card Fraud Detection 데이터셋 전용 피처 엔지니어링."""

    def extract_features(
        self, transactions: Iterable[RawTransaction]
    ) -> Iterator[Feature]:
        """원본 거래에서 피처를 추출한다. 반환값은 1회성 Iterator."""
        return (self._to_feature(txn) for txn in transactions)

    @staticmethod
    def _to_feature(txn: RawTransaction) -> Feature:
        """단일 RawTransaction을 Feature로 변환한다."""
        hour_of_day = (int(txn.time_seconds) % SECONDS_PER_DAY) // SECONDS_PER_HOUR
        day_of_week = (int(txn.time_seconds) // SECONDS_PER_DAY) % DAYS_PER_WEEK

        if txn.amount < AMOUNT_LOW_UPPER:
            amount_bin = "low"
        elif txn.amount < AMOUNT_MEDIUM_UPPER:
            amount_bin = "medium"
        elif txn.amount < AMOUNT_HIGH_UPPER:
            amount_bin = "high"
        else:
            amount_bin = "very_high"

        return Feature(
            transaction_id=txn.transaction_id,
            amount=txn.amount,
            hour_of_day=hour_of_day,
            day_of_week=day_of_week,
            amount_bin=amount_bin,
            is_fraud=txn.is_fraud,
        )
