from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class RawTransaction:
    """원본 거래 데이터."""

    transaction_id: str
    amount: Decimal
    timestamp: datetime
    merchant_id: str
    customer_id: str
    is_fraud: bool


@dataclass(frozen=True)
class Feature:
    """엔지니어링된 피처."""

    transaction_id: str
    amount: Decimal
    hour_of_day: int
    day_of_week: int
    is_weekend: bool
    amount_bin: str
    is_fraud: bool
