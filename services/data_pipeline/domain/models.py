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
