from __future__ import annotations

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
    amount_bin: str
    is_fraud: bool

    def __post_init__(self) -> None:
        if not (0 <= self.hour_of_day <= 23):
            raise ValueError(f"hour_of_day must be 0-23, got {self.hour_of_day}")
        if not (0 <= self.day_of_week <= 6):
            raise ValueError(f"day_of_week must be 0-6, got {self.day_of_week}")

    @property
    def is_weekend(self) -> bool:
        return self.day_of_week in (5, 6)


@dataclass(frozen=True)
class ProcessDataResult:
    """데이터 처리 결과."""

    total_records: int
    valid_records: int
    features_path: str
    validation_report_path: str


@dataclass(frozen=True)
class ValidationError:
    """검증 오류."""

    field: str
    message: str
    record_index: int


@dataclass(frozen=True)
class ValidationReport:
    """데이터 검증 결과."""

    total_records: int
    valid_records: int
    errors: tuple[ValidationError, ...]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def error_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return len(self.errors) / self.total_records
