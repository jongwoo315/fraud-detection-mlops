from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class DomainEvent:
    """도메인 이벤트 베이스 클래스."""

    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class DataProcessingCompleted(DomainEvent):
    """데이터 처리 완료 이벤트."""

    total_records: int = 0
    valid_records: int = 0
    features_path: str = ""
