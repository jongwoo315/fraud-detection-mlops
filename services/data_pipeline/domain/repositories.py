from abc import ABC, abstractmethod
from pathlib import Path

from services.data_pipeline.domain.models import Feature, RawTransaction, ValidationReport


class TransactionRepository(ABC):
    """거래 데이터 저장소 인터페이스."""

    @abstractmethod
    def load_raw_transactions(self, source: str) -> list[RawTransaction]:
        """원본 거래 데이터를 로드한다."""

    @abstractmethod
    def save_features(self, features: list[Feature], destination: str) -> Path:
        """엔지니어링된 피처를 저장한다."""


class ValidationReportRepository(ABC):
    """검증 리포트 저장소 인터페이스."""

    @abstractmethod
    def save_report(self, report: ValidationReport, destination: str) -> Path:
        """검증 리포트를 저장한다."""
