from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessDataResult:
    """데이터 처리 결과."""

    total_records: int
    valid_records: int
    features_path: str
    validation_report_path: str


class ProcessDataUseCase(ABC):
    """데이터 처리 유스케이스 인터페이스.

    구현체는 infrastructure 레이어에서 리포지토리를 주입받아 처리한다.
    """

    @abstractmethod
    def execute(self, source: str, destination: str) -> ProcessDataResult:
        """원본 데이터를 로드 → 검증 → 피처 엔지니어링 → 저장."""
