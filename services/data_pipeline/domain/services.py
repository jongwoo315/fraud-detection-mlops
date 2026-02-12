from abc import ABC, abstractmethod

from services.data_pipeline.domain.models import Feature, RawTransaction


class FeatureEngineeringService(ABC):
    """피처 엔지니어링 도메인 서비스 인터페이스."""

    @abstractmethod
    def extract_features(self, transactions: list[RawTransaction]) -> list[Feature]:
        """원본 거래에서 피처를 추출한다."""
