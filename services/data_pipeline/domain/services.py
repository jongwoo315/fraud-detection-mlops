from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator

from services.data_pipeline.domain.models import Feature, RawTransaction


class FeatureEngineeringService(ABC):
    """피처 엔지니어링 도메인 서비스 인터페이스."""

    @abstractmethod
    def extract_features(self, transactions: Iterable[RawTransaction]) -> Iterator[Feature]:
        """원본 거래에서 피처를 추출한다. 반환값은 1회성 Iterator."""
