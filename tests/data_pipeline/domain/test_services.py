import inspect

import pytest

from services.data_pipeline.domain.services import FeatureEngineeringService


class TestFeatureEngineeringServiceInterface:
    def test_is_abstract(self):
        """FeatureEngineeringService는 인스턴스화 불가."""
        with pytest.raises(TypeError):
            FeatureEngineeringService()

    def test_has_extract_features_method(self):
        assert hasattr(FeatureEngineeringService, "extract_features")
        sig = inspect.signature(FeatureEngineeringService.extract_features)
        params = list(sig.parameters.keys())
        assert "transactions" in params
