import inspect

from services.data_pipeline.domain.services import FeatureEngineeringService


class TestFeatureEngineeringServiceInterface:
    def test_is_abstract(self):
        """FeatureEngineeringService는 인스턴스화 불가."""
        try:
            FeatureEngineeringService()
            assert False, "Should not instantiate abstract class"
        except TypeError:
            pass

    def test_has_extract_features_method(self):
        assert hasattr(FeatureEngineeringService, "extract_features")
        sig = inspect.signature(FeatureEngineeringService.extract_features)
        params = list(sig.parameters.keys())
        assert "transactions" in params
