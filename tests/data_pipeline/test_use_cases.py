import inspect

import pytest

from services.data_pipeline.application.use_cases import ProcessDataUseCase


class TestProcessDataUseCaseInterface:
    def test_is_abstract(self):
        with pytest.raises(TypeError):
            ProcessDataUseCase()

    def test_has_execute_method(self):
        assert hasattr(ProcessDataUseCase, "execute")
        sig = inspect.signature(ProcessDataUseCase.execute)
        params = list(sig.parameters.keys())
        assert "source" in params
        assert "destination" in params
