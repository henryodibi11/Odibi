import pytest

from odibi.engine.registry import get_engine_class, register_engine, _ENGINES
from odibi.engine.base import Engine
from odibi.engine.pandas_engine import PandasEngine
from odibi.connections.base import BaseConnection


class TestGetEngineClass:
    def test_returns_pandas_engine(self):
        result = get_engine_class("pandas")
        assert result is PandasEngine

    def test_nonexistent_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported engine: nonexistent"):
            get_engine_class("nonexistent")


class TestRegisterEngine:
    def test_register_stores_engine(self):
        class DummyEngine(Engine):
            pass

        register_engine("dummy", DummyEngine)
        assert _ENGINES["dummy"] is DummyEngine

    def test_get_engine_class_retrieves_registered(self):
        class AnotherEngine(Engine):
            pass

        register_engine("another", AnotherEngine)
        assert get_engine_class("another") is AnotherEngine

    def teardown_method(self):
        _ENGINES.pop("dummy", None)
        _ENGINES.pop("another", None)


class TestBaseConnection:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseConnection()

    def test_concrete_subclass_works(self):
        class MyConnection(BaseConnection):
            def get_path(self, relative_path: str) -> str:
                return f"/data/{relative_path}"

            def validate(self) -> None:
                pass

        conn = MyConnection()
        assert conn.get_path("table") == "/data/table"
        conn.validate()
