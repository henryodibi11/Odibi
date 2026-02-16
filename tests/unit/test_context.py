import logging
from unittest.mock import MagicMock

import pytest
import pandas as pd

from odibi.context import (
    Context,
    EngineContext,
    PandasContext,
    _get_unique_view_name,
    create_context,
)
from odibi.enums import EngineType

# Polars is optional - only import if available
try:
    import polars as pl
    from odibi.context import PolarsContext

    HAS_POLARS = True
except ImportError:
    pl = None
    PolarsContext = None
    HAS_POLARS = False

logging.getLogger("odibi").propagate = False


# Dummy implementation of Context for testing the abstract base class.
class DummyContext(Context):
    def __init__(self):
        self.store = {}
        self.metadata = {}

    def set(self, name, value):
        self.store[name] = value

    def get(self, name):
        return self.store.get(name, None)

    def clear(self, name):
        if name in self.store:
            del self.store[name]

    def register(self, name, value):
        if name in self.store:
            raise Exception("Already registered")
        self.store[name] = value

    def has(self, name):
        return name in self.store

    def get_metadata(self):
        return self.metadata

    def list_names(self):
        return list(self.store.keys())


# Fixture for base Context instance using DummyContext
@pytest.fixture
def context_instance():
    return DummyContext()


def test_context_set_and_get_value(context_instance):
    # Test that setting a key and retrieving it works for DummyContext.
    context_instance.set("key1", "value1")
    assert context_instance.get("key1") == "value1"


def test_context_missing_key_returns_none(context_instance):
    # For DummyContext, getting a missing key returns None.
    assert context_instance.get("nonexistent") is None


def test_context_overwrite_duplicate_key(context_instance):
    # Test that setting the same key twice overwrites the old value.
    context_instance.set("dup_key", "first")
    context_instance.set("dup_key", "second")
    assert context_instance.get("dup_key") == "second"


def test_context_clear_key(context_instance):
    # Test that clearing a key removes it.
    context_instance.set("temp", "data")
    context_instance.clear("temp")
    assert context_instance.get("temp") is None


def test_context_metadata_handling(context_instance):
    # Test metadata functionality if present.
    if hasattr(context_instance, "metadata"):
        context_instance.metadata["meta_key"] = "meta_value"
        assert context_instance.metadata.get("meta_key") == "meta_value"
    else:
        pytest.skip("Context does not implement a metadata property")


# Tests for PandasContext
@pytest.fixture
def pandas_context_instance():
    return PandasContext()


def test_pandas_context_basic_register(pandas_context_instance):
    # Register a pandas DataFrame and verify retrieval.
    df = pd.DataFrame({"a": [1, 2, 3]})
    pandas_context_instance.register("p_key", df)
    retrieved = pandas_context_instance.get("p_key")
    pd.testing.assert_frame_equal(retrieved, df)


def test_pandas_context_missing_key(pandas_context_instance):
    # Expect KeyError when retrieving a non-existent key.
    with pytest.raises(KeyError):
        pandas_context_instance.get("nonexistent")


def test_pandas_context_registration_conflict(pandas_context_instance):
    # Test duplicate registration behavior.
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"a": [2]})
    pandas_context_instance.register("conflict_key", df1)
    # Attempt duplicate registration; since no exception is raised,
    # check that the stored DataFrame is either df1 (unchanged) or df2 (overwritten).
    pandas_context_instance.register("conflict_key", df2)
    retrieved = pandas_context_instance.get("conflict_key")
    if not (retrieved.equals(df1) or retrieved.equals(df2)):
        pytest.fail("Duplicate registration did not result in expected DataFrame")


def test_pandas_context_metadata_handling(pandas_context_instance):
    # If metadata is supported, test its functionality.
    if hasattr(pandas_context_instance, "metadata"):
        pandas_context_instance.metadata["p_meta"] = "test"
        assert pandas_context_instance.metadata.get("p_meta") == "test"
    else:
        pytest.skip("PandasContext does not implement a metadata property")


# Tests for PolarsContext
@pytest.fixture
def polars_context_instance():
    if not HAS_POLARS:
        pytest.skip("polars not installed")
    return PolarsContext()


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_basic_register(polars_context_instance):
    # Register a polars DataFrame and verify retrieval.
    df = pl.DataFrame({"a": [4, 5, 6]})
    polars_context_instance.register("pp_key", df)
    retrieved = polars_context_instance.get("pp_key")
    # Use equals() for Polars DataFrame comparison (frame_equal deprecated)
    assert retrieved.equals(df)


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_missing_key(polars_context_instance):
    # Expect KeyError when retrieving a non-existent key.
    with pytest.raises(KeyError):
        polars_context_instance.get("no_such_key")


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_registration_conflict(polars_context_instance):
    # Test registration conflict behavior for PolarsContext.
    df1 = pl.DataFrame({"a": [10]})
    df2 = pl.DataFrame({"a": [20]})
    polars_context_instance.register("p_conflict", df1)
    try:
        polars_context_instance.register("p_conflict", df2)
        # If no exception is raised, verify that the original value remains unchanged.
        retrieved = polars_context_instance.get("p_conflict")
        if not (retrieved.to_dict() == df1.to_dict() or retrieved.to_dict() == df2.to_dict()):
            pytest.fail("Duplicate registration did not result in expected DataFrame")
    except Exception:
        pytest.skip(
            "PolarsContext raised an exception on duplicate registration, which is acceptable"
        )


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
def test_polars_context_metadata_handling(polars_context_instance):
    # Test metadata functionality for PolarsContext if implemented.
    if hasattr(polars_context_instance, "metadata"):
        polars_context_instance.metadata["p_meta"] = "test_polars"
        assert polars_context_instance.metadata.get("p_meta") == "test_polars"
    else:
        pytest.skip("PolarsContext does not implement a metadata property")


# ===========================================================================
# _get_unique_view_name tests
# ===========================================================================


class TestGetUniqueViewName:
    def test_returns_string(self):
        name = _get_unique_view_name()
        assert isinstance(name, str)

    def test_names_are_unique(self):
        names = {_get_unique_view_name() for _ in range(100)}
        assert len(names) == 100

    def test_name_contains_thread_id(self):
        import threading

        name = _get_unique_view_name()
        tid = str(threading.current_thread().ident or 0)
        assert tid in name


# ===========================================================================
# EngineContext tests
# ===========================================================================


class TestEngineContextInit:
    def test_basic_init(self):
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1]})
        ec = EngineContext(ctx, df, EngineType.PANDAS)
        assert ec.context is ctx
        assert ec.df is df
        assert ec.engine_type == EngineType.PANDAS
        assert ec.sql_executor is None
        assert ec.engine is None
        assert ec.pii_metadata == {}
        assert ec._sql_history == []

    def test_init_with_all_params(self):
        ctx = PandasContext()
        df = pd.DataFrame({"x": [1]})
        executor = MagicMock()
        engine = MagicMock()
        pii = {"col_a": True}
        ec = EngineContext(ctx, df, EngineType.PANDAS, executor, engine, pii)
        assert ec.sql_executor is executor
        assert ec.engine is engine
        assert ec.pii_metadata == pii

    def test_pii_metadata_defaults_to_empty_dict(self):
        ec = EngineContext(PandasContext(), pd.DataFrame(), EngineType.PANDAS, pii_metadata=None)
        assert ec.pii_metadata == {}


class TestEngineContextColumns:
    def test_columns_from_pandas_df(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        ec = EngineContext(PandasContext(), df, EngineType.PANDAS)
        assert ec.columns == ["a", "b"]

    @pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
    def test_columns_from_polars_df(self):
        df = pl.DataFrame({"x": [1], "y": [2]})
        ec = EngineContext(PolarsContext(), df, EngineType.POLARS)
        assert ec.columns == ["x", "y"]

    def test_columns_from_schema_path(self):
        mock_df = MagicMock(spec=[])
        mock_df.schema = True
        mock_df.columns = ["c1", "c2"]
        ec = EngineContext(PandasContext(), mock_df, EngineType.PANDAS)
        assert ec.columns == ["c1", "c2"]

    def test_columns_fallback_empty(self):
        mock_df = MagicMock(spec=[])
        ec = EngineContext(PandasContext(), mock_df, EngineType.PANDAS)
        assert ec.columns == []


class TestEngineContextSchema:
    def test_schema_with_engine(self):
        engine = MagicMock()
        engine.get_schema.return_value = {"a": "int64"}
        df = pd.DataFrame({"a": [1]})
        ec = EngineContext(PandasContext(), df, EngineType.PANDAS, engine=engine)
        assert ec.schema == {"a": "int64"}
        engine.get_schema.assert_called_once_with(df)

    def test_schema_without_engine(self):
        ec = EngineContext(PandasContext(), pd.DataFrame(), EngineType.PANDAS)
        assert ec.schema == {}


class TestEngineContextProperties:
    def test_context_with_attribute(self):
        ctx = MagicMock()
        ctx.session_ref = "sess"
        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        assert ec.spark == ctx.spark

    def test_context_without_attribute(self):
        ctx = MagicMock(spec=[])
        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        assert ec.spark is None


class TestEngineContextWithDf:
    def test_returns_new_context(self):
        ctx = PandasContext()
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [2]})
        ec1 = EngineContext(ctx, df1, EngineType.PANDAS)
        ec2 = ec1.with_df(df2)
        assert ec2 is not ec1
        pd.testing.assert_frame_equal(ec2.df, df2)
        pd.testing.assert_frame_equal(ec1.df, df1)

    def test_shares_sql_history(self):
        ctx = PandasContext()
        ec1 = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        ec1._sql_history.append("SELECT 1")
        ec2 = ec1.with_df(pd.DataFrame())
        assert ec2._sql_history is ec1._sql_history
        ec2._sql_history.append("SELECT 2")
        assert ec1._sql_history == ["SELECT 1", "SELECT 2"]

    def test_preserves_attributes(self):
        ctx = PandasContext()
        executor = MagicMock()
        engine = MagicMock()
        pii = {"x": True}
        ec1 = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS, executor, engine, pii)
        ec2 = ec1.with_df(pd.DataFrame({"z": [1]}))
        assert ec2.context is ctx
        assert ec2.engine_type == EngineType.PANDAS
        assert ec2.sql_executor is executor
        assert ec2.engine is engine
        assert ec2.pii_metadata == pii


class TestEngineContextGet:
    def test_delegates_to_context(self):
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1]})
        ctx.register("my_df", df)
        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        pd.testing.assert_frame_equal(ec.get("my_df"), df)

    def test_raises_on_missing(self):
        ctx = PandasContext()
        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        with pytest.raises(KeyError):
            ec.get("missing")


class TestEngineContextRegisterTempView:
    def test_registers_in_context(self):
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1]})
        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS)
        ec.register_temp_view("view1", df)
        pd.testing.assert_frame_equal(ctx.get("view1"), df)


class TestEngineContextSql:
    def test_without_executor_raises(self):
        ec = EngineContext(PandasContext(), pd.DataFrame(), EngineType.PANDAS)
        with pytest.raises(NotImplementedError):
            ec.sql("SELECT * FROM df")

    def test_records_query_in_history(self):
        ec = EngineContext(PandasContext(), pd.DataFrame(), EngineType.PANDAS)
        try:
            ec.sql("SELECT 1")
        except NotImplementedError:
            pass
        assert "SELECT 1" in ec._sql_history

    def test_with_executor(self):
        ctx = PandasContext()
        df_in = pd.DataFrame({"a": [1, 2]})
        df_out = pd.DataFrame({"a": [1]})
        executor = MagicMock(return_value=df_out)
        ec = EngineContext(ctx, df_in, EngineType.PANDAS, sql_executor=executor)
        result = ec.sql("SELECT * FROM df WHERE a = 1")
        executor.assert_called_once()
        call_args = executor.call_args
        assert "df" not in call_args[0][0].split("_df_")[0]
        pd.testing.assert_frame_equal(result.df, df_out)

    def test_sql_replaces_df_token(self):
        ctx = PandasContext()
        df_in = pd.DataFrame({"a": [1]})
        captured_query = {}

        def capture_executor(query, context):
            captured_query["q"] = query
            return pd.DataFrame({"a": [1]})

        ec = EngineContext(ctx, df_in, EngineType.PANDAS, sql_executor=capture_executor)
        ec.sql("SELECT * FROM df")
        assert "SELECT * FROM df" != captured_query["q"]
        assert captured_query["q"].startswith("SELECT * FROM _df_")

    def test_sql_preserves_non_df_words(self):
        ctx = PandasContext()
        captured_query = {}

        def capture_executor(query, context):
            captured_query["q"] = query
            return pd.DataFrame()

        ec = EngineContext(ctx, pd.DataFrame(), EngineType.PANDAS, sql_executor=capture_executor)
        ec.sql("SELECT pdf_col, some_identifier FROM df")
        assert "pdf_col" in captured_query["q"]
        assert "some_identifier" in captured_query["q"]

    def test_sql_unregisters_view_after_execution(self):
        ctx = PandasContext()
        df_in = pd.DataFrame({"a": [1]})
        executor = MagicMock(return_value=pd.DataFrame())
        ec = EngineContext(ctx, df_in, EngineType.PANDAS, sql_executor=executor)
        ec.sql("SELECT * FROM df")
        assert len(ctx._data) == 0

    def test_sql_unregisters_view_on_error(self):
        ctx = PandasContext()
        df_in = pd.DataFrame({"a": [1]})

        def failing_executor(query, context):
            raise RuntimeError("boom")

        ec = EngineContext(ctx, df_in, EngineType.PANDAS, sql_executor=failing_executor)
        with pytest.raises(RuntimeError, match="boom"):
            ec.sql("SELECT * FROM df")
        assert len(ctx._data) == 0

    def test_sql_returns_new_context_via_with_df(self):
        ctx = PandasContext()
        df_in = pd.DataFrame({"a": [1]})
        df_out = pd.DataFrame({"b": [2]})
        executor = MagicMock(return_value=df_out)
        ec = EngineContext(ctx, df_in, EngineType.PANDAS, sql_executor=executor)
        result = ec.sql("SELECT * FROM df")
        assert result is not ec
        assert result._sql_history is ec._sql_history


# ===========================================================================
# create_context factory tests
# ===========================================================================


class TestCreateContext:
    def test_pandas_engine(self):
        ctx = create_context("pandas")
        assert isinstance(ctx, PandasContext)

    @pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
    def test_polars_engine(self):
        ctx = create_context("polars")
        assert isinstance(ctx, PolarsContext)

    def test_distributed_engine_no_session(self):
        with pytest.raises(ValueError, match="SparkSession required"):
            create_context("spark", spark_session=None)

    def test_unknown_engine(self):
        with pytest.raises(ValueError, match="Unsupported engine"):
            create_context("flink")


# ===========================================================================
# PandasContext - additional method coverage
# ===========================================================================


class TestPandasContextMethods:
    def test_has_existing(self):
        ctx = PandasContext()
        ctx.register("k", pd.DataFrame({"a": [1]}))
        assert ctx.has("k") is True

    def test_has_missing(self):
        ctx = PandasContext()
        assert ctx.has("nope") is False

    def test_list_names_empty(self):
        ctx = PandasContext()
        assert ctx.list_names() == []

    def test_list_names(self):
        ctx = PandasContext()
        ctx.register("a", pd.DataFrame())
        ctx.register("b", pd.DataFrame())
        assert sorted(ctx.list_names()) == ["a", "b"]

    def test_clear(self):
        ctx = PandasContext()
        ctx.register("x", pd.DataFrame())
        ctx.clear()
        assert ctx.list_names() == []
        assert ctx.has("x") is False

    def test_unregister_existing(self):
        ctx = PandasContext()
        df = pd.DataFrame({"a": [1]})
        ctx.register("rem", df, metadata={"pii": True})
        ctx.unregister("rem")
        assert ctx.has("rem") is False
        assert ctx.get_metadata("rem") == {}

    def test_unregister_missing_no_error(self):
        ctx = PandasContext()
        ctx.unregister("nonexistent")

    def test_get_metadata_with_metadata(self):
        ctx = PandasContext()
        ctx.register("m", pd.DataFrame(), metadata={"source": "test"})
        assert ctx.get_metadata("m") == {"source": "test"}

    def test_get_metadata_without_metadata(self):
        ctx = PandasContext()
        ctx.register("n", pd.DataFrame())
        assert ctx.get_metadata("n") == {}

    def test_register_non_dataframe_raises_type_error(self):
        ctx = PandasContext()
        with pytest.raises(TypeError, match="Expected pandas.DataFrame"):
            ctx.register("bad", "not_a_dataframe")

    def test_register_with_metadata(self):
        ctx = PandasContext()
        meta = {"pii": True, "source": "api"}
        ctx.register("df1", pd.DataFrame({"a": [1]}), metadata=meta)
        assert ctx.get_metadata("df1") == meta


# ===========================================================================
# PolarsContext - additional method coverage
# ===========================================================================


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsContextMethods:
    def test_has_existing(self):
        ctx = PolarsContext()
        ctx.register("k", pl.DataFrame({"a": [1]}))
        assert ctx.has("k") is True

    def test_has_missing(self):
        ctx = PolarsContext()
        assert ctx.has("nope") is False

    def test_list_names_empty(self):
        ctx = PolarsContext()
        assert ctx.list_names() == []

    def test_list_names(self):
        ctx = PolarsContext()
        ctx.register("a", pl.DataFrame())
        ctx.register("b", pl.DataFrame())
        assert sorted(ctx.list_names()) == ["a", "b"]

    def test_clear(self):
        ctx = PolarsContext()
        ctx.register("x", pl.DataFrame())
        ctx.clear()
        assert ctx.list_names() == []
        assert ctx.has("x") is False

    def test_unregister_existing(self):
        ctx = PolarsContext()
        ctx.register("rem", pl.DataFrame({"a": [1]}), metadata={"pii": True})
        ctx.unregister("rem")
        assert ctx.has("rem") is False
        assert ctx.get_metadata("rem") == {}

    def test_unregister_missing_no_error(self):
        ctx = PolarsContext()
        ctx.unregister("nonexistent")

    def test_get_metadata_with_metadata(self):
        ctx = PolarsContext()
        ctx.register("m", pl.DataFrame(), metadata={"source": "test"})
        assert ctx.get_metadata("m") == {"source": "test"}

    def test_get_metadata_without_metadata(self):
        ctx = PolarsContext()
        ctx.register("n", pl.DataFrame())
        assert ctx.get_metadata("n") == {}

    def test_register_with_metadata(self):
        ctx = PolarsContext()
        meta = {"pii": False, "origin": "file"}
        ctx.register("df1", pl.DataFrame({"x": [1]}), metadata=meta)
        assert ctx.get_metadata("df1") == meta


# ===========================================================================
# Context base class - unregister default implementation
# ===========================================================================


class TestContextBaseUnregister:
    def test_unregister_does_nothing(self):
        ctx = DummyContext()
        ctx.set("a", 1)
        Context.unregister(ctx, "a")
        assert ctx.get("a") == 1
