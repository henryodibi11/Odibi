"""
Comprehensive PandasEngine tests targeting 100% coverage.
Uses monkeypatch to avoid heavy dependencies while testing all code paths.
"""

import builtins
import io
import sys
import types
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pytest

from odibi.engine.pandas_engine import PandasEngine
from odibi.exceptions import TransformError

# pytestmark = pytest.mark.skip(reason="Environment issues with LazyDataset persistence in CI")


class FakeConnection:
    """Fake connection with pandas_storage_options method."""

    def __init__(self, base_path, storage_options=None):
        self.base_path = Path(base_path)
        self._storage_options = storage_options or {}

    def get_path(self, p):
        parsed = urlparse(p)
        if parsed.scheme and parsed.scheme not in ["", "file"]:
            return p
        return str(self.base_path / p)

    def pandas_storage_options(self):
        return self._storage_options


class FakeConnectionNoStorage:
    """Fake connection without pandas_storage_options method."""

    def __init__(self, base_path):
        self.base_path = Path(base_path)

    def get_path(self, p):
        parsed = urlparse(p)
        if parsed.scheme and parsed.scheme not in ["", "file"]:
            return p
        return str(self.base_path / p)


class FakePandasContext:
    """Fake PandasContext for SQL testing."""

    def __init__(self, dataframes):
        self._dfs = dict(dataframes)

    def list_names(self):
        return list(self._dfs.keys())

    def get(self, name):
        return self._dfs[name]


@pytest.fixture
def engine():
    return PandasEngine()


@pytest.fixture
def tmp_conn(tmp_path):
    return FakeConnection(tmp_path, storage_options={"from_conn": "C1"})


@pytest.fixture
def tmp_conn_no_storage(tmp_path):
    return FakeConnectionNoStorage(tmp_path)


# ========================
# Storage Options Merging
# ========================


class TestMergeStorageOptions:
    """Test _merge_storage_options helper method."""

    def test_merge_with_connection_and_user_override(self, engine, tmp_conn):
        """User options override connection options."""
        merged = engine._merge_storage_options(
            tmp_conn,
            options={"storage_options": {"from_user": "U1", "from_conn": "Uoverride"}, "opt": 1},
        )
        assert merged["storage_options"]["from_conn"] == "Uoverride"
        assert merged["storage_options"]["from_user"] == "U1"
        assert merged["opt"] == 1

    def test_merge_no_connection_storage_options(self, engine, tmp_conn_no_storage):
        """Connection without pandas_storage_options method returns unchanged."""
        opts = {"x": 1}
        merged = engine._merge_storage_options(tmp_conn_no_storage, opts)
        assert merged == opts


# ========================
# Read Operations
# ========================


class TestPandasEngineRead:
    """Test read operations for all formats."""

    def test_read_requires_path_or_table(self, engine, tmp_conn):
        """Read without path or table raises ValueError."""
        with pytest.raises(ValueError, match="Either path or table must be provided"):
            engine.read(tmp_conn, "csv")

    @pytest.mark.parametrize(
        "fmt,func_name",
        [
            ("csv", "read_csv"),
            ("parquet", "read_parquet"),
            ("json", "read_json"),
            ("excel", "read_excel"),
        ],
    )
    def test_read_basic_formats(self, engine, monkeypatch, fmt, func_name, tmp_path):
        """Read basic formats with merged storage options."""
        called = {}

        def fake_reader(path, **kwargs):
            called["path"] = path
            called["kwargs"] = kwargs
            return pd.DataFrame({"ok": [1]})

        monkeypatch.setattr(pd, func_name, fake_reader)

        # Create connection with storage options
        conn = FakeConnection(tmp_path, storage_options={"from_conn": "C1"})

        df = engine.read(
            conn,
            fmt,
            table="table_name",
            options={"storage_options": {"from_user": "U1"}, "other": 2},
        )
        assert isinstance(df, pd.DataFrame)
        assert "table_name" in called["path"]
        assert called["kwargs"]["storage_options"]["from_user"] == "U1"
        assert called["kwargs"]["storage_options"]["from_conn"] == "C1"
        assert called["kwargs"]["other"] == 2

    def test_read_unsupported_format(self, engine, tmp_conn):
        """Read unsupported format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.read(tmp_conn, "not_a_format", path="x")

    def test_read_delta_import_error(self, engine, tmp_conn, monkeypatch):
        """Delta read without deltalake raises ImportError."""
        orig_import = builtins.__import__

        def blocker(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("no deltalake")
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocker)
        with pytest.raises(ImportError, match="Delta Lake support requires"):
            engine.read(tmp_conn, "delta", path="dtable")

    def test_read_delta_success(self, engine, tmp_conn, monkeypatch):
        """Read Delta with version and storage options."""
        captured = {}

        class FakeDeltaTable:
            def __init__(self, path, storage_options=None, version=None):
                captured["path"] = path
                captured["storage_options"] = storage_options
                captured["version"] = version

            # Updated Mock to accept kwargs (arrow_options, partitions)
            def to_pandas(self, **kwargs):
                captured["to_pandas_kwargs"] = kwargs
                return pd.DataFrame({"a": [1]})

            def to_pyarrow_table(self):
                # Mock for older deltalake version fallback
                captured["to_pyarrow_table_called"] = True

                # Return a mock PyArrow Table that has to_pandas
                class MockArrowTable:
                    def to_pandas(self, **kwargs):
                        return pd.DataFrame({"a": [1]})

                return MockArrowTable()

        fake_mod = types.SimpleNamespace(DeltaTable=FakeDeltaTable)
        monkeypatch.setitem(sys.modules, "deltalake", fake_mod)

        df = engine.read(
            tmp_conn,
            "delta",
            path="delta_path",
            options={"versionAsOf": 7, "storage_options": {"from_user": "U1"}},
        )
        assert isinstance(df, pd.DataFrame)
        assert "delta_path" in captured["path"]
        assert captured["version"] == 7
        assert captured["storage_options"]["from_conn"] == "C1"
        assert captured["storage_options"]["from_user"] == "U1"

    def test_read_avro_import_error(self, engine, tmp_conn, monkeypatch):
        """Avro read without fastavro raises ImportError."""
        orig_import = builtins.__import__

        def blocker(name, *args, **kwargs):
            if name == "fastavro":
                raise ImportError("no fastavro")
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocker)
        with pytest.raises(ImportError, match="Avro support requires"):
            engine.read(tmp_conn, "avro", path="x.avro")

    def test_read_avro_local(self, engine, tmp_conn, tmp_path, monkeypatch):
        """Read Avro from local file."""
        p = tmp_path / "data.avro"
        p.write_bytes(b"\x00\x00")

        fake_fastavro = types.SimpleNamespace(reader=lambda f: [{"x": 1}, {"x": 2}])
        monkeypatch.setitem(sys.modules, "fastavro", fake_fastavro)

        df = engine.read(tmp_conn, "avro", path="data.avro")
        assert df.to_dict("list")["x"] == [1, 2]

    def test_read_avro_remote_with_fsspec(self, engine, tmp_conn, monkeypatch):
        """Read Avro from remote path using fsspec."""
        fake_fastavro_records = [{"a": "b"}]
        fake_fastavro = types.SimpleNamespace(reader=lambda f: fake_fastavro_records)
        monkeypatch.setitem(sys.modules, "fastavro", fake_fastavro)

        captured = {}

        class FakeCM:
            def __enter__(self):
                return io.BytesIO(b"")

            def __exit__(self, *args):
                return False

        def fake_fsspec_open(path, mode, **kwargs):
            captured["path"] = path
            captured["mode"] = mode
            captured["kwargs"] = kwargs
            return FakeCM()

        fake_fsspec = types.SimpleNamespace(open=fake_fsspec_open)
        monkeypatch.setitem(sys.modules, "fsspec", fake_fsspec)

        engine.read(
            tmp_conn,
            "avro",
            path="s3://bucket/data.avro",
            options={"storage_options": {"from_user": "U1"}},
        )
        assert captured["path"].startswith("s3://")
        assert captured["mode"] == "rb"
        assert captured["kwargs"]["from_conn"] == "C1"
        assert captured["kwargs"]["from_user"] == "U1"


# ========================
# Write Operations
# ========================


class TestPandasEngineWrite:
    """Test write operations for all formats."""

    def test_write_requires_path_or_table(self, engine, tmp_conn):
        """Write without path or table raises ValueError."""
        with pytest.raises(ValueError, match="Either path or table must be provided"):
            engine.write(pd.DataFrame(), tmp_conn, "csv")

    def test_write_unsupported_format(self, engine, tmp_conn):
        """Write unsupported format raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported format"):
            engine.write(pd.DataFrame(), tmp_conn, "nope", path="x")

    @pytest.mark.parametrize(
        "fmt,method_name",
        [("parquet", "to_parquet"), ("excel", "to_excel")],
    )
    def test_write_parquet_excel(self, engine, tmp_conn, monkeypatch, fmt, method_name):
        """Write Parquet and Excel formats."""
        df = pd.DataFrame({"x": [1]})
        called = {}

        def stub(self, path, index=False, **kwargs):
            called["path"] = path
            called["kwargs"] = kwargs

        monkeypatch.setattr(pd.DataFrame, method_name, stub)
        engine.write(df, tmp_conn, fmt, path=f"out.{fmt}", options={"x": 1})
        assert "out." in called["path"]
        assert called["kwargs"]["x"] == 1

    def test_write_csv_modes(self, engine, tmp_conn, monkeypatch):
        """Write CSV with overwrite and append modes."""
        df = pd.DataFrame({"x": [1]})
        calls = []

        def to_csv_stub(self, path, mode=None, index=False, **kwargs):
            calls.append((path, mode, kwargs))

        monkeypatch.setattr(pd.DataFrame, "to_csv", to_csv_stub)

        engine.write(df, tmp_conn, "csv", path="file.csv", mode="overwrite")
        engine.write(df, tmp_conn, "csv", path="file.csv", mode="append", options={"opt": 2})

        assert calls[0][1] == "w"
        assert calls[1][1] == "a"
        assert calls[1][2]["opt"] == 2

    def test_write_json_no_mkdir_for_remote(self, engine, tmp_conn, monkeypatch):
        """JSON write to remote path doesn't create local directories."""
        df = pd.DataFrame({"x": [1]})

        def mkdir_raise(self, *a, **k):
            raise AssertionError("mkdir should not be called for remote paths")

        monkeypatch.setattr(Path, "mkdir", mkdir_raise)

        calls = []

        def to_json_stub(self, path, orient=None, **kwargs):
            calls.append((path, orient, kwargs))

        monkeypatch.setattr(pd.DataFrame, "to_json", to_json_stub)

        engine.write(df, tmp_conn, "json", path="s3://bucket/file.json", mode="append")
        assert calls and calls[0][1] == "records"

    def test_write_delta_import_error(self, engine, tmp_conn, monkeypatch):
        """Delta write without deltalake raises ImportError."""
        orig_import = builtins.__import__

        def blocker(name, *args, **kwargs):
            if name == "deltalake":
                raise ImportError("no deltalake")
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocker)
        with pytest.raises(ImportError, match="Delta Lake support requires"):
            engine.write(pd.DataFrame({"x": [1]}), tmp_conn, "delta", path="d")

    def test_write_delta_modes_and_partition_warning(self, engine, tmp_conn, monkeypatch):
        """Delta write with modes and partition warning."""
        calls = {}

        def write_deltalake(path, df, mode=None, partition_by=None, storage_options=None):
            calls.setdefault("writes", []).append(
                dict(
                    path=path, mode=mode, partition_by=partition_by, storage_options=storage_options
                )
            )

        class FakeDeltaTable:
            def __init__(self, path, storage_options=None):
                pass

            def history(self, limit=None):
                return [{"version": 1, "timestamp": 1000, "operation": "WRITE", "readVersion": 0}]

            def version(self):
                return 1

        fake_mod = types.SimpleNamespace(
            write_deltalake=write_deltalake,
            DeltaTable=FakeDeltaTable,
        )
        monkeypatch.setitem(sys.modules, "deltalake", fake_mod)

        df = pd.DataFrame({"x": [1]})

        # overwrite mode
        engine.write(df, tmp_conn, "delta", path="d1", mode="overwrite")

        # append with partition_by warns
        with pytest.warns(UserWarning, match="Partitioning can cause performance"):
            engine.write(
                df, tmp_conn, "delta", path="d2", mode="append", options={"partition_by": ["x"]}
            )

        assert calls["writes"][0]["mode"] == "overwrite"
        assert calls["writes"][1]["mode"] == "append"
        assert calls["writes"][0]["storage_options"]["from_conn"] == "C1"

    def test_write_avro_import_error(self, engine, tmp_conn, monkeypatch):
        """Avro write without fastavro raises ImportError."""
        orig_import = builtins.__import__

        def blocker(name, *args, **kwargs):
            if name == "fastavro":
                raise ImportError("no fastavro")
            return orig_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocker)
        with pytest.raises(ImportError, match="Avro support requires"):
            engine.write(pd.DataFrame({"x": [1]}), tmp_conn, "avro", path="a.avro")

    def test_write_avro_local_modes(self, engine, tmp_conn, tmp_path, monkeypatch):
        """Avro write to local path with modes."""
        df = pd.DataFrame({"i": [1, None], "s": ["a", "b"]})
        captured = {}

        def fake_writer(f, schema, records):
            captured["schema"] = schema
            captured["records"] = records

        fake_fastavro = types.SimpleNamespace(writer=fake_writer)
        monkeypatch.setitem(sys.modules, "fastavro", fake_fastavro)

        engine.write(df, tmp_conn, "avro", path="a.avro", mode="overwrite")

        assert captured["schema"]["type"] == "record"
        i_field = [f for f in captured["schema"]["fields"] if f["name"] == "i"][0]
        assert isinstance(i_field["type"], list) and "null" in i_field["type"]

    def test_write_avro_remote_with_fsspec(self, engine, tmp_conn, monkeypatch):
        """Avro write to remote path uses fsspec."""
        fake_fastavro = types.SimpleNamespace(writer=lambda f, s, r: None)
        monkeypatch.setitem(sys.modules, "fastavro", fake_fastavro)

        captured = {}

        class FakeCM:
            def __init__(self, mode):
                self.mode = mode

            def __enter__(self):
                return io.BytesIO()

            def __exit__(self, *a):
                return False

        def fake_fsspec_open(path, mode, **kwargs):
            captured.setdefault("calls", []).append(dict(path=path, mode=mode, kwargs=kwargs))
            return FakeCM(mode)

        monkeypatch.setitem(sys.modules, "fsspec", types.SimpleNamespace(open=fake_fsspec_open))

        df = pd.DataFrame({"x": [1]})
        engine.write(df, tmp_conn, "avro", path="s3://b/a.avro", mode="overwrite")
        engine.write(
            df,
            tmp_conn,
            "avro",
            path="s3://b/a.avro",
            mode="append",
            options={"storage_options": {"from_user": "U1"}},
        )

        assert captured["calls"][0]["mode"] == "wb"
        assert captured["calls"][1]["mode"] == "ab"
        assert captured["calls"][1]["kwargs"]["from_conn"] == "C1"
        assert captured["calls"][1]["kwargs"]["from_user"] == "U1"


# ========================
# SQL Execution
# ========================


class TestPandasEngineExecuteSQL:
    """Test SQL execution with DuckDB and PandasSQL."""

    def test_execute_sql_requires_pandas_context(self, engine, monkeypatch):
        """Execute SQL requires PandasContext type."""
        from odibi.engine import pandas_engine as mod

        class MyPandasContext:
            pass

        monkeypatch.setattr(mod, "PandasContext", MyPandasContext)

        with pytest.raises(TypeError, match="PandasEngine requires PandasContext"):
            engine.execute_sql("SELECT 1", context=object())

    def test_execute_sql_duckdb_success(self, engine, monkeypatch):
        """Execute SQL using DuckDB."""
        from odibi.engine import pandas_engine as mod

        monkeypatch.setattr(mod, "PandasContext", FakePandasContext)

        registers = []

        class FakeConn:
            def register(self, name, df):
                registers.append(name)

            class _Exec:
                def __init__(self, df):
                    self._df = df

                def df(self):
                    return self._df

            def execute(self, sql):
                return FakeConn._Exec(pd.DataFrame({"r": [1]}))

            def close(self):
                pass

        fake_duckdb = types.SimpleNamespace(connect=lambda _: FakeConn())
        monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)

        ctx = FakePandasContext({"t1": pd.DataFrame({"a": [1]}), "t2": pd.DataFrame({"b": [2]})})
        out = engine.execute_sql("select * from t1", context=ctx)

        assert isinstance(out, pd.DataFrame)
        assert set(registers) == {"t1", "t2"}

    def test_execute_sql_fallback_to_pandasql(self, engine, monkeypatch):
        """Execute SQL falls back to pandasql when duckdb unavailable."""
        from odibi.engine import pandas_engine as mod

        monkeypatch.setattr(mod, "PandasContext", FakePandasContext)

        orig_import = builtins.__import__

        def blocker(name, *a, **k):
            if name == "duckdb":
                raise ImportError("no duckdb")
            return orig_import(name, *a, **k)

        monkeypatch.setattr(builtins, "__import__", blocker)

        captured = {}

        def sqldf(sql, locals_dict):
            captured["sql"] = sql
            captured["locals"] = locals_dict
            return pd.DataFrame({"ok": [1]})

        fake_pandasql = types.SimpleNamespace(sqldf=sqldf)
        monkeypatch.setitem(sys.modules, "pandasql", fake_pandasql)

        ctx = FakePandasContext({"t": pd.DataFrame({"x": [1]})})
        out = engine.execute_sql("select * from t", context=ctx)

        assert "t" in captured["locals"]
        assert isinstance(out, pd.DataFrame)

    def test_execute_sql_no_engines_raises(self, engine, monkeypatch):
        """Execute SQL without duckdb or pandasql raises TransformError."""
        from odibi.engine import pandas_engine as mod

        monkeypatch.setattr(mod, "PandasContext", FakePandasContext)

        orig_import = builtins.__import__

        def blocker(name, *a, **k):
            if name in ("duckdb", "pandasql"):
                raise ImportError(f"no {name}")
            return orig_import(name, *a, **k)

        monkeypatch.setattr(builtins, "__import__", blocker)

        with pytest.raises(TransformError, match="SQL execution requires"):
            engine.execute_sql("select 1", context=FakePandasContext({"t": pd.DataFrame()}))


# ========================
# Operations
# ========================


class TestPandasEngineOperations:
    """Test execute_operation method."""

    def test_execute_operation_pivot(self, engine):
        """Execute pivot operation."""
        df = pd.DataFrame({"g": [1, 1, 2, 2], "p": ["A", "B", "A", "B"], "v": [10, 20, 30, 40]})

        out = engine.execute_operation(
            "pivot",
            {
                "group_by": ["g"],
                "pivot_column": "p",
                "value_column": "v",
                "agg_func": ["sum", "mean"],
            },
            df,
        )
        assert "g" in out.columns

    def test_execute_operation_unsupported(self, engine):
        """Execute unsupported operation raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported operation"):
            engine.execute_operation("nope", {}, pd.DataFrame())


# ========================
# Schema Operations
# ========================


class TestPandasEngineSchemaOperations:
    """Test schema introspection methods."""

    def test_get_schema(self, engine):
        """Get schema returns column list."""
        df = pd.DataFrame({"a": [1, 2], "b": [None, 2.0], "c": ["x", "y"]})
        schema = engine.get_schema(df)
        assert schema["a"] == "int64"
        assert schema["b"] == "float64"
        assert schema["c"] == "object"

    def test_get_shape(self, engine):
        """Get shape returns (rows, cols)."""
        df = pd.DataFrame({"a": [1, 2], "b": [None, 2.0], "c": ["x", "y"]})
        assert engine.get_shape(df) == (2, 3)

    def test_count_rows(self, engine):
        """Count rows returns row count."""
        df = pd.DataFrame({"a": [1, 2], "b": [None, 2.0]})
        assert engine.count_rows(df) == 2

    def test_count_nulls_success(self, engine):
        """Count nulls for specified columns."""
        df = pd.DataFrame({"a": [1, 2], "b": [None, 2.0], "c": ["x", "y"]})
        counts = engine.count_nulls(df, ["a", "b"])
        assert counts["a"] == 0
        assert counts["b"] == 1

    def test_count_nulls_missing_column(self, engine):
        """Count nulls with missing column raises ValueError."""
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Column 'missing' not found"):
            engine.count_nulls(df, ["missing"])


# ========================
# Schema Validation
# ========================


class TestPandasEngineValidateSchema:
    """Test schema validation."""

    def test_validate_schema_all_failures(self, engine):
        """Validate schema with all types of failures."""
        df = pd.DataFrame({"id": [1], "val": [1.0], "flag": [True]})
        rules = {
            "required_columns": ["id", "missing"],
            "types": {"id": "int", "val": "int", "absent": "str"},
        }
        failures = engine.validate_schema(df, rules)

        assert any("Missing required columns" in f for f in failures)
        assert any("has type" in f for f in failures)
        assert any("not found for type validation" in f for f in failures)


# ========================
# Avro Schema Inference
# ========================


class TestPandasEngineAvroSchema:
    """Test Avro schema inference."""

    def test_infer_avro_schema(self, engine):
        """Infer Avro schema from DataFrame."""
        df = pd.DataFrame(
            {
                "i": pd.Series([1, None], dtype="float64"),
                "i64": pd.Series([1, 2], dtype="int64"),
                "f64": pd.Series([1.0, 2.0], dtype="float64"),
                "b": pd.Series([True, False], dtype="bool"),
                "s": pd.Series(["a", "b"], dtype="object"),
                "s2": pd.Series(["a", None], dtype="string"),
            }
        )

        schema = engine._infer_avro_schema(df)

        assert schema["type"] == "record"
        assert schema["name"] == "DataFrame"

        name_to_type = {f["name"]: f["type"] for f in schema["fields"]}
        assert isinstance(name_to_type["i"], list) and "null" in name_to_type["i"]
        assert name_to_type["i64"] == "long"
        assert name_to_type["f64"] == "double"
        assert name_to_type["b"] == "boolean"
        assert name_to_type["s"] == "string"
        assert isinstance(name_to_type["s2"], list) and "string" in name_to_type["s2"]


# ========================
# Delta Lake Utilities
# ========================


class TestPandasEngineDeltaUtilities:
    """Test Delta Lake utility methods."""

    def test_vacuum_delta_import_error(self, engine, tmp_conn, monkeypatch):
        """Vacuum without deltalake raises ImportError."""
        orig_import = builtins.__import__

        def blocker(name, *a, **k):
            if name == "deltalake":
                raise ImportError("no deltalake")
            return orig_import(name, *a, **k)

        monkeypatch.setattr(builtins, "__import__", blocker)

        with pytest.raises(ImportError, match="Delta Lake support requires"):
            engine.vacuum_delta(tmp_conn, "table_path")

    def test_vacuum_delta_success(self, engine, tmp_conn, monkeypatch):
        """Vacuum Delta table successfully."""

        class FakeDeltaTable:
            def __init__(self, path, storage_options=None):
                self.path = path
                self.storage_options = storage_options

            def vacuum(self, retention_hours, dry_run, enforce_retention_duration):
                assert retention_hours == 24
                assert dry_run is True
                assert enforce_retention_duration is False
                return ["a", "b", "c"]

        fake_mod = types.SimpleNamespace(DeltaTable=FakeDeltaTable)
        monkeypatch.setitem(sys.modules, "deltalake", fake_mod)

        out = engine.vacuum_delta(
            tmp_conn, "x", retention_hours=24, dry_run=True, enforce_retention_duration=False
        )
        assert out == {"files_deleted": 3}

    def test_get_delta_history_success(self, engine, tmp_conn, monkeypatch):
        """Get Delta table history."""
        history_called = {}

        class FakeDeltaTable:
            def __init__(self, path, storage_options=None):
                self.path = path
                self.storage_options = storage_options

            def history(self, limit=None):
                history_called["limit"] = limit
                return [{"version": 1}]

        monkeypatch.setitem(
            sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaTable)
        )

        hist = engine.get_delta_history(tmp_conn, "p", limit=5)
        assert hist == [{"version": 1}]
        assert history_called["limit"] == 5

    def test_restore_delta_success(self, engine, tmp_conn, monkeypatch):
        """Restore Delta table to version."""
        called = {}

        class FakeDeltaTable:
            def __init__(self, path, storage_options=None):
                called["path"] = path
                called["storage_options"] = storage_options

            def restore(self, version):
                called["version"] = version

        monkeypatch.setitem(
            sys.modules, "deltalake", types.SimpleNamespace(DeltaTable=FakeDeltaTable)
        )

        engine.restore_delta(tmp_conn, "p", version=3)
        assert called["version"] == 3
        assert "p" in called["path"]
        assert called["storage_options"]["from_conn"] == "C1"
