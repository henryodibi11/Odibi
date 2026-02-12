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
        with pytest.raises(ValueError, match="neither 'path' nor 'table' was provided"):
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

        def write_deltalake(path, df, mode=None, partition_by=None, storage_options=None, **kwargs):
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

    def test_infer_avro_schema_datetime(self, engine):
        """Infer Avro schema with datetime columns uses logical types."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "created_at": pd.to_datetime(["2024-01-01", "2024-01-02"]),
                "updated_at": pd.to_datetime(["2024-01-01 10:00:00", None]),
            }
        )

        schema = engine._infer_avro_schema(df)
        name_to_type = {f["name"]: f["type"] for f in schema["fields"]}

        # Non-nullable datetime should be timestamp-micros
        assert name_to_type["created_at"]["type"] == "long"
        assert name_to_type["created_at"]["logicalType"] == "timestamp-micros"

        # Nullable datetime should be union with timestamp-micros
        assert isinstance(name_to_type["updated_at"], list)
        assert "null" in name_to_type["updated_at"]
        ts_type = [t for t in name_to_type["updated_at"] if t != "null"][0]
        assert ts_type["type"] == "long"
        assert ts_type["logicalType"] == "timestamp-micros"


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


# ========================
# Materialize
# ========================


class TestMaterialize:
    """Test materialize method for LazyDataset."""

    def test_materialize_dataframe_passthrough(self, engine):
        """Materialize on regular DataFrame returns it unchanged."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = engine.materialize(df)
        assert result is df

    def test_materialize_lazy_dataset(self, engine, tmp_path):
        """Materialize LazyDataset calls _read_file."""
        from odibi.engine.pandas_engine import LazyDataset

        # Create engine without arrow to avoid PyArrow reshape issue
        engine_no_arrow = PandasEngine(config={"performance": {"use_arrow": False}})

        csv_path = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(csv_path, index=False)

        lazy = LazyDataset(path=str(csv_path), format="csv", options={})
        
        result = engine_no_arrow.materialize(lazy)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a"]
        assert len(result) == 2


# ========================
# Anonymize
# ========================


class TestAnonymize:
    """Test anonymize method for PII protection."""

    def test_anonymize_hash_without_salt(self, engine):
        """Hash method creates consistent hashes without salt."""
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        result = engine.anonymize(df, columns=["name"], method="hash")
        
        # Check that values are hashed (64 char hex)
        assert len(result["name"][0]) == 64
        assert len(result["name"][1]) == 64
        assert result["name"][0] != "Alice"
        # Age column unchanged
        assert list(result["age"]) == [30, 25]

    def test_anonymize_hash_with_salt(self, engine):
        """Hash method with salt produces different hashes."""
        df = pd.DataFrame({"ssn": ["123-45-6789"]})
        result1 = engine.anonymize(df, columns=["ssn"], method="hash", salt="salt1")
        result2 = engine.anonymize(df, columns=["ssn"], method="hash", salt="salt2")
        
        assert result1["ssn"][0] != result2["ssn"][0]

    def test_anonymize_hash_preserves_nulls(self, engine):
        """Hash method preserves null values."""
        df = pd.DataFrame({"email": ["user@test.com", None, "admin@test.com"]})
        result = engine.anonymize(df, columns=["email"], method="hash")
        
        assert pd.isna(result["email"][1])
        assert not pd.isna(result["email"][0])
        assert not pd.isna(result["email"][2])

    def test_anonymize_mask_last_four(self, engine):
        """Mask method exposes last 4 characters."""
        df = pd.DataFrame({"card": ["1234567890123456", "9876543210"]})
        result = engine.anonymize(df, columns=["card"], method="mask")
        
        assert result["card"][0] == "************3456"
        assert result["card"][1] == "******3210"

    def test_anonymize_mask_preserves_nulls(self, engine):
        """Mask method preserves null values."""
        df = pd.DataFrame({"phone": ["555-1234", None]})
        result = engine.anonymize(df, columns=["phone"], method="mask")
        
        assert result["phone"][0] == "****1234"
        assert pd.isna(result["phone"][1])

    def test_anonymize_redact(self, engine):
        """Redact method replaces all values."""
        df = pd.DataFrame({"secret": ["classified", "top_secret"], "public": [1, 2]})
        result = engine.anonymize(df, columns=["secret"], method="redact")
        
        assert all(result["secret"] == "[REDACTED]")
        assert list(result["public"]) == [1, 2]

    def test_anonymize_missing_column_skipped(self, engine):
        """Anonymize skips columns that don't exist."""
        df = pd.DataFrame({"a": [1, 2]})
        result = engine.anonymize(df, columns=["a", "nonexistent"], method="redact")
        
        assert all(result["a"] == "[REDACTED]")
        assert "nonexistent" not in result.columns


# ========================
# Harmonize Schema
# ========================


class TestHarmonizeSchema:
    """Test harmonize_schema method."""

    def test_harmonize_schema_evolve_add_missing(self, engine):
        """EVOLVE mode with ADD_NULLABLE adds missing columns."""
        from odibi.config import SchemaMode, OnMissingColumns, OnNewColumns
        
        class Policy:
            mode = SchemaMode.EVOLVE
            on_missing_columns = OnMissingColumns.FILL_NULL
            on_new_columns = OnNewColumns.ADD_NULLABLE
        
        df = pd.DataFrame({"a": [1, 2]})
        target_schema = {"a": "int64", "b": "int64", "c": "int64"}
        
        result = engine.harmonize_schema(df, target_schema, Policy())
        assert set(result.columns) == {"a", "b", "c"}
        assert pd.isna(result["b"]).all()
        assert pd.isna(result["c"]).all()

    def test_harmonize_schema_enforce_drops_new(self, engine):
        """ENFORCE mode drops new columns not in target."""
        from odibi.config import SchemaMode, OnMissingColumns, OnNewColumns
        
        class Policy:
            mode = SchemaMode.ENFORCE
            on_missing_columns = OnMissingColumns.FILL_NULL
            on_new_columns = OnNewColumns.IGNORE
        
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4], "extra": [5, 6]})
        target_schema = {"a": "int64", "b": "int64"}
        
        result = engine.harmonize_schema(df, target_schema, Policy())
        assert list(result.columns) == ["a", "b"]
        assert "extra" not in result.columns

    def test_harmonize_schema_fail_on_missing(self, engine):
        """FAIL on missing columns raises ValueError."""
        from odibi.config import SchemaMode, OnMissingColumns, OnNewColumns
        
        class Policy:
            mode = SchemaMode.ENFORCE
            on_missing_columns = OnMissingColumns.FAIL
            on_new_columns = OnNewColumns.IGNORE
        
        df = pd.DataFrame({"a": [1, 2]})
        target_schema = {"a": "int64", "b": "int64"}
        
        with pytest.raises(ValueError, match="Missing columns"):
            engine.harmonize_schema(df, target_schema, Policy())

    def test_harmonize_schema_fail_on_new(self, engine):
        """FAIL on new columns raises ValueError."""
        from odibi.config import SchemaMode, OnMissingColumns, OnNewColumns
        
        class Policy:
            mode = SchemaMode.ENFORCE
            on_missing_columns = OnMissingColumns.FILL_NULL
            on_new_columns = OnNewColumns.FAIL
        
        df = pd.DataFrame({"a": [1, 2], "extra": [3, 4]})
        target_schema = {"a": "int64"}
        
        with pytest.raises(ValueError, match="New columns"):
            engine.harmonize_schema(df, target_schema, Policy())


# ========================
# Validate Data
# ========================


class TestValidateData:
    """Test validate_data method."""

    def test_validate_data_empty_check(self, engine):
        """not_empty validation fails on empty DataFrame."""
        from types import SimpleNamespace
        
        config = SimpleNamespace(
            not_empty=True,
            no_nulls=[],
            schema_validation=None,
            ranges={},
            allowed_values={}
        )
        
        df = pd.DataFrame({"a": []})
        failures = engine.validate_data(df, config)
        
        assert len(failures) == 1
        assert "empty" in failures[0].lower()

    def test_validate_data_no_nulls(self, engine):
        """no_nulls validation detects null values."""
        from types import SimpleNamespace
        
        config = SimpleNamespace(
            not_empty=False,
            no_nulls=["name", "age"],
            schema_validation=None,
            ranges={},
            allowed_values={}
        )
        
        df = pd.DataFrame({"name": ["Alice", None], "age": [30, 25]})
        failures = engine.validate_data(df, config)
        
        assert len(failures) == 1
        assert "name" in failures[0]
        assert "null" in failures[0].lower()

    def test_validate_data_ranges(self, engine):
        """Range validation detects out-of-range values."""
        from types import SimpleNamespace
        
        config = SimpleNamespace(
            not_empty=False,
            no_nulls=[],
            schema_validation=None,
            ranges={"age": {"min": 18, "max": 65}},
            allowed_values={}
        )
        
        df = pd.DataFrame({"age": [17, 30, 70]})
        failures = engine.validate_data(df, config)
        
        assert len(failures) == 2
        assert any("< 18" in f for f in failures)
        assert any("> 65" in f for f in failures)

    def test_validate_data_allowed_values(self, engine):
        """allowed_values validation detects invalid values."""
        from types import SimpleNamespace
        
        config = SimpleNamespace(
            not_empty=False,
            no_nulls=[],
            schema_validation=None,
            ranges={},
            allowed_values={"status": ["active", "inactive"]}
        )
        
        df = pd.DataFrame({"status": ["active", "deleted", "inactive"]})
        failures = engine.validate_data(df, config)
        
        assert len(failures) == 1
        assert "status" in failures[0]
        assert "invalid" in failures[0].lower()

    def test_validate_data_all_pass(self, engine):
        """Valid data returns no failures."""
        from types import SimpleNamespace
        
        config = SimpleNamespace(
            not_empty=True,
            no_nulls=["name"],
            schema_validation=None,
            ranges={"age": {"min": 0, "max": 150}},
            allowed_values={"role": ["admin", "user"]}
        )
        
        df = pd.DataFrame({
            "name": ["Alice", "Bob"],
            "age": [30, 25],
            "role": ["admin", "user"]
        })
        failures = engine.validate_data(df, config)
        
        assert len(failures) == 0


# ========================
# Get Sample
# ========================


class TestGetSample:
    """Test get_sample method."""

    def test_get_sample_dataframe(self, engine):
        """get_sample returns first n rows as dict list."""
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        result = engine.get_sample(df, n=3)
        
        assert len(result) == 3
        assert result[0] == {"x": 1}
        assert result[2] == {"x": 3}

    def test_get_sample_default_n(self, engine):
        """get_sample defaults to 10 rows."""
        df = pd.DataFrame({"x": range(100)})
        result = engine.get_sample(df)
        
        assert len(result) == 10

    def test_get_sample_fewer_rows(self, engine):
        """get_sample returns all rows when df has fewer than n."""
        df = pd.DataFrame({"x": [1, 2]})
        result = engine.get_sample(df, n=10)
        
        assert len(result) == 2


# ========================
# Table Exists
# ========================


class TestTableExists:
    """Test table_exists method."""

    def test_table_exists_true(self, engine, tmp_path):
        """table_exists returns True for existing path."""
        file_path = tmp_path / "data.csv"
        file_path.write_text("a,b\n1,2\n")
        
        conn = FakeConnection(tmp_path)
        result = engine.table_exists(conn, path="data.csv")
        
        assert result is True

    def test_table_exists_false(self, engine, tmp_path):
        """table_exists returns False for non-existing path."""
        conn = FakeConnection(tmp_path)
        result = engine.table_exists(conn, path="nonexistent.csv")
        
        assert result is False

    def test_table_exists_no_path(self, engine, tmp_path):
        """table_exists returns False when no path provided."""
        conn = FakeConnection(tmp_path)
        result = engine.table_exists(conn)
        
        assert result is False


# ========================
# Profile Nulls
# ========================


class TestProfileNulls:
    """Test profile_nulls method."""

    def test_profile_nulls_no_nulls(self, engine):
        """profile_nulls returns 0.0 for columns without nulls."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        result = engine.profile_nulls(df)
        
        assert result == {"a": 0.0, "b": 0.0}

    def test_profile_nulls_some_nulls(self, engine):
        """profile_nulls returns correct percentages."""
        df = pd.DataFrame({
            "a": [1, None, 3, None],
            "b": [None, None, None, None]
        })
        result = engine.profile_nulls(df)
        
        assert result["a"] == 0.5
        assert result["b"] == 1.0

    def test_profile_nulls_empty_dataframe(self, engine):
        """profile_nulls works on empty DataFrame."""
        df = pd.DataFrame({"a": [], "b": []})
        result = engine.profile_nulls(df)
        
        # Empty DataFrames return NaN for mean, which should be handled
        assert "a" in result
        assert "b" in result


# ========================
# Filter Greater Than
# ========================


class TestFilterGreaterThan:
    """Test filter_greater_than method."""

    def test_filter_greater_than_numeric(self, engine):
        """filter_greater_than works with numeric columns."""
        df = pd.DataFrame({"age": [10, 20, 30, 40]})
        result = engine.filter_greater_than(df, "age", 25)
        
        assert len(result) == 2
        assert list(result["age"]) == [30, 40]

    def test_filter_greater_than_datetime_column(self, engine):
        """filter_greater_than handles datetime columns."""
        df = pd.DataFrame({
            "date": pd.to_datetime(["2020-01-01", "2021-01-01", "2022-01-01"])
        })
        result = engine.filter_greater_than(df, "date", "2020-12-31")
        
        assert len(result) == 2

    def test_filter_greater_than_string_to_datetime(self, engine):
        """filter_greater_than converts string columns to datetime."""
        df = pd.DataFrame({"date_str": ["2020-01-01", "2021-01-01", "2022-01-01"]})
        result = engine.filter_greater_than(df, "date_str", "2020-12-31")
        
        assert len(result) == 2

    def test_filter_greater_than_missing_column(self, engine):
        """filter_greater_than raises ValueError for missing column."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        
        with pytest.raises(ValueError, match="not found"):
            engine.filter_greater_than(df, "nonexistent", 5)


# ========================
# Filter Coalesce
# ========================


class TestFilterCoalesce:
    """Test filter_coalesce method."""

    def test_filter_coalesce_col1_priority(self, engine):
        """filter_coalesce uses col1 when both present."""
        df = pd.DataFrame({
            "primary": [10, None, 30],
            "fallback": [100, 200, 300]
        })
        result = engine.filter_coalesce(df, "primary", "fallback", ">=", 25)
        
        # Row 0: 10 < 25 (excluded)
        # Row 1: 200 >= 25 (included, fallback to col2)
        # Row 2: 30 >= 25 (included)
        assert len(result) == 2

    def test_filter_coalesce_fallback_to_col2(self, engine):
        """filter_coalesce uses col2 when col1 is null."""
        df = pd.DataFrame({
            "col1": [None, None],
            "col2": [10, 20]
        })
        result = engine.filter_coalesce(df, "col1", "col2", ">", 15)
        
        assert len(result) == 1
        assert result["col2"].iloc[0] == 20

    def test_filter_coalesce_missing_col2(self, engine):
        """filter_coalesce works when col2 doesn't exist."""
        df = pd.DataFrame({"col1": [10, 20, 30]})
        result = engine.filter_coalesce(df, "col1", "nonexistent", ">=", 20)
        
        assert len(result) == 2

    def test_filter_coalesce_operators(self, engine):
        """filter_coalesce supports all operators."""
        df = pd.DataFrame({"a": [10, 20, 30], "b": [100, 200, 300]})
        
        # Test >=
        result = engine.filter_coalesce(df, "a", "b", ">=", 20)
        assert len(result) == 2
        
        # Test >
        result = engine.filter_coalesce(df, "a", "b", ">", 20)
        assert len(result) == 1
        
        # Test <=
        result = engine.filter_coalesce(df, "a", "b", "<=", 20)
        assert len(result) == 2
        
        # Test <
        result = engine.filter_coalesce(df, "a", "b", "<", 20)
        assert len(result) == 1
        
        # Test ==
        result = engine.filter_coalesce(df, "a", "b", "==", 20)
        assert len(result) == 1

    def test_filter_coalesce_datetime_conversion(self, engine):
        """filter_coalesce converts strings to datetime."""
        df = pd.DataFrame({
            "date1": ["2020-01-01", "2021-01-01"],
            "date2": ["2019-01-01", "2022-01-01"]
        })
        result = engine.filter_coalesce(df, "date1", "date2", ">=", "2020-06-01")
        
        assert len(result) == 1

    def test_filter_coalesce_missing_col1(self, engine):
        """filter_coalesce raises ValueError when col1 missing."""
        df = pd.DataFrame({"a": [1, 2]})
        
        with pytest.raises(ValueError, match="not found"):
            engine.filter_coalesce(df, "nonexistent", "a", ">=", 5)

    def test_filter_coalesce_invalid_operator(self, engine):
        """filter_coalesce raises ValueError for invalid operator."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        
        with pytest.raises(ValueError, match="Unsupported operator"):
            engine.filter_coalesce(df, "a", "b", "!=", 5)


# ========================
# Add Write Metadata
# ========================


class TestAddWriteMetadata:
    """Test add_write_metadata method."""

    def test_add_write_metadata_all_defaults(self, engine):
        """add_write_metadata with True adds enabled metadata."""
        df = pd.DataFrame({"a": [1, 2]})
        result = engine.add_write_metadata(
            df,
            metadata_config=True,
            source_connection="my_conn",
            source_table="my_table",
            source_path="/path/to/file.csv",
            is_file_source=True
        )
        
        # Default WriteMetadataConfig enables: extracted_at, source_file
        # but NOT source_connection or source_table
        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns
        # These are False by default
        assert "_source_connection" not in result.columns
        assert "_source_table" not in result.columns
        assert all(result["_source_file"] == "/path/to/file.csv")

    def test_add_write_metadata_file_only_for_file_source(self, engine):
        """add_write_metadata only adds source_file for file sources."""
        from odibi.config import WriteMetadataConfig
        
        df = pd.DataFrame({"a": [1]})
        config = WriteMetadataConfig(
            extracted_at=False,
            source_file=True,
            source_connection=False,
            source_table=False
        )
        
        # File source
        result1 = engine.add_write_metadata(
            df, config, source_path="/file.csv", is_file_source=True
        )
        assert "_source_file" in result1.columns
        
        # SQL source
        result2 = engine.add_write_metadata(
            df, config, source_path="/file.csv", is_file_source=False
        )
        assert "_source_file" not in result2.columns

    def test_add_write_metadata_selective_config(self, engine):
        """add_write_metadata respects config selections."""
        from odibi.config import WriteMetadataConfig
        
        df = pd.DataFrame({"a": [1]})
        config = WriteMetadataConfig(
            extracted_at=True,
            source_file=False,
            source_connection=True,
            source_table=False
        )
        
        result = engine.add_write_metadata(
            df,
            config,
            source_connection="conn1",
            source_table="table1",
            source_path="/file.csv",
            is_file_source=True
        )
        
        assert "_extracted_at" in result.columns
        assert "_source_connection" in result.columns
        assert "_source_file" not in result.columns
        assert "_source_table" not in result.columns

    def test_add_write_metadata_none_returns_unchanged(self, engine):
        """add_write_metadata with None returns DataFrame unchanged."""
        df = pd.DataFrame({"a": [1, 2]})
        result = engine.add_write_metadata(df, metadata_config=None)
        
        assert result.equals(df)
        assert "_extracted_at" not in result.columns

    def test_add_write_metadata_preserves_original(self, engine):
        """add_write_metadata doesn't modify original DataFrame."""
        df = pd.DataFrame({"a": [1, 2]})
        original_cols = df.columns.tolist()
        
        engine.add_write_metadata(df, metadata_config=True, is_file_source=False)
        
        assert df.columns.tolist() == original_cols
