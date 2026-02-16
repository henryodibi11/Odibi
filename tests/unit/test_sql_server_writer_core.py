"""Unit tests for SQL Server writer core helpers, validation, and schema evolution."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

from odibi.config import (
    SqlServerMergeValidationConfig,
    SqlServerSchemaEvolutionConfig,
    SqlServerSchemaEvolutionMode,
)
from odibi.writers.sql_server_writer import (
    MergeResult,
    SqlServerMergeWriter,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    """Mock SQL Server connection with execute_sql returning empty list."""
    conn = MagicMock()
    conn.execute_sql = MagicMock(return_value=[])
    return conn


@pytest.fixture
def writer(mock_conn):
    """SqlServerMergeWriter backed by a mock connection."""
    return SqlServerMergeWriter(mock_conn)


# ---------------------------------------------------------------------------
# MergeResult
# ---------------------------------------------------------------------------


class TestMergeResult:
    def test_total_affected_all_zero(self):
        assert MergeResult().total_affected == 0

    def test_total_affected_sum(self):
        assert MergeResult(inserted=3, updated=5, deleted=2).total_affected == 10


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_defaults(self):
        r = ValidationResult()
        assert r.is_valid is True
        assert r.null_key_count == 0
        assert r.duplicate_key_count == 0
        assert r.errors == []

    def test_errors_default_to_empty_list(self):
        """Each instance must get its own list (no shared mutable default)."""
        a = ValidationResult()
        b = ValidationResult()
        a.errors.append("x")
        assert b.errors == []


# ---------------------------------------------------------------------------
# get_staging_table_name
# ---------------------------------------------------------------------------


class TestGetStagingTableName:
    def test_with_schema_prefix(self, writer):
        result = writer.get_staging_table_name("sales.fact_orders", "stg")
        assert result == "[stg].[fact_orders_staging]"

    def test_without_schema(self, writer):
        result = writer.get_staging_table_name("fact_orders", "staging")
        assert result == "[staging].[fact_orders_staging]"

    def test_bracket_stripping(self, writer):
        result = writer.get_staging_table_name("sales.[fact_orders]", "stg")
        assert result == "[stg].[fact_orders_staging]"


# ---------------------------------------------------------------------------
# escape_column / parse_table_name / get_escaped_table_name
# ---------------------------------------------------------------------------


class TestEscapeColumn:
    def test_plain(self, writer):
        assert writer.escape_column("col") == "[col]"

    def test_already_bracketed(self, writer):
        assert writer.escape_column("[col]") == "[col]"


class TestParseTableName:
    def test_with_schema(self, writer):
        assert writer.parse_table_name("sales.orders") == ("sales", "orders")

    def test_without_schema(self, writer):
        assert writer.parse_table_name("orders") == ("dbo", "orders")

    def test_brackets_stripped(self, writer):
        assert writer.parse_table_name("[sales].[orders]") == ("sales", "orders")


class TestGetEscapedTableName:
    def test_basic(self, writer):
        assert writer.get_escaped_table_name("sales.orders") == "[sales].[orders]"

    def test_default_schema(self, writer):
        assert writer.get_escaped_table_name("orders") == "[dbo].[orders]"


# ---------------------------------------------------------------------------
# check_table_exists / check_schema_exists / create_schema
# ---------------------------------------------------------------------------


class TestCheckTableExists:
    def test_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer.check_table_exists("sales.orders") is True

    def test_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer.check_table_exists("sales.orders") is False


class TestCheckSchemaExists:
    def test_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer.check_schema_exists("staging") is True

    def test_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer.check_schema_exists("staging") is False


class TestCreateSchema:
    def test_creates_when_missing(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [[], None]  # check returns empty, then create
        writer.create_schema("stg")
        assert mock_conn.execute_sql.call_count == 2

    def test_skips_when_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        writer.create_schema("stg")
        assert mock_conn.execute_sql.call_count == 1  # only the check query


# ---------------------------------------------------------------------------
# get_hash_column_name
# ---------------------------------------------------------------------------


class TestGetHashColumnName:
    def test_explicit_found(self, writer):
        assert writer.get_hash_column_name(["id", "my_hash"], "my_hash") == "my_hash"

    def test_explicit_not_found(self, writer):
        assert writer.get_hash_column_name(["id", "val"], "missing") is None

    def test_auto_detect(self, writer):
        assert writer.get_hash_column_name(["id", "_hash_diff"], None) == "_hash_diff"

    def test_auto_detect_hash(self, writer):
        assert writer.get_hash_column_name(["id", "_hash"], None) == "_hash"

    def test_missing(self, writer):
        assert writer.get_hash_column_name(["id", "name"], None) is None


# ---------------------------------------------------------------------------
# compute_hash_pandas / compute_hash_polars
# ---------------------------------------------------------------------------


class TestComputeHashPandas:
    def test_deterministic(self, writer):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = writer.compute_hash_pandas(df, ["a", "b"])
        assert "_computed_hash" in result.columns
        assert len(result) == 2
        # Same input → same hash
        result2 = writer.compute_hash_pandas(df, ["a", "b"])
        assert list(result["_computed_hash"]) == list(result2["_computed_hash"])

    def test_custom_name(self, writer):
        df = pd.DataFrame({"a": [1]})
        result = writer.compute_hash_pandas(df, ["a"], hash_col_name="h")
        assert "h" in result.columns

    def test_none_handling(self, writer):
        df = pd.DataFrame({"a": [None, 1]})
        result = writer.compute_hash_pandas(df, ["a"])
        assert result["_computed_hash"].notna().all()


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestComputeHashPolars:
    def test_deterministic(self, writer):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = writer.compute_hash_polars(df, ["a", "b"])
        assert "_computed_hash" in result.columns
        assert result.height == 2
        result2 = writer.compute_hash_polars(df, ["a", "b"])
        assert result["_computed_hash"].to_list() == result2["_computed_hash"].to_list()

    def test_custom_name(self, writer):
        df = pl.DataFrame({"a": [1]})
        result = writer.compute_hash_polars(df, ["a"], hash_col_name="h")
        assert "h" in result.columns

    def test_null_handling(self, writer):
        df = pl.DataFrame({"a": [None, 1]})
        result = writer.compute_hash_polars(df, ["a"])
        assert result["_computed_hash"].null_count() == 0


# ---------------------------------------------------------------------------
# filter_changed_rows_pandas / filter_changed_rows_polars
# ---------------------------------------------------------------------------


class TestFilterChangedRowsPandas:
    def test_empty_target(self, writer):
        src = pd.DataFrame({"id": [1, 2], "hash": ["a", "b"]})
        result = writer.filter_changed_rows_pandas(src, [], ["id"], "hash")
        assert len(result) == 2

    def test_new_rows(self, writer):
        src = pd.DataFrame({"id": [1, 2, 3], "hash": ["a", "b", "c"]})
        target = [{"id": 1, "hash": "a"}]
        result = writer.filter_changed_rows_pandas(src, target, ["id"], "hash")
        assert set(result["id"]) == {2, 3}

    def test_changed_rows(self, writer):
        src = pd.DataFrame({"id": [1], "hash": ["new_hash"]})
        target = [{"id": 1, "hash": "old_hash"}]
        result = writer.filter_changed_rows_pandas(src, target, ["id"], "hash")
        assert len(result) == 1

    def test_unchanged_rows(self, writer):
        src = pd.DataFrame({"id": [1], "hash": ["same"]})
        target = [{"id": 1, "hash": "same"}]
        result = writer.filter_changed_rows_pandas(src, target, ["id"], "hash")
        assert len(result) == 0


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestFilterChangedRowsPolars:
    def test_empty_target(self, writer):
        src = pl.DataFrame({"id": [1, 2], "hash": ["a", "b"]})
        result = writer.filter_changed_rows_polars(src, [], ["id"], "hash")
        assert result.height == 2

    def test_new_rows(self, writer):
        src = pl.DataFrame({"id": [1, 2, 3], "hash": ["a", "b", "c"]})
        target = [{"id": 1, "hash": "a"}]
        result = writer.filter_changed_rows_polars(src, target, ["id"], "hash")
        assert set(result["id"].to_list()) == {2, 3}

    def test_changed_rows(self, writer):
        src = pl.DataFrame({"id": [1], "hash": ["new_hash"]})
        target = [{"id": 1, "hash": "old_hash"}]
        result = writer.filter_changed_rows_polars(src, target, ["id"], "hash")
        assert result.height == 1

    def test_unchanged_rows(self, writer):
        src = pl.DataFrame({"id": [1], "hash": ["same"]})
        target = [{"id": 1, "hash": "same"}]
        result = writer.filter_changed_rows_polars(src, target, ["id"], "hash")
        assert result.height == 0


# ---------------------------------------------------------------------------
# validate_keys_pandas / validate_keys_polars
# ---------------------------------------------------------------------------


class TestValidateKeysPandas:
    def test_valid(self, writer):
        df = pd.DataFrame({"k": [1, 2, 3]})
        r = writer.validate_keys_pandas(df, ["k"])
        assert r.is_valid is True
        assert r.errors == []

    def test_null_keys(self, writer):
        df = pd.DataFrame({"k": [1, None, 3]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=True, check_duplicate_keys=False)
        r = writer.validate_keys_pandas(df, ["k"], cfg)
        assert r.is_valid is False
        assert r.null_key_count == 1

    def test_duplicate_keys(self, writer):
        df = pd.DataFrame({"k": [1, 1, 2]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=True)
        r = writer.validate_keys_pandas(df, ["k"], cfg)
        assert r.is_valid is False
        assert r.duplicate_key_count > 0

    def test_checks_disabled(self, writer):
        df = pd.DataFrame({"k": [1, 1, None]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=False)
        r = writer.validate_keys_pandas(df, ["k"], cfg)
        assert r.is_valid is True


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestValidateKeysPolars:
    def test_valid(self, writer):
        df = pl.DataFrame({"k": [1, 2, 3]})
        r = writer.validate_keys_polars(df, ["k"])
        assert r.is_valid is True

    def test_null_keys(self, writer):
        df = pl.DataFrame({"k": [1, None, 3]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=True, check_duplicate_keys=False)
        r = writer.validate_keys_polars(df, ["k"], cfg)
        assert r.is_valid is False
        assert r.null_key_count == 1

    def test_duplicate_keys(self, writer):
        df = pl.DataFrame({"k": [1, 1, 2]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=True)
        r = writer.validate_keys_polars(df, ["k"], cfg)
        assert r.is_valid is False
        assert r.duplicate_key_count > 0

    def test_checks_disabled(self, writer):
        df = pl.DataFrame({"k": [1, 1, None]})
        cfg = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=False)
        r = writer.validate_keys_polars(df, ["k"], cfg)
        assert r.is_valid is True


# ---------------------------------------------------------------------------
# infer_sql_type_pandas / polars / spark
# ---------------------------------------------------------------------------


class TestInferSqlTypePandas:
    def test_int64(self, writer):
        assert writer.infer_sql_type_pandas("int64") == "BIGINT"

    def test_float64(self, writer):
        assert writer.infer_sql_type_pandas("float64") == "FLOAT"

    def test_object(self, writer):
        assert writer.infer_sql_type_pandas("object") == "NVARCHAR(MAX)"

    def test_bool(self, writer):
        assert writer.infer_sql_type_pandas("bool") == "BIT"

    def test_unknown_fallback(self, writer):
        assert writer.infer_sql_type_pandas("unknown_type_xyz") == "NVARCHAR(MAX)"


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestInferSqlTypePolars:
    def test_int64(self, writer):
        assert writer.infer_sql_type_polars("Int64") == "BIGINT"

    def test_float64(self, writer):
        assert writer.infer_sql_type_polars("Float64") == "FLOAT"

    def test_utf8(self, writer):
        assert writer.infer_sql_type_polars("Utf8") == "NVARCHAR(MAX)"

    def test_unknown_fallback(self, writer):
        assert writer.infer_sql_type_polars("SomeNewType") == "NVARCHAR(MAX)"


class TestInferSqlTypeSpark:
    def test_mapped_types(self, writer):
        for type_name, expected_sql in [
            ("StringType", "NVARCHAR(MAX)"),
            ("IntegerType", "INT"),
            ("LongType", "BIGINT"),
            ("DoubleType", "FLOAT"),
            ("BooleanType", "BIT"),
            ("DateType", "DATE"),
            ("TimestampType", "DATETIME2"),
        ]:
            mock_dtype = MagicMock()
            mock_dtype.__class__ = type(type_name, (), {})
            type(mock_dtype).__name__ = type_name
            assert writer.infer_sql_type_spark(mock_dtype) == expected_sql

    def test_decimal_type_from_map(self, writer):
        mock_dtype = MagicMock()
        type(mock_dtype).__name__ = "DecimalType"
        mock_dtype.precision = 10
        mock_dtype.scale = 4
        # DecimalType is in SPARK_TO_SQL_TYPE_MAP, so it returns plain DECIMAL
        assert writer.infer_sql_type_spark(mock_dtype) == "DECIMAL"

    def test_unknown_fallback(self, writer):
        mock_dtype = MagicMock()
        type(mock_dtype).__name__ = "WeirdType"
        assert writer.infer_sql_type_spark(mock_dtype) == "NVARCHAR(MAX)"


# ---------------------------------------------------------------------------
# create_table_from_pandas / create_table_from_polars
# ---------------------------------------------------------------------------


class TestCreateTableFromPandas:
    def test_ddl_generated(self, writer, mock_conn):
        df = pd.DataFrame({"id": pd.array([1], dtype="int64"), "name": ["a"]})
        writer.create_table_from_pandas(df, "dbo.test_tbl")
        sql_arg = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE" in sql_arg
        assert "[id]" in sql_arg
        assert "[name]" in sql_arg
        assert "BIGINT" in sql_arg

    def test_schema_parsed(self, writer, mock_conn):
        df = pd.DataFrame({"x": [1.0]})
        writer.create_table_from_pandas(df, "myschema.tbl")
        sql_arg = mock_conn.execute_sql.call_args[0][0]
        assert "[myschema].[tbl]" in sql_arg


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestCreateTableFromPolars:
    def test_ddl_generated(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["hello"]})
        writer.create_table_from_polars(df, "dbo.test_tbl")
        sql_arg = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE" in sql_arg
        assert "[id]" in sql_arg
        assert "[val]" in sql_arg

    def test_schema_parsed(self, writer, mock_conn):
        df = pl.DataFrame({"x": [1.5]})
        writer.create_table_from_polars(df, "myschema.tbl")
        sql_arg = mock_conn.execute_sql.call_args[0][0]
        assert "[myschema].[tbl]" in sql_arg


# ---------------------------------------------------------------------------
# handle_schema_evolution_pandas / handle_schema_evolution_polars
# ---------------------------------------------------------------------------


class TestHandleSchemaEvolutionPandas:
    def test_none_config_returns_all_cols(self, writer):
        df = pd.DataFrame({"a": [1], "b": [2]})
        assert writer.handle_schema_evolution_pandas(df, "t", None) == ["a", "b"]

    def test_strict_raises_on_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("a", "int", None, None, None)]
        df = pd.DataFrame({"a": [1], "new_col": [2]})
        cfg = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.STRICT)
        with pytest.raises(ValueError, match="strict"):
            writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)

    def test_evolve_adds_columns(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("a", "int", None, None, None)],  # get_table_columns
            None,  # add_columns ALTER TABLE
        ]
        df = pd.DataFrame({"a": [1], "b": ["x"]})
        cfg = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=True
        )
        cols = writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)
        assert set(cols) == {"a", "b"}
        assert mock_conn.execute_sql.call_count == 2

    def test_ignore_filters_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("a", "int", None, None, None)]
        df = pd.DataFrame({"a": [1], "extra": [2]})
        cfg = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.IGNORE)
        cols = writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)
        assert cols == ["a"]


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestHandleSchemaEvolutionPolars:
    def test_none_config_returns_all_cols(self, writer):
        df = pl.DataFrame({"a": [1], "b": [2]})
        assert set(writer.handle_schema_evolution_polars(df, "t", None)) == {"a", "b"}

    def test_strict_raises_on_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("a", "int", None, None, None)]
        df = pl.DataFrame({"a": [1], "new_col": [2]})
        cfg = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.STRICT)
        with pytest.raises(ValueError, match="strict"):
            writer.handle_schema_evolution_polars(df, "dbo.t", cfg)

    def test_evolve_adds_columns(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("a", "int", None, None, None)],  # get_table_columns
            None,  # add_columns ALTER TABLE
        ]
        df = pl.DataFrame({"a": [1], "b": ["x"]})
        cfg = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=True
        )
        cols = writer.handle_schema_evolution_polars(df, "dbo.t", cfg)
        assert set(cols) == {"a", "b"}
        assert mock_conn.execute_sql.call_count == 2

    def test_ignore_filters_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("a", "int", None, None, None)]
        df = pl.DataFrame({"a": [1], "extra": [2]})
        cfg = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.IGNORE)
        cols = writer.handle_schema_evolution_polars(df, "dbo.t", cfg)
        assert cols == ["a"]


# ---------------------------------------------------------------------------
# get_table_columns
# ---------------------------------------------------------------------------


class TestGetTableColumns:
    def test_dict_rows(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
            {
                "COLUMN_NAME": "name",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": 100,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cols = writer.get_table_columns("dbo.t")
        assert cols == {"id": "int", "name": "nvarchar(100)"}

    def test_tuple_rows(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            ("id", "int", None, None, None),
            ("val", "nvarchar", -1, None, None),
        ]
        cols = writer.get_table_columns("dbo.t")
        assert cols == {"id": "int", "val": "nvarchar(MAX)"}

    def test_decimal_type(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "price",
                "DATA_TYPE": "decimal",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": 18,
                "NUMERIC_SCALE": 2,
            },
        ]
        cols = writer.get_table_columns("dbo.t")
        assert cols == {"price": "decimal(18,2)"}


# ---------------------------------------------------------------------------
# add_columns
# ---------------------------------------------------------------------------


class TestAddColumns:
    def test_adds_each_column(self, writer, mock_conn):
        writer.add_columns("dbo.t", {"col_a": "INT", "col_b": "NVARCHAR(50)"})
        assert mock_conn.execute_sql.call_count == 2
        sqls = [c[0][0] for c in mock_conn.execute_sql.call_args_list]
        assert any("[col_a] INT NULL" in s for s in sqls)
        assert any("[col_b] NVARCHAR(50) NULL" in s for s in sqls)

    def test_empty_dict_noop(self, writer, mock_conn):
        writer.add_columns("dbo.t", {})
        mock_conn.execute_sql.assert_not_called()


# ---------------------------------------------------------------------------
# create_primary_key
# ---------------------------------------------------------------------------


class TestCreatePrimaryKey:
    def test_generates_pk(self, writer, mock_conn):
        # get_table_columns returns column info, then ALTER NOT NULL, then ADD CONSTRAINT
        mock_conn.execute_sql.side_effect = [
            [("id", "int", None, None, None)],  # get_table_columns
            None,  # ALTER COLUMN NOT NULL
            None,  # ADD CONSTRAINT PK
        ]
        writer.create_primary_key("dbo.orders", ["id"])
        assert mock_conn.execute_sql.call_count == 3
        pk_sql = mock_conn.execute_sql.call_args_list[2][0][0]
        assert "PRIMARY KEY" in pk_sql
        assert "[PK_orders]" in pk_sql


# ---------------------------------------------------------------------------
# create_index
# ---------------------------------------------------------------------------


class TestCreateIndex:
    def test_generates_index(self, writer, mock_conn):
        writer.create_index("dbo.orders", ["col_a", "col_b"])
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE NONCLUSTERED INDEX" in sql
        assert "[col_a]" in sql
        assert "[col_b]" in sql

    def test_custom_index_name(self, writer, mock_conn):
        writer.create_index("dbo.orders", ["a"], index_name="my_idx")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "[my_idx]" in sql


# ---------------------------------------------------------------------------
# truncate_staging / truncate_table / delete_from_table / drop_table
# ---------------------------------------------------------------------------


class TestTruncateStaging:
    def test_sql(self, writer, mock_conn):
        writer.truncate_staging("[stg].[tbl_staging]")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "TRUNCATE TABLE" in sql
        assert "[stg].[tbl_staging]" in sql


class TestTruncateTable:
    def test_sql(self, writer, mock_conn):
        writer.truncate_table("dbo.fact")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "TRUNCATE TABLE [dbo].[fact]" in sql


class TestDeleteFromTable:
    def test_returns_count_dict(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [{"deleted_count": 42}]
        assert writer.delete_from_table("dbo.t") == 42

    def test_returns_count_tuple(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(7,)]
        assert writer.delete_from_table("dbo.t") == 7

    def test_empty_result(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer.delete_from_table("dbo.t") == 0


class TestDropTable:
    def test_sql(self, writer, mock_conn):
        writer.drop_table("sales.dim")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "DROP TABLE IF EXISTS [sales].[dim]" in sql
