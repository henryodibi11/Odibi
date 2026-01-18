"""Tests for SQL Server MERGE and overwrite writer."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from odibi.config import (
    SqlServerAuditColsConfig,
    SqlServerMergeOptions,
    SqlServerMergeValidationConfig,
    SqlServerOverwriteOptions,
    SqlServerOverwriteStrategy,
    SqlServerSchemaEvolutionConfig,
    SqlServerSchemaEvolutionMode,
)
from odibi.writers.sql_server_writer import (
    MergeResult,
    OverwriteResult,
    PANDAS_TO_SQL_TYPE_MAP,
    POLARS_TO_SQL_TYPE_MAP,
    SqlServerMergeWriter,
    ValidationResult,
)

# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


def create_mock_connection():
    """Create a mock connection with standard methods."""
    conn = MagicMock()
    conn.execute_sql = MagicMock(return_value=[])
    conn.write_table = MagicMock()
    conn.get_spark_options = MagicMock(return_value={})
    return conn


def create_sample_pandas_df():
    """Create a sample Pandas DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.0, 200.0, 300.0],
        }
    )


# =============================================================================
# Tests for Dataclasses
# =============================================================================


class TestMergeResult:
    """Tests for MergeResult dataclass."""

    def test_default_values(self):
        """Should have zero defaults for all counts."""
        result = MergeResult()
        assert result.inserted == 0
        assert result.updated == 0
        assert result.deleted == 0

    def test_total_affected(self):
        """Should compute total affected correctly."""
        result = MergeResult(inserted=10, updated=5, deleted=2)
        assert result.total_affected == 17

    def test_total_affected_empty(self):
        """Should return 0 for empty result."""
        result = MergeResult()
        assert result.total_affected == 0

    def test_custom_values(self):
        """Should store custom values correctly."""
        result = MergeResult(inserted=100, updated=50, deleted=25)
        assert result.inserted == 100
        assert result.updated == 50
        assert result.deleted == 25


class TestOverwriteResult:
    """Tests for OverwriteResult dataclass."""

    def test_default_values(self):
        """Should have sensible defaults."""
        result = OverwriteResult()
        assert result.rows_written == 0
        assert result.strategy == "truncate_insert"

    def test_custom_values(self):
        """Should store custom values correctly."""
        result = OverwriteResult(rows_written=500, strategy="drop_create")
        assert result.rows_written == 500
        assert result.strategy == "drop_create"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_default_values(self):
        """Should have valid defaults."""
        result = ValidationResult()
        assert result.is_valid is True
        assert result.null_key_count == 0
        assert result.duplicate_key_count == 0
        assert result.errors == []

    def test_post_init_creates_empty_list(self):
        """Should create empty errors list if None."""
        result = ValidationResult(errors=None)
        assert result.errors == []

    def test_custom_errors(self):
        """Should store custom errors."""
        result = ValidationResult(
            is_valid=False,
            null_key_count=5,
            duplicate_key_count=3,
            errors=["error1", "error2"],
        )
        assert result.is_valid is False
        assert result.null_key_count == 5
        assert result.duplicate_key_count == 3
        assert result.errors == ["error1", "error2"]


# =============================================================================
# Tests for Type Mappings
# =============================================================================


class TestTypeMappings:
    """Tests for type mapping dictionaries."""

    def test_pandas_type_map_has_expected_types(self):
        """Should have common Pandas types mapped."""
        assert "int64" in PANDAS_TO_SQL_TYPE_MAP
        assert "float64" in PANDAS_TO_SQL_TYPE_MAP
        assert "object" in PANDAS_TO_SQL_TYPE_MAP
        assert "bool" in PANDAS_TO_SQL_TYPE_MAP

    def test_pandas_type_map_returns_sql_types(self):
        """Should map to valid SQL Server types."""
        assert PANDAS_TO_SQL_TYPE_MAP["int64"] == "BIGINT"
        assert PANDAS_TO_SQL_TYPE_MAP["float64"] == "FLOAT"
        assert PANDAS_TO_SQL_TYPE_MAP["object"] == "NVARCHAR(MAX)"
        assert PANDAS_TO_SQL_TYPE_MAP["bool"] == "BIT"

    def test_polars_type_map_has_expected_types(self):
        """Should have common Polars types mapped."""
        assert "Int64" in POLARS_TO_SQL_TYPE_MAP
        assert "Float64" in POLARS_TO_SQL_TYPE_MAP
        assert "Utf8" in POLARS_TO_SQL_TYPE_MAP
        assert "Boolean" in POLARS_TO_SQL_TYPE_MAP

    def test_polars_type_map_returns_sql_types(self):
        """Should map to valid SQL Server types."""
        assert POLARS_TO_SQL_TYPE_MAP["Int64"] == "BIGINT"
        assert POLARS_TO_SQL_TYPE_MAP["Float64"] == "FLOAT"
        assert POLARS_TO_SQL_TYPE_MAP["Utf8"] == "NVARCHAR(MAX)"
        assert POLARS_TO_SQL_TYPE_MAP["Boolean"] == "BIT"


# =============================================================================
# Tests for SqlServerMergeWriter Initialization
# =============================================================================


class TestSqlServerMergeWriterInit:
    """Tests for SqlServerMergeWriter initialization."""

    def test_init_stores_connection(self):
        """Should store connection reference."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        assert writer.connection == conn

    @patch("odibi.writers.sql_server_writer.get_logging_context")
    def test_init_gets_logging_context(self, mock_get_ctx):
        """Should initialize logging context."""
        mock_ctx = MagicMock()
        mock_get_ctx.return_value = mock_ctx
        conn = create_mock_connection()

        writer = SqlServerMergeWriter(conn)

        mock_get_ctx.assert_called_once()
        assert writer.ctx == mock_ctx


# =============================================================================
# Tests for Helper Methods
# =============================================================================


class TestGetStagingTableName:
    """Tests for get_staging_table_name method."""

    def test_with_schema_qualified_name(self):
        """Should extract table name and create staging name."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_staging_table_name("sales.fact_orders", "staging")

        assert result == "[staging].[fact_orders_staging]"

    def test_without_schema(self):
        """Should use table name directly."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_staging_table_name("fact_orders", "stg")

        assert result == "[stg].[fact_orders_staging]"

    def test_strips_brackets(self):
        """Should strip existing brackets from table name."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_staging_table_name("[dbo].[orders]", "staging")

        assert result == "[staging].[orders_staging]"


class TestEscapeColumn:
    """Tests for escape_column method."""

    def test_adds_brackets(self):
        """Should add brackets to column name."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.escape_column("column_name")

        assert result == "[column_name]"

    def test_strips_existing_brackets(self):
        """Should handle already bracketed names."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.escape_column("[already_bracketed]")

        assert result == "[already_bracketed]"


class TestParseTableName:
    """Tests for parse_table_name method."""

    def test_with_schema(self):
        """Should split schema and table."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        schema, table = writer.parse_table_name("sales.orders")

        assert schema == "sales"
        assert table == "orders"

    def test_without_schema(self):
        """Should default to dbo schema."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        schema, table = writer.parse_table_name("orders")

        assert schema == "dbo"
        assert table == "orders"

    def test_strips_brackets(self):
        """Should strip brackets from both parts."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        schema, table = writer.parse_table_name("[sales].[orders]")

        assert schema == "sales"
        assert table == "orders"


class TestGetEscapedTableName:
    """Tests for get_escaped_table_name method."""

    def test_with_schema(self):
        """Should return fully escaped name."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_escaped_table_name("sales.orders")

        assert result == "[sales].[orders]"

    def test_without_schema(self):
        """Should add dbo schema and escape."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_escaped_table_name("orders")

        assert result == "[dbo].[orders]"


# =============================================================================
# Tests for Table Operations
# =============================================================================


class TestCheckTableExists:
    """Tests for check_table_exists method."""

    def test_returns_true_when_exists(self):
        """Should return True when table exists."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [(1,)]
        writer = SqlServerMergeWriter(conn)

        result = writer.check_table_exists("dbo.orders")

        assert result is True
        conn.execute_sql.assert_called_once()

    def test_returns_false_when_not_exists(self):
        """Should return False when table doesn't exist."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        result = writer.check_table_exists("dbo.nonexistent")

        assert result is False

    def test_sql_query_contains_schema_and_table(self):
        """Should query INFORMATION_SCHEMA with correct params."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        writer.check_table_exists("sales.orders")

        sql = conn.execute_sql.call_args[0][0]
        assert "TABLE_SCHEMA = 'sales'" in sql
        assert "TABLE_NAME = 'orders'" in sql


class TestCheckSchemaExists:
    """Tests for check_schema_exists method."""

    def test_returns_true_when_exists(self):
        """Should return True when schema exists."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [(1,)]
        writer = SqlServerMergeWriter(conn)

        result = writer.check_schema_exists("sales")

        assert result is True

    def test_returns_false_when_not_exists(self):
        """Should return False when schema doesn't exist."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        result = writer.check_schema_exists("nonexistent")

        assert result is False


class TestCreateSchema:
    """Tests for create_schema method."""

    def test_creates_schema_if_not_exists(self):
        """Should create schema when it doesn't exist."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        writer.create_schema("new_schema")

        assert conn.execute_sql.call_count == 2
        create_call = conn.execute_sql.call_args_list[1][0][0]
        assert "CREATE SCHEMA [new_schema]" in create_call

    def test_does_not_create_if_exists(self):
        """Should not create schema when it already exists."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [(1,)]
        writer = SqlServerMergeWriter(conn)

        writer.create_schema("existing_schema")

        assert conn.execute_sql.call_count == 1


class TestDropTable:
    """Tests for drop_table method."""

    def test_executes_drop_if_exists(self):
        """Should execute DROP TABLE IF EXISTS."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.drop_table("dbo.orders")

        sql = conn.execute_sql.call_args[0][0]
        assert "DROP TABLE IF EXISTS [dbo].[orders]" in sql


class TestTruncateTable:
    """Tests for truncate_table method."""

    def test_executes_truncate(self):
        """Should execute TRUNCATE TABLE."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.truncate_table("dbo.orders")

        sql = conn.execute_sql.call_args[0][0]
        assert "TRUNCATE TABLE [dbo].[orders]" in sql


class TestTruncateStaging:
    """Tests for truncate_staging method."""

    def test_executes_conditional_truncate(self):
        """Should truncate staging table if exists."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.truncate_staging("[staging].[orders_staging]")

        sql = conn.execute_sql.call_args[0][0]
        assert "IF OBJECT_ID" in sql
        assert "TRUNCATE TABLE [staging].[orders_staging]" in sql


class TestDeleteFromTable:
    """Tests for delete_from_table method."""

    def test_executes_delete_and_returns_count(self):
        """Should execute DELETE and return row count."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [{"deleted_count": 100}]
        writer = SqlServerMergeWriter(conn)

        result = writer.delete_from_table("dbo.orders")

        assert result == 100
        sql = conn.execute_sql.call_args[0][0]
        assert "DELETE FROM [dbo].[orders]" in sql

    def test_returns_zero_when_no_rows(self):
        """Should return 0 when no rows deleted."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        result = writer.delete_from_table("dbo.empty_table")

        assert result == 0


# =============================================================================
# Tests for Column Operations
# =============================================================================


class TestGetTableColumns:
    """Tests for get_table_columns method."""

    def test_returns_column_dict(self):
        """Should return dictionary of columns and types."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 0,
            },
            {
                "COLUMN_NAME": "name",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        writer = SqlServerMergeWriter(conn)

        result = writer.get_table_columns("dbo.users")

        assert "id" in result
        assert "name" in result
        assert result["name"] == "nvarchar(255)"

    def test_handles_max_length(self):
        """Should handle MAX length columns."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "description",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": -1,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        writer = SqlServerMergeWriter(conn)

        result = writer.get_table_columns("dbo.products")

        assert result["description"] == "nvarchar(MAX)"

    def test_handles_tuple_rows(self):
        """Should handle tuple-style rows."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
            ("id", "int", None, 10, 0),
            ("name", "nvarchar", 100, None, None),
        ]
        writer = SqlServerMergeWriter(conn)

        result = writer.get_table_columns("dbo.users")

        assert result["id"] == "int"
        assert result["name"] == "nvarchar(100)"


class TestAddColumns:
    """Tests for add_columns method."""

    def test_adds_multiple_columns(self):
        """Should add multiple columns via ALTER TABLE."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.add_columns("dbo.users", {"age": "INT", "email": "NVARCHAR(255)"})

        assert conn.execute_sql.call_count == 2

    def test_does_nothing_for_empty_dict(self):
        """Should not execute any SQL for empty dict."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.add_columns("dbo.users", {})

        conn.execute_sql.assert_not_called()


class TestInferSqlTypePandas:
    """Tests for infer_sql_type_pandas method."""

    def test_infers_int64(self):
        """Should infer BIGINT for int64."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.infer_sql_type_pandas("int64")

        assert result == "BIGINT"

    def test_infers_float64(self):
        """Should infer FLOAT for float64."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.infer_sql_type_pandas("float64")

        assert result == "FLOAT"

    def test_defaults_to_nvarchar_max(self):
        """Should default to NVARCHAR(MAX) for unknown types."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.infer_sql_type_pandas("unknown_type")

        assert result == "NVARCHAR(MAX)"


class TestInferSqlTypePolars:
    """Tests for infer_sql_type_polars method."""

    def test_infers_int64(self):
        """Should infer BIGINT for Int64."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.infer_sql_type_polars("Int64")

        assert result == "BIGINT"

    def test_defaults_to_nvarchar_max(self):
        """Should default to NVARCHAR(MAX) for unknown types."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.infer_sql_type_polars("CustomType")

        assert result == "NVARCHAR(MAX)"


# =============================================================================
# Tests for Hash Column Detection
# =============================================================================


class TestGetHashColumnName:
    """Tests for get_hash_column_name method."""

    def test_returns_explicit_column_if_exists(self):
        """Should return explicit hash column if it exists."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_hash_column_name(["id", "my_hash"], "my_hash")

        assert result == "my_hash"

    def test_returns_none_if_explicit_not_found(self):
        """Should return None if explicit column not found."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_hash_column_name(["id", "name"], "missing_hash")

        assert result is None

    def test_auto_detects_hash_diff(self):
        """Should auto-detect _hash_diff column."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_hash_column_name(["id", "_hash_diff", "name"], None)

        assert result == "_hash_diff"

    def test_auto_detects_hash(self):
        """Should auto-detect _hash column."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_hash_column_name(["id", "_hash", "name"], None)

        assert result == "_hash"

    def test_returns_none_if_no_hash_column(self):
        """Should return None if no hash column detected."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        result = writer.get_hash_column_name(["id", "name", "value"], None)

        assert result is None


# =============================================================================
# Tests for Compute Hash Operations
# =============================================================================


class TestComputeHashPandas:
    """Tests for compute_hash_pandas method."""

    def test_adds_hash_column(self):
        """Should add hash column to DataFrame."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()

        result = writer.compute_hash_pandas(df, ["name", "value"])

        assert "_computed_hash" in result.columns
        assert len(result["_computed_hash"]) == 3

    def test_uses_custom_column_name(self):
        """Should use custom hash column name."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()

        result = writer.compute_hash_pandas(df, ["name"], "my_hash")

        assert "my_hash" in result.columns

    def test_does_not_modify_original(self):
        """Should not modify original DataFrame."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()
        original_cols = list(df.columns)

        writer.compute_hash_pandas(df, ["name"])

        assert list(df.columns) == original_cols


# =============================================================================
# Tests for Filter Changed Rows
# =============================================================================


class TestFilterChangedRowsPandas:
    """Tests for filter_changed_rows_pandas method."""

    def test_returns_all_rows_when_no_target(self):
        """Should return all rows when target is empty."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, 2, 3], "_hash": ["a", "b", "c"]})

        result = writer.filter_changed_rows_pandas(df, [], ["id"], "_hash")

        assert len(result) == 3

    def test_filters_unchanged_rows(self):
        """Should filter out rows with matching hash."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        source_df = pd.DataFrame({"id": [1, 2, 3], "_hash": ["a", "b", "c"]})
        target_hashes = [
            {"id": 1, "_hash": "a"},
            {"id": 2, "_hash": "x"},  # Changed
        ]

        result = writer.filter_changed_rows_pandas(source_df, target_hashes, ["id"], "_hash")

        assert len(result) == 2
        assert 2 in result["id"].values
        assert 3 in result["id"].values

    def test_includes_new_rows(self):
        """Should include rows not in target."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        source_df = pd.DataFrame({"id": [1, 2, 3], "_hash": ["a", "b", "c"]})
        target_hashes = [{"id": 1, "_hash": "a"}]

        result = writer.filter_changed_rows_pandas(source_df, target_hashes, ["id"], "_hash")

        assert 2 in result["id"].values
        assert 3 in result["id"].values


# =============================================================================
# Tests for Validation
# =============================================================================


class TestValidateKeysPandas:
    """Tests for validate_keys_pandas method."""

    def test_valid_data_returns_valid_result(self):
        """Should return valid result for clean data."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        result = writer.validate_keys_pandas(df, ["id"])

        assert result.is_valid is True
        assert result.null_key_count == 0
        assert result.duplicate_key_count == 0

    def test_detects_null_keys(self):
        """Should detect null values in keys."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, None, 3], "name": ["A", "B", "C"]})
        config = SqlServerMergeValidationConfig(check_null_keys=True)

        result = writer.validate_keys_pandas(df, ["id"], config)

        assert result.is_valid is False
        assert result.null_key_count == 1

    def test_detects_duplicate_keys(self):
        """Should detect duplicate key combinations."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, 1, 2], "name": ["A", "B", "C"]})
        config = SqlServerMergeValidationConfig(check_duplicate_keys=True)

        result = writer.validate_keys_pandas(df, ["id"], config)

        assert result.is_valid is False
        assert result.duplicate_key_count > 0

    def test_respects_config_flags(self):
        """Should respect validation config flags."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, None, 1], "name": ["A", "B", "C"]})
        config = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=False)

        result = writer.validate_keys_pandas(df, ["id"], config)

        assert result.is_valid is True


# =============================================================================
# Tests for Build Merge SQL
# =============================================================================


class TestBuildMergeSql:
    """Tests for build_merge_sql method."""

    def test_generates_basic_merge(self):
        """Should generate basic MERGE statement."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name", "value"],
        )

        assert "MERGE [dbo].[orders] AS target" in sql
        assert "USING [staging].[orders_staging] AS source" in sql
        assert "ON target.[id] = source.[id]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql
        assert "INSERT" in sql
        assert "UPDATE SET" in sql

    def test_includes_update_condition(self):
        """Should include update condition when specified."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        options = SqlServerMergeOptions(update_condition="source._hash != target._hash")

        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name"],
            options=options,
        )

        assert "WHEN MATCHED AND source._hash != target._hash THEN" in sql

    def test_includes_delete_condition(self):
        """Should include delete clause when condition specified."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        options = SqlServerMergeOptions(delete_condition="source._is_deleted = 1")

        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name"],
            options=options,
        )

        assert "WHEN MATCHED AND source._is_deleted = 1 THEN" in sql
        assert "DELETE" in sql

    def test_handles_audit_columns(self):
        """Should use GETUTCDATE() for audit columns."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )

        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name", "created_ts", "updated_ts"],
            options=options,
        )

        assert "GETUTCDATE()" in sql

    def test_excludes_specified_columns(self):
        """Should exclude columns from merge."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        options = SqlServerMergeOptions(exclude_columns=["temp_col"])

        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name", "temp_col"],
            options=options,
        )

        assert "[temp_col]" not in sql.split("INSERT")[1]


# =============================================================================
# Tests for Execute Merge
# =============================================================================


class TestExecuteMerge:
    """Tests for execute_merge method."""

    def test_returns_merge_result(self):
        """Should return MergeResult with counts."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [{"inserted": 10, "updated": 5, "deleted": 2}]
        writer = SqlServerMergeWriter(conn)

        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name"],
        )

        assert isinstance(result, MergeResult)
        assert result.inserted == 10
        assert result.updated == 5
        assert result.deleted == 2

    def test_handles_tuple_result(self):
        """Should handle tuple-style result rows."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [(10, 5, 2)]
        writer = SqlServerMergeWriter(conn)

        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name"],
        )

        assert result.inserted == 10
        assert result.updated == 5
        assert result.deleted == 2

    def test_handles_empty_result(self):
        """Should return empty MergeResult for no results."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []
        writer = SqlServerMergeWriter(conn)

        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name"],
        )

        assert result.inserted == 0
        assert result.updated == 0
        assert result.deleted == 0

    def test_raises_on_error(self):
        """Should propagate database errors."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = Exception("Database error")
        writer = SqlServerMergeWriter(conn)

        with pytest.raises(Exception, match="Database error"):
            writer.execute_merge(
                target_table="dbo.orders",
                staging_table="[staging].[orders_staging]",
                merge_keys=["id"],
                columns=["id", "name"],
            )


# =============================================================================
# Tests for Merge Pandas
# =============================================================================


class TestMergePandas:
    """Tests for merge_pandas method."""

    def test_basic_merge(self):
        """Should execute merge operation for Pandas DataFrame."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [(1,)],  # check_schema_exists
            [(1,)],  # check_table_exists
            [],  # check_schema_exists for staging
            [],  # create staging schema
            [],  # truncate_staging
            [{"inserted": 3, "updated": 0, "deleted": 0}],  # execute_merge
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()

        result = writer.merge_pandas(
            df=df,
            target_table="dbo.orders",
            merge_keys=["id"],
            options=SqlServerMergeOptions(auto_create_schema=True),
        )

        assert isinstance(result, MergeResult)
        conn.write_table.assert_called_once()

    def test_raises_when_table_not_exists_and_no_auto_create(self):
        """Should raise error when table doesn't exist and auto_create is False."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [],  # check_table_exists - table doesn't exist
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()

        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_pandas(
                df=df,
                target_table="dbo.orders",
                merge_keys=["id"],
                options=SqlServerMergeOptions(auto_create_table=False),
            )


# =============================================================================
# Tests for Overwrite Pandas
# =============================================================================


class TestOverwritePandas:
    """Tests for overwrite_pandas method."""

    def test_truncate_insert_strategy(self):
        """Should use truncate_insert strategy by default."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
            [],  # truncate_table
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()

        result = writer.overwrite_pandas(df, "dbo.orders")

        assert isinstance(result, OverwriteResult)
        assert result.rows_written == 3
        assert result.strategy == "truncate_insert"

    def test_drop_create_strategy(self):
        """Should drop and recreate table."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
            [],  # drop_table
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)

        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.strategy == "drop_create"

    def test_delete_insert_strategy(self):
        """Should delete all rows then insert."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
            [{"deleted_count": 10}],  # delete_from_table
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)

        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.strategy == "delete_insert"

    def test_auto_creates_schema(self):
        """Should auto-create schema when configured."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [],  # check_schema_exists - doesn't exist
            [],  # create_schema
            [],  # check_table_exists - doesn't exist
            [],  # write_table creates the table (no extra SQL needed)
        ]
        writer = SqlServerMergeWriter(conn)
        df = create_sample_pandas_df()
        options = SqlServerOverwriteOptions(auto_create_schema=True)

        writer.overwrite_pandas(df, "new_schema.orders", options)

        assert conn.execute_sql.call_count >= 2


# =============================================================================
# Tests for Schema Evolution
# =============================================================================


class TestHandleSchemaEvolutionPandas:
    """Tests for handle_schema_evolution_pandas method."""

    def test_returns_all_columns_when_no_config(self):
        """Should return all columns when no config."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1], "name": ["A"], "new_col": [1]})

        result = writer.handle_schema_evolution_pandas(df, "dbo.orders", None)

        assert result == ["id", "name", "new_col"]

    def test_strict_mode_raises_on_new_columns(self):
        """Should raise error in strict mode with new columns."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1], "new_col": [1]})
        config = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.STRICT)

        with pytest.raises(ValueError, match="new columns"):
            writer.handle_schema_evolution_pandas(df, "dbo.orders", config)

    def test_ignore_mode_returns_matching_columns(self):
        """Should return only matching columns in ignore mode."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
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
                "CHARACTER_MAXIMUM_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1], "name": ["A"], "new_col": [1]})
        config = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.IGNORE)

        result = writer.handle_schema_evolution_pandas(df, "dbo.orders", config)

        assert "id" in result
        assert "name" in result
        assert "new_col" not in result


# =============================================================================
# Tests for Create Table Operations
# =============================================================================


class TestCreateTableFromPandas:
    """Tests for create_table_from_pandas method."""

    def test_creates_table_with_correct_types(self):
        """Should create table with inferred SQL types."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame(
            {
                "id": pd.Series([1, 2], dtype="int64"),
                "name": pd.Series(["A", "B"], dtype="object"),
                "value": pd.Series([1.5, 2.5], dtype="float64"),
            }
        )

        writer.create_table_from_pandas(df, "dbo.test_table")

        sql = conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE [dbo].[test_table]" in sql
        assert "[id]" in sql
        assert "[name]" in sql
        assert "[value]" in sql

    def test_adds_audit_columns(self):
        """Should add audit columns when configured."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)
        df = pd.DataFrame({"id": [1, 2]})
        audit_cols = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")

        writer.create_table_from_pandas(df, "dbo.test_table", audit_cols)

        sql = conn.execute_sql.call_args[0][0]
        assert "[created_ts]" in sql
        assert "[updated_ts]" in sql
        assert "DATETIME2" in sql


# =============================================================================
# Tests for Primary Key and Index Operations
# =============================================================================


class TestCreatePrimaryKey:
    """Tests for create_primary_key method."""

    def test_creates_clustered_primary_key(self):
        """Should create clustered primary key."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": 10,
                    "NUMERIC_SCALE": 0,
                }
            ],
            [],  # ALTER COLUMN NOT NULL
            [],  # ADD CONSTRAINT
        ]
        writer = SqlServerMergeWriter(conn)

        writer.create_primary_key("dbo.orders", ["id"])

        calls = [c[0][0] for c in conn.execute_sql.call_args_list]
        assert any("PRIMARY KEY CLUSTERED" in c for c in calls)

    def test_raises_when_column_not_found(self):
        """Should raise error when PK column doesn't exist."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = []  # No columns returned
        writer = SqlServerMergeWriter(conn)

        with pytest.raises(ValueError, match="not found"):
            writer.create_primary_key("dbo.orders", ["missing_col"])


class TestCreateIndex:
    """Tests for create_index method."""

    def test_creates_nonclustered_index(self):
        """Should create nonclustered index."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.create_index("dbo.orders", ["customer_id", "order_date"])

        sql = conn.execute_sql.call_args[0][0]
        assert "CREATE NONCLUSTERED INDEX" in sql
        assert "[customer_id]" in sql
        assert "[order_date]" in sql

    def test_uses_custom_index_name(self):
        """Should use custom index name when provided."""
        conn = create_mock_connection()
        writer = SqlServerMergeWriter(conn)

        writer.create_index("dbo.orders", ["customer_id"], "IX_custom_name")

        sql = conn.execute_sql.call_args[0][0]
        assert "[IX_custom_name]" in sql


# =============================================================================
# Tests for Read Target Hashes
# =============================================================================


class TestReadTargetHashes:
    """Tests for read_target_hashes method."""

    def test_returns_empty_when_hash_column_missing(self):
        """Should return empty list when hash column doesn't exist."""
        conn = create_mock_connection()
        conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": 10,
                "NUMERIC_SCALE": 0,
            },
        ]
        writer = SqlServerMergeWriter(conn)

        result = writer.read_target_hashes("dbo.orders", ["id"], "_hash")

        assert result == []

    def test_returns_hashes_when_column_exists(self):
        """Should return hash data when column exists."""
        conn = create_mock_connection()
        conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": 10,
                    "NUMERIC_SCALE": 0,
                },
                {
                    "COLUMN_NAME": "_hash",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": 64,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [{"id": 1, "_hash": "abc"}, {"id": 2, "_hash": "def"}],
        ]
        writer = SqlServerMergeWriter(conn)

        result = writer.read_target_hashes("dbo.orders", ["id"], "_hash")

        assert len(result) == 2


# =============================================================================
# Tests for Writers Module __init__
# =============================================================================


class TestWritersModuleExports:
    """Tests for writers module exports."""

    def test_exports_merge_result(self):
        """Should export MergeResult class."""
        from odibi.writers import MergeResult as ExportedMergeResult

        assert ExportedMergeResult is MergeResult

    def test_exports_overwrite_result(self):
        """Should export OverwriteResult class."""
        from odibi.writers import OverwriteResult as ExportedOverwriteResult

        assert ExportedOverwriteResult is OverwriteResult

    def test_exports_validation_result(self):
        """Should export ValidationResult class."""
        from odibi.writers import ValidationResult as ExportedValidationResult

        assert ExportedValidationResult is ValidationResult

    def test_exports_sql_server_merge_writer(self):
        """Should export SqlServerMergeWriter class."""
        from odibi.writers import SqlServerMergeWriter as ExportedWriter

        assert ExportedWriter is SqlServerMergeWriter
