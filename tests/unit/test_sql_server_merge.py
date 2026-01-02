"""Tests for SQL Server MERGE writer."""

from unittest.mock import MagicMock

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
    WriteConfig,
    WriteMode,
)
from odibi.writers.sql_server_writer import (
    MergeResult,
    OverwriteResult,
    SqlServerMergeWriter,
    ValidationResult,
    PANDAS_TO_SQL_TYPE_MAP,
    POLARS_TO_SQL_TYPE_MAP,
)


class TestMergeResult:
    """Tests for MergeResult dataclass."""

    def test_merge_result_defaults(self):
        """Should have zero defaults."""
        result = MergeResult()
        assert result.inserted == 0
        assert result.updated == 0
        assert result.deleted == 0
        assert result.total_affected == 0

    def test_merge_result_total_affected(self):
        """Should calculate total affected rows."""
        result = MergeResult(inserted=10, updated=5, deleted=2)
        assert result.total_affected == 17


class TestSqlServerMergeWriter:
    """Tests for SqlServerMergeWriter class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        conn.get_spark_options = MagicMock(
            return_value={
                "url": "jdbc:sqlserver://server:1433;database=db",
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            }
        )
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_get_staging_table_name_with_schema(self, writer):
        """Should generate staging table name from target with schema."""
        result = writer.get_staging_table_name("oee.oee_fact", "staging")
        assert result == "[staging].[oee_fact_staging]"

    def test_get_staging_table_name_without_schema(self, writer):
        """Should generate staging table name for dbo table."""
        result = writer.get_staging_table_name("customers", "staging")
        assert result == "[staging].[customers_staging]"

    def test_get_staging_table_name_with_brackets(self, writer):
        """Should handle table names with brackets."""
        result = writer.get_staging_table_name("[oee].[oee_fact]", "staging")
        assert result == "[staging].[oee_fact_staging]"

    def test_escape_column(self, writer):
        """Should escape column names."""
        assert writer.escape_column("DateId") == "[DateId]"
        assert writer.escape_column("[DateId]") == "[DateId]"

    def test_parse_table_name_with_schema(self, writer):
        """Should parse schema.table format."""
        schema, table = writer.parse_table_name("oee.oee_fact")
        assert schema == "oee"
        assert table == "oee_fact"

    def test_parse_table_name_without_schema(self, writer):
        """Should default to dbo schema."""
        schema, table = writer.parse_table_name("customers")
        assert schema == "dbo"
        assert table == "customers"

    def test_parse_table_name_with_brackets(self, writer):
        """Should strip brackets from names."""
        schema, table = writer.parse_table_name("[oee].[oee_fact]")
        assert schema == "oee"
        assert table == "oee_fact"

    def test_check_table_exists_true(self, writer, mock_connection):
        """Should return True when table exists."""
        mock_connection.execute_sql.return_value = [(1,)]
        assert writer.check_table_exists("oee.oee_fact") is True

    def test_check_table_exists_false(self, writer, mock_connection):
        """Should return False when table doesn't exist."""
        mock_connection.execute_sql.return_value = []
        assert writer.check_table_exists("oee.oee_fact") is False

    def test_build_merge_sql_basic(self, writer):
        """Should build basic MERGE SQL."""
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId", "P_ID"],
            columns=["DateId", "P_ID", "value", "description"],
        )

        assert "MERGE [oee].[oee_fact] AS target" in sql
        assert "USING [staging].[oee_fact_staging] AS source" in sql
        assert "target.[DateId] = source.[DateId]" in sql
        assert "target.[P_ID] = source.[P_ID]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql
        assert "UPDATE SET" in sql
        assert "[value] = source.[value]" in sql
        assert "[description] = source.[description]" in sql
        assert "INSERT ([DateId], [P_ID], [value], [description])" in sql
        assert "OUTPUT $action INTO @MergeActions" in sql

    def test_build_merge_sql_with_update_condition(self, writer):
        """Should include update condition in MERGE."""
        options = SqlServerMergeOptions(update_condition="source._hash_diff != target._hash_diff")
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value", "_hash_diff"],
            options=options,
        )

        assert "WHEN MATCHED AND source._hash_diff != target._hash_diff THEN" in sql

    def test_build_merge_sql_with_delete_condition(self, writer):
        """Should include delete clause in MERGE."""
        options = SqlServerMergeOptions(delete_condition="source._is_deleted = 1")
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value", "_is_deleted"],
            options=options,
        )

        assert "WHEN MATCHED AND source._is_deleted = 1 THEN" in sql
        assert "DELETE" in sql

    def test_build_merge_sql_with_insert_condition(self, writer):
        """Should include insert condition in MERGE."""
        options = SqlServerMergeOptions(insert_condition="source.is_valid = 1")
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value", "is_valid"],
            options=options,
        )

        assert "WHEN NOT MATCHED BY TARGET AND source.is_valid = 1 THEN" in sql

    def test_build_merge_sql_with_exclude_columns(self, writer):
        """Should exclude specified columns from MERGE."""
        options = SqlServerMergeOptions(exclude_columns=["_hash_diff", "_is_deleted"])
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value", "_hash_diff", "_is_deleted"],
            options=options,
        )

        assert "[_hash_diff]" not in sql
        assert "[_is_deleted]" not in sql
        assert "[value]" in sql

    def test_build_merge_sql_with_audit_cols(self, writer):
        """Should include audit columns with GETUTCDATE()."""
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        sql = writer.build_merge_sql(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value", "created_ts", "updated_ts"],
            options=options,
        )

        assert "[updated_ts] = GETUTCDATE()" in sql
        assert "source.[created_ts]" not in sql.split("UPDATE SET")[1].split("WHEN")[0]
        assert "GETUTCDATE()" in sql.split("VALUES")[1]

    def test_execute_merge_returns_counts(self, writer, mock_connection):
        """Should parse and return merge counts."""
        mock_connection.execute_sql.return_value = [{"inserted": 10, "updated": 5, "deleted": 2}]

        result = writer.execute_merge(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value"],
        )

        assert result.inserted == 10
        assert result.updated == 5
        assert result.deleted == 2

    def test_execute_merge_handles_tuple_result(self, writer, mock_connection):
        """Should handle tuple result format."""
        mock_connection.execute_sql.return_value = [(10, 5, 2)]

        result = writer.execute_merge(
            target_table="oee.oee_fact",
            staging_table="[staging].[oee_fact_staging]",
            merge_keys=["DateId"],
            columns=["DateId", "value"],
        )

        assert result.inserted == 10
        assert result.updated == 5
        assert result.deleted == 2

    def test_merge_fails_if_target_not_exists(self, writer, mock_connection):
        """Should raise error if target table doesn't exist."""
        mock_connection.execute_sql.return_value = []

        mock_df = MagicMock()
        mock_df.columns = ["DateId", "value"]

        with pytest.raises(ValueError, match="does not exist"):
            writer.merge(
                df=mock_df,
                spark_engine=MagicMock(),
                target_table="oee.oee_fact",
                merge_keys=["DateId"],
            )


class TestWriteConfigMergeValidation:
    """Tests for WriteConfig merge validation."""

    def test_merge_mode_requires_merge_keys(self):
        """Should raise error when merge mode without merge_keys."""
        with pytest.raises(ValueError, match="merge_keys.*required"):
            WriteConfig(
                connection="azure_sql",
                format="sql_server",
                table="oee.oee_fact",
                mode=WriteMode.MERGE,
            )

    def test_merge_mode_with_merge_keys_valid(self):
        """Should accept merge mode with merge_keys."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
        )
        assert config.mode == WriteMode.MERGE
        assert config.merge_keys == ["DateId", "P_ID"]

    def test_merge_mode_with_options(self):
        """Should accept merge mode with full options."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
            merge_options=SqlServerMergeOptions(
                update_condition="source._hash_diff != target._hash_diff",
                exclude_columns=["_hash_diff"],
                audit_cols=SqlServerAuditColsConfig(
                    created_col="created_ts", updated_col="updated_ts"
                ),
            ),
        )
        assert config.merge_options.update_condition == "source._hash_diff != target._hash_diff"
        assert config.merge_options.exclude_columns == ["_hash_diff"]
        assert config.merge_options.audit_cols.created_col == "created_ts"


class TestSqlServerMergeOptions:
    """Tests for SqlServerMergeOptions model."""

    def test_default_values(self):
        """Should have sensible defaults."""
        options = SqlServerMergeOptions()
        assert options.staging_schema == "staging"
        assert options.exclude_columns == []
        assert options.update_condition is None
        assert options.delete_condition is None
        assert options.insert_condition is None
        assert options.audit_cols is None

    def test_custom_staging_schema(self):
        """Should accept custom staging schema."""
        options = SqlServerMergeOptions(staging_schema="temp")
        assert options.staging_schema == "temp"

    def test_audit_cols_config(self):
        """Should accept audit cols configuration."""
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_at", updated_col="modified_at")
        )
        assert options.audit_cols.created_col == "created_at"
        assert options.audit_cols.updated_col == "modified_at"


# =============================================================================
# Phase 2 Tests: Overwrite Strategies and Validations
# =============================================================================


class TestOverwriteResult:
    """Tests for OverwriteResult dataclass."""

    def test_overwrite_result_defaults(self):
        """Should have sensible defaults."""
        result = OverwriteResult()
        assert result.rows_written == 0
        assert result.strategy == "truncate_insert"

    def test_overwrite_result_custom(self):
        """Should accept custom values."""
        result = OverwriteResult(rows_written=1000, strategy="drop_create")
        assert result.rows_written == 1000
        assert result.strategy == "drop_create"


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_validation_result_defaults(self):
        """Should default to valid."""
        result = ValidationResult()
        assert result.is_valid is True
        assert result.null_key_count == 0
        assert result.duplicate_key_count == 0
        assert result.errors == []

    def test_validation_result_with_errors(self):
        """Should track validation errors."""
        result = ValidationResult(
            is_valid=False,
            null_key_count=5,
            duplicate_key_count=3,
            errors=["Found nulls", "Found duplicates"],
        )
        assert result.is_valid is False
        assert result.null_key_count == 5
        assert result.duplicate_key_count == 3
        assert len(result.errors) == 2


class TestSqlServerMergeValidationConfig:
    """Tests for SqlServerMergeValidationConfig model."""

    def test_default_values(self):
        """Should have sensible defaults."""
        config = SqlServerMergeValidationConfig()
        assert config.check_null_keys is True
        assert config.check_duplicate_keys is True
        assert config.fail_on_validation_error is True

    def test_disable_checks(self):
        """Should allow disabling checks."""
        config = SqlServerMergeValidationConfig(
            check_null_keys=False,
            check_duplicate_keys=False,
            fail_on_validation_error=False,
        )
        assert config.check_null_keys is False
        assert config.check_duplicate_keys is False
        assert config.fail_on_validation_error is False


class TestSqlServerOverwriteOptions:
    """Tests for SqlServerOverwriteOptions model."""

    def test_default_strategy(self):
        """Should default to truncate_insert."""
        options = SqlServerOverwriteOptions()
        assert options.strategy == SqlServerOverwriteStrategy.TRUNCATE_INSERT

    def test_drop_create_strategy(self):
        """Should accept drop_create strategy."""
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        assert options.strategy == SqlServerOverwriteStrategy.DROP_CREATE

    def test_delete_insert_strategy(self):
        """Should accept delete_insert strategy."""
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        assert options.strategy == SqlServerOverwriteStrategy.DELETE_INSERT

    def test_with_audit_cols(self):
        """Should accept audit columns."""
        options = SqlServerOverwriteOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        assert options.audit_cols.created_col == "created_ts"


class TestValidationsPandas:
    """Tests for Pandas DataFrame validation."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_validate_null_keys_pandas(self, writer):
        """Should detect null values in merge keys."""
        df = pd.DataFrame(
            {
                "key1": [1, 2, None, 4],
                "key2": ["a", "b", "c", None],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_pandas(df, ["key1", "key2"])

        assert result.is_valid is False
        assert result.null_key_count == 2
        assert len(result.errors) > 0

    def test_validate_duplicate_keys_pandas(self, writer):
        """Should detect duplicate key combinations."""
        df = pd.DataFrame(
            {
                "key1": [1, 2, 1, 3],
                "key2": ["a", "b", "a", "c"],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_pandas(df, ["key1", "key2"])

        assert result.is_valid is False
        assert result.duplicate_key_count > 0

    def test_validate_valid_data_pandas(self, writer):
        """Should pass for valid data."""
        df = pd.DataFrame(
            {
                "key1": [1, 2, 3, 4],
                "key2": ["a", "b", "c", "d"],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_pandas(df, ["key1", "key2"])

        assert result.is_valid is True
        assert result.null_key_count == 0
        assert result.duplicate_key_count == 0

    def test_validation_disabled_pandas(self, writer):
        """Should skip checks when disabled."""
        df = pd.DataFrame(
            {
                "key1": [1, None, 1],
                "value": [10, 20, 30],
            }
        )

        config = SqlServerMergeValidationConfig(
            check_null_keys=False,
            check_duplicate_keys=False,
        )
        result = writer.validate_keys_pandas(df, ["key1"], config)

        assert result.is_valid is True


class TestOverwriteStrategies:
    """Tests for overwrite strategy SQL generation."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_truncate_table(self, writer, mock_connection):
        """Should call TRUNCATE TABLE."""
        writer.truncate_table("oee.oee_fact")
        mock_connection.execute_sql.assert_called_once()
        call_sql = mock_connection.execute_sql.call_args[0][0]
        assert "TRUNCATE TABLE [oee].[oee_fact]" in call_sql

    def test_delete_from_table(self, writer, mock_connection):
        """Should call DELETE FROM and return count."""
        mock_connection.execute_sql.return_value = [{"deleted_count": 100}]
        count = writer.delete_from_table("oee.oee_fact")
        assert count == 100
        call_sql = mock_connection.execute_sql.call_args[0][0]
        assert "DELETE FROM [oee].[oee_fact]" in call_sql

    def test_drop_table(self, writer, mock_connection):
        """Should call DROP TABLE IF EXISTS."""
        writer.drop_table("oee.oee_fact")
        mock_connection.execute_sql.assert_called_once()
        call_sql = mock_connection.execute_sql.call_args[0][0]
        assert "DROP TABLE IF EXISTS [oee].[oee_fact]" in call_sql

    def test_get_escaped_table_name(self, writer):
        """Should escape table names properly."""
        assert writer.get_escaped_table_name("oee.oee_fact") == "[oee].[oee_fact]"
        assert writer.get_escaped_table_name("customers") == "[dbo].[customers]"


class TestWriteConfigOverwriteOptions:
    """Tests for WriteConfig with overwrite_options."""

    def test_overwrite_with_options(self):
        """Should accept overwrite mode with options."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="fact.combined_downtime",
            mode=WriteMode.OVERWRITE,
            overwrite_options=SqlServerOverwriteOptions(
                strategy=SqlServerOverwriteStrategy.DROP_CREATE,
                audit_cols=SqlServerAuditColsConfig(
                    created_col="created_ts", updated_col="updated_ts"
                ),
            ),
        )
        assert config.overwrite_options.strategy == SqlServerOverwriteStrategy.DROP_CREATE
        assert config.overwrite_options.audit_cols.created_col == "created_ts"


class TestMergeWithValidations:
    """Tests for merge with validation config."""

    def test_merge_options_with_validations(self):
        """Should accept validations in merge_options."""
        options = SqlServerMergeOptions(
            validations=SqlServerMergeValidationConfig(
                check_null_keys=True,
                check_duplicate_keys=True,
                fail_on_validation_error=False,
            )
        )
        assert options.validations.check_null_keys is True
        assert options.validations.fail_on_validation_error is False

    def test_write_config_with_validations(self):
        """Should accept full config with validations."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
            merge_options=SqlServerMergeOptions(
                update_condition="source._hash_diff != target._hash_diff",
                validations=SqlServerMergeValidationConfig(
                    check_null_keys=True,
                    check_duplicate_keys=True,
                ),
            ),
        )
        assert config.merge_options.validations.check_null_keys is True


# =============================================================================
# Phase 4 Tests: Advanced Features
# =============================================================================


class TestSchemaEvolutionConfig:
    """Tests for SqlServerSchemaEvolutionConfig model (Phase 4)."""

    def test_default_values(self):
        """Should default to strict mode with no auto DDL."""
        config = SqlServerSchemaEvolutionConfig()
        assert config.mode == SqlServerSchemaEvolutionMode.STRICT
        assert config.add_columns is False

    def test_evolve_mode(self):
        """Should accept evolve mode with add_columns."""
        config = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE,
            add_columns=True,
        )
        assert config.mode == SqlServerSchemaEvolutionMode.EVOLVE
        assert config.add_columns is True

    def test_ignore_mode(self):
        """Should accept ignore mode."""
        config = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.IGNORE,
        )
        assert config.mode == SqlServerSchemaEvolutionMode.IGNORE


class TestMergeOptionsPhase4:
    """Tests for Phase 4 options in SqlServerMergeOptions."""

    def test_auto_create_schema_default(self):
        """Should default auto_create_schema to False."""
        options = SqlServerMergeOptions()
        assert options.auto_create_schema is False

    def test_auto_create_table_default(self):
        """Should default auto_create_table to False."""
        options = SqlServerMergeOptions()
        assert options.auto_create_table is False

    def test_batch_size_default(self):
        """Should default batch_size to None."""
        options = SqlServerMergeOptions()
        assert options.batch_size is None

    def test_full_phase4_config(self):
        """Should accept all Phase 4 options together."""
        options = SqlServerMergeOptions(
            auto_create_schema=True,
            auto_create_table=True,
            batch_size=10000,
            schema_evolution=SqlServerSchemaEvolutionConfig(
                mode=SqlServerSchemaEvolutionMode.EVOLVE,
                add_columns=True,
            ),
        )
        assert options.auto_create_schema is True
        assert options.auto_create_table is True
        assert options.batch_size == 10000
        assert options.schema_evolution.mode == SqlServerSchemaEvolutionMode.EVOLVE


class TestOverwriteOptionsPhase4:
    """Tests for Phase 4 options in SqlServerOverwriteOptions."""

    def test_auto_create_schema_default(self):
        """Should default auto_create_schema to False."""
        options = SqlServerOverwriteOptions()
        assert options.auto_create_schema is False

    def test_auto_create_table_default(self):
        """Should default auto_create_table to False."""
        options = SqlServerOverwriteOptions()
        assert options.auto_create_table is False

    def test_batch_size(self):
        """Should accept batch_size option."""
        options = SqlServerOverwriteOptions(batch_size=5000)
        assert options.batch_size == 5000


class TestTypeMaps:
    """Tests for type mapping dictionaries."""

    def test_pandas_type_map_has_common_types(self):
        """Should map common Pandas types to SQL Server types."""
        assert "int64" in PANDAS_TO_SQL_TYPE_MAP
        assert "float64" in PANDAS_TO_SQL_TYPE_MAP
        assert "object" in PANDAS_TO_SQL_TYPE_MAP
        assert PANDAS_TO_SQL_TYPE_MAP["int64"] == "BIGINT"
        assert PANDAS_TO_SQL_TYPE_MAP["float64"] == "FLOAT"

    def test_polars_type_map_has_common_types(self):
        """Should map common Polars types to SQL Server types."""
        assert "Int64" in POLARS_TO_SQL_TYPE_MAP
        assert "Float64" in POLARS_TO_SQL_TYPE_MAP
        assert "Utf8" in POLARS_TO_SQL_TYPE_MAP
        assert POLARS_TO_SQL_TYPE_MAP["Int64"] == "BIGINT"
        assert POLARS_TO_SQL_TYPE_MAP["Float64"] == "FLOAT"


class TestSchemaCreation:
    """Tests for auto schema creation (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_check_schema_exists_true(self, writer, mock_connection):
        """Should return True when schema exists."""
        mock_connection.execute_sql.return_value = [(1,)]
        assert writer.check_schema_exists("staging") is True

    def test_check_schema_exists_false(self, writer, mock_connection):
        """Should return False when schema doesn't exist."""
        mock_connection.execute_sql.return_value = []
        assert writer.check_schema_exists("staging") is False

    def test_create_schema(self, writer, mock_connection):
        """Should call CREATE SCHEMA when schema doesn't exist."""
        mock_connection.execute_sql.return_value = []
        writer.create_schema("staging")
        calls = mock_connection.execute_sql.call_args_list
        assert len(calls) == 2
        assert "CREATE SCHEMA [staging]" in calls[1][0][0]


class TestTableCreation:
    """Tests for auto table creation (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_infer_sql_type_pandas_int(self, writer):
        """Should infer INT for int32."""
        assert writer.infer_sql_type_pandas("int32") == "INT"

    def test_infer_sql_type_pandas_float(self, writer):
        """Should infer FLOAT for float64."""
        assert writer.infer_sql_type_pandas("float64") == "FLOAT"

    def test_infer_sql_type_pandas_object(self, writer):
        """Should infer NVARCHAR for object."""
        assert writer.infer_sql_type_pandas("object") == "NVARCHAR(MAX)"

    def test_infer_sql_type_pandas_unknown(self, writer):
        """Should default to NVARCHAR for unknown types."""
        assert writer.infer_sql_type_pandas("customtype") == "NVARCHAR(MAX)"

    def test_infer_sql_type_polars_int(self, writer):
        """Should infer BIGINT for Int64."""
        assert writer.infer_sql_type_polars("Int64") == "BIGINT"

    def test_infer_sql_type_polars_float(self, writer):
        """Should infer FLOAT for Float64."""
        assert writer.infer_sql_type_polars("Float64") == "FLOAT"

    def test_infer_sql_type_polars_string(self, writer):
        """Should infer NVARCHAR for Utf8."""
        assert writer.infer_sql_type_polars("Utf8") == "NVARCHAR(MAX)"

    def test_create_table_from_pandas(self, writer, mock_connection):
        """Should generate CREATE TABLE from Pandas DataFrame."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [1.0, 2.0, 3.0],
            }
        )
        writer.create_table_from_pandas(df, "test.my_table")
        call_sql = mock_connection.execute_sql.call_args[0][0]
        assert "CREATE TABLE [test].[my_table]" in call_sql
        assert "[id]" in call_sql
        assert "[name]" in call_sql
        assert "[value]" in call_sql

    def test_add_columns(self, writer, mock_connection):
        """Should generate ALTER TABLE ADD COLUMN."""
        writer.add_columns("test.my_table", {"new_col": "NVARCHAR(MAX)"})
        call_sql = mock_connection.execute_sql.call_args[0][0]
        assert "ALTER TABLE [test].[my_table] ADD [new_col] NVARCHAR(MAX) NULL" in call_sql


class TestSchemaEvolutionPandas:
    """Tests for schema evolution with Pandas (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_strict_mode_no_new_cols(self, writer, mock_connection):
        """Should pass with strict mode when no new columns."""
        mock_connection.execute_sql.return_value = [
            {"COLUMN_NAME": "id", "DATA_TYPE": "int"},
            {"COLUMN_NAME": "name", "DATA_TYPE": "nvarchar"},
        ]
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        config = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.STRICT)
        result = writer.handle_schema_evolution_pandas(df, "test.table", config)
        assert set(result) == {"id", "name"}

    def test_strict_mode_fails_on_new_cols(self, writer, mock_connection):
        """Should fail with strict mode when there are new columns."""
        mock_connection.execute_sql.return_value = [
            {"COLUMN_NAME": "id", "DATA_TYPE": "int"},
        ]
        df = pd.DataFrame({"id": [1], "new_col": ["a"]})
        config = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.STRICT)
        with pytest.raises(ValueError, match="new columns"):
            writer.handle_schema_evolution_pandas(df, "test.table", config)

    def test_ignore_mode_filters_columns(self, writer, mock_connection):
        """Should filter to only matching columns with ignore mode."""
        mock_connection.execute_sql.return_value = [
            {"COLUMN_NAME": "id", "DATA_TYPE": "int"},
        ]
        df = pd.DataFrame({"id": [1], "new_col": ["a"], "other": [1.0]})
        config = SqlServerSchemaEvolutionConfig(mode=SqlServerSchemaEvolutionMode.IGNORE)
        result = writer.handle_schema_evolution_pandas(df, "test.table", config)
        assert result == ["id"]


class TestValidationsPolars:
    """Tests for Polars DataFrame validation (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_validate_null_keys_polars(self, writer):
        """Should detect null values in merge keys with Polars."""
        pytest.importorskip("polars")
        import polars as pl

        df = pl.DataFrame(
            {
                "key1": [1, 2, None, 4],
                "key2": ["a", "b", "c", None],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_polars(df, ["key1", "key2"])

        assert result.is_valid is False
        assert result.null_key_count == 2

    def test_validate_duplicate_keys_polars(self, writer):
        """Should detect duplicate key combinations with Polars."""
        pytest.importorskip("polars")
        import polars as pl

        df = pl.DataFrame(
            {
                "key1": [1, 2, 1, 3],
                "key2": ["a", "b", "a", "c"],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_polars(df, ["key1", "key2"])

        assert result.is_valid is False
        assert result.duplicate_key_count == 1

    def test_validate_valid_data_polars(self, writer):
        """Should pass for valid data with Polars."""
        pytest.importorskip("polars")
        import polars as pl

        df = pl.DataFrame(
            {
                "key1": [1, 2, 3, 4],
                "key2": ["a", "b", "c", "d"],
                "value": [10, 20, 30, 40],
            }
        )

        result = writer.validate_keys_polars(df, ["key1", "key2"])

        assert result.is_valid is True
        assert result.null_key_count == 0
        assert result.duplicate_key_count == 0

    def test_validate_lazy_frame(self, writer):
        """Should handle LazyFrame by collecting."""
        pytest.importorskip("polars")
        import polars as pl

        df = pl.DataFrame(
            {
                "key1": [1, 2, 3],
                "value": [10, 20, 30],
            }
        ).lazy()

        result = writer.validate_keys_polars(df, ["key1"])

        assert result.is_valid is True


class TestMergePolars:
    """Tests for Polars merge operation (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        conn.write_table = MagicMock()
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_merge_polars_requires_table_exists_by_default(self, writer, mock_connection):
        """Should fail if target table doesn't exist without auto_create_table."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.return_value = []
        df = pl.DataFrame({"key": [1, 2], "value": [10, 20]})

        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_polars(df, "test.my_table", ["key"])

    def test_merge_polars_auto_create_table(self, writer, mock_connection):
        """Should create table if auto_create_table is True."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.side_effect = [
            [],
            [],
            [{"inserted": 2, "updated": 0, "deleted": 0}],
        ]
        df = pl.DataFrame({"key": [1, 2], "value": [10, 20]})

        options = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_polars(df, "test.my_table", ["key"], options)

        assert result.inserted == 2


class TestOverwritePolars:
    """Tests for Polars overwrite operation (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        conn.write_table = MagicMock()
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_overwrite_polars_drop_create(self, writer, mock_connection):
        """Should drop and create table with drop_create strategy."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.return_value = [(1,)]
        df = pl.DataFrame({"key": [1, 2], "value": [10, 20]})

        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_polars(df, "test.my_table", options)

        assert result.rows_written == 2
        assert result.strategy == "drop_create"

    def test_overwrite_polars_truncate_insert(self, writer, mock_connection):
        """Should truncate and insert with truncate_insert strategy."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.return_value = [(1,)]
        df = pl.DataFrame({"key": [1, 2, 3], "value": [10, 20, 30]})

        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_polars(df, "test.my_table", options)

        assert result.rows_written == 3
        assert result.strategy == "truncate_insert"

    def test_overwrite_polars_with_batch_size(self, writer, mock_connection):
        """Should chunk writes when batch_size is specified."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.return_value = []
        df = pl.DataFrame({"key": list(range(100)), "value": list(range(100))})

        options = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            batch_size=30,
        )
        result = writer.overwrite_polars(df, "test.my_table", options)

        assert mock_connection.write_table.call_count == 4
        assert result.rows_written == 100


class TestBatchProcessing:
    """Tests for batch processing (Phase 4)."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        conn.write_table = MagicMock()
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_merge_polars_with_batch_size(self, writer, mock_connection):
        """Should chunk staging writes when batch_size is specified."""
        pytest.importorskip("polars")
        import polars as pl

        mock_connection.execute_sql.side_effect = [
            [(1,)],
            [{"inserted": 50, "updated": 0, "deleted": 0}],
        ]
        df = pl.DataFrame({"key": list(range(50)), "value": list(range(50))})

        options = SqlServerMergeOptions(batch_size=20)
        writer.merge_polars(df, "test.my_table", ["key"], options)

        assert mock_connection.write_table.call_count == 3


class TestWriteConfigPhase4:
    """Tests for WriteConfig with Phase 4 options."""

    def test_merge_options_with_phase4_fields(self):
        """Should accept Phase 4 fields in merge_options."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
            merge_options=SqlServerMergeOptions(
                auto_create_schema=True,
                auto_create_table=True,
                batch_size=5000,
                schema_evolution=SqlServerSchemaEvolutionConfig(
                    mode=SqlServerSchemaEvolutionMode.EVOLVE,
                    add_columns=True,
                ),
            ),
        )
        assert config.merge_options.auto_create_schema is True
        assert config.merge_options.auto_create_table is True
        assert config.merge_options.batch_size == 5000
        assert config.merge_options.schema_evolution.add_columns is True

    def test_overwrite_options_with_phase4_fields(self):
        """Should accept Phase 4 fields in overwrite_options."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="fact.combined_downtime",
            mode=WriteMode.OVERWRITE,
            overwrite_options=SqlServerOverwriteOptions(
                strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT,
                auto_create_schema=True,
                auto_create_table=True,
                batch_size=10000,
            ),
        )
        assert config.overwrite_options.auto_create_schema is True
        assert config.overwrite_options.auto_create_table is True
        assert config.overwrite_options.batch_size == 10000


class TestPrimaryKeyAndIndexCreation:
    """Tests for primary key and index creation on merge keys."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection object."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=None)
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer instance with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_merge_options_primary_key_on_merge_keys_default(self):
        """Should default to False for primary_key_on_merge_keys."""
        options = SqlServerMergeOptions()
        assert options.primary_key_on_merge_keys is False

    def test_merge_options_index_on_merge_keys_default(self):
        """Should default to False for index_on_merge_keys."""
        options = SqlServerMergeOptions()
        assert options.index_on_merge_keys is False

    def test_merge_options_with_primary_key_enabled(self):
        """Should accept primary_key_on_merge_keys=True."""
        options = SqlServerMergeOptions(
            primary_key_on_merge_keys=True,
            auto_create_table=True,
        )
        assert options.primary_key_on_merge_keys is True

    def test_merge_options_with_index_enabled(self):
        """Should accept index_on_merge_keys=True."""
        options = SqlServerMergeOptions(
            index_on_merge_keys=True,
        )
        assert options.index_on_merge_keys is True

    def test_create_primary_key_sql(self, writer, mock_connection):
        """Should generate correct CREATE PRIMARY KEY SQL."""
        # Mock get_table_columns to return column types
        mock_connection.execute_sql.return_value = [
            ("DateId", "int"),
            ("P_ID", "int"),
        ]
        writer.create_primary_key("oee.oee_fact", ["DateId", "P_ID"])

        # Should be called 3 times: get_table_columns, ALTER DateId, ALTER P_ID, CREATE PK
        assert mock_connection.execute_sql.call_count == 4

        # Last call should be the CREATE PRIMARY KEY
        sql = mock_connection.execute_sql.call_args_list[-1][0][0]
        assert "ALTER TABLE [oee].[oee_fact]" in sql
        assert "PRIMARY KEY CLUSTERED" in sql
        assert "[DateId]" in sql
        assert "[P_ID]" in sql
        assert "PK_oee_fact" in sql

        # Check that ALTER COLUMN NOT NULL was called
        alter_calls = [
            call[0][0]
            for call in mock_connection.execute_sql.call_args_list
            if "NOT NULL" in call[0][0]
        ]
        assert len(alter_calls) == 2  # One for each column

    def test_create_index_sql(self, writer, mock_connection):
        """Should generate correct CREATE INDEX SQL."""
        writer.create_index("oee.oee_fact", ["DateId", "P_ID"])

        mock_connection.execute_sql.assert_called_once()
        sql = mock_connection.execute_sql.call_args[0][0]
        assert "CREATE NONCLUSTERED INDEX" in sql
        assert "[oee].[oee_fact]" in sql
        assert "[DateId]" in sql
        assert "[P_ID]" in sql
        assert "IX_oee_fact_DateId_P_ID" in sql

    def test_create_index_custom_name(self, writer, mock_connection):
        """Should use custom index name if provided."""
        writer.create_index("oee.oee_fact", ["DateId", "P_ID"], index_name="MyCustomIndex")

        sql = mock_connection.execute_sql.call_args[0][0]
        assert "[MyCustomIndex]" in sql

    def test_write_config_with_primary_key_option(self):
        """Should accept primary_key_on_merge_keys in WriteConfig."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
            merge_options=SqlServerMergeOptions(
                auto_create_table=True,
                primary_key_on_merge_keys=True,
            ),
        )
        assert config.merge_options.primary_key_on_merge_keys is True


class TestAuditColumnCreation:
    """Tests for audit column auto-creation during merge/table creation."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_create_table_from_pandas_with_audit_cols(self, writer, mock_connection):
        """Should add audit columns to CREATE TABLE when audit_cols config provided."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        audit_cols = SqlServerAuditColsConfig(
            created_col="created_ts",
            updated_col="updated_ts",
        )

        writer.create_table_from_pandas(df, "test.my_table", audit_cols=audit_cols)

        mock_connection.execute_sql.assert_called_once()
        sql = mock_connection.execute_sql.call_args[0][0]
        assert "[created_ts] DATETIME2 NULL" in sql
        assert "[updated_ts] DATETIME2 NULL" in sql
        assert "[id]" in sql
        assert "[name]" in sql

    def test_create_table_from_pandas_without_audit_cols(self, writer, mock_connection):
        """Should NOT add audit columns when audit_cols is None."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        writer.create_table_from_pandas(df, "test.my_table", audit_cols=None)

        mock_connection.execute_sql.assert_called_once()
        sql = mock_connection.execute_sql.call_args[0][0]
        assert "created_ts" not in sql
        assert "updated_ts" not in sql

    def test_create_table_from_pandas_audit_cols_not_duplicated(self, writer, mock_connection):
        """Should NOT duplicate audit columns if already in DataFrame."""
        df = pd.DataFrame({"id": [1, 2], "created_ts": [None, None], "updated_ts": [None, None]})
        audit_cols = SqlServerAuditColsConfig(
            created_col="created_ts",
            updated_col="updated_ts",
        )

        writer.create_table_from_pandas(df, "test.my_table", audit_cols=audit_cols)

        mock_connection.execute_sql.assert_called_once()
        sql = mock_connection.execute_sql.call_args[0][0]
        # Count occurrences - should only appear once each
        assert sql.count("[created_ts]") == 1
        assert sql.count("[updated_ts]") == 1

    def test_merge_pandas_adds_audit_cols_to_columns_list(self, writer, mock_connection):
        """Audit columns should be included in MERGE SQL even if not in DataFrame."""
        mock_connection.execute_sql.side_effect = [
            [(1,)],  # check_table_exists - table exists
            [{"inserted": 2, "updated": 0, "deleted": 0}],  # execute_merge
        ]
        mock_connection.write_table = MagicMock()

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(
                created_col="created_ts",
                updated_col="updated_ts",
            ),
        )

        writer.merge_pandas(df, "test.my_table", merge_keys=["id"], options=options)

        # Find the MERGE SQL call
        merge_sql = None
        for call in mock_connection.execute_sql.call_args_list:
            sql = call[0][0]
            if "MERGE" in sql:
                merge_sql = sql
                break

        assert merge_sql is not None, "MERGE SQL should have been executed"
        assert "[created_ts]" in merge_sql
        assert "[updated_ts]" in merge_sql
        assert "GETUTCDATE()" in merge_sql

    def test_merge_pandas_auto_create_table_with_audit_cols(self, writer, mock_connection):
        """Auto-created table should include audit columns."""
        mock_connection.execute_sql.side_effect = [
            [],  # create_schema
            [],  # check_table_exists - table does not exist
            [],  # create_table
            [],  # write_table
            [{"inserted": 2, "updated": 0, "deleted": 0}],  # execute_merge
        ]
        mock_connection.write_table = MagicMock()

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        options = SqlServerMergeOptions(
            auto_create_schema=True,
            auto_create_table=True,
            audit_cols=SqlServerAuditColsConfig(
                created_col="created_ts",
                updated_col="updated_ts",
            ),
        )

        writer.merge_pandas(df, "test.my_table", merge_keys=["id"], options=options)

        # Find the CREATE TABLE call
        create_sql = None
        for call in mock_connection.execute_sql.call_args_list:
            sql = call[0][0]
            if "CREATE TABLE" in sql:
                create_sql = sql
                break

        assert create_sql is not None, "CREATE TABLE should have been called"
        assert "[created_ts] DATETIME2 NULL" in create_sql
        assert "[updated_ts] DATETIME2 NULL" in create_sql


class TestIncrementalMerge:
    """Tests for incremental merge optimization."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock SQL Server connection."""
        conn = MagicMock()
        conn.execute_sql = MagicMock(return_value=[])
        conn.write_table = MagicMock()
        return conn

    @pytest.fixture
    def writer(self, mock_connection):
        """Create a writer with mock connection."""
        return SqlServerMergeWriter(mock_connection)

    def test_incremental_option_defaults(self):
        """Incremental should be False by default."""
        options = SqlServerMergeOptions()
        assert options.incremental is False
        assert options.hash_column is None
        assert options.change_detection_columns is None

    def test_incremental_with_hash_column(self):
        """Should accept hash_column configuration."""
        options = SqlServerMergeOptions(
            incremental=True,
            hash_column="_hash_diff",
        )
        assert options.incremental is True
        assert options.hash_column == "_hash_diff"

    def test_incremental_with_change_detection_columns(self):
        """Should accept change_detection_columns configuration."""
        options = SqlServerMergeOptions(
            incremental=True,
            change_detection_columns=["col1", "col2", "col3"],
        )
        assert options.incremental is True
        assert options.change_detection_columns == ["col1", "col2", "col3"]

    def test_get_hash_column_name_explicit(self, writer):
        """Should return explicitly configured hash column."""
        result = writer.get_hash_column_name(
            df_columns=["id", "name", "_hash_diff"],
            options_hash_column="_hash_diff",
        )
        assert result == "_hash_diff"

    def test_get_hash_column_name_auto_detect(self, writer):
        """Should auto-detect common hash column names."""
        result = writer.get_hash_column_name(
            df_columns=["id", "name", "_hash_diff"],
            options_hash_column=None,
        )
        assert result == "_hash_diff"

    def test_get_hash_column_name_not_found(self, writer):
        """Should return None when no hash column found."""
        result = writer.get_hash_column_name(
            df_columns=["id", "name", "value"],
            options_hash_column=None,
        )
        assert result is None

    def test_compute_hash_pandas(self, writer):
        """Should compute hash column for Pandas DataFrame."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [10, 20]})
        result = writer.compute_hash_pandas(df, ["name", "value"], "_computed_hash")

        assert "_computed_hash" in result.columns
        assert len(result) == 2
        # Hashes should be different for different rows
        assert result.iloc[0]["_computed_hash"] != result.iloc[1]["_computed_hash"]

    def test_filter_changed_rows_pandas_all_new(self, writer):
        """Should return all rows when target is empty."""
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "_hash": ["h1", "h2"]})
        result = writer.filter_changed_rows_pandas(
            source_df=df,
            target_hashes=[],
            merge_keys=["id"],
            hash_column="_hash",
        )
        assert len(result) == 2

    def test_filter_changed_rows_pandas_some_changed(self, writer):
        """Should filter to only new/changed rows."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "_hash": ["h1_new", "h2", "h3"],  # id=1 changed, id=3 is new
            }
        )
        target_hashes = [
            {"id": 1, "_hash": "h1_old"},  # Changed
            {"id": 2, "_hash": "h2"},  # Unchanged
        ]
        result = writer.filter_changed_rows_pandas(
            source_df=df,
            target_hashes=target_hashes,
            merge_keys=["id"],
            hash_column="_hash",
        )
        assert len(result) == 2  # id=1 (changed) and id=3 (new)
        assert set(result["id"].tolist()) == {1, 3}

    def test_filter_changed_rows_pandas_none_changed(self, writer):
        """Should return empty when no rows changed."""
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "_hash": ["h1", "h2"],
            }
        )
        target_hashes = [
            {"id": 1, "_hash": "h1"},
            {"id": 2, "_hash": "h2"},
        ]
        result = writer.filter_changed_rows_pandas(
            source_df=df,
            target_hashes=target_hashes,
            merge_keys=["id"],
            hash_column="_hash",
        )
        assert len(result) == 0

    def test_read_target_hashes(self, writer, mock_connection):
        """Should generate correct SQL for reading target hashes."""
        mock_connection.execute_sql.return_value = [
            {"id": 1, "_hash": "h1"},
            {"id": 2, "_hash": "h2"},
        ]

        result = writer.read_target_hashes(
            target_table="test.my_table",
            merge_keys=["id"],
            hash_column="_hash",
        )

        mock_connection.execute_sql.assert_called_once()
        sql = mock_connection.execute_sql.call_args[0][0]
        assert "[id]" in sql
        assert "[_hash]" in sql
        assert "[test].[my_table]" in sql
        assert len(result) == 2

    def test_write_config_with_incremental(self):
        """Should accept incremental options in WriteConfig."""
        config = WriteConfig(
            connection="azure_sql",
            format="sql_server",
            table="oee.oee_fact",
            mode=WriteMode.MERGE,
            merge_keys=["DateId", "P_ID"],
            merge_options=SqlServerMergeOptions(
                incremental=True,
                hash_column="_hash_diff",
            ),
        )
        assert config.merge_options.incremental is True
        assert config.merge_options.hash_column == "_hash_diff"
