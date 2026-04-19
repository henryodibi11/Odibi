"""Tests for odibi.writers.sql_server_writer — Pandas, Polars, and helper paths."""

import hashlib
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pytest

from odibi.writers.sql_server_writer import (
    MergeResult,
    OverwriteResult,
    SqlServerMergeWriter,
    ValidationResult,
)
from odibi.config import (
    SqlServerAuditColsConfig,
    SqlServerMergeOptions,
    SqlServerMergeValidationConfig,
    SqlServerOverwriteOptions,
    SqlServerOverwriteStrategy,
    SqlServerSchemaEvolutionConfig,
    SqlServerSchemaEvolutionMode,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.execute_sql = MagicMock(return_value=[])
    conn.write_table = MagicMock()
    return conn


@pytest.fixture
def writer(mock_conn):
    with patch("odibi.writers.sql_server_writer.get_logging_context") as mock_ctx:
        mock_ctx.return_value = MagicMock()
        w = SqlServerMergeWriter(mock_conn)
    return w


# ---------------------------------------------------------------------------
# Dataclass results
# ---------------------------------------------------------------------------


class TestMergeResult:
    def test_defaults(self):
        r = MergeResult()
        assert r.inserted == 0 and r.updated == 0 and r.deleted == 0

    def test_total_affected(self):
        r = MergeResult(inserted=5, updated=3, deleted=1)
        assert r.total_affected == 9

    def test_custom_values(self):
        r = MergeResult(inserted=100, updated=50, deleted=10)
        assert r.inserted == 100


class TestOverwriteResult:
    def test_defaults(self):
        r = OverwriteResult()
        assert r.rows_written == 0
        assert r.strategy == "truncate_insert"

    def test_custom(self):
        r = OverwriteResult(rows_written=42, strategy="drop_create")
        assert r.rows_written == 42


class TestValidationResult:
    def test_defaults(self):
        r = ValidationResult()
        assert r.is_valid is True
        assert r.errors == []

    def test_post_init_none(self):
        r = ValidationResult(errors=None)
        assert r.errors == []

    def test_with_errors(self):
        r = ValidationResult(is_valid=False, null_key_count=2, errors=["bad"])
        assert not r.is_valid
        assert r.null_key_count == 2


# ---------------------------------------------------------------------------
# Helper methods
# ---------------------------------------------------------------------------


class TestInit:
    def test_sets_connection(self, writer, mock_conn):
        assert writer.connection is mock_conn


class TestGetStagingTableName:
    def test_with_schema(self, writer):
        result = writer.get_staging_table_name("sales.fact_orders", "staging")
        assert result == "[staging].[fact_orders_staging]"

    def test_without_schema(self, writer):
        result = writer.get_staging_table_name("fact_orders", "stg")
        assert result == "[stg].[fact_orders_staging]"

    def test_brackets_stripped(self, writer):
        result = writer.get_staging_table_name("[dbo].[orders]", "staging")
        assert result == "[staging].[orders_staging]"


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

    def test_strips_brackets(self, writer):
        assert writer.parse_table_name("[sales].[orders]") == ("sales", "orders")


class TestGetEscapedTableName:
    def test_basic(self, writer):
        assert writer.get_escaped_table_name("sales.orders") == "[sales].[orders]"

    def test_default_schema(self, writer):
        assert writer.get_escaped_table_name("orders") == "[dbo].[orders]"


class TestCheckTableExists:
    def test_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer.check_table_exists("dbo.orders") is True

    def test_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer.check_table_exists("dbo.orders") is False


class TestGetHashColumnName:
    def test_explicit(self, writer):
        assert writer.get_hash_column_name(["a", "myhash"], "myhash") == "myhash"

    def test_explicit_not_found(self, writer):
        assert writer.get_hash_column_name(["a", "b"], "myhash") is None

    def test_auto_detect(self, writer):
        assert writer.get_hash_column_name(["a", "_hash_diff"], None) == "_hash_diff"

    def test_auto_detect_hash(self, writer):
        assert writer.get_hash_column_name(["a", "_hash"], None) == "_hash"

    def test_none_found(self, writer):
        assert writer.get_hash_column_name(["a", "b"], None) is None


class TestReadTargetHashes:
    def test_hash_col_missing(self, writer, mock_conn):
        # get_table_columns returns cols without hash
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            }
        ]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == []

    def test_empty_result(self, writer, mock_conn):
        # First call: get_table_columns -> includes hash col
        # Second call: SELECT -> empty
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "_hash",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [],
        ]
        assert writer.read_target_hashes("dbo.t", ["id"], "_hash") == []

    def test_asdict_rows(self, writer, mock_conn):
        row = MagicMock()
        row._asdict.return_value = {"id": 1, "_hash": "abc"}
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "_hash",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [row],
        ]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == [{"id": 1, "_hash": "abc"}]

    def test_mapping_rows(self, writer, mock_conn):
        row = MagicMock(spec=[])
        del row._asdict
        row._mapping = {"id": 1, "_hash": "abc"}
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "_hash",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [row],
        ]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == [{"id": 1, "_hash": "abc"}]

    def test_tuple_rows(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
                {
                    "COLUMN_NAME": "_hash",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                },
            ],
            [(1, "abc")],
        ]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == [{"id": 1, "_hash": "abc"}]


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------


class TestComputeHashPandas:
    def test_adds_hash_column(self, writer):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = writer.compute_hash_pandas(df, ["a", "b"])
        assert "_computed_hash" in result.columns
        assert len(result) == 2
        # Verify deterministic
        expected = hashlib.md5("1||x".encode()).hexdigest()
        assert result["_computed_hash"].iloc[0] == expected

    def test_custom_name(self, writer):
        df = pd.DataFrame({"a": [1]})
        result = writer.compute_hash_pandas(df, ["a"], "my_hash")
        assert "my_hash" in result.columns


class TestComputeHashPolars:
    def test_adds_hash_column(self, writer):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = writer.compute_hash_polars(df, ["a", "b"])
        assert "_computed_hash" in result.columns
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Filter changed rows
# ---------------------------------------------------------------------------


class TestFilterChangedRowsPandas:
    def test_empty_target(self, writer):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"], "_hash": ["h1", "h2"]})
        result = writer.filter_changed_rows_pandas(df, [], ["id"], "_hash")
        assert len(result) == 2

    def test_new_and_changed(self, writer):
        source = pd.DataFrame({"id": [1, 2, 3], "_hash": ["h1", "h2_new", "h3"]})
        target = [{"id": 1, "_hash": "h1"}, {"id": 2, "_hash": "h2_old"}]
        result = writer.filter_changed_rows_pandas(source, target, ["id"], "_hash")
        assert set(result["id"].tolist()) == {2, 3}

    def test_no_changes(self, writer):
        source = pd.DataFrame({"id": [1], "_hash": ["h1"]})
        target = [{"id": 1, "_hash": "h1"}]
        result = writer.filter_changed_rows_pandas(source, target, ["id"], "_hash")
        assert len(result) == 0


class TestFilterChangedRowsPolars:
    def test_empty_target(self, writer):
        df = pl.DataFrame({"id": [1, 2], "_hash": ["h1", "h2"]})
        result = writer.filter_changed_rows_polars(df, [], ["id"], "_hash")
        assert len(result) == 2

    def test_new_and_changed(self, writer):
        source = pl.DataFrame({"id": [1, 2, 3], "_hash": ["h1", "h2_new", "h3"]})
        target = [{"id": 1, "_hash": "h1"}, {"id": 2, "_hash": "h2_old"}]
        result = writer.filter_changed_rows_polars(source, target, ["id"], "_hash")
        assert len(result) == 2  # id=2 (changed) + id=3 (new)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidateKeysPandas:
    def test_valid(self, writer):
        df = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        r = writer.validate_keys_pandas(df, ["id"])
        assert r.is_valid is True

    def test_null_keys(self, writer):
        df = pd.DataFrame({"id": [1, None, 3]})
        r = writer.validate_keys_pandas(df, ["id"])
        assert r.is_valid is False
        assert r.null_key_count == 1

    def test_duplicate_keys(self, writer):
        df = pd.DataFrame({"id": [1, 1, 2]})
        r = writer.validate_keys_pandas(df, ["id"])
        assert r.is_valid is False
        assert r.duplicate_key_count > 0

    def test_skip_checks(self, writer):
        cfg = SqlServerMergeValidationConfig(check_null_keys=False, check_duplicate_keys=False)
        df = pd.DataFrame({"id": [None, None]})
        r = writer.validate_keys_pandas(df, ["id"], cfg)
        assert r.is_valid is True


class TestValidateKeysPolars:
    def test_valid(self, writer):
        df = pl.DataFrame({"id": [1, 2, 3]})
        r = writer.validate_keys_polars(df, ["id"])
        assert r.is_valid is True

    def test_null_keys(self, writer):
        df = pl.DataFrame({"id": [1, None, 3]})
        r = writer.validate_keys_polars(df, ["id"])
        assert r.is_valid is False
        assert r.null_key_count == 1

    def test_duplicate_keys(self, writer):
        df = pl.DataFrame({"id": [1, 1, 2]})
        r = writer.validate_keys_polars(df, ["id"])
        assert r.is_valid is False

    def test_lazyframe(self, writer):
        df = pl.DataFrame({"id": [1, None]}).lazy()
        r = writer.validate_keys_polars(df, ["id"])
        assert not r.is_valid


# ---------------------------------------------------------------------------
# Schema / DDL methods
# ---------------------------------------------------------------------------


class TestCheckSchemaExists:
    def test_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer.check_schema_exists("dbo") is True

    def test_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer.check_schema_exists("dbo") is False


class TestCreateSchema:
    def test_creates_if_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [[], None]
        writer.create_schema("myschema")
        assert mock_conn.execute_sql.call_count == 2

    def test_skips_if_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        writer.create_schema("myschema")
        assert mock_conn.execute_sql.call_count == 1


class TestGetTableColumns:
    def test_dict_rows(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "name",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": 255,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cols = writer.get_table_columns("dbo.test")
        assert cols["name"] == "nvarchar(255)"

    def test_max_length(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "desc",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": -1,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cols = writer.get_table_columns("dbo.test")
        assert cols["desc"] == "nvarchar(MAX)"

    def test_decimal_type(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "amt",
                "DATA_TYPE": "decimal",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": 18,
                "NUMERIC_SCALE": 2,
            },
        ]
        cols = writer.get_table_columns("dbo.test")
        assert cols["amt"] == "decimal(18,2)"

    def test_tuple_rows(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("id", "int", None, None, None)]
        cols = writer.get_table_columns("dbo.test")
        assert cols["id"] == "int"

    def test_no_char_len(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "val",
                "DATA_TYPE": "varchar",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cols = writer.get_table_columns("dbo.test")
        assert cols["val"] == "varchar(MAX)"


class TestInferSqlTypes:
    def test_pandas_known(self, writer):
        assert writer.infer_sql_type_pandas("int64") == "BIGINT"
        assert writer.infer_sql_type_pandas("float64") == "FLOAT"
        assert writer.infer_sql_type_pandas("object") == "NVARCHAR(MAX)"
        assert writer.infer_sql_type_pandas("bool") == "BIT"

    def test_pandas_fallback(self, writer):
        assert writer.infer_sql_type_pandas("complex128") == "NVARCHAR(MAX)"

    def test_polars_known(self, writer):
        assert writer.infer_sql_type_polars("Int64") == "BIGINT"
        assert writer.infer_sql_type_polars("Float64") == "FLOAT"
        assert writer.infer_sql_type_polars("Utf8") == "NVARCHAR(MAX)"
        assert writer.infer_sql_type_polars("Boolean") == "BIT"

    def test_polars_fallback(self, writer):
        assert writer.infer_sql_type_polars("SomeCustomType") == "NVARCHAR(MAX)"


class TestCreateTableFromPandas:
    def test_basic(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        writer.create_table_from_pandas(df, "dbo.test")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE" in sql
        assert "[id]" in sql
        assert "[name]" in sql

    def test_with_audit_cols(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        writer.create_table_from_pandas(df, "dbo.test", audit_cols=audit)
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "[created_ts]" in sql
        assert "[updated_ts]" in sql

    def test_audit_cols_already_exist(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "created_ts": [None]})
        audit = SqlServerAuditColsConfig(created_col="created_ts")
        writer.create_table_from_pandas(df, "dbo.test", audit_cols=audit)
        sql = mock_conn.execute_sql.call_args[0][0]
        # created_ts should appear only once
        assert sql.count("[created_ts]") == 1


class TestCreateTableFromPolars:
    def test_basic(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "name": ["a"]})
        writer.create_table_from_polars(df, "dbo.test")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE" in sql

    def test_lazyframe(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]}).lazy()
        writer.create_table_from_polars(df, "dbo.test")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "CREATE TABLE" in sql


class TestAddColumns:
    def test_empty(self, writer, mock_conn):
        writer.add_columns("dbo.t", {})
        mock_conn.execute_sql.assert_not_called()

    def test_adds(self, writer, mock_conn):
        writer.add_columns("dbo.t", {"new_col": "NVARCHAR(100)"})
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "ALTER TABLE" in sql
        assert "[new_col]" in sql


class TestFixMaxColumnsForIndexing:
    def test_nvarchar_max(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "name",
                    "DATA_TYPE": "nvarchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                }
            ],
            None,
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["name"])
        alter_sql = mock_conn.execute_sql.call_args_list[1][0][0]
        assert "NVARCHAR(450)" in alter_sql

    def test_varchar_max(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "val",
                    "DATA_TYPE": "varchar",
                    "CHARACTER_MAXIMUM_LENGTH": -1,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                }
            ],
            None,
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["val"])
        alter_sql = mock_conn.execute_sql.call_args_list[1][0][0]
        assert "VARCHAR(900)" in alter_sql

    def test_non_max_skipped(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "id",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        assert mock_conn.execute_sql.call_count == 1  # only the get_table_columns call


class TestCreatePrimaryKey:
    def test_creates_pk(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "id",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                }
            ],
            None,  # ALTER NOT NULL
            None,  # ADD CONSTRAINT
        ]
        writer.create_primary_key("dbo.t", ["id"])
        assert mock_conn.execute_sql.call_count == 3

    def test_missing_column_raises(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "other",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        with pytest.raises(ValueError, match="not found"):
            writer.create_primary_key("dbo.t", ["missing_col"])


class TestCreateIndex:
    def test_auto_name(self, writer, mock_conn):
        writer.create_index("dbo.t", ["col1", "col2"])
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "IX_t_col1_col2" in sql

    def test_custom_name(self, writer, mock_conn):
        writer.create_index("dbo.t", ["col1"], index_name="my_idx")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "my_idx" in sql


class TestTruncateAndDrop:
    def test_truncate_staging(self, writer, mock_conn):
        writer.truncate_staging("[stg].[t]")
        assert mock_conn.execute_sql.called

    def test_truncate_table(self, writer, mock_conn):
        writer.truncate_table("dbo.t")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "TRUNCATE" in sql

    def test_delete_from_table(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [{"deleted_count": 5}]
        count = writer.delete_from_table("dbo.t")
        assert count == 5

    def test_delete_from_table_tuple(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(3,)]
        count = writer.delete_from_table("dbo.t")
        assert count == 3

    def test_delete_from_table_empty(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        count = writer.delete_from_table("dbo.t")
        assert count == 0

    def test_drop_table(self, writer, mock_conn):
        writer.drop_table("dbo.t")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "DROP TABLE" in sql


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


class TestHandleSchemaEvolutionPandas:
    def test_none_config(self, writer):
        df = pd.DataFrame({"a": [1], "b": [2]})
        assert writer.handle_schema_evolution_pandas(df, "dbo.t", None) == ["a", "b"]

    def test_strict_no_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "a",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.STRICT
        df = pd.DataFrame({"a": [1]})
        result = writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)
        assert "a" in result

    def test_strict_raises_on_new_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "a",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.STRICT
        df = pd.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="strict"):
            writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)

    def test_evolve_adds_columns(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [
                {
                    "COLUMN_NAME": "a",
                    "DATA_TYPE": "int",
                    "CHARACTER_MAXIMUM_LENGTH": None,
                    "NUMERIC_PRECISION": None,
                    "NUMERIC_SCALE": None,
                }
            ],
            None,  # add_columns call
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.EVOLVE
        cfg.add_columns = True
        df = pd.DataFrame({"a": [1], "b": ["x"]})
        result = writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)
        assert "a" in result and "b" in result

    def test_ignore_filters_cols(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "a",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.IGNORE
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = writer.handle_schema_evolution_pandas(df, "dbo.t", cfg)
        assert result == ["a"]


class TestHandleSchemaEvolutionPolars:
    def test_none_config(self, writer):
        df = pl.DataFrame({"a": [1], "b": [2]})
        assert writer.handle_schema_evolution_polars(df, "dbo.t", None) == ["a", "b"]

    def test_none_config_lazyframe(self, writer):
        df = pl.DataFrame({"a": [1]}).lazy()
        result = writer.handle_schema_evolution_polars(df, "dbo.t", None)
        assert "a" in result

    def test_strict_raises(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "a",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.STRICT
        df = pl.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="strict"):
            writer.handle_schema_evolution_polars(df, "dbo.t", cfg)

    def test_ignore(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [
            {
                "COLUMN_NAME": "a",
                "DATA_TYPE": "int",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        cfg = MagicMock()
        cfg.mode = SqlServerSchemaEvolutionMode.IGNORE
        df = pl.DataFrame({"a": [1], "b": [2]})
        result = writer.handle_schema_evolution_polars(df, "dbo.t", cfg)
        assert result == ["a"]


# ---------------------------------------------------------------------------
# build_merge_sql
# ---------------------------------------------------------------------------


class TestBuildMergeSql:
    def test_basic(self, writer):
        sql = writer.build_merge_sql(
            "dbo.orders",
            "[stg].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "name", "val"],
        )
        assert "MERGE" in sql
        assert "target.[id] = source.[id]" in sql
        assert "[name] = source.[name]" in sql
        assert "INSERT" in sql

    def test_with_audit_cols(self, writer):
        opts = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        sql = writer.build_merge_sql(
            "dbo.orders",
            "[stg].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "val", "created_ts", "updated_ts"],
            options=opts,
        )
        assert "GETUTCDATE()" in sql
        # created_ts should NOT be in UPDATE SET
        assert "updated_ts] = GETUTCDATE()" in sql

    def test_with_conditions(self, writer):
        opts = SqlServerMergeOptions(
            update_condition="source.active = 1",
            delete_condition="source.is_deleted = 1",
            insert_condition="source.active = 1",
        )
        sql = writer.build_merge_sql(
            "dbo.t",
            "[stg].[t_staging]",
            merge_keys=["id"],
            columns=["id", "val"],
            options=opts,
        )
        assert "source.active = 1" in sql
        assert "source.is_deleted = 1" in sql


# ---------------------------------------------------------------------------
# execute_merge
# ---------------------------------------------------------------------------


class TestExecuteMerge:
    def test_empty_target_uses_fast_insert(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT SQL
            [(5,)],  # COUNT(*) staging
        ]
        result = writer.execute_merge("dbo.t", "[stg].[t_staging]", ["id"], ["id", "val"])
        assert result.inserted == 5

    def test_non_empty_target_uses_merge(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [(10,)],  # COUNT(*) -> 10
            [{"inserted": 3, "updated": 2, "deleted": 0}],  # MERGE result
        ]
        result = writer.execute_merge("dbo.t", "[stg].[t_staging]", ["id"], ["id", "val"])
        assert result.inserted == 3
        assert result.updated == 2

    def test_tuple_result(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [(10,)],
            [(1, 2, 0)],
        ]
        result = writer.execute_merge("dbo.t", "[stg].[t_staging]", ["id"], ["id", "val"])
        assert result.inserted == 1

    def test_empty_result(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [(10,)],
            [],
        ]
        result = writer.execute_merge("dbo.t", "[stg].[t_staging]", ["id"], ["id", "val"])
        assert result.total_affected == 0

    def test_error_raises(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [(10,)],
            Exception("DB error"),
        ]
        with pytest.raises(Exception, match="DB error"):
            writer.execute_merge("dbo.t", "[stg].[t_staging]", ["id"], ["id", "val"])


class TestExecuteInsertFromStaging:
    def test_basic(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [None, [(3,)]]
        opts = SqlServerMergeOptions()
        result = writer._execute_insert_from_staging(
            "dbo.t", "[stg].[t_staging]", ["id", "val"], opts
        )
        assert result.inserted == 3

    def test_with_audit_cols(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [None, [(2,)]]
        opts = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        result = writer._execute_insert_from_staging(
            "dbo.t", "[stg].[t_staging]", ["id", "val", "created_ts", "updated_ts"], opts
        )
        assert result.inserted == 2
        sql = mock_conn.execute_sql.call_args_list[0][0][0]
        assert "GETUTCDATE()" in sql


# ---------------------------------------------------------------------------
# merge_pandas
# ---------------------------------------------------------------------------


class TestMergePandas:
    def test_table_not_exists_auto_create(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        # auto_create_schema=False (default) -> no create_schema call
        # check_table_exists -> False
        # create_table_from_pandas -> execute_sql (CREATE TABLE)
        # write_table -> mock (initial direct INSERT)
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
        ]
        opts = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 2
        mock_conn.write_table.assert_called_once()

    def test_table_not_exists_no_auto_raises(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerMergeOptions(auto_create_table=False)
        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_pandas(df, "dbo.t", ["id"], opts)

    def test_table_exists_merge(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        # check_table_exists -> True
        # staging write -> write_table (mock)
        # execute_merge: COUNT(*) -> non-empty, MERGE -> result dict
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write done via write_table (mock)
            [(10,)],  # COUNT(*) for execute_merge
            [{"inserted": 1, "updated": 0, "deleted": 0}],  # MERGE result
        ]
        result = writer.merge_pandas(df, "dbo.t", ["id"])
        assert result.inserted == 1

    def test_validation_fail_raises(self, writer, mock_conn):
        df = pd.DataFrame({"id": [None], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
        ]
        opts = SqlServerMergeOptions(
            validations=SqlServerMergeValidationConfig(fail_on_validation_error=True)
        )
        with pytest.raises(ValueError, match="validation failed"):
            writer.merge_pandas(df, "dbo.t", ["id"], opts)

    def test_validation_warning_continues(self, writer, mock_conn):
        df = pd.DataFrame({"id": [None], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
            # staging write via write_table (mock)
            [(5,)],  # COUNT(*) for execute_merge
            [{"inserted": 0, "updated": 0, "deleted": 0}],
        ]
        opts = SqlServerMergeOptions(
            validations=SqlServerMergeValidationConfig(fail_on_validation_error=False)
        )
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result is not None


# ---------------------------------------------------------------------------
# overwrite_pandas
# ---------------------------------------------------------------------------


class TestOverwritePandas:
    def test_drop_create_table_exists(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1, 2]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema_exists
            [(1,)],  # check_table_exists -> True
            None,  # DROP TABLE
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 2
        assert result.strategy == "drop_create"
        mock_conn.write_table.assert_called_once()

    def test_truncate_insert_table_exists(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # check_table_exists -> True
            None,  # TRUNCATE
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_truncate_insert_no_table(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_delete_insert_table_exists(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # table_exists
            [(0,)],  # DELETE count
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_auto_create_schema(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_schema_exists -> not exists
            None,  # CREATE SCHEMA
            [(1,)],  # check_table_exists
            None,  # DROP TABLE
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            auto_create_schema=True,
        )
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1


# ---------------------------------------------------------------------------
# overwrite_polars
# ---------------------------------------------------------------------------


class TestOverwritePolars:
    def test_drop_create(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1, 2]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # table_exists
            None,  # DROP
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 2
        mock_conn.write_table.assert_called_once()

    def test_truncate_insert(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # table_exists
            None,  # TRUNCATE
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_lazyframe_collected(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]}).lazy()
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # table_exists
            None,  # DROP
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_batch_writes(self, writer, mock_conn):
        df = pl.DataFrame({"id": list(range(10))})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [],  # table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            batch_size=3,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 10
        # 10 rows / batch_size 3 = 4 batches
        assert mock_conn.write_table.call_count == 4

    def test_delete_insert_no_table(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [],  # table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1


# ---------------------------------------------------------------------------
# merge_polars
# ---------------------------------------------------------------------------


class TestMergePolars:
    def test_table_not_exists_auto_create(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        # auto_create_schema=False -> no create_schema
        # check_table_exists -> False
        # CREATE TABLE
        # staging write via write_table (mock)
        # execute_merge: COUNT(*) -> 0, INSERT, COUNT staging
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            # staging write via write_table (mock)
            [(0,)],  # COUNT(*) -> 0 (fast INSERT)
            None,  # INSERT SQL
            [(1,)],  # COUNT(*) staging
        ]
        opts = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_table_not_exists_no_auto_raises(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerMergeOptions(auto_create_table=False)
        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_polars(df, "dbo.t", ["id"], opts)

    def test_lazyframe_collected(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]}).lazy()
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via write_table (mock)
            [(5,)],  # COUNT(*) -> non-empty
            [{"inserted": 1, "updated": 0, "deleted": 0}],
        ]
        result = writer.merge_polars(df, "dbo.t", ["id"])
        assert result.inserted == 1


# ===========================================================================
# Phase 5 additions — covering remaining non-Spark uncovered lines
# ===========================================================================


# ---------------------------------------------------------------------------
# _fix_max_columns_for_indexing — VARBINARY(MAX) and unknown MAX
# ---------------------------------------------------------------------------


class TestFixMaxColumnsForIndexingExtended:
    def test_nvarchar_max(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("id", "NVARCHAR(MAX)", None, None, None)],  # get_table_columns
            None,  # ALTER TABLE
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        alter_call = mock_conn.execute_sql.call_args_list[-1]
        assert "NVARCHAR(450)" in alter_call[0][0]

    def test_varchar_max(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("id", "VARCHAR(MAX)", None, None, None)],
            None,
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        alter_call = mock_conn.execute_sql.call_args_list[-1]
        assert "VARCHAR(900)" in alter_call[0][0]

    def test_varbinary_max(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("id", "VARBINARY(MAX)", None, None, None)],
            None,
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        alter_call = mock_conn.execute_sql.call_args_list[-1]
        assert "VARBINARY(900)" in alter_call[0][0]

    def test_unknown_max_skipped(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("id", "XML(MAX)", None, None, None)],
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        assert mock_conn.execute_sql.call_count == 1  # only get_table_columns

    def test_non_max_col_skipped(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [("id", "INT", None, None, None)],
        ]
        writer._fix_max_columns_for_indexing("dbo.t", ["id"])
        assert mock_conn.execute_sql.call_count == 1


# ---------------------------------------------------------------------------
# handle_schema_evolution_polars — LazyFrame, evolve, ignore
# ---------------------------------------------------------------------------


class TestHandleSchemaEvolutionPolarsExtended:
    def test_no_config(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "b": [2]})
        result = writer.handle_schema_evolution_polars(df, "dbo.t", None)
        assert sorted(result) == ["a", "b"]

    def test_no_config_lazyframe(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        result = writer.handle_schema_evolution_polars(df, "dbo.t", None)
        assert sorted(result) == ["a", "b"]

    def test_strict_no_new_cols(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1]})
        config = MagicMock()
        config.mode = SqlServerSchemaEvolutionMode.STRICT
        mock_conn.execute_sql.return_value = [("a", "INT", None, None, None)]
        result = writer.handle_schema_evolution_polars(df, "dbo.t", config)
        assert "a" in result

    def test_strict_raises_on_new_cols(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "b": [2]})
        config = MagicMock()
        config.mode = SqlServerSchemaEvolutionMode.STRICT
        mock_conn.execute_sql.return_value = [("a", "INT", None, None, None)]
        with pytest.raises(ValueError, match="strict"):
            writer.handle_schema_evolution_polars(df, "dbo.t", config)

    def test_evolve_adds_columns(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "b": [2]})
        config = MagicMock()
        config.mode = SqlServerSchemaEvolutionMode.EVOLVE
        config.add_columns = True
        mock_conn.execute_sql.side_effect = [
            [("a", "INT", None, None, None)],  # get_table_columns
            None,  # ALTER TABLE ADD
        ]
        result = writer.handle_schema_evolution_polars(df, "dbo.t", config)
        assert "b" in result

    def test_evolve_lazyframe(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "new_col": ["x"]}).lazy()
        config = MagicMock()
        config.mode = SqlServerSchemaEvolutionMode.EVOLVE
        config.add_columns = True
        mock_conn.execute_sql.side_effect = [
            [("a", "INT", None, None, None)],
            None,
        ]
        result = writer.handle_schema_evolution_polars(df, "dbo.t", config)
        assert "new_col" in result

    def test_ignore_mode(self, writer, mock_conn):
        df = pl.DataFrame({"a": [1], "b": [2], "extra": [3]})
        config = MagicMock()
        config.mode = SqlServerSchemaEvolutionMode.IGNORE
        mock_conn.execute_sql.return_value = [
            ("a", "INT", None, None, None),
            ("b", "INT", None, None, None),
        ]
        result = writer.handle_schema_evolution_polars(df, "dbo.t", config)
        assert "extra" not in result
        assert "a" in result


# ---------------------------------------------------------------------------
# create_table_from_polars — audit cols paths
# ---------------------------------------------------------------------------


class TestCreateTableFromPolarsAudit:
    def test_with_audit_cols(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "name": ["a"]})
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        mock_conn.execute_sql.return_value = None
        writer.create_table_from_polars(df, "dbo.t", audit_cols=audit)
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "created_ts" in sql
        assert "updated_ts" in sql

    def test_lazyframe_audit(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]}).lazy()
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        mock_conn.execute_sql.return_value = None
        writer.create_table_from_polars(df, "dbo.t", audit_cols=audit)
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "DATETIME2" in sql

    def test_audit_cols_already_exist(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "created_ts": ["2024-01-01"]})
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        mock_conn.execute_sql.return_value = None
        writer.create_table_from_polars(df, "dbo.t", audit_cols=audit)
        sql = mock_conn.execute_sql.call_args[0][0]
        # created_ts already in df, only updated_ts added
        assert "updated_ts" in sql


# ---------------------------------------------------------------------------
# _execute_insert_from_staging — error path
# ---------------------------------------------------------------------------


class TestExecuteInsertFromStagingError:
    def test_insert_failure(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = Exception("deadlock")
        opts = SqlServerMergeOptions()
        with pytest.raises(Exception, match="deadlock"):
            writer._execute_insert_from_staging(
                "dbo.target", "[staging].[t_staging]", ["id", "val"], opts
            )


# ---------------------------------------------------------------------------
# merge_pandas — auto-create with PK/index, incremental, schema_evolution, cleanup
# ---------------------------------------------------------------------------


class TestMergePandasExtended:
    def test_auto_create_with_pk(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            [("id", "NVARCHAR(MAX)", None, None, None)],  # get_table_columns (fix max)
            None,  # ALTER TABLE (fix max)
            [("id", "NVARCHAR(450)", None, None, None)],  # get_table_columns (PK)
            None,  # ALTER COLUMN NOT NULL
            None,  # ADD CONSTRAINT PK
        ]
        opts = SqlServerMergeOptions(auto_create_table=True, primary_key_on_merge_keys=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_auto_create_with_index(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            [("id", "NVARCHAR(MAX)", None, None, None)],  # fix max
            None,  # ALTER TABLE (fix max)
            None,  # CREATE INDEX
        ]
        opts = SqlServerMergeOptions(auto_create_table=True, index_on_merge_keys=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_audit_cols_appended(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via write_table
            [(0,)],  # COUNT(*) -> 0 (fast INSERT)
            None,  # INSERT SQL
            [(1,)],  # COUNT(*) staging
        ]
        opts = SqlServerMergeOptions(audit_cols=audit)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_incremental_with_hash_detection(self, writer, mock_conn):
        df = pd.DataFrame({"id": ["1", "2"], "val": ["a", "b"], "_hash_diff": ["h1", "h2"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns
            [("id", "INT", None, None, None), ("_hash_diff", "NVARCHAR(256)", None, None, None)],
            # read_target_hashes: SELECT
            [("1", "h1"), ("2", "h_old")],
            # staging write via write_table
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT SQL
            [(1,)],  # COUNT(*) staging
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1  # only changed row

    def test_incremental_no_changes(self, writer, mock_conn):
        df = pd.DataFrame({"id": ["1"], "val": ["a"], "_hash_diff": ["h1"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns
            [("id", "INT", None, None, None), ("_hash_diff", "NVARCHAR(256)", None, None, None)],
            # read_target_hashes: SELECT -> same hash (tuples → dict via zip)
            [("1", "h1")],
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 0
        assert result.updated == 0

    def test_incremental_auto_hash(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns -> no hash col
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # read_target_hashes returns [] (hash col missing)
            # staging write via write_table
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_incremental_change_detection_columns(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns -> no hash col
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # read_target_hashes returns [] (hash col missing)
            # staging write via write_table
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(incremental=True, change_detection_columns=["val"])
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_schema_evolution_in_merge(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"], "new_col": ["x"]})
        evolution = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=True
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via write_table
            # schema evolution: get_table_columns (target)
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # schema evolution: get_table_columns (staging) — for new_col type
            [
                ("id", "INT", None, None, None),
                ("val", "NVARCHAR(MAX)", None, None, None),
                ("new_col", "NVARCHAR(MAX)", None, None, None),
            ],
            None,  # ALTER TABLE ADD new_col
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(schema_evolution=evolution)
        result = writer.merge_pandas(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_merge_error_cleanup(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via write_table
            [(5,)],  # COUNT(*) -> non-empty -> full MERGE
            Exception("merge failed"),  # MERGE SQL fails
            None,  # truncate staging cleanup
        ]
        with pytest.raises(Exception, match="merge failed"):
            writer.merge_pandas(df, "dbo.t", ["id"])

    def test_merge_error_cleanup_also_fails(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via write_table
            [(5,)],  # COUNT(*) -> non-empty
            Exception("merge failed"),  # MERGE fails
            Exception("cleanup failed"),  # truncate staging also fails
        ]
        with pytest.raises(Exception, match="merge failed"):
            writer.merge_pandas(df, "dbo.t", ["id"])


# ---------------------------------------------------------------------------
# overwrite_pandas — truncate_insert no table, delete_insert no table
# ---------------------------------------------------------------------------


class TestOverwritePandasExtended:
    def test_truncate_insert_no_table(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1
        mock_conn.write_table.assert_called_once()

    def test_delete_insert_no_table(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_auto_create_schema(self, writer, mock_conn):
        df = pd.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_schema_exists (in overwrite_pandas)
            [],  # check_schema_exists (inside create_schema)
            None,  # CREATE SCHEMA
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            auto_create_schema=True,
        )
        writer.overwrite_pandas(df, "dbo.t", opts)


# ---------------------------------------------------------------------------
# merge_polars — extended coverage
# ---------------------------------------------------------------------------


class TestMergePolarsExtended:
    def test_auto_create_schema(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [],  # create_schema(dbo) -> check_schema_exists -> False
            None,  # CREATE SCHEMA dbo
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            # staging write
            [],  # create_schema(staging) -> check_schema_exists -> False
            None,  # CREATE SCHEMA staging
            [(0,)],  # COUNT(*) -> 0 (execute_merge -> fast INSERT)
            None,  # INSERT SQL
            [(1,)],  # COUNT(*) staging
        ]
        opts = SqlServerMergeOptions(auto_create_table=True, auto_create_schema=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_auto_create_with_pk(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            [("id", "NVARCHAR(MAX)", None, None, None)],  # fix max
            None,  # ALTER TABLE fix max
            [("id", "NVARCHAR(450)", None, None, None)],  # get_table_columns PK
            None,  # ALTER NOT NULL
            None,  # ADD PK
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(auto_create_table=True, primary_key_on_merge_keys=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_auto_create_with_index(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            [("id", "NVARCHAR(MAX)", None, None, None)],  # fix max
            None,  # ALTER TABLE fix max
            None,  # CREATE INDEX
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(auto_create_table=True, index_on_merge_keys=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_schema_evolution_existing_table(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        evolution = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=False
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # handle_schema_evolution_polars: get_table_columns
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(schema_evolution=evolution)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_audit_cols(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        audit = SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(audit_cols=audit)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_validations_fail(self, writer, mock_conn):
        df = pl.DataFrame({"id": [None, 1], "val": ["a", "b"]})
        validations = SqlServerMergeValidationConfig(
            check_null_keys=True, fail_on_validation_error=True
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
        ]
        opts = SqlServerMergeOptions(validations=validations)
        with pytest.raises(ValueError, match="validation failed"):
            writer.merge_polars(df, "dbo.t", ["id"], opts)

    def test_validations_warn(self, writer, mock_conn):
        df = pl.DataFrame({"id": [None, 1], "val": ["a", "b"]})
        validations = SqlServerMergeValidationConfig(
            check_null_keys=True, fail_on_validation_error=False
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(2,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(validations=validations)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 2

    def test_incremental_with_hash(self, writer, mock_conn):
        df = pl.DataFrame({"id": ["1", "2"], "val": ["a", "b"], "_hash_diff": ["h1", "h2"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns
            [("id", "INT", None, None, None), ("_hash_diff", "NVARCHAR(256)", None, None, None)],
            # read_target_hashes: SELECT
            [("1", "h1"), ("2", "h_old")],
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_incremental_no_changes(self, writer, mock_conn):
        df = pl.DataFrame({"id": ["1"], "val": ["a"], "_hash_diff": ["h1"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns
            [("id", "INT", None, None, None), ("_hash_diff", "NVARCHAR(256)", None, None, None)],
            [("1", "h1")],
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 0

    def test_incremental_auto_hash(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns -> no hash col
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(incremental=True)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_incremental_change_detection_cols(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # read_target_hashes: get_table_columns -> no hash col
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # staging write
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(incremental=True, change_detection_columns=["val"])
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1

    def test_batch_writing(self, writer, mock_conn):
        df = pl.DataFrame({"id": list(range(10)), "val": ["a"] * 10})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # staging write via batches (write_table calls)
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(10,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(batch_size=3)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 10
        # 10 rows / 3 batch = 4 batches
        assert mock_conn.write_table.call_count == 4

    def test_schema_evolution_in_merge(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "val": ["a"], "new_col": ["x"]})
        evolution = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=True
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # handle_schema_evolution_polars: get_table_columns
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            None,  # ALTER TABLE ADD new_col
            # staging write via write_table
            # schema evolution in merge: get_table_columns target
            [("id", "INT", None, None, None), ("val", "NVARCHAR(MAX)", None, None, None)],
            # staging cols
            [
                ("id", "INT", None, None, None),
                ("val", "NVARCHAR(MAX)", None, None, None),
                ("new_col", "NVARCHAR(MAX)", None, None, None),
            ],
            None,  # ALTER TABLE ADD (merge schema evo)
            [(0,)],  # COUNT(*) -> 0
            None,  # INSERT
            [(1,)],  # COUNT staging
        ]
        opts = SqlServerMergeOptions(schema_evolution=evolution)
        result = writer.merge_polars(df, "dbo.t", ["id"], opts)
        assert result.inserted == 1


# ---------------------------------------------------------------------------
# overwrite_polars — extended batch/schema paths
# ---------------------------------------------------------------------------


class TestOverwritePolarsExtended:
    def test_auto_create_schema(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_schema_exists (in overwrite_polars)
            [],  # check_schema_exists (in create_schema)
            None,  # CREATE SCHEMA
            [],  # check_table_exists -> False (no auto_create)
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            auto_create_schema=True,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_truncate_insert_batch(self, writer, mock_conn):
        df = pl.DataFrame({"id": list(range(10))})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [(1,)],  # check_table_exists -> True
            None,  # TRUNCATE
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT,
            batch_size=3,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 10
        assert mock_conn.write_table.call_count == 4

    def test_truncate_insert_no_table(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_delete_insert_batch(self, writer, mock_conn):
        df = pl.DataFrame({"id": list(range(10))})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            [(10,)],  # DELETE returns row count
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DELETE_INSERT,
            batch_size=3,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 10
        assert mock_conn.write_table.call_count == 4

    def test_delete_insert_no_table(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists -> False
        ]
        opts = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_schema_evolution(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1], "new_col": ["x"]})
        evolution = SqlServerSchemaEvolutionConfig(
            mode=SqlServerSchemaEvolutionMode.EVOLVE, add_columns=True
        )
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists -> True
            # handle_schema_evolution_polars: get_table_columns
            [("id", "INT", None, None, None)],
            None,  # ALTER TABLE ADD new_col
            # drop_create: DROP TABLE
            None,
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            schema_evolution=evolution,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1

    def test_auto_create_table(self, writer, mock_conn):
        df = pl.DataFrame({"id": [1]})
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_schema
            [],  # check_table_exists -> False
            None,  # CREATE TABLE
            # DROP TABLE (strategy=drop_create, table now exists)
            None,
        ]
        opts = SqlServerOverwriteOptions(
            strategy=SqlServerOverwriteStrategy.DROP_CREATE,
            auto_create_table=True,
        )
        result = writer.overwrite_polars(df, "dbo.t", opts)
        assert result.rows_written == 1


# ---------------------------------------------------------------------------
# _execute_bulk_insert
# ---------------------------------------------------------------------------


class TestExecuteBulkInsert:
    def test_parquet_format(self, writer, mock_conn):
        writer._execute_bulk_insert("dbo.target", "my_eds", "path/data.parquet", "PARQUET")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "OPENROWSET" in sql
        assert "PARQUET" in sql

    def test_csv_format(self, writer, mock_conn):
        writer._execute_bulk_insert("dbo.target", "my_eds", "path/data.csv", "CSV")
        sql = mock_conn.execute_sql.call_args[0][0]
        assert "BULK INSERT" in sql
        assert "CSV" in sql
        assert "FIRSTROW = 2" in sql


# ---------------------------------------------------------------------------
# _validate_bulk_insert_schema
# ---------------------------------------------------------------------------


class TestValidateBulkInsertSchema:
    def test_match(self, writer, mock_conn):
        df = MagicMock()
        df.columns = ["id", "name"]
        mock_conn.execute_sql.return_value = [
            ("id", "INT", None, None, None),
            ("name", "NVARCHAR(MAX)", None, None, None),
        ]
        writer._validate_bulk_insert_schema(df, "dbo.t")  # no error

    def test_missing_in_df(self, writer, mock_conn):
        df = MagicMock()
        df.columns = ["id"]
        mock_conn.execute_sql.return_value = [
            ("id", "INT", None, None, None),
            ("name", "NVARCHAR(MAX)", None, None, None),
        ]
        with pytest.raises(ValueError, match="Schema mismatch"):
            writer._validate_bulk_insert_schema(df, "dbo.t")

    def test_extra_in_df(self, writer, mock_conn):
        df = MagicMock()
        df.columns = ["id", "name", "extra"]
        mock_conn.execute_sql.return_value = [
            ("id", "INT", None, None, None),
            ("name", "NVARCHAR(MAX)", None, None, None),
        ]
        with pytest.raises(ValueError, match="Schema mismatch"):
            writer._validate_bulk_insert_schema(df, "dbo.t")

    def test_ignore_columns(self, writer, mock_conn):
        # ignore_columns removes audit_ts from table set check
        # df has 2 cols, table has 3 but audit_ts is ignored → match on {id, name}
        df = MagicMock()
        df.columns = ["id", "name"]
        mock_conn.execute_sql.return_value = [
            ("id", "INT", None, None, None),
            ("name", "NVARCHAR(MAX)", None, None, None),
            ("audit_ts", "DATETIME2", None, None, None),  # ignored
        ]
        # Should pass the set-based check but fail the column count check (2 != 3)
        # So this test verifies the column count mismatch branch
        with pytest.raises(ValueError, match="Column count mismatch"):
            writer._validate_bulk_insert_schema(df, "dbo.t", ignore_columns=["audit_ts"])


# ---------------------------------------------------------------------------
# _is_azure_sql_database
# ---------------------------------------------------------------------------


class TestIsAzureSqlDatabase:
    def test_edition_5_azure_sql_db(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(5,)]
        assert writer._is_azure_sql_database() is True

    def test_edition_6_synapse(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(6,)]
        assert writer._is_azure_sql_database() is False

    def test_edition_8_managed_instance(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(8,)]
        assert writer._is_azure_sql_database() is True

    def test_dict_result(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [{"EngineEdition": 5}]
        assert writer._is_azure_sql_database() is True

    def test_no_result(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer._is_azure_sql_database() is True  # default

    def test_none_result(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = None
        assert writer._is_azure_sql_database() is True  # default via exception

    def test_error_defaults(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = Exception("fail")
        assert writer._is_azure_sql_database() is True

    def test_edition_1_onprem(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer._is_azure_sql_database() is True  # default path

    def test_string_edition(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("5",)]
        assert writer._is_azure_sql_database() is True

    def test_unconvertible_edition(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [("unknown",)]
        # ValueError during int() conversion, falls through
        assert writer._is_azure_sql_database() is True


# ---------------------------------------------------------------------------
# _extract_relative_path_from_fsspec
# ---------------------------------------------------------------------------


class TestExtractRelativePath:
    def test_with_container_prefix(self, writer):
        conn = MagicMock()
        conn.container = "mycontainer"
        result = writer._extract_relative_path_from_fsspec("mycontainer/path/file.csv", conn)
        assert result == "path/file.csv"

    def test_without_container_prefix(self, writer):
        conn = MagicMock()
        conn.container = "other"
        result = writer._extract_relative_path_from_fsspec("path/file.csv", conn)
        assert result == "path/file.csv"


# ---------------------------------------------------------------------------
# _find_single_csv_file
# ---------------------------------------------------------------------------


class TestFindSingleCsvFile:
    def test_found(self, writer):
        conn = MagicMock()
        conn.account = "acct"
        conn.auth_mode = "direct_key"
        conn.account_key = "key"
        mock_fs = MagicMock()
        mock_fs.ls.return_value = ["container/dir/_SUCCESS", "container/dir/part-00000-abc.csv"]
        with patch("fsspec.filesystem", return_value=mock_fs):
            result = writer._find_single_csv_file(conn, "container/dir")
        assert "part-" in result

    def test_not_found(self, writer):
        conn = MagicMock()
        conn.account = "acct"
        conn.auth_mode = "sas_token"
        conn.sas_token = "tok"
        mock_fs = MagicMock()
        mock_fs.ls.return_value = ["container/dir/_SUCCESS"]
        with patch("fsspec.filesystem", return_value=mock_fs):
            result = writer._find_single_csv_file(conn, "container/dir")
        assert result is None

    def test_dict_entries(self, writer):
        conn = MagicMock()
        conn.account = "acct"
        conn.auth_mode = "direct_key"
        conn.account_key = "key"
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [{"name": "container/dir/part-00000.csv"}]
        with patch("fsspec.filesystem", return_value=mock_fs):
            result = writer._find_single_csv_file(conn, "container/dir")
        assert result is not None

    def test_import_error(self, writer):
        conn = MagicMock()
        conn.account = "acct"
        conn.auth_mode = "direct_key"
        with patch("builtins.__import__", side_effect=ImportError("no fsspec")):
            result = writer._find_single_csv_file(conn, "dir")
        assert result is None

    def test_other_error(self, writer):
        conn = MagicMock()
        conn.account = "acct"
        conn.auth_mode = "direct_key"
        conn.account_key = "key"
        with patch("fsspec.filesystem", side_effect=Exception("bad")):
            result = writer._find_single_csv_file(conn, "dir")
        assert result is None


# ---------------------------------------------------------------------------
# _cleanup_staging_files
# ---------------------------------------------------------------------------


class TestCleanupStagingFiles:
    def test_with_delete_method(self, writer):
        conn = MagicMock()
        conn.delete = MagicMock()
        writer._cleanup_staging_files(conn, "staging/file.csv", "abfs://container/staging/file.csv")
        conn.delete.assert_called_once_with("staging/file.csv")

    def test_fsspec_fallback(self, writer):
        conn = MagicMock(spec=[])  # no delete method
        conn.account = "acct"
        conn.auth_mode = "direct_key"
        conn.account_key = "key"
        mock_fs = MagicMock()
        mock_fs.exists.return_value = True
        with patch("fsspec.filesystem", return_value=mock_fs):
            writer._cleanup_staging_files(
                conn, "path", "https://acct.dfs.core.windows.net/container/path"
            )
        mock_fs.rm.assert_called_once()

    def test_no_delete_no_fsspec(self, writer):
        conn = MagicMock(spec=[])
        # No fsspec available
        with patch("builtins.__import__", side_effect=ImportError("no fsspec")):
            writer._cleanup_staging_files(conn, "path", "/local/path")
        # Just logs warning, no error

    def test_cleanup_error_swallowed(self, writer):
        conn = MagicMock()
        conn.delete.side_effect = Exception("delete failed")
        writer._cleanup_staging_files(conn, "path", "full/path")
        # No exception raised


# ---------------------------------------------------------------------------
# setup_bulk_copy_external_source
# ---------------------------------------------------------------------------


class TestSetupBulkCopyExternalSource:
    def test_already_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]  # exists
        result = writer.setup_bulk_copy_external_source(MagicMock(), "eds_name")
        assert result is True

    def test_force_recreate(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "sas_token"
        conn.sas_token = "?sv=2021&sig=xxx"
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # exists check -> True
            None,  # DROP external data source
            None,  # check master key
            [(1,)],  # master key exists
            None,  # DROP credential
            None,  # CREATE credential
            None,  # DROP eds
            None,  # CREATE eds
        ]
        result = writer.setup_bulk_copy_external_source(conn, "eds_name", force_recreate=True)
        assert result is True

    def test_sas_token_strips_question(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "sas_token"
        conn.sas_token = "?sv=2021&sig=xxx"
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
            [(1,)],  # master key exists
            None,  # DROP credential
            None,  # CREATE credential
            None,  # DROP eds
            None,  # CREATE eds
        ]
        result = writer.setup_bulk_copy_external_source(conn, "eds_name")
        assert result is True

    def test_managed_identity(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "managed_identity"
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
            [(1,)],  # master key exists
            None,  # DROP eds
            None,  # CREATE eds (no credential)
        ]
        result = writer.setup_bulk_copy_external_source(conn, "eds_name")
        assert result is True

    def test_direct_key_raises(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "direct_key"
        conn.account_key = "key123"
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
            [(1,)],  # master key exists
        ]
        with pytest.raises(ValueError, match="does not support storage account keys"):
            writer.setup_bulk_copy_external_source(conn, "eds_name")

    def test_key_vault_no_resolved_key(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "key_vault"
        conn._cached_storage_key = None
        conn._cached_key = None
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
            [(1,)],  # master key exists
        ]
        with pytest.raises(ValueError, match="key to be resolved first"):
            writer.setup_bulk_copy_external_source(conn, "eds_name")

    def test_unsupported_auth(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = "acct"
        conn.container = "container"
        conn.auth_mode = "unknown_mode"
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
            [(1,)],  # master key exists
        ]
        with pytest.raises(ValueError, match="Unsupported auth mode"):
            writer.setup_bulk_copy_external_source(conn, "eds_name")

    def test_missing_account_container(self, writer, mock_conn):
        conn = MagicMock()
        conn.account = None
        conn.container = None
        mock_conn.execute_sql.return_value = []  # not exists
        # Should skip past exists check then fail
        mock_conn.execute_sql.side_effect = [
            [],  # not exists
        ]
        with pytest.raises(ValueError, match="account and container"):
            writer.setup_bulk_copy_external_source(conn, "eds_name")


# ---------------------------------------------------------------------------
# _check_external_data_source_exists / _drop / _ensure_master_key
# ---------------------------------------------------------------------------


class TestExternalDataSourceHelpers:
    def test_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        assert writer._check_external_data_source_exists("test") is True

    def test_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        assert writer._check_external_data_source_exists("test") is False

    def test_error(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = Exception("fail")
        assert writer._check_external_data_source_exists("test") is False

    def test_drop(self, writer, mock_conn):
        writer._drop_external_data_source("test")
        assert mock_conn.execute_sql.called

    def test_ensure_master_key_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        writer._ensure_master_key()
        assert mock_conn.execute_sql.call_count == 1

    def test_ensure_master_key_creates(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [],  # check -> not exists
            None,  # CREATE MASTER KEY
        ]
        writer._ensure_master_key()
        assert mock_conn.execute_sql.call_count == 2


# ---------------------------------------------------------------------------
# _create_credential_with_key / _create_credential_with_sas
# ---------------------------------------------------------------------------


class TestCredentials:
    def test_key_raises(self, writer):
        with pytest.raises(ValueError, match="does not support storage account keys"):
            writer._create_credential_with_key("cred", "key123")

    def test_sas(self, writer, mock_conn):
        writer._create_credential_with_sas("cred", "sv=2021&sig=xxx")
        assert mock_conn.execute_sql.call_count == 2  # DROP + CREATE


# ---------------------------------------------------------------------------
# _extract_secret_from_connection_string
# ---------------------------------------------------------------------------


class TestExtractSecretFromConnStr:
    def test_account_key(self, writer):
        result = writer._extract_secret_from_connection_string(
            "AccountKey=mykey123;EndpointSuffix=x"
        )
        assert result == "mykey123"

    def test_sas(self, writer):
        result = writer._extract_secret_from_connection_string("SharedAccessSignature=sv=2021")
        assert result == "sv=2021"

    def test_missing(self, writer):
        with pytest.raises(ValueError, match="AccountKey or SharedAccessSignature"):
            writer._extract_secret_from_connection_string("EndpointSuffix=x")


# ---------------------------------------------------------------------------
# _create_external_data_source
# ---------------------------------------------------------------------------


class TestCreateExternalDataSource:
    def test_with_credential(self, writer, mock_conn):
        writer._create_external_data_source("eds", "https://acct.blob.core.windows.net/c", "cred")
        sql = mock_conn.execute_sql.call_args_list[-1][0][0]
        assert "CREDENTIAL" in sql

    def test_without_credential(self, writer, mock_conn):
        writer._create_external_data_source("eds", "https://acct.blob.core.windows.net/c", None)
        sql = mock_conn.execute_sql.call_args_list[-1][0][0]
        assert "CREDENTIAL" not in sql

    def test_error(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [None, Exception("fail")]
        with pytest.raises(Exception, match="fail"):
            writer._create_external_data_source(
                "eds", "https://acct.blob.core.windows.net/c", "cred"
            )
