"""Unit tests for SqlServerMergeWriter: MERGE SQL builder, execution,
overwrite strategies, merge_pandas, merge_polars, and read_target_hashes."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

try:
    import polars as pl
except ImportError:
    pl = None  # type: ignore[assignment]

from odibi.config import (
    SqlServerAuditColsConfig,
    SqlServerMergeOptions,
    SqlServerOverwriteOptions,
    SqlServerOverwriteStrategy,
    SqlServerSchemaEvolutionConfig,
    SqlServerSchemaEvolutionMode,
)
from odibi.writers.sql_server_writer import SqlServerMergeWriter


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    """Mock SQL Server connection with execute_sql and write_table."""
    conn = MagicMock()
    conn.execute_sql = MagicMock(return_value=[])
    conn.write_table = MagicMock()
    return conn


@pytest.fixture
def writer(mock_conn):
    """SqlServerMergeWriter backed by a mock connection."""
    return SqlServerMergeWriter(mock_conn)


# ===========================================================================
# build_merge_sql
# ===========================================================================


class TestBuildMergeSql:
    """Tests for build_merge_sql method."""

    def test_basic_merge(self, writer):
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["order_id"],
            columns=["order_id", "amount", "status"],
        )
        assert "MERGE [dbo].[orders] AS target" in sql
        assert "USING [staging].[orders_staging] AS source" in sql
        assert "target.[order_id] = source.[order_id]" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "[amount] = source.[amount]" in sql
        assert "[status] = source.[status]" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql
        assert "INSERT ([order_id], [amount], [status])" in sql
        assert "OUTPUT $action INTO @MergeActions" in sql

    def test_with_audit_cols(self, writer):
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(
                created_col="created_ts",
                updated_col="updated_ts",
            )
        )
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["order_id"],
            columns=["order_id", "amount", "created_ts", "updated_ts"],
            options=options,
        )
        update_section = sql.split("UPDATE SET")[1].split("WHEN")[0]
        assert "[updated_ts] = GETUTCDATE()" in update_section
        assert "[created_ts]" not in update_section

        values_section = sql.split("VALUES")[1]
        assert values_section.count("GETUTCDATE()") == 2

    def test_with_update_condition(self, writer):
        options = SqlServerMergeOptions(update_condition="source._hash != target._hash")
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "val", "_hash"],
            options=options,
        )
        assert "WHEN MATCHED AND source._hash != target._hash THEN" in sql

    def test_with_delete_condition(self, writer):
        options = SqlServerMergeOptions(delete_condition="source.is_deleted = 1")
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "val", "is_deleted"],
            options=options,
        )
        assert "WHEN MATCHED AND source.is_deleted = 1 THEN" in sql
        assert "DELETE" in sql

    def test_with_insert_condition(self, writer):
        options = SqlServerMergeOptions(insert_condition="source.active = 1")
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "val", "active"],
            options=options,
        )
        assert "WHEN NOT MATCHED BY TARGET AND source.active = 1 THEN" in sql

    def test_with_exclude_columns(self, writer):
        options = SqlServerMergeOptions(exclude_columns=["_hash", "_source"])
        sql = writer.build_merge_sql(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "val", "_hash", "_source"],
            options=options,
        )
        assert "[_hash]" not in sql
        assert "[_source]" not in sql
        assert "[val]" in sql
        assert "[id]" in sql

    def test_with_all_conditions_and_audit(self, writer):
        options = SqlServerMergeOptions(
            update_condition="source._hash != target._hash",
            delete_condition="source.is_deleted = 1",
            insert_condition="source.active = 1",
            exclude_columns=["_hash"],
            audit_cols=SqlServerAuditColsConfig(created_col="created_at", updated_col="updated_at"),
        )
        sql = writer.build_merge_sql(
            target_table="sales.fact",
            staging_table="[staging].[fact_staging]",
            merge_keys=["k1", "k2"],
            columns=["k1", "k2", "val", "_hash", "created_at", "updated_at"],
            options=options,
        )
        assert "WHEN MATCHED AND source._hash != target._hash THEN" in sql
        assert "WHEN MATCHED AND source.is_deleted = 1 THEN" in sql
        assert "WHEN NOT MATCHED BY TARGET AND source.active = 1 THEN" in sql
        assert "[_hash]" not in sql.replace("source._hash", "").replace("target._hash", "")
        assert "[updated_at] = GETUTCDATE()" in sql


# ===========================================================================
# execute_merge
# ===========================================================================


class TestExecuteMerge:
    """Tests for execute_merge method."""

    def test_empty_target_fast_insert(self, writer, mock_conn):
        """When target is empty, should use fast INSERT path."""
        mock_conn.execute_sql.side_effect = [
            [(0,)],  # COUNT(*) — target is empty
            None,  # INSERT INTO ... SELECT
            [(5,)],  # COUNT(*) from staging
        ]
        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "amount"],
        )
        assert result.inserted == 5
        assert result.updated == 0
        assert result.deleted == 0

    def test_populated_target_full_merge_dict(self, writer, mock_conn):
        """When target has data and result is a dict, should parse correctly."""
        mock_conn.execute_sql.side_effect = [
            [(10,)],  # COUNT(*) — target has data
            [{"inserted": 3, "updated": 7, "deleted": 1}],
        ]
        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "amount"],
        )
        assert result.inserted == 3
        assert result.updated == 7
        assert result.deleted == 1

    def test_populated_target_full_merge_tuple(self, writer, mock_conn):
        """When result row is a tuple, should parse by index."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],
            [(4, 2, 0)],
        ]
        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "amount"],
        )
        assert result.inserted == 4
        assert result.updated == 2
        assert result.deleted == 0

    def test_empty_merge_result(self, writer, mock_conn):
        """When MERGE returns no result rows, should default to zero counts."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],
            [],
        ]
        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "amount"],
        )
        assert result.total_affected == 0

    def test_merge_propagates_exception(self, writer, mock_conn):
        """SQL errors should propagate."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],
            RuntimeError("deadlock"),
        ]
        with pytest.raises(RuntimeError, match="deadlock"):
            writer.execute_merge(
                target_table="dbo.orders",
                staging_table="[staging].[orders_staging]",
                merge_keys=["id"],
                columns=["id", "amount"],
            )

    def test_none_values_in_result_treated_as_zero(self, writer, mock_conn):
        """None values in result row should be treated as 0."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],
            [{"inserted": None, "updated": None, "deleted": None}],
        ]
        result = writer.execute_merge(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            merge_keys=["id"],
            columns=["id", "amount"],
        )
        assert result.inserted == 0
        assert result.updated == 0
        assert result.deleted == 0


# ===========================================================================
# _execute_insert_from_staging
# ===========================================================================


class TestExecuteInsertFromStaging:
    """Tests for _execute_insert_from_staging method."""

    def test_basic_insert(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            None,  # INSERT INTO
            [(3,)],  # COUNT(*)
        ]
        result = writer._execute_insert_from_staging(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            columns=["id", "amount", "status"],
            options=SqlServerMergeOptions(),
        )
        assert result.inserted == 3
        assert result.updated == 0

        insert_sql = mock_conn.execute_sql.call_args_list[0][0][0]
        assert "INSERT INTO [dbo].[orders]" in insert_sql
        assert "SELECT" in insert_sql
        assert "FROM [staging].[orders_staging]" in insert_sql

    def test_with_audit_cols(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            None,  # INSERT INTO
            [(2,)],  # COUNT(*)
        ]
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        result = writer._execute_insert_from_staging(
            target_table="dbo.orders",
            staging_table="[staging].[orders_staging]",
            columns=["id", "amount", "created_ts", "updated_ts"],
            options=options,
        )
        assert result.inserted == 2

        insert_sql = mock_conn.execute_sql.call_args_list[0][0][0]
        assert "[created_ts]" in insert_sql
        assert "[updated_ts]" in insert_sql
        assert insert_sql.count("GETUTCDATE()") == 2

    def test_insert_failure_propagates(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = RuntimeError("timeout")
        with pytest.raises(RuntimeError, match="timeout"):
            writer._execute_insert_from_staging(
                target_table="dbo.orders",
                staging_table="[staging].[orders_staging]",
                columns=["id", "amount"],
                options=SqlServerMergeOptions(),
            )


# ===========================================================================
# overwrite_pandas
# ===========================================================================


class TestOverwritePandas:
    """Tests for overwrite_pandas method."""

    def _make_df(self):
        return pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})

    def test_truncate_insert_existing_table(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]  # table exists
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.rows_written == 3
        assert result.strategy == "truncate_insert"
        truncate_calls = [c for c in mock_conn.execute_sql.call_args_list if "TRUNCATE" in str(c)]
        assert len(truncate_calls) >= 1
        mock_conn.write_table.assert_called_once()
        kw = mock_conn.write_table.call_args
        assert kw[1]["if_exists"] == "append"

    def test_drop_recreate_existing_table(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]  # table exists
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.rows_written == 3
        assert result.strategy == "drop_create"
        drop_calls = [c for c in mock_conn.execute_sql.call_args_list if "DROP" in str(c)]
        assert len(drop_calls) >= 1
        mock_conn.write_table.assert_called_once()
        kw = mock_conn.write_table.call_args
        assert kw[1]["if_exists"] == "replace"

    def test_truncate_insert_table_not_exists(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []  # table does not exist
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.rows_written == 3
        kw = mock_conn.write_table.call_args
        assert kw[1]["if_exists"] == "replace"

    def test_delete_insert_existing_table(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DELETE_INSERT)
        result = writer.overwrite_pandas(df, "dbo.orders", options)

        assert result.rows_written == 3
        assert result.strategy == "delete_insert"


# ===========================================================================
# overwrite_polars
# ===========================================================================


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestOverwritePolars:
    """Tests for overwrite_polars method."""

    def _make_df(self):
        return pl.DataFrame({"id": [1, 2], "val": [10, 20]})

    def test_truncate_insert(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.TRUNCATE_INSERT)
        result = writer.overwrite_polars(df, "dbo.orders", options)
        assert result.rows_written == 2
        assert result.strategy == "truncate_insert"

    def test_drop_create(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = [(1,)]
        df = self._make_df()
        options = SqlServerOverwriteOptions(strategy=SqlServerOverwriteStrategy.DROP_CREATE)
        result = writer.overwrite_polars(df, "dbo.orders", options)
        assert result.rows_written == 2
        assert result.strategy == "drop_create"


# ===========================================================================
# merge_pandas
# ===========================================================================


class TestMergePandas:
    """Tests for merge_pandas end-to-end with mock connection."""

    def test_auto_create_table_first_run(self, writer, mock_conn):
        """First run with auto_create_table should INSERT directly."""
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists — no
            [],  # create_table_from_pandas
        ]
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        options = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_pandas(df, "dbo.orders", ["id"], options)

        assert result.inserted == 2
        assert result.updated == 0
        mock_conn.write_table.assert_called_once()

    def test_existing_table_merge(self, writer, mock_conn):
        """Existing table should go through staging + MERGE."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists — yes
            [(1,)],  # COUNT(*) — target has data
            [{"inserted": 1, "updated": 1, "deleted": 0}],
        ]
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        result = writer.merge_pandas(df, "dbo.orders", ["id"])

        assert result.inserted == 1
        assert result.updated == 1
        mock_conn.write_table.assert_called_once()

    def test_audit_cols_in_merge_sql(self, writer, mock_conn):
        """Audit columns should appear in MERGE SQL with GETUTCDATE()."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
            [(1,)],  # COUNT(*)
            [{"inserted": 2, "updated": 0, "deleted": 0}],
        ]
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        options = SqlServerMergeOptions(
            audit_cols=SqlServerAuditColsConfig(created_col="created_ts", updated_col="updated_ts")
        )
        writer.merge_pandas(df, "dbo.orders", ["id"], options)

        merge_sql = None
        for c in mock_conn.execute_sql.call_args_list:
            if "MERGE" in str(c):
                merge_sql = c[0][0]
                break
        assert merge_sql is not None
        assert "GETUTCDATE()" in merge_sql
        assert "[created_ts]" in merge_sql
        assert "[updated_ts]" in merge_sql

    def test_incremental_no_changes_skips_merge(self, writer, mock_conn):
        """When incremental filtering removes all rows, should skip merge."""
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists
        ]
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR(256)"})
        writer.read_target_hashes = MagicMock(
            return_value=[{"id": 1, "_hash": "h1"}, {"id": 2, "_hash": "h2"}]
        )

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "_hash": ["h1", "h2"]})
        options = SqlServerMergeOptions(incremental=True, hash_column="_hash")
        result = writer.merge_pandas(df, "dbo.orders", ["id"], options)

        assert result.inserted == 0
        assert result.updated == 0
        assert result.deleted == 0

    def test_table_not_exists_no_auto_create_raises(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        df = pd.DataFrame({"id": [1], "val": [10]})
        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_pandas(df, "dbo.orders", ["id"])


# ===========================================================================
# merge_polars
# ===========================================================================


@pytest.mark.skipif(pl is None, reason="polars not installed")
class TestMergePolars:
    """Tests for merge_polars with mock connection."""

    def test_auto_create_table(self, writer, mock_conn):
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists — no
            [],  # create_table_from_polars
            [(1,)],  # COUNT(*) for execute_merge
            [{"inserted": 2, "updated": 0, "deleted": 0}],
        ]
        df = pl.DataFrame({"id": [1, 2], "val": [10, 20]})
        options = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_polars(df, "dbo.items", ["id"], options)

        assert result.inserted == 2
        mock_conn.write_table.assert_called_once()

    def test_schema_evolution_evolve(self, writer, mock_conn):
        """Schema evolution should add new columns before MERGE."""
        col_info_row = {
            "COLUMN_NAME": "id",
            "DATA_TYPE": "int",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": None,
        }
        col_info_with_new = [
            col_info_row,
            {
                "COLUMN_NAME": "new_col",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": -1,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
            },
        ]
        mock_conn.execute_sql.side_effect = [
            [(1,)],  # check_table_exists — yes
            [col_info_row],  # get_table_columns for handle_schema_evolution
            [],  # ALTER TABLE ADD new_col
            [col_info_row],  # get_table_columns for schema evolution before MERGE
            col_info_with_new,  # get_table_columns for staging table
            [],  # ALTER TABLE ADD new_col to target
            [(1,)],  # COUNT(*) for execute_merge
            [{"inserted": 1, "updated": 0, "deleted": 0}],
        ]
        df = pl.DataFrame({"id": [1], "new_col": ["x"]})
        options = SqlServerMergeOptions(
            schema_evolution=SqlServerSchemaEvolutionConfig(
                mode=SqlServerSchemaEvolutionMode.EVOLVE,
                add_columns=True,
            )
        )
        result = writer.merge_polars(df, "dbo.items", ["id"], options)
        assert result.inserted == 1

    def test_table_not_exists_no_auto_create_raises(self, writer, mock_conn):
        mock_conn.execute_sql.return_value = []
        df = pl.DataFrame({"id": [1], "val": [10]})
        with pytest.raises(ValueError, match="does not exist"):
            writer.merge_polars(df, "dbo.items", ["id"])

    def test_lazy_frame_collected(self, writer, mock_conn):
        """LazyFrame should be collected before processing."""
        mock_conn.execute_sql.side_effect = [
            [],  # check_table_exists
            [],  # create_table
            [(1,)],  # COUNT(*)
            [{"inserted": 2, "updated": 0, "deleted": 0}],
        ]
        lf = pl.DataFrame({"id": [1, 2], "val": [10, 20]}).lazy()
        options = SqlServerMergeOptions(auto_create_table=True)
        result = writer.merge_polars(lf, "dbo.items", ["id"], options)
        assert result.inserted == 2


# ===========================================================================
# read_target_hashes
# ===========================================================================


class TestReadTargetHashes:
    """Tests for read_target_hashes result parsing."""

    def test_asdict_dict_rows(self, writer, mock_conn):
        """Named-tuple-like rows with _asdict returning expected data."""
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR"})

        class Row1:
            def _asdict(self):
                return {"id": 1, "_hash": "aaa"}

        class Row2:
            def _asdict(self):
                return {"id": 2, "_hash": "bbb"}

        mock_conn.execute_sql.return_value = [Row1(), Row2()]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert len(result) == 2
        assert result[0] == {"id": 1, "_hash": "aaa"}
        assert result[1] == {"id": 2, "_hash": "bbb"}

    def test_tuple_rows(self, writer, mock_conn):
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR"})
        mock_conn.execute_sql.return_value = [
            (1, "aaa"),
            (2, "bbb"),
        ]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert len(result) == 2
        assert result[0] == {"id": 1, "_hash": "aaa"}

    def test_asdict_rows(self, writer, mock_conn):
        """Rows with _asdict() method (e.g. named tuples)."""
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR"})

        class FakeRow:
            def _asdict(self):
                return {"id": 1, "_hash": "xxx"}

        mock_conn.execute_sql.return_value = [FakeRow()]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert len(result) == 1
        assert result[0] == {"id": 1, "_hash": "xxx"}

    def test_mapping_rows(self, writer, mock_conn):
        """Rows with _mapping attribute (SQLAlchemy Row)."""
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR"})

        class FakeRow:
            _mapping = {"id": 99, "_hash": "zzz"}

        mock_conn.execute_sql.return_value = [FakeRow()]
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert len(result) == 1
        assert result[0] == {"id": 99, "_hash": "zzz"}

    def test_hash_column_not_found(self, writer, mock_conn):
        """When hash column not in target table, should return empty list."""
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "name": "NVARCHAR"})
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == []
        mock_conn.execute_sql.assert_not_called()

    def test_empty_result(self, writer, mock_conn):
        writer.get_table_columns = MagicMock(return_value={"id": "INT", "_hash": "NVARCHAR"})
        mock_conn.execute_sql.return_value = []
        result = writer.read_target_hashes("dbo.t", ["id"], "_hash")
        assert result == []
