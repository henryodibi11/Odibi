"""Regression tests for incremental-HWM correctness + SQL Server writer hardening.

Covers four verified bugs:
- 2A: HWM capture on SQL sources / first-run ignored fallback_column while the
      pushdown filter used COALESCE(column, fallback) -> watermark lagged the data
      it admitted -> re-processing. Capture must use the same fallback.
- 2B: the local deltalake state backend read picked row [0] with no ORDER BY, so a
      duplicate state row could surface an OLDER watermark (watermark goes backward).
      Read is now latest-wins by updated_at.
- item1: SqlServerMergeWriter.escape_column now escapes ']' so a bracketed column
      name can't break out of identifier quoting; get_table_columns binds params.
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.node import NodeExecutor


# --- 2A: HWM capture uses fallback on SQL + first-run paths -----------------


class _PandasEngineStub:
    # A real pandas engine has no `.spark` attribute; a bare MagicMock would
    # auto-create one and push _get_column_max down the Spark branch.
    name = "pandas"


def _executor():
    return NodeExecutor(MagicMock(), _PandasEngineStub(), {"src": MagicMock(), "dst": MagicMock()})


def _sql_config(fallback=True):
    inc = {"mode": "stateful", "column": "updated_at", "state_key": "k"}
    if fallback:
        inc["fallback_column"] = "created_at"
    return NodeConfig(
        name="n",
        read={"connection": "src", "format": "sql", "table": "t", "incremental": inc},
        write={"connection": "dst", "format": "delta", "table": "out"},
    )


def test_sql_capture_uses_fallback_for_hwm():
    # Row with the latest effective timestamp has a NULL primary column; its value
    # lives in the fallback. Captured HWM must reflect COALESCE(updated_at, created_at).
    df = pd.DataFrame(
        {
            "updated_at": [pd.Timestamp("2024-01-01"), None],
            "created_at": [pd.Timestamp("2023-01-01"), pd.Timestamp("2024-06-01")],
        }
    )
    _, hwm = _executor()._apply_incremental_filtering(df, _sql_config(fallback=True), None)
    assert hwm is not None
    state_key, new_max = hwm
    assert state_key == "k"
    # Must be the fallback's 2024-06-01, NOT max(updated_at)=2024-01-01.
    assert pd.Timestamp(new_max) == pd.Timestamp("2024-06-01")


def test_sql_capture_without_fallback_uses_primary_only():
    df = pd.DataFrame({"updated_at": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-01")]})
    _, hwm = _executor()._apply_incremental_filtering(df, _sql_config(fallback=False), None)
    assert pd.Timestamp(hwm[1]) == pd.Timestamp("2024-02-01")


# --- 2B: local state backend read is latest-wins, write doesn't dup ---------


def test_get_hwm_local_latest_wins_with_duplicate_rows(tmp_path, monkeypatch):
    deltalake = pytest.importorskip("deltalake")
    from odibi.state import CatalogStateBackend

    state_path = str(tmp_path / "meta_state")
    backend = CatalogStateBackend(
        meta_state_path=state_path, meta_runs_path=state_path + "_runs", environment="test"
    )

    # Simulate a duplicate-row history (older row written after newer in file order)
    # by appending two rows for the same key with different updated_at.
    import pyarrow as pa

    rows = pa.table(
        {
            "key": ["k", "k"],
            "value": ['"OLD"', '"NEW"'],
            "environment": ["test", "test"],
            "updated_at": [
                pd.Timestamp("2024-01-01", tz="UTC"),
                pd.Timestamp("2024-06-01", tz="UTC"),
            ],
        }
    )
    deltalake.write_deltalake(state_path, rows, mode="overwrite")

    # Latest-wins by updated_at -> "NEW", never the older "OLD".
    assert backend.get_hwm("k") == "NEW"


def test_set_hwm_local_does_not_append_duplicate_on_existing_table(tmp_path):
    pytest.importorskip("deltalake")
    import deltalake

    from odibi.state import CatalogStateBackend

    state_path = str(tmp_path / "meta_state2")
    backend = CatalogStateBackend(
        meta_state_path=state_path, meta_runs_path=state_path + "_runs", environment="test"
    )

    backend.set_hwm("k", "v1")  # creates table
    backend.set_hwm("k", "v2")  # must UPDATE in place, not append a 2nd row

    dt = deltalake.DeltaTable(state_path)
    df = dt.to_pandas()
    assert len(df[df["key"] == "k"]) == 1, "merge should upsert, not duplicate the key"
    assert backend.get_hwm("k") == "v2"


# --- item 1: SQL Server writer identifier hardening + bound params ----------


def test_escape_column_escapes_internal_bracket():
    from odibi.writers.sql_server_writer import SqlServerMergeWriter

    w = SqlServerMergeWriter(MagicMock())
    # A ']' inside the name must be doubled so it can't terminate the identifier.
    assert w.escape_column("a]b") == "[a]]b]"
    # Normal + already-bracketed names unchanged.
    assert w.escape_column("col") == "[col]"
    assert w.escape_column("[col]") == "[col]"


def test_get_table_columns_uses_bound_params():
    from odibi.writers.sql_server_writer import SqlServerMergeWriter

    conn = MagicMock()
    conn.execute_sql.return_value = []
    w = SqlServerMergeWriter(conn)
    w.get_table_columns("sales.orders")

    _, kwargs = conn.execute_sql.call_args
    sql = conn.execute_sql.call_args[0][0]
    # No interpolated literals; schema/table passed as bound params.
    assert "'sales'" not in sql and "'orders'" not in sql
    assert ":schema" in sql and ":table_name" in sql
    assert kwargs["params"] == {"schema": "sales", "table_name": "orders"}
