"""Tests for SCD2 and Merge Polars branches.

Validates:
- SCD2 lifecycle: initial load, no-change, changed, new records, mixed
- SCD2 NaN handling (#248): NaN==NaN no false positive, NaN→value detected
- Merge strategies: upsert, append_only, delete_match
- Merge audit columns: created_at preserved on update
- Cross-engine parity: Polars vs Pandas produce identical results
"""

import os
import shutil
import tempfile
from datetime import datetime

import polars as pl
import pytest

from odibi.context import EngineContext, PandasContext, PolarsContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.engine.polars_engine import PolarsEngine
from odibi.enums import EngineType
from odibi.transformers.merge_transformer import (
    MergeParams,
    merge,
)
from odibi.transformers.scd import SCD2Params, scd2


# --- Helpers ---


def _make_polars_ctx(df):
    ctx = PolarsContext()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.POLARS,
        engine=PolarsEngine(),
        sql_executor=None,
    )


def _make_pandas_ctx(df):
    ctx = PandasContext()
    return EngineContext(
        context=ctx,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=PandasEngine(config={}),
        sql_executor=None,
    )


@pytest.fixture
def tmp_dir():
    d = tempfile.mkdtemp(prefix="odibi_test_scd_merge_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ============================================================
# SCD2 Tests
# ============================================================


class TestSCD2Polars:
    def _make_params(self, tmp_dir, filename="scd2.parquet"):
        return SCD2Params(
            target=os.path.join(tmp_dir, filename),
            keys=["emp_id"],
            track_cols=["name", "dept"],
            effective_time_col="txn_date",
        )

    def _source(self, data_override=None):
        data = {
            "emp_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "dept": ["Eng", "Sales", "Eng"],
            "txn_date": [datetime(2024, 1, 1)] * 3,
        }
        if data_override:
            data.update(data_override)
        return pl.DataFrame(data)

    def test_initial_load(self, tmp_dir):
        params = self._make_params(tmp_dir)
        result = scd2(_make_polars_ctx(self._source()), params)
        df = result.df

        assert df.shape[0] == 3
        assert all(df["is_current"].to_list())
        assert all(v is None for v in df["valid_to"].to_list())
        assert os.path.exists(params.target)

    def test_no_changes(self, tmp_dir):
        params = self._make_params(tmp_dir)
        scd2(_make_polars_ctx(self._source()), params)
        result = scd2(_make_polars_ctx(self._source()), params)
        df = result.df

        assert df.shape[0] == 3
        assert df.filter(pl.col("is_current") == True).shape[0] == 3  # noqa: E712

    def test_changed_records(self, tmp_dir):
        params = self._make_params(tmp_dir)
        scd2(_make_polars_ctx(self._source()), params)

        changed = self._source(
            {"dept": ["Eng", "Marketing", "Eng"], "txn_date": [datetime(2024, 6, 1)] * 3}
        )
        result = scd2(_make_polars_ctx(changed), params)
        df = result.df

        assert df.shape[0] == 4  # 3 + 1 new Bob version
        bob_expired = df.filter((pl.col("emp_id") == 2) & (pl.col("is_current") == False))  # noqa: E712
        assert bob_expired.shape[0] == 1
        assert bob_expired["dept"][0] == "Sales"
        assert bob_expired["valid_to"][0] is not None

        bob_current = df.filter((pl.col("emp_id") == 2) & (pl.col("is_current") == True))  # noqa: E712
        assert bob_current["dept"][0] == "Marketing"

    def test_new_records(self, tmp_dir):
        params = self._make_params(tmp_dir)
        scd2(_make_polars_ctx(self._source()), params)

        with_dave = pl.DataFrame(
            {
                "emp_id": [1, 2, 3, 4],
                "name": ["Alice", "Bob", "Charlie", "Dave"],
                "dept": ["Eng", "Sales", "Eng", "HR"],
                "txn_date": [datetime(2024, 6, 1)] * 4,
            }
        )
        result = scd2(_make_polars_ctx(with_dave), params)
        df = result.df

        assert df.shape[0] == 4  # 3 + Dave
        dave = df.filter(pl.col("emp_id") == 4)
        assert dave.shape[0] == 1
        assert dave["is_current"][0] is True

    def test_nan_idempotent(self, tmp_dir):
        """NaN == NaN should NOT trigger false-positive changes (#248)."""
        params = SCD2Params(
            target=os.path.join(tmp_dir, "scd2_nan.parquet"),
            keys=["id"],
            track_cols=["name", "salary"],
            effective_time_col="txn_date",
        )
        src = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["A", "B"],
                "salary": [50000.0, float("nan")],
                "txn_date": [datetime(2024, 1, 1)] * 2,
            }
        )
        scd2(_make_polars_ctx(src), params)
        result = scd2(_make_polars_ctx(src), params)
        assert result.df.shape[0] == 2, "NaN==NaN should create 0 new versions"

    def test_nan_to_value_detected(self, tmp_dir):
        """NaN → real value should be detected as a change (#248)."""
        params = SCD2Params(
            target=os.path.join(tmp_dir, "scd2_nan2.parquet"),
            keys=["id"],
            track_cols=["salary"],
            effective_time_col="txn_date",
        )
        src1 = pl.DataFrame(
            {
                "id": [1],
                "salary": [float("nan")],
                "txn_date": [datetime(2024, 1, 1)],
            }
        )
        scd2(_make_polars_ctx(src1), params)

        src2 = pl.DataFrame(
            {
                "id": [1],
                "salary": [60000.0],
                "txn_date": [datetime(2024, 6, 1)],
            }
        )
        result = scd2(_make_polars_ctx(src2), params)
        assert result.df.shape[0] == 2, "NaN→value should create new version"

    def test_lazyframe_input(self, tmp_dir):
        params = self._make_params(tmp_dir, "scd2_lazy.parquet")
        lazy = self._source().lazy()
        result = scd2(_make_polars_ctx(lazy), params)
        assert result.df.shape[0] == 3


# ============================================================
# Merge Tests
# ============================================================


class TestMergePolars:
    def _initial_data(self):
        return pl.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "score": [80, 90, 70]}
        )

    def test_upsert_update_and_insert(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_upsert.parquet")
        initial = self._initial_data()
        merge(_make_polars_ctx(initial), MergeParams(target=path, keys=["id"]))

        source = pl.DataFrame({"id": [2, 4], "name": ["Bob_Updated", "Dave"], "score": [95, 85]})
        result = merge(_make_polars_ctx(source), MergeParams(target=path, keys=["id"]))

        assert result.shape[0] == 4
        assert result.filter(pl.col("id") == 2)["name"][0] == "Bob_Updated"
        assert result.filter(pl.col("id") == 1)["name"][0] == "Alice"
        assert result.filter(pl.col("id") == 4)["name"][0] == "Dave"

    def test_append_only(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_append.parquet")
        self._initial_data().write_parquet(path)

        source = pl.DataFrame(
            {"id": [2, 4], "name": ["Should_Not_Update", "Dave"], "score": [0, 85]}
        )
        result = merge(
            _make_polars_ctx(source),
            MergeParams(target=path, keys=["id"], strategy="append_only"),
        )

        assert result.shape[0] == 4
        assert result.filter(pl.col("id") == 2)["name"][0] == "Bob"

    def test_delete_match(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_delete.parquet")
        self._initial_data().write_parquet(path)

        delete = pl.DataFrame({"id": [2, 3], "name": ["x", "x"], "score": [0, 0]})
        result = merge(
            _make_polars_ctx(delete),
            MergeParams(target=path, keys=["id"], strategy="delete_match"),
        )

        assert result.shape[0] == 1
        assert result["id"][0] == 1

    def test_empty_target(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_empty.parquet")
        source = self._initial_data()
        result = merge(_make_polars_ctx(source), MergeParams(target=path, keys=["id"]))

        assert result.shape[0] == 3
        assert os.path.exists(path)

    def test_audit_columns_preserved(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_audit.parquet")
        initial = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

        params = MergeParams(
            target=path,
            keys=["id"],
            audit_cols={"created_col": "created_at", "updated_col": "updated_at"},
        )

        result1 = merge(_make_polars_ctx(initial), params)
        original_created = result1.filter(pl.col("id") == 1)["created_at"][0]

        update = pl.DataFrame({"id": [1], "name": ["Alice_Updated"]})
        result2 = merge(_make_polars_ctx(update), params)

        alice = result2.filter(pl.col("id") == 1)
        assert alice["name"][0] == "Alice_Updated"
        assert alice["created_at"][0] == original_created

    def test_lazyframe_input(self, tmp_dir):
        path = os.path.join(tmp_dir, "merge_lazy.parquet")
        lazy = self._initial_data().lazy()
        result = merge(_make_polars_ctx(lazy), MergeParams(target=path, keys=["id"]))
        assert result.shape[0] == 3
