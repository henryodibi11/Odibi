"""Tests for odibi.patterns.fact — Pandas paths only."""

import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.engine.pandas_engine import PandasEngine
from odibi.enums import EngineType
from odibi.patterns.fact import FactPattern


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _engine():
    return PandasEngine(config={})


def _pattern(params: dict) -> FactPattern:
    """Create FactPattern using a mock config to bypass Pydantic validation."""
    engine = _engine()
    cfg = MagicMock()
    cfg.name = "test_fact"
    cfg.params = params
    return FactPattern(engine=engine, config=cfg)


def _ctx(df: pd.DataFrame, dimensions: dict | None = None) -> EngineContext:
    pandas_context = PandasContext()
    if dimensions:
        for name, dim_df in dimensions.items():
            pandas_context.register(name, dim_df)
    return EngineContext(
        context=pandas_context,
        df=df,
        engine_type=EngineType.PANDAS,
        engine=_engine(),
    )


# ---------------------------------------------------------------------------
# validate() tests
# ---------------------------------------------------------------------------


class TestValidate:
    def test_valid_empty_params(self):
        p = _pattern({})
        p.validate()

    def test_deduplicate_without_keys_raises(self):
        p = _pattern({"deduplicate": True})
        with pytest.raises(ValueError, match="keys"):
            p.validate()

    def test_deduplicate_with_keys_ok(self):
        p = _pattern({"deduplicate": True, "keys": ["id"]})
        p.validate()

    def test_invalid_orphan_handling_raises(self):
        p = _pattern({"orphan_handling": "invalid"})
        with pytest.raises(ValueError, match="orphan_handling"):
            p.validate()

    def test_valid_orphan_handling_values(self):
        for oh in ("unknown", "reject", "quarantine"):
            params = {"orphan_handling": oh}
            if oh == "quarantine":
                params["quarantine"] = {"connection": "local", "path": "orphans.parquet"}
            _pattern(params).validate()

    def test_quarantine_without_config_raises(self):
        p = _pattern({"orphan_handling": "quarantine"})
        with pytest.raises(ValueError, match="quarantine"):
            p.validate()

    def test_quarantine_without_connection_raises(self):
        p = _pattern(
            {
                "orphan_handling": "quarantine",
                "quarantine": {"path": "orphans.parquet"},
            }
        )
        with pytest.raises(ValueError, match="connection"):
            p.validate()

    def test_quarantine_without_path_or_table_raises(self):
        p = _pattern(
            {
                "orphan_handling": "quarantine",
                "quarantine": {"connection": "local"},
            }
        )
        with pytest.raises(ValueError, match="path.*table"):
            p.validate()

    def test_dimension_missing_required_key(self):
        for missing_key in ["source_column", "dimension_table", "dimension_key", "surrogate_key"]:
            dim = {
                "source_column": "cust_id",
                "dimension_table": "dim_customer",
                "dimension_key": "customer_id",
                "surrogate_key": "customer_sk",
            }
            del dim[missing_key]
            p = _pattern({"dimensions": [dim]})
            with pytest.raises(ValueError, match=missing_key):
                p.validate()

    def test_valid_dimension_config(self):
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
            }
        )
        p.validate()


# ---------------------------------------------------------------------------
# _deduplicate() tests
# ---------------------------------------------------------------------------


class TestDeduplicate:
    def test_removes_duplicates(self):
        df = pd.DataFrame(
            {
                "id": [1, 1, 2],
                "value": ["a", "b", "c"],
            }
        )
        p = _pattern({"deduplicate": True, "keys": ["id"]})
        ctx = _ctx(df)
        result = p._deduplicate(ctx, df, ["id"])
        assert len(result) == 2

    def test_no_duplicates_unchanged(self):
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        p = _pattern({})
        ctx = _ctx(df)
        result = p._deduplicate(ctx, df, ["id"])
        assert len(result) == 3

    def test_multiple_keys(self):
        df = pd.DataFrame(
            {
                "id": [1, 1, 1],
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "value": [10, 20, 30],
            }
        )
        p = _pattern({})
        ctx = _ctx(df)
        result = p._deduplicate(ctx, df, ["id", "date"])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Dimension lookup tests
# ---------------------------------------------------------------------------


class TestDimensionLookup:
    def _dim_df(self):
        return pd.DataFrame(
            {
                "customer_id": [1, 2, 3],
                "customer_sk": [101, 102, 103],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )

    def _fact_df(self):
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "cust_id": [1, 2, 3, 99],
                "amount": [10, 20, 30, 40],
            }
        )

    def test_basic_lookup_adds_sk(self):
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
                "orphan_handling": "unknown",
            }
        )
        ctx = _ctx(self._fact_df(), {"dim_customer": self._dim_df()})
        result = p.execute(ctx)
        assert "customer_sk" in result.columns
        # Orphan (cust_id=99) should have SK=0
        orphan_row = result[result["cust_id"] == 99]
        assert orphan_row["customer_sk"].iloc[0] == 0

    def test_scd2_dimension_filters_is_current(self):
        dim = pd.DataFrame(
            {
                "customer_id": [1, 1, 2],
                "customer_sk": [101, 102, 103],
                "is_current": [False, True, True],
            }
        )
        fact = pd.DataFrame({"cust_id": [1, 2], "amount": [10, 20]})
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                        "scd2": True,
                    }
                ],
                "orphan_handling": "unknown",
            }
        )
        ctx = _ctx(fact, {"dim_customer": dim})
        result = p.execute(ctx)
        # cust_id=1 should get SK=102 (is_current=True), not 101
        row1 = result[result["cust_id"] == 1]
        assert row1["customer_sk"].iloc[0] == 102

    def test_orphan_reject_raises(self):
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
                "orphan_handling": "reject",
            }
        )
        ctx = _ctx(self._fact_df(), {"dim_customer": self._dim_df()})
        with pytest.raises(ValueError, match="orphan"):
            p.execute(ctx)

    def test_orphan_quarantine_separates(self):
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
                "orphan_handling": "quarantine",
                "quarantine": {
                    "connection": "local",
                    "path": "orphans.parquet",
                    "add_columns": {
                        "_rejection_reason": True,
                        "_rejected_at": True,
                        "_source_dimension": True,
                    },
                },
            }
        )
        # Mock _write_quarantine to avoid file IO
        p._write_quarantine = MagicMock()
        ctx = _ctx(self._fact_df(), {"dim_customer": self._dim_df()})
        result = p.execute(ctx)
        # Orphan row (cust_id=99) should be removed from result
        assert len(result) == 3
        assert 99 not in result["cust_id"].values
        # Quarantine write should have been called
        p._write_quarantine.assert_called_once()

    def test_missing_dimension_raises(self):
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_nonexistent",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
            }
        )
        ctx = _ctx(self._fact_df())
        with pytest.raises(ValueError, match="not found"):
            p.execute(ctx)

    def test_all_records_match_no_orphans(self):
        fact = pd.DataFrame({"cust_id": [1, 2, 3], "amount": [10, 20, 30]})
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
                "orphan_handling": "unknown",
            }
        )
        ctx = _ctx(fact, {"dim_customer": self._dim_df()})
        result = p.execute(ctx)
        assert len(result) == 3
        assert (result["customer_sk"] != 0).all()

    def test_multiple_dimension_lookups(self):
        dim_cust = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "customer_sk": [101, 102],
            }
        )
        dim_prod = pd.DataFrame(
            {
                "product_id": [10, 20],
                "product_sk": [201, 202],
            }
        )
        fact = pd.DataFrame(
            {
                "cust_id": [1, 2],
                "prod_id": [10, 20],
                "amount": [100, 200],
            }
        )
        p = _pattern(
            {
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    },
                    {
                        "source_column": "prod_id",
                        "dimension_table": "dim_product",
                        "dimension_key": "product_id",
                        "surrogate_key": "product_sk",
                    },
                ],
                "orphan_handling": "unknown",
            }
        )
        ctx = _ctx(fact, {"dim_customer": dim_cust, "dim_product": dim_prod})
        result = p.execute(ctx)
        assert "customer_sk" in result.columns
        assert "product_sk" in result.columns
        assert len(result) == 2


# ---------------------------------------------------------------------------
# _apply_measures() tests
# ---------------------------------------------------------------------------


class TestMeasures:
    def test_string_passthrough(self):
        df = pd.DataFrame({"quantity": [10, 20], "price": [5, 10]})
        p = _pattern({"measures": ["quantity"]})
        ctx = _ctx(df)
        result = p._apply_measures(ctx, df, ["quantity"])
        assert "quantity" in result.columns

    def test_dict_rename(self):
        df = pd.DataFrame({"qty": [10, 20]})
        p = _pattern({})
        ctx = _ctx(df)
        result = p._apply_measures(ctx, df, [{"quantity": "qty"}])
        assert "quantity" in result.columns

    def test_dict_calculated_expression(self):
        df = pd.DataFrame({"quantity": [10, 20], "price": [5, 10]})
        p = _pattern({})
        ctx = _ctx(df)
        result = p._apply_measures(ctx, df, [{"total": "quantity * price"}])
        assert "total" in result.columns
        assert result["total"].iloc[0] == 50
        assert result["total"].iloc[1] == 200

    def test_is_expression_parentheses(self):
        p = _pattern({})
        assert p._is_expression("SUM(amount)") is True

    def test_is_expression_spaced_operators(self):
        p = _pattern({})
        assert p._is_expression("quantity * price") is True
        assert p._is_expression("a + b") is True
        assert p._is_expression("a - b") is True
        assert p._is_expression("a / b") is True

    def test_is_expression_column_name_with_hyphen(self):
        p = _pattern({})
        # Hyphen without spaces = column name, NOT expression
        assert p._is_expression("total-cost") is False

    def test_mixed_measures(self):
        df = pd.DataFrame(
            {
                "quantity": [10, 20],
                "price": [5, 10],
                "old_name": [1, 2],
            }
        )
        p = _pattern({})
        ctx = _ctx(df)
        measures = [
            "quantity",
            {"renamed": "old_name"},
            {"total": "quantity * price"},
        ]
        result = p._apply_measures(ctx, df, measures)
        assert "quantity" in result.columns
        assert "renamed" in result.columns
        assert "total" in result.columns


# ---------------------------------------------------------------------------
# _validate_grain() tests
# ---------------------------------------------------------------------------


class TestGrainValidation:
    def test_valid_grain_no_duplicates(self):
        df = pd.DataFrame({"order_id": [1, 2, 3], "amount": [10, 20, 30]})
        p = _pattern({})
        ctx = _ctx(df)
        p._validate_grain(ctx, df, ["order_id"])  # should not raise

    def test_duplicate_grain_raises(self):
        df = pd.DataFrame({"order_id": [1, 1, 2], "amount": [10, 20, 30]})
        p = _pattern({})
        ctx = _ctx(df)
        with pytest.raises(ValueError, match="duplicate"):
            p._validate_grain(ctx, df, ["order_id"])

    def test_multiple_grain_columns(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01", "2024-01-02"],
                "product": ["A", "B", "A"],
                "amount": [10, 20, 30],
            }
        )
        p = _pattern({})
        ctx = _ctx(df)
        p._validate_grain(ctx, df, ["date", "product"])  # should not raise

    def test_multiple_grain_with_duplicates(self):
        df = pd.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-01"],
                "product": ["A", "A"],
                "amount": [10, 20],
            }
        )
        p = _pattern({})
        ctx = _ctx(df)
        with pytest.raises(ValueError, match="duplicate"):
            p._validate_grain(ctx, df, ["date", "product"])


# ---------------------------------------------------------------------------
# Quarantine metadata tests
# ---------------------------------------------------------------------------


class TestQuarantineMetadata:
    def test_rejection_reason_added(self):
        df = pd.DataFrame({"cust_id": [99], "amount": [40]})
        p = _pattern({})
        result = p._add_quarantine_metadata_pandas(
            df,
            "dim_customer",
            "cust_id",
            {"add_columns": {"_rejection_reason": True}},
        )
        assert "_rejection_reason" in result.columns
        assert "dim_customer" in result["_rejection_reason"].iloc[0]

    def test_rejected_at_added(self):
        df = pd.DataFrame({"cust_id": [99], "amount": [40]})
        p = _pattern({})
        result = p._add_quarantine_metadata_pandas(
            df,
            "dim_customer",
            "cust_id",
            {"add_columns": {"_rejected_at": True}},
        )
        assert "_rejected_at" in result.columns
        assert result["_rejected_at"].iloc[0].tzinfo is not None

    def test_source_dimension_added(self):
        df = pd.DataFrame({"cust_id": [99], "amount": [40]})
        p = _pattern({})
        result = p._add_quarantine_metadata_pandas(
            df,
            "dim_customer",
            "cust_id",
            {"add_columns": {"_source_dimension": True}},
        )
        assert "_source_dimension" in result.columns
        assert result["_source_dimension"].iloc[0] == "dim_customer"

    def test_no_metadata_columns(self):
        df = pd.DataFrame({"cust_id": [99], "amount": [40]})
        p = _pattern({})
        result = p._add_quarantine_metadata_pandas(
            df,
            "dim_customer",
            "cust_id",
            {},
        )
        assert "_rejection_reason" not in result.columns


# ---------------------------------------------------------------------------
# Quarantine write tests
# ---------------------------------------------------------------------------


class TestQuarantineWrite:
    def test_write_csv_new(self, tmp_path):
        df = pd.DataFrame({"id": [1], "reason": ["orphan"]})
        p = _pattern({})
        path = str(tmp_path / "orphans.csv")
        ctx = _ctx(df)
        p._write_quarantine_pandas(ctx, df, "local", path, None)
        assert os.path.exists(path)
        loaded = pd.read_csv(path)
        assert len(loaded) == 1

    def test_write_csv_append(self, tmp_path):
        path = str(tmp_path / "orphans.csv")
        pd.DataFrame({"id": [1]}).to_csv(path, index=False)
        df = pd.DataFrame({"id": [2]})
        p = _pattern({})
        ctx = _ctx(df)
        p._write_quarantine_pandas(ctx, df, "local", path, None)
        loaded = pd.read_csv(path)
        assert len(loaded) == 2

    def test_write_parquet(self, tmp_path):
        df = pd.DataFrame({"id": [1], "reason": ["orphan"]})
        p = _pattern({})
        path = str(tmp_path / "orphans.parquet")
        ctx = _ctx(df)
        p._write_quarantine_pandas(ctx, df, "local", path, None)
        assert os.path.exists(path)
        loaded = pd.read_parquet(path)
        assert len(loaded) == 1

    def test_write_json(self, tmp_path):
        df = pd.DataFrame({"id": [1], "reason": ["orphan"]})
        p = _pattern({})
        path = str(tmp_path / "orphans.json")
        ctx = _ctx(df)
        p._write_quarantine_pandas(ctx, df, "local", path, None)
        assert os.path.exists(path)

    def test_write_parquet_append(self, tmp_path):
        path = str(tmp_path / "orphans.parquet")
        pd.DataFrame({"id": [1]}).to_parquet(path, index=False)
        df = pd.DataFrame({"id": [2]})
        p = _pattern({})
        ctx = _ctx(df)
        p._write_quarantine_pandas(ctx, df, "local", path, None)
        loaded = pd.read_parquet(path)
        assert len(loaded) == 2


# ---------------------------------------------------------------------------
# Audit column tests
# ---------------------------------------------------------------------------


class TestAudit:
    def test_load_timestamp(self):
        df = pd.DataFrame({"id": [1, 2]})
        p = _pattern({"audit": {"load_timestamp": True}})
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert "load_timestamp" in result.columns

    def test_source_system(self):
        df = pd.DataFrame({"id": [1, 2]})
        p = _pattern({"audit": {"source_system": "erp"}})
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "erp"

    def test_no_audit(self):
        df = pd.DataFrame({"id": [1, 2]})
        p = _pattern({})
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert "load_timestamp" not in result.columns


# ---------------------------------------------------------------------------
# End-to-end execute() tests
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_simple_fact_no_dimensions(self):
        df = pd.DataFrame({"order_id": [1, 2], "amount": [10, 20]})
        p = _pattern({})
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert len(result) == 2

    def test_full_pipeline(self):
        dim = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "customer_sk": [101, 102],
            }
        )
        fact = pd.DataFrame(
            {
                "order_id": [1, 1, 2, 3],
                "cust_id": [1, 1, 2, 99],
                "amount": [10, 10, 20, 30],
            }
        )
        p = _pattern(
            {
                "deduplicate": True,
                "keys": ["order_id"],
                "grain": ["order_id"],
                "dimensions": [
                    {
                        "source_column": "cust_id",
                        "dimension_table": "dim_customer",
                        "dimension_key": "customer_id",
                        "surrogate_key": "customer_sk",
                    }
                ],
                "orphan_handling": "unknown",
                "measures": ["amount", {"total_amount": "amount"}],
                "audit": {"load_timestamp": True, "source_system": "pos"},
            }
        )
        ctx = _ctx(fact, {"dim_customer": dim})
        result = p.execute(ctx)
        assert "customer_sk" in result.columns
        assert "load_timestamp" in result.columns
        assert "source_system" in result.columns
        # Orphan (cust_id=99) gets SK=0
        orphan_row = result[result["cust_id"] == 99]
        assert orphan_row["customer_sk"].iloc[0] == 0

    def test_fact_with_dedup_and_grain(self):
        df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "amount": [10, 20, 30],
            }
        )
        p = _pattern(
            {
                "deduplicate": True,
                "keys": ["order_id"],
                "grain": ["order_id"],
            }
        )
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert len(result) == 3

    def test_fact_with_calculated_measure(self):
        df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "quantity": [5, 10],
                "price": [2.0, 3.0],
            }
        )
        p = _pattern(
            {
                "measures": [{"total": "quantity * price"}],
            }
        )
        ctx = _ctx(df)
        result = p.execute(ctx)
        assert "total" in result.columns
        assert result["total"].iloc[0] == 10.0
        assert result["total"].iloc[1] == 30.0
