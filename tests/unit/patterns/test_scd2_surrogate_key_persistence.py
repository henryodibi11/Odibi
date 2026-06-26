"""End-to-end regression for SCD2 Pandas surrogate-key persistence + stability.

These run the REAL scd2 transformer (not a mock) across two loads against a
parquet target, so they exercise the actual write path (DuckDB if installed,
else the pure-pandas fallback). They guard the bug where the surrogate key was
assigned only to the in-memory result and never persisted, so it renumbered
from 1 every run and silently broke fact->dimension joins.
"""

from unittest.mock import MagicMock

import pandas as pd

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.dimension import DimensionPattern


def _context(df):
    engine = MagicMock()
    engine.connections = {}
    return EngineContext(
        context=PandasContext(), df=df, engine_type=EngineType.PANDAS, engine=engine
    )


def _pattern(target):
    cfg = MagicMock(spec=NodeConfig)
    cfg.name = "dim_customer"
    cfg.params = {
        "natural_key": "id",
        "surrogate_key": "sk",
        "scd_type": 2,
        "track_cols": ["name"],
        "target": str(target),
        "audit": {"load_timestamp": False},
    }
    engine = MagicMock()
    engine.connections = {}
    return DimensionPattern(engine=engine, config=cfg)


def test_scd2_surrogate_key_persisted_to_target(tmp_path):
    target = tmp_path / "dim.parquet"

    # Run 1: initial load of two entities.
    _pattern(target).execute(_context(pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"]})))

    persisted = pd.read_parquet(target)
    assert "sk" in persisted.columns, "surrogate key must be persisted in the target"
    assert persisted["sk"].notna().all()
    assert persisted["sk"].nunique() == len(persisted)  # unique keys


def test_scd2_surrogate_keys_stable_across_runs(tmp_path):
    target = tmp_path / "dim.parquet"

    # Run 1.
    _pattern(target).execute(_context(pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"]})))
    after_run1 = pd.read_parquet(target)
    sk_A = after_run1.loc[after_run1["id"] == "A", "sk"].iloc[0]
    sk_B = after_run1.loc[after_run1["id"] == "B", "sk"].iloc[0]
    max_sk_run1 = int(after_run1["sk"].max())

    # Run 2: A changes (new version), B unchanged, C is new.
    _pattern(target).execute(
        _context(pd.DataFrame({"id": ["A", "B", "C"], "name": ["a_v2", "b", "c"]}))
    )
    after_run2 = pd.read_parquet(target)

    # The surrogate key never renumbers from 1: the keys A and B already had keep
    # their original values; brand-new physical rows get keys above the prior max.
    assert sk_A in after_run2["sk"].values, "existing A surrogate key must survive run 2"
    assert sk_B in after_run2["sk"].values, "existing B surrogate key must survive run 2"
    assert after_run2["sk"].notna().all()
    assert after_run2["sk"].is_unique, "surrogate keys must stay unique across runs"
    new_keys = after_run2.loc[~after_run2["sk"].isin(after_run1["sk"]), "sk"]
    assert (new_keys > max_sk_run1).all(), "new rows must get keys above the prior max"


def test_scd2_surrogate_key_is_integer(tmp_path):
    target = tmp_path / "dim.parquet"
    _pattern(target).execute(_context(pd.DataFrame({"id": ["A"], "name": ["a"]})))
    persisted = pd.read_parquet(target)
    assert pd.api.types.is_integer_dtype(persisted["sk"]), "surrogate key should be integer-typed"
