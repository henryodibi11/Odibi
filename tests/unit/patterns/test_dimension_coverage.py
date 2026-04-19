"""Comprehensive unit tests for DimensionPattern (Pandas paths only)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from odibi.config import NodeConfig
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.patterns.dimension import DimensionPattern


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------


def _make_context(df, engine=None):
    """Create a Pandas EngineContext wrapping the given DataFrame."""
    return EngineContext(
        context=PandasContext(),
        df=df,
        engine_type=EngineType.PANDAS,
        engine=engine,
    )


@pytest.fixture
def engine():
    mock = MagicMock()
    mock.connections = {}
    return mock


@pytest.fixture
def cfg():
    """Return a mock NodeConfig with sensible defaults."""
    c = MagicMock(spec=NodeConfig)
    c.name = "test_dim"
    c.params = {}
    return c


def _pattern(engine, cfg):
    return DimensionPattern(engine=engine, config=cfg)


# ===================================================================
# 1. validate() tests
# ===================================================================


class TestValidate:
    def test_valid_scd0_no_track_cols_needed(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
        }
        _pattern(engine, cfg).validate()  # should not raise

    def test_valid_scd1(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
        }
        _pattern(engine, cfg).validate()

    def test_valid_scd2(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
            "target": "gold/dim",
        }
        _pattern(engine, cfg).validate()

    def test_missing_natural_key(self, engine, cfg):
        cfg.params = {"surrogate_key": "sk"}
        with pytest.raises(ValueError, match="natural_key"):
            _pattern(engine, cfg).validate()

    def test_missing_surrogate_key(self, engine, cfg):
        cfg.params = {"natural_key": "id"}
        with pytest.raises(ValueError, match="surrogate_key"):
            _pattern(engine, cfg).validate()

    def test_invalid_scd_type(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 5,
        }
        with pytest.raises(ValueError, match="scd_type"):
            _pattern(engine, cfg).validate()

    def test_scd2_without_target(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
        }
        with pytest.raises(ValueError, match="target"):
            _pattern(engine, cfg).validate()

    def test_scd1_without_track_cols(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
        }
        with pytest.raises(ValueError, match="track_cols"):
            _pattern(engine, cfg).validate()

    def test_scd2_without_track_cols(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "target": "gold/dim",
        }
        with pytest.raises(ValueError, match="track_cols"):
            _pattern(engine, cfg).validate()

    def test_default_scd_type_is_1(self, engine, cfg):
        """When scd_type is omitted it defaults to 1, which requires track_cols."""
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            # scd_type omitted → defaults to 1
        }
        with pytest.raises(ValueError, match="track_cols"):
            _pattern(engine, cfg).validate()


# ===================================================================
# 2. _get_max_sk() tests
# ===================================================================


class TestGetMaxSK:
    def _call(self, engine, cfg, df, sk_col="sk"):
        pattern = _pattern(engine, cfg)
        return pattern._get_max_sk(df, sk_col, EngineType.PANDAS)

    def test_normal_max(self, engine, cfg):
        df = pd.DataFrame({"sk": [1, 5, 3]})
        assert self._call(engine, cfg, df) == 5

    def test_none_df_returns_zero(self, engine, cfg):
        assert self._call(engine, cfg, None) == 0

    def test_missing_column_returns_zero(self, engine, cfg):
        df = pd.DataFrame({"other": [10, 20]})
        assert self._call(engine, cfg, df) == 0

    def test_nan_values_return_zero(self, engine, cfg):
        df = pd.DataFrame({"sk": [np.nan, np.nan]})
        assert self._call(engine, cfg, df) == 0

    def test_empty_dataframe_returns_zero(self, engine, cfg):
        df = pd.DataFrame({"sk": pd.Series([], dtype="int64")})
        assert self._call(engine, cfg, df) == 0

    def test_mixed_nan_and_values(self, engine, cfg):
        df = pd.DataFrame({"sk": [np.nan, 7, np.nan, 2]})
        assert self._call(engine, cfg, df) == 7


# ===================================================================
# 3. _generate_surrogate_keys() tests
# ===================================================================


class TestGenerateSurrogateKeys:
    def _call(self, engine, cfg, df, nk, sk, start):
        pattern = _pattern(engine, cfg)
        ctx = _make_context(df, engine)
        return pattern._generate_surrogate_keys(ctx, df, nk, sk, start)

    def test_sequential_keys_from_one(self, engine, cfg):
        df = pd.DataFrame({"nk": ["B", "A", "C"]})
        result = self._call(engine, cfg, df, "nk", "sk", 0)
        # Sorted by nk → A=1, B=2, C=3
        assert list(result.sort_values("nk")["sk"]) == [1, 2, 3]

    def test_starts_from_offset(self, engine, cfg):
        df = pd.DataFrame({"nk": ["X", "Y"]})
        result = self._call(engine, cfg, df, "nk", "sk", 10)
        assert list(result.sort_values("nk")["sk"]) == [11, 12]

    def test_sorts_by_natural_key(self, engine, cfg):
        df = pd.DataFrame({"nk": ["Z", "A", "M"], "val": [1, 2, 3]})
        result = self._call(engine, cfg, df, "nk", "sk", 0)
        assert list(result["nk"]) == ["A", "M", "Z"]


# ===================================================================
# 4. SCD0 tests
# ===================================================================


class TestSCD0:
    def test_initial_load_generates_all_sks(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["A", "B", "C"], "name": ["a", "b", "c"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert "sk" in result.columns
        assert sorted(result["sk"].tolist()) == [1, 2, 3]

    def test_with_existing_adds_only_new(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame({"id": ["A"], "name": ["a"], "sk": [1]})
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a_new", "b"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 2
        # A should keep original name
        assert result[result["id"] == "A"].iloc[0]["name"] == "a"
        # B gets sk=2
        assert result[result["id"] == "B"].iloc[0]["sk"] == 2

    def test_no_new_records_returns_existing(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"], "sk": [1, 2]})
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["x", "y"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 2
        assert set(result["id"]) == {"A", "B"}
        # Names unchanged (SCD0 never updates)
        assert result[result["id"] == "A"].iloc[0]["name"] == "a"

    def test_mixed_new_and_existing(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"], "sk": [1, 2]})
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["B", "C", "D"], "name": ["b2", "c", "d"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 4  # A, B from existing + C, D new
        new_sks = sorted(result[result["id"].isin(["C", "D"])]["sk"].tolist())
        assert new_sks == [3, 4]

    def test_initial_load_empty_source(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": pd.Series([], dtype="str"), "name": pd.Series([], dtype="str")})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))
        assert len(result) == 0


# ===================================================================
# 5. SCD1 tests
# ===================================================================


class TestSCD1:
    def test_initial_load_generates_all_sks(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["B", "A"], "name": ["b", "a"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert "sk" in result.columns
        assert len(result) == 2

    def test_updates_matching_adds_new(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["A", "B"],
                "name": ["a", "b"],
                "sk": [1, 2],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "C"], "name": ["a_updated", "c"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        # A updated, B unchanged, C new
        assert len(result) == 3
        a_row = result[result["id"] == "A"]
        assert a_row.iloc[0]["name"] == "a_updated"
        assert a_row.iloc[0]["sk"] == 1

        c_row = result[result["id"] == "C"]
        assert c_row.iloc[0]["sk"] == 3

        b_row = result[result["id"] == "B"]
        assert b_row.iloc[0]["name"] == "b"

    def test_only_new_records(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame({"id": ["A"], "name": ["a"], "sk": [1]})
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["B", "C"], "name": ["b", "c"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 3
        new_sks = sorted(result[result["id"].isin(["B", "C"])]["sk"].tolist())
        assert new_sks == [2, 3]

    def test_only_updates_no_new(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["A", "B"],
                "name": ["a", "b"],
                "sk": [1, 2],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a2", "b2"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 2
        assert result[result["id"] == "A"].iloc[0]["name"] == "a2"
        assert result[result["id"] == "B"].iloc[0]["name"] == "b2"

    def test_no_changes_returns_same_data(self, engine, cfg, tmp_path):
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["A", "B"],
                "name": ["a", "b"],
                "sk": [1, 2],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert len(result) == 2
        assert set(result["sk"]) == {1, 2}

    def test_scd1_preserves_sk_on_update(self, engine, cfg, tmp_path):
        """Updated records keep their original SK."""
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["X"],
                "name": ["old"],
                "sk": [42],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 1,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["X"], "name": ["new"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert result.iloc[0]["sk"] == 42
        assert result.iloc[0]["name"] == "new"


# ===================================================================
# 6. SCD2 tests (mock scd2 transformer)
# ===================================================================


class TestSCD2:
    def test_initial_load_no_existing(self, engine, cfg, tmp_path):
        """SCD2 initial load: no existing target, generates SKs for all rows."""
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
            "target": str(tmp_path / "nonexistent.parquet"),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"]})

        # The scd2 transformer returns a result context; mock it.
        def fake_scd2(ctx, params):
            df = ctx.df.copy()
            df["valid_to"] = None
            df["is_current"] = True
            return ctx.with_df(df)

        with patch("odibi.patterns.dimension.scd2", side_effect=fake_scd2):
            result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert "sk" in result.columns
        assert len(result) == 2
        assert sorted(result["sk"].tolist()) == [1, 2]

    def test_scd2_with_existing_generates_new_sks(self, engine, cfg, tmp_path):
        """SCD2 with existing data: new/changed rows get SKs > max existing."""
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["A"],
                "name": ["a"],
                "sk": [1],
                "valid_from": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a_changed", "b"]})

        def fake_scd2(ctx, params):
            ctx.df.copy()
            # Simulate: existing A row closed + new A row + new B row → 3 rows, no SK yet
            rows = pd.DataFrame(
                {
                    "id": ["A", "A", "B"],
                    "name": ["a", "a_changed", "b"],
                    "valid_from": [
                        datetime(2024, 1, 1, tzinfo=timezone.utc),
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc),
                    ],
                    "valid_to": [datetime.now(timezone.utc), None, None],
                    "is_current": [False, True, True],
                }
            )
            return ctx.with_df(rows)

        with patch("odibi.patterns.dimension.scd2", side_effect=fake_scd2):
            result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert "sk" in result.columns
        assert len(result) == 3
        # All should have unique SKs
        assert result["sk"].nunique() == 3

    def test_scd2_null_sk_filled(self, engine, cfg, tmp_path):
        """SCD2: rows with null SK get assigned sequential keys."""
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["A"],
                "name": ["a"],
                "sk": [5],
                "valid_from": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
                "valid_to": [None],
                "is_current": [True],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
            "target": str(target),
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A", "B"], "name": ["a_v2", "b"]})

        def fake_scd2(ctx, params):
            # Return df WITH sk column, some null
            rows = pd.DataFrame(
                {
                    "id": ["A", "A", "B"],
                    "name": ["a", "a_v2", "b"],
                    "sk": [5, np.nan, np.nan],
                    "valid_from": [
                        datetime(2024, 1, 1, tzinfo=timezone.utc),
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc),
                    ],
                    "valid_to": [datetime.now(timezone.utc), None, None],
                    "is_current": [False, True, True],
                }
            )
            return ctx.with_df(rows)

        with patch("odibi.patterns.dimension.scd2", side_effect=fake_scd2):
            result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert result["sk"].notna().all()
        # Original row keeps sk=5, two new rows get 6 and 7
        assert 5 in result["sk"].values
        null_filled = result[result["sk"] > 5]
        assert len(null_filled) == 2
        assert sorted(null_filled["sk"].tolist()) == [6, 7]


# ===================================================================
# 7. Audit column tests
# ===================================================================


class TestAuditColumns:
    def test_default_load_timestamp_true(self, engine, cfg):
        """Dimension override: load_timestamp defaults to True (unlike base)."""
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            # No audit config → dimension defaults load_timestamp=True
        }
        df = pd.DataFrame({"id": ["A"], "name": ["a"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert "load_timestamp" in result.columns
        ts = result["load_timestamp"].iloc[0]
        assert ts is not None and ts is not pd.NaT

    def test_explicit_load_timestamp_false(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["A"], "name": ["a"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert "load_timestamp" not in result.columns

    def test_source_system_added(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "audit": {"load_timestamp": False, "source_system": "erp"},
        }
        df = pd.DataFrame({"id": ["A"], "name": ["a"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert "source_system" in result.columns
        assert result["source_system"].iloc[0] == "erp"


# ===================================================================
# 8. Unknown member tests
# ===================================================================


class TestUnknownMember:
    def test_adds_sk_zero_row(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["A", "B"], "name": ["a", "b"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        assert len(result) == 3
        unknown = result[result["sk"] == 0]
        assert len(unknown) == 1
        assert unknown.iloc[0]["id"] == "-1"

    def test_existing_sk_zero_skips(self, engine, cfg, tmp_path):
        """If SK=0 already exists, do not add another."""
        target = tmp_path / "dim.parquet"
        existing = pd.DataFrame(
            {
                "id": ["-1", "A"],
                "name": ["Unknown", "a"],
                "sk": [0, 1],
            }
        )
        existing.to_parquet(target)

        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "target": str(target),
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["B"], "name": ["b"]})
        result = _pattern(engine, cfg).execute(_make_context(source, engine))

        assert (result["sk"] == 0).sum() == 1

    def test_string_columns_get_unknown(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["A"], "city": ["NYC"], "country": ["US"]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        unknown = result[result["sk"] == 0].iloc[0]
        assert unknown["city"] == "Unknown"
        assert unknown["country"] == "Unknown"

    def test_numeric_columns_get_zero(self, engine, cfg):
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 0,
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }
        df = pd.DataFrame({"id": ["A"], "amount": [100.0], "qty": [5]})
        result = _pattern(engine, cfg).execute(_make_context(df, engine))

        unknown = result[result["sk"] == 0].iloc[0]
        assert unknown["amount"] == 0
        assert unknown["qty"] == 0

    def test_scd2_columns_defaults(self, engine, cfg, tmp_path):
        """Unknown member gets correct valid_from, valid_to, is_current for SCD2."""
        cfg.params = {
            "natural_key": "id",
            "surrogate_key": "sk",
            "scd_type": 2,
            "track_cols": ["name"],
            "target": str(tmp_path / "nonexistent.parquet"),
            "unknown_member": True,
            "audit": {"load_timestamp": False},
        }
        source = pd.DataFrame({"id": ["A"], "name": ["a"]})

        def fake_scd2(ctx, params):
            df = ctx.df.copy()
            df["valid_to"] = None
            df["is_current"] = True
            return ctx.with_df(df)

        with patch("odibi.patterns.dimension.scd2", side_effect=fake_scd2):
            result = _pattern(engine, cfg).execute(_make_context(source, engine))

        unknown = result[result["sk"] == 0].iloc[0]
        assert unknown["valid_from"] == datetime(1900, 1, 1, tzinfo=timezone.utc)
        assert unknown["valid_to"] is None
        assert unknown["is_current"] == True  # noqa: E712 (numpy bool)
