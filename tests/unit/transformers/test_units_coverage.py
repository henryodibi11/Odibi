"""Additional coverage tests for odibi.transformers.units (lines 276-540)."""

import logging

import numpy as np
import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.units import (
    ConversionSpec,
    UnitConvertParams,
    _convert_column_pandas,
    _convert_column_polars,
    convert,
    list_units,
    unit_convert,
)

from odibi.transformers.units import (
    _convert_value,
    _parse_gauge_offset,
    get_unit_registry,
)

logging.getLogger("odibi").propagate = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pandas_context(df: pd.DataFrame) -> EngineContext:
    ctx = PandasContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.PANDAS)


# ---------------------------------------------------------------------------
# _convert_column_polars (lines 328-365)
# ---------------------------------------------------------------------------


class TestConvertColumnPolars:
    """Cover _convert_column_polars branches not hit by existing tests."""

    def test_basic_temperature_conversion(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"temp": [32.0, 212.0, -40.0]})
        result = _convert_column_polars(df, "temp", "degF", "degC", "temp_c", 14.696, "ignore")
        assert result["temp_c"].to_list() == pytest.approx([0.0, 100.0, -40.0], rel=1e-3)

    def test_psig_from_unit(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"p": [0.0, 50.0]})
        result = _convert_column_polars(df, "p", "psig", "psi", "p_abs", 14.696, "ignore")
        assert result["p_abs"].to_list() == pytest.approx([14.696, 64.696], rel=1e-3)

    def test_to_psig_conversion(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"p": [14.696, 64.696]})
        result = _convert_column_polars(df, "p", "psi", "psig", "p_gauge", 14.696, "ignore")
        assert result["p_gauge"].to_list() == pytest.approx([0.0, 50.0], rel=1e-3)

    def test_errors_raise(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"x": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Failed to convert"):
            _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "raise")

    def test_errors_null(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"x": [1.0, 2.0]})
        result = _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "null")
        assert result["out"].to_list() == [None, None]

    def test_errors_coerce_returns_original(self):
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"x": [1.0, 2.0]})
        result = _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "ignore")
        # errors="ignore" returns original df unchanged (no "out" column added)
        assert "out" not in result.columns


# ---------------------------------------------------------------------------
# unit_convert main function (lines 373-430)
# ---------------------------------------------------------------------------


class TestUnitConvertPandas:
    """Cover unit_convert Pandas engine path with EngineContext pattern."""

    def test_single_conversion(self):
        df = pd.DataFrame({"temp_f": [32.0, 212.0]})
        ctx = _make_pandas_context(df)
        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )
        result = unit_convert(ctx, params)
        assert result.df["temp_c"].values == pytest.approx([0.0, 100.0], rel=1e-3)

    def test_multiple_conversions(self):
        df = pd.DataFrame({"temp": [212.0], "pressure": [14.696]})
        ctx = _make_pandas_context(df)
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
                "pressure": ConversionSpec(from_unit="psia", to="bar", output="p_bar"),
            }
        )
        result = unit_convert(ctx, params)
        assert result.df["temp_c"].iloc[0] == pytest.approx(100.0, rel=1e-3)
        assert result.df["p_bar"].iloc[0] == pytest.approx(1.01325, rel=1e-3)


class TestUnitConvertPolars:
    """Cover unit_convert Polars engine path."""

    def test_polars_engine_path(self):
        pl = pytest.importorskip("polars")
        from odibi.context import PolarsContext

        df = pl.DataFrame({"temp_f": [32.0, 212.0]})
        ctx = PolarsContext()
        engine_ctx = EngineContext(context=ctx, df=df, engine_type=EngineType.POLARS)
        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )
        result = unit_convert(engine_ctx, params)
        assert result.df["temp_c"].to_list() == pytest.approx([0.0, 100.0], rel=1e-3)


class TestUnitConvertUnsupportedEngine:
    """Cover unsupported engine branch."""

    def test_raises_value_error(self):
        df = pd.DataFrame({"temp": [100.0]})
        ctx = PandasContext()
        engine_ctx = EngineContext(context=ctx, df=df, engine_type="unknown")
        params = UnitConvertParams(
            conversions={"temp": ConversionSpec(from_unit="degF", to="degC")}
        )
        with pytest.raises(ValueError, match="Unsupported engine"):
            unit_convert(engine_ctx, params)


# ---------------------------------------------------------------------------
# Utility functions (lines 438-540)
# ---------------------------------------------------------------------------


class TestConvertUtility:
    """Cover convert() utility function."""

    def test_temperature(self):
        assert convert(212, "degF", "degC") == pytest.approx(100.0, rel=1e-3)

    def test_pressure(self):
        assert convert(14.696, "psia", "bar") == pytest.approx(1.01325, rel=1e-3)

    def test_energy(self):
        assert convert(1, "mmbtu", "GJ") == pytest.approx(1.055, rel=1e-2)

    def test_psig_to_bar_via_convert_value(self):
        # convert() doesn't handle psig specially, but _convert_value does.
        # Use _convert_column_pandas to test psig→bar path.
        df = pd.DataFrame({"p": [0.0]})
        _convert_column_pandas(df, "p", "psig", "bar", "p_bar", 14.696, "null")
        assert df["p_bar"].iloc[0] == pytest.approx(1.01325, rel=1e-2)


class TestListUnits:
    """Cover list_units() utility function."""

    def test_with_category_pressure(self):
        result = list_units("pressure")
        assert isinstance(result, list)
        assert "psi" in result
        assert "psig" in result

    def test_with_category_energy(self):
        result = list_units("energy")
        assert isinstance(result, list)
        assert "BTU" in result

    def test_without_category_returns_dict(self):
        result = list_units()
        assert isinstance(result, dict)
        assert "pressure" in result
        assert "temperature" in result
        assert "energy" in result

    def test_invalid_category_returns_empty(self):
        result = list_units("nonexistent_category")
        assert result == []


# ---------------------------------------------------------------------------
# UnitConvertParams model validation (lines 87-174)
# ---------------------------------------------------------------------------


class TestUnitConvertParamsValidation:
    """Cover UnitConvertParams pydantic validation."""

    def test_valid_params(self):
        params = UnitConvertParams(conversions={"col": ConversionSpec(from_unit="psi", to="bar")})
        assert params.errors == "null"
        assert len(params.conversions) == 1

    def test_multiple_conversions(self):
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
                "pressure": ConversionSpec(from_unit="psia", to="bar", output="p_bar"),
            }
        )
        assert len(params.conversions) == 2
        assert params.conversions["temp"].output == "temp_c"
        assert params.conversions["pressure"].from_unit == "psia"

    def test_invalid_errors_value_raises(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="errors"):
            UnitConvertParams(
                conversions={"col": ConversionSpec(from_unit="psi", to="bar")},
                errors="bad_value",
            )


# ---------------------------------------------------------------------------
# _parse_gauge_offset (lines 182-192)
# ---------------------------------------------------------------------------


class TestParseGaugeOffset:
    def test_valid_offset(self):
        result = _parse_gauge_offset("14.696 psia")
        assert result == pytest.approx(14.696, rel=1e-3)

    def test_custom_offset(self):
        result = _parse_gauge_offset("1 bar")
        assert result == pytest.approx(14.5038, rel=1e-2)

    def test_invalid_offset_returns_default(self):
        result = _parse_gauge_offset("not_a_unit")
        assert result == pytest.approx(14.696, rel=1e-3)


# ---------------------------------------------------------------------------
# _convert_value (lines 195-222)
# ---------------------------------------------------------------------------


class TestConvertValue:
    def test_basic_conversion(self):
        # _convert_value uses multiplication (value * ureg), which doesn't work with
        # offset units like degC. Use non-offset units for this test.
        result = _convert_value(1.0, "psi", "bar")
        assert result == pytest.approx(0.06895, rel=1e-2)

    def test_psig_from_unit(self):
        # psig 0 = 14.696 psi absolute
        result = _convert_value(0.0, "psig", "psi")
        assert result == pytest.approx(14.696, rel=1e-3)

    def test_to_psig(self):
        # 14.696 psi absolute = 0 psig
        result = _convert_value(14.696, "psi", "psig")
        assert result == pytest.approx(0.0, rel=1e-2)

    def test_none_returns_none(self):
        result = _convert_value(None, "degF", "degC")
        assert result is None

    def test_nan_returns_none(self):
        result = _convert_value(float("nan"), "degF", "degC")
        assert result is None

    def test_custom_gauge_offset(self):
        result = _convert_value(0.0, "psig", "psi", gauge_offset=15.0)
        assert result == pytest.approx(15.0, rel=1e-3)


# ---------------------------------------------------------------------------
# _convert_column_pandas error paths (lines 258-262)
# ---------------------------------------------------------------------------


class TestConvertColumnPandasErrors:
    def test_errors_raise(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Failed to convert"):
            _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "raise")

    def test_errors_null(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "null")
        assert all(np.isnan(df["out"]))

    def test_errors_ignore(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "ignore")
        # Column should not be added when errors="ignore"
        assert "out" not in df.columns

    def test_psig_to_output(self):
        """Test the to_psig path in _convert_column_pandas (line 254)."""
        df = pd.DataFrame({"p": [14.696, 29.392]})
        _convert_column_pandas(df, "p", "psi", "psig", "p_gauge", 14.696, "null")
        assert df["p_gauge"].iloc[0] == pytest.approx(0.0, abs=0.1)
        assert df["p_gauge"].iloc[1] == pytest.approx(14.696, abs=0.1)


# ---------------------------------------------------------------------------
# get_unit_registry (line 59)
# ---------------------------------------------------------------------------


class TestGetUnitRegistry:
    def test_returns_registry(self):
        ureg = get_unit_registry()
        assert ureg is not None
        # Verify our custom units are defined
        q = ureg.Quantity(1, "mmbtu")
        assert q.to("BTU").magnitude == pytest.approx(1e6, rel=1e-3)


# ---------------------------------------------------------------------------
# unit_convert with in-place (no output col) and gauge_pressure_offset
# ---------------------------------------------------------------------------


class TestUnitConvertInPlace:
    def test_overwrite_source_column(self):
        df = pd.DataFrame({"temp": [32.0, 212.0]})
        ctx = _make_pandas_context(df)
        params = UnitConvertParams(
            conversions={"temp": ConversionSpec(from_unit="degF", to="degC")}
        )
        result = unit_convert(ctx, params)
        assert result.df["temp"].values == pytest.approx([0.0, 100.0], rel=1e-3)

    def test_custom_gauge_pressure_offset(self):
        df = pd.DataFrame({"p": [0.0]})
        ctx = _make_pandas_context(df)
        params = UnitConvertParams(
            conversions={"p": ConversionSpec(from_unit="psig", to="psi", output="p_abs")},
            gauge_pressure_offset="15.0 psia",
        )
        result = unit_convert(ctx, params)
        assert result.df["p_abs"].iloc[0] == pytest.approx(15.0, rel=1e-2)
