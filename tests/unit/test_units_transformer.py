"""Tests for odibi.transformers.units — unit conversion transformer."""

import numpy as np
import pandas as pd
import polars as pl
import pint
import pytest

from odibi.context import EngineContext, PandasContext, PolarsContext
from odibi.enums import EngineType
from odibi.transformers.units import (
    ConversionSpec,
    UnitConvertParams,
    _convert_column_pandas,
    _convert_column_polars,
    _convert_value,
    _parse_gauge_offset,
    convert,
    get_unit_registry,
    list_units,
    unit_convert,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pandas_context(df: pd.DataFrame) -> EngineContext:
    ctx = PandasContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.PANDAS)


def _make_polars_context(df: pl.DataFrame) -> EngineContext:
    ctx = PolarsContext()
    return EngineContext(context=ctx, df=df, engine_type=EngineType.POLARS)


# ---------------------------------------------------------------------------
# ConversionSpec
# ---------------------------------------------------------------------------


class TestConversionSpec:
    def test_from_alias(self):
        spec = ConversionSpec(**{"from": "psig", "to": "bar"})
        assert spec.from_unit == "psig"
        assert spec.to == "bar"

    def test_output_optional(self):
        spec = ConversionSpec(**{"from": "degF", "to": "degC"})
        assert spec.output is None

    def test_output_specified(self):
        spec = ConversionSpec(**{"from": "degF", "to": "degC", "output": "temp_c"})
        assert spec.output == "temp_c"


# ---------------------------------------------------------------------------
# UnitConvertParams
# ---------------------------------------------------------------------------


class TestUnitConvertParams:
    def test_valid_errors_null(self):
        p = UnitConvertParams(
            conversions={"col": ConversionSpec(**{"from": "psi", "to": "bar"})},
            errors="null",
        )
        assert p.errors == "null"

    def test_valid_errors_raise(self):
        p = UnitConvertParams(
            conversions={"col": ConversionSpec(**{"from": "psi", "to": "bar"})},
            errors="raise",
        )
        assert p.errors == "raise"

    def test_valid_errors_ignore(self):
        p = UnitConvertParams(
            conversions={"col": ConversionSpec(**{"from": "psi", "to": "bar"})},
            errors="ignore",
        )
        assert p.errors == "ignore"

    def test_invalid_errors(self):
        with pytest.raises(ValueError, match="errors must be"):
            UnitConvertParams(
                conversions={"col": ConversionSpec(**{"from": "psi", "to": "bar"})},
                errors="coerce",
            )


# ---------------------------------------------------------------------------
# _parse_gauge_offset
# ---------------------------------------------------------------------------


class TestParseGaugeOffset:
    def test_valid_string(self):
        result = _parse_gauge_offset("14.696 psia")
        assert result == pytest.approx(14.696, rel=1e-3)

    def test_custom_value(self):
        result = _parse_gauge_offset("1 atm")
        assert result == pytest.approx(14.696, rel=1e-2)

    def test_invalid_string_returns_default(self):
        result = _parse_gauge_offset("not_a_unit")
        assert result == pytest.approx(14.696)


# ---------------------------------------------------------------------------
# _convert_value
# ---------------------------------------------------------------------------


class TestConvertValue:
    def test_standard_conversion(self):
        result = _convert_value(1.0, "meter", "foot")
        assert result == pytest.approx(3.28084, rel=1e-3)

    def test_psig_to_psia(self):
        result = _convert_value(0.0, "psig", "psia", gauge_offset=14.696)
        assert result == pytest.approx(14.696, rel=1e-3)

    def test_anything_to_psig(self):
        result = _convert_value(14.696, "psia", "psig", gauge_offset=14.696)
        assert result == pytest.approx(0.0, abs=1e-3)

    def test_nan_returns_none(self):
        assert _convert_value(float("nan"), "degF", "degC") is None

    def test_none_returns_none(self):
        assert _convert_value(None, "degF", "degC") is None


# ---------------------------------------------------------------------------
# _convert_column_pandas
# ---------------------------------------------------------------------------


class TestConvertColumnPandas:
    def test_temperature_conversion(self):
        df = pd.DataFrame({"temp": [32.0, 212.0, -40.0]})
        _convert_column_pandas(df, "temp", "degF", "degC", "temp_c", 14.696, "null")
        assert df["temp_c"].values == pytest.approx([0.0, 100.0, -40.0], rel=1e-3)

    def test_pressure_psig_to_bar(self):
        df = pd.DataFrame({"p": [0.0, 14.696]})
        _convert_column_pandas(df, "p", "psig", "bar", "p_bar", 14.696, "null")
        expected_0 = 14.696 * 0.0689476
        expected_1 = (14.696 + 14.696) * 0.0689476
        assert df["p_bar"].values[0] == pytest.approx(expected_0, rel=1e-2)
        assert df["p_bar"].values[1] == pytest.approx(expected_1, rel=1e-2)

    def test_error_raise(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        with pytest.raises(ValueError):
            _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "raise")

    def test_error_null(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "null")
        assert all(np.isnan(df["out"]))

    def test_error_ignore(self):
        df = pd.DataFrame({"x": [1.0, 2.0]})
        _convert_column_pandas(df, "x", "degF", "meter", "out", 14.696, "ignore")
        assert "out" not in df.columns


# ---------------------------------------------------------------------------
# _convert_column_polars
# ---------------------------------------------------------------------------


class TestConvertColumnPolars:
    def test_temperature_conversion(self):
        df = pl.DataFrame({"temp": [32.0, 212.0, -40.0]})
        result = _convert_column_polars(df, "temp", "degF", "degC", "temp_c", 14.696, "null")
        assert result["temp_c"].to_list() == pytest.approx([0.0, 100.0, -40.0], rel=1e-3)

    def test_pressure_psig_to_bar(self):
        df = pl.DataFrame({"p": [0.0, 14.696]})
        result = _convert_column_polars(df, "p", "psig", "bar", "p_bar", 14.696, "null")
        expected_0 = 14.696 * 0.0689476
        expected_1 = (14.696 + 14.696) * 0.0689476
        assert result["p_bar"].to_list()[0] == pytest.approx(expected_0, rel=1e-2)
        assert result["p_bar"].to_list()[1] == pytest.approx(expected_1, rel=1e-2)

    def test_error_raise(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        with pytest.raises(ValueError):
            _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "raise")

    def test_error_null(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        result = _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "null")
        assert result["out"].to_list() == [None, None]

    def test_error_ignore(self):
        df = pl.DataFrame({"x": [1.0, 2.0]})
        result = _convert_column_polars(df, "x", "degF", "meter", "out", 14.696, "ignore")
        assert "out" not in result.columns


# ---------------------------------------------------------------------------
# unit_convert — Pandas path
# ---------------------------------------------------------------------------


class TestUnitConvertPandas:
    def test_multiple_conversions(self):
        df = pd.DataFrame({"temp": [212.0], "pressure": [0.0]})
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(**{"from": "degF", "to": "degC", "output": "temp_c"}),
                "pressure": ConversionSpec(
                    **{"from": "psig", "to": "bar", "output": "pressure_bar"}
                ),
            }
        )
        ctx = _make_pandas_context(df)
        result = unit_convert(ctx, params)
        assert "temp_c" in result.df.columns
        assert "pressure_bar" in result.df.columns
        assert result.df["temp_c"].iloc[0] == pytest.approx(100.0, rel=1e-3)

    def test_in_place(self):
        df = pd.DataFrame({"temp": [32.0, 212.0]})
        params = UnitConvertParams(
            conversions={"temp": ConversionSpec(**{"from": "degF", "to": "degC"})}
        )
        ctx = _make_pandas_context(df)
        result = unit_convert(ctx, params)
        assert result.df["temp"].values == pytest.approx([0.0, 100.0], rel=1e-3)

    def test_with_output_column(self):
        df = pd.DataFrame({"temp": [32.0]})
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(**{"from": "degF", "to": "degC", "output": "temp_c"})
            }
        )
        ctx = _make_pandas_context(df)
        result = unit_convert(ctx, params)
        assert result.df["temp"].iloc[0] == pytest.approx(32.0)
        assert result.df["temp_c"].iloc[0] == pytest.approx(0.0, abs=0.1)


# ---------------------------------------------------------------------------
# unit_convert — Polars path
# ---------------------------------------------------------------------------


class TestUnitConvertPolars:
    def test_multiple_conversions(self):
        df = pl.DataFrame({"temp": [212.0], "pressure": [0.0]})
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(**{"from": "degF", "to": "degC", "output": "temp_c"}),
                "pressure": ConversionSpec(
                    **{"from": "psig", "to": "bar", "output": "pressure_bar"}
                ),
            }
        )
        ctx = _make_polars_context(df)
        result = unit_convert(ctx, params)
        assert "temp_c" in result.df.columns
        assert "pressure_bar" in result.df.columns
        assert result.df["temp_c"].to_list()[0] == pytest.approx(100.0, rel=1e-3)

    def test_in_place(self):
        df = pl.DataFrame({"temp": [32.0, 212.0]})
        params = UnitConvertParams(
            conversions={"temp": ConversionSpec(**{"from": "degF", "to": "degC"})}
        )
        ctx = _make_polars_context(df)
        result = unit_convert(ctx, params)
        assert result.df["temp"].to_list() == pytest.approx([0.0, 100.0], rel=1e-3)

    def test_with_output_column(self):
        df = pl.DataFrame({"temp": [32.0]})
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(**{"from": "degF", "to": "degC", "output": "temp_c"})
            }
        )
        ctx = _make_polars_context(df)
        result = unit_convert(ctx, params)
        assert result.df["temp"].to_list()[0] == pytest.approx(32.0)
        assert result.df["temp_c"].to_list()[0] == pytest.approx(0.0, abs=0.1)


# ---------------------------------------------------------------------------
# unit_convert — unsupported engine
# ---------------------------------------------------------------------------


class TestUnitConvertUnsupportedEngine:
    def test_raises_value_error(self):
        df = pd.DataFrame({"temp": [100.0]})
        ctx = PandasContext()
        engine_ctx = EngineContext(context=ctx, df=df, engine_type="unknown")
        params = UnitConvertParams(
            conversions={"temp": ConversionSpec(**{"from": "degF", "to": "degC"})}
        )
        with pytest.raises(ValueError, match="Unsupported engine"):
            unit_convert(engine_ctx, params)


# ---------------------------------------------------------------------------
# convert utility
# ---------------------------------------------------------------------------


class TestConvertUtility:
    def test_degf_to_degc(self):
        assert convert(212, "degF", "degC") == pytest.approx(100.0, rel=1e-3)

    def test_psia_to_bar(self):
        assert convert(14.696, "psia", "bar") == pytest.approx(1.01325, rel=1e-3)


# ---------------------------------------------------------------------------
# list_units
# ---------------------------------------------------------------------------


class TestListUnits:
    def test_with_category(self):
        pressure = list_units("pressure")
        assert isinstance(pressure, list)
        assert "psi" in pressure
        assert "psig" in pressure

    def test_without_category(self):
        result = list_units()
        assert isinstance(result, dict)
        assert "pressure" in result
        assert "temperature" in result

    def test_unknown_category(self):
        assert list_units("nonexistent") == []


# ---------------------------------------------------------------------------
# get_unit_registry
# ---------------------------------------------------------------------------


class TestGetUnitRegistry:
    def test_returns_registry(self):
        reg = get_unit_registry()
        assert isinstance(reg, pint.UnitRegistry)

    def test_custom_definitions(self):
        reg = get_unit_registry()
        q = reg.Quantity(1, "psig")
        assert q.magnitude == 1
