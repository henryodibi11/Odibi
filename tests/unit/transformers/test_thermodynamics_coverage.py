"""Coverage tests for odibi/transformers/thermodynamics.py using real CoolProp."""

import logging

import pandas as pd
import pytest
from pydantic import ValidationError

pytest.importorskip("CoolProp")

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.thermodynamics import (
    FluidPropertiesParams,
    PropertyOutputConfig,
    PsychrometricOutputConfig,
    PsychrometricsParams,
    SaturationPropertiesParams,
    _compute_fluid_properties_row,
    _compute_psychrometrics_row,
    _ensure_coolprop,
    _pressure_from_pa,
    _pressure_to_pa,
    _temp_from_kelvin,
    _temp_to_kelvin,
    fluid_properties,
    psychrometrics,
    saturation_properties,
)

logging.getLogger("odibi").propagate = False

CP = _ensure_coolprop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(df):
    ctx = PandasContext()
    return EngineContext(ctx, df, EngineType.PANDAS)


def _make_polars_ctx(df_pd):
    pl = pytest.importorskip("polars")
    ctx = PandasContext()
    return EngineContext(ctx, pl.from_pandas(df_pd), EngineType.POLARS)


# ===========================================================================
# 1. _ensure_coolprop
# ===========================================================================


class TestEnsureCoolprop:
    def test_returns_cp_module(self):
        mod = _ensure_coolprop()
        assert hasattr(mod, "PropsSI")
        assert hasattr(mod, "HAPropsSI")


# ===========================================================================
# 2. _temp_to_kelvin
# ===========================================================================


class TestTempToKelvinCoverage:
    def test_kelvin(self):
        assert _temp_to_kelvin(300.0, "K") == 300.0

    def test_degc(self):
        assert _temp_to_kelvin(100.0, "degC") == pytest.approx(373.15)

    def test_degf(self):
        assert _temp_to_kelvin(212.0, "degF") == pytest.approx(373.15)

    def test_degr(self):
        assert _temp_to_kelvin(491.67, "degR") == pytest.approx(273.15, rel=1e-4)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown temperature unit"):
            _temp_to_kelvin(100.0, "degZ")


# ===========================================================================
# 3. _temp_from_kelvin
# ===========================================================================


class TestTempFromKelvinCoverage:
    def test_kelvin(self):
        assert _temp_from_kelvin(373.15, "K") == 373.15

    def test_degc(self):
        assert _temp_from_kelvin(373.15, "degC") == pytest.approx(100.0)

    def test_degf(self):
        assert _temp_from_kelvin(373.15, "degF") == pytest.approx(212.0)

    def test_degr(self):
        assert _temp_from_kelvin(273.15, "degR") == pytest.approx(491.67)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown temperature unit"):
            _temp_from_kelvin(300.0, "degZ")


# ===========================================================================
# 4. _pressure_to_pa
# ===========================================================================


class TestPressureToPaCoverage:
    def test_pa(self):
        assert _pressure_to_pa(101325.0, "Pa") == 101325.0

    def test_kpa(self):
        assert _pressure_to_pa(101.325, "kPa") == pytest.approx(101325.0)

    def test_bar(self):
        assert _pressure_to_pa(1.0, "bar") == pytest.approx(100_000.0)

    def test_psia(self):
        assert _pressure_to_pa(14.696, "psia") == pytest.approx(101325.0, rel=1e-3)

    def test_psig_default_offset(self):
        result = _pressure_to_pa(0.0, "psig")
        expected = 14.696 * 6894.757293168
        assert result == pytest.approx(expected)

    def test_psig_custom_offset(self):
        result = _pressure_to_pa(10.0, "psig", gauge_offset=14.7)
        expected = (10.0 + 14.7) * 6894.757293168
        assert result == pytest.approx(expected)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown pressure unit"):
            _pressure_to_pa(100.0, "mmHg")


# ===========================================================================
# 5. _pressure_from_pa
# ===========================================================================


class TestPressureFromPaCoverage:
    def test_psia(self):
        assert _pressure_from_pa(101325.0, "psia") == pytest.approx(14.696, rel=1e-3)

    def test_psig_default_offset(self):
        pa_in = 14.696 * 6894.757293168
        assert _pressure_from_pa(pa_in, "psig") == pytest.approx(0.0, abs=0.01)

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="Unknown pressure unit"):
            _pressure_from_pa(100.0, "mmHg")


# ===========================================================================
# 6-9. FluidPropertiesParams validation
# ===========================================================================


class TestFluidPropertiesParamsValidation:
    def test_valid_pt(self):
        p = FluidPropertiesParams(pressure=101325.0, temperature=373.15)
        assert p.pressure == 101325.0
        assert p.temperature == 373.15

    def test_valid_pq(self):
        p = FluidPropertiesParams(pressure_col="p", quality=1.0)
        assert p.quality == 1.0

    def test_one_state_property_raises(self):
        with pytest.raises(ValidationError, match="2 independent properties"):
            FluidPropertiesParams(pressure=101325.0)

    def test_no_state_properties_raises(self):
        with pytest.raises(ValidationError, match="2 independent properties"):
            FluidPropertiesParams()


# ===========================================================================
# 10-19. _compute_fluid_properties_row
# ===========================================================================


class TestComputeFluidPropertiesRow:
    def test_water_100c_1atm_enthalpy(self):
        """Water at 100°C, 1 atm → superheated-ish steam enthalpy."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            temperature=373.15,
            outputs=[PropertyOutputConfig(property="H")],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert "H" in result
        assert result["H"] == pytest.approx(2675000, rel=0.01)

    def test_pressure_col_and_temperature_col(self):
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H")],
        )
        row = {"p": 101325.0, "t": 373.15}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["H"] == pytest.approx(2675000, rel=0.01)

    def test_fixed_pressure_plus_temperature_col(self):
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H")],
        )
        row = {"t": 373.15}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["H"] == pytest.approx(2675000, rel=0.01)

    def test_quality_1_steam(self):
        """Saturated vapor (Q=1) at 1 atm."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            quality=1.0,
            outputs=[PropertyOutputConfig(property="H")],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["H"] == pytest.approx(2675000, rel=0.01)

    def test_enthalpy_col_input(self):
        """Use P + H as state inputs, get T output."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            enthalpy_col="h_in",
            outputs=[PropertyOutputConfig(property="T")],
        )
        row = {"h_in": 2675000.0}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["T"] == pytest.approx(373.15, rel=0.01)

    def test_custom_output_units(self):
        """Output enthalpy in kJ/kg, temperature in degC."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            quality=1.0,
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg"),
                PropertyOutputConfig(property="T", unit="degC"),
            ],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["H"] == pytest.approx(2675, rel=0.01)
        assert result["T"] == pytest.approx(100.0, rel=0.01)

    def test_multiple_output_properties(self):
        """H, S, D, T all returned."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            quality=1.0,
            outputs=[
                PropertyOutputConfig(property="H"),
                PropertyOutputConfig(property="S"),
                PropertyOutputConfig(property="D"),
                PropertyOutputConfig(property="T"),
            ],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert len(result) == 4
        assert result["H"] > 0
        assert result["S"] > 0
        assert result["D"] > 0
        assert result["T"] == pytest.approx(373.15, rel=0.01)

    def test_null_pressure_returns_none(self):
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature=373.15,
            outputs=[PropertyOutputConfig(property="H", output_column="h_out")],
        )
        row = {"p": None}
        result = _compute_fluid_properties_row(row, params, CP)
        assert result["h_out"] is None

    def test_prefix(self):
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            quality=1.0,
            prefix="steam",
            outputs=[PropertyOutputConfig(property="H")],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert "steam_H" in result

    def test_output_column_override(self):
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            quality=1.0,
            outputs=[PropertyOutputConfig(property="H", output_column="my_enthalpy")],
        )
        row = {}
        result = _compute_fluid_properties_row(row, params, CP)
        assert "my_enthalpy" in result


# ===========================================================================
# 20-23. fluid_properties (engine paths)
# ===========================================================================


class TestFluidPropertiesEngine:
    def test_pandas_engine(self):
        df = pd.DataFrame({"p": [101325.0], "t": [373.15]})
        ctx = _make_ctx(df)
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H", unit="kJ/kg", output_column="h")],
        )
        result = fluid_properties(ctx, params)
        assert "h" in result.df.columns
        assert result.df["h"].iloc[0] == pytest.approx(2675, rel=0.01)

    def test_pandas_fixed_pressure_plus_temp_col(self):
        df = pd.DataFrame({"t": [373.15]})
        ctx = _make_ctx(df)
        params = FluidPropertiesParams(
            fluid="Water",
            pressure=101325.0,
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H", unit="kJ/kg", output_column="h")],
        )
        result = fluid_properties(ctx, params)
        assert result.df["h"].iloc[0] == pytest.approx(2675, rel=0.01)

    def test_polars_engine(self):
        pl = pytest.importorskip("polars")
        df = pd.DataFrame({"p": [101325.0], "t": [373.15]})
        ctx = _make_polars_ctx(df)
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H", unit="kJ/kg", output_column="h")],
        )
        result = fluid_properties(ctx, params)
        assert isinstance(result.df, pl.DataFrame)
        assert "h" in result.df.columns

    def test_unsupported_engine_raises(self):
        df = pd.DataFrame({"p": [101325.0], "t": [373.15]})
        ctx = _make_ctx(df)
        ctx.engine_type = "unsupported"
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature_col="t",
        )
        with pytest.raises(ValueError, match="Unsupported engine"):
            fluid_properties(ctx, params)


# ===========================================================================
# 24. _fluid_properties_pandas multi-row
# ===========================================================================


class TestFluidPropertiesPandasMultiRow:
    def test_vectorized_apply(self):
        df = pd.DataFrame(
            {
                "p": [101325.0, 101325.0, 200000.0],
                "t": [373.15, 400.0, 400.0],
            }
        )
        ctx = _make_ctx(df)
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="p",
            temperature_col="t",
            outputs=[PropertyOutputConfig(property="H", output_column="h")],
        )
        result = fluid_properties(ctx, params)
        assert len(result.df) == 3
        assert result.df["h"].notna().all()


# ===========================================================================
# 25-28. SaturationPropertiesParams
# ===========================================================================


class TestSaturationPropertiesParams:
    def test_valid_with_pressure(self):
        p = SaturationPropertiesParams(pressure=101325.0)
        assert p.pressure == 101325.0
        assert p.phase == "vapor"

    def test_valid_with_temperature(self):
        p = SaturationPropertiesParams(temperature=373.15)
        assert p.temperature == 373.15

    def test_neither_p_nor_t_raises(self):
        with pytest.raises(ValidationError, match="pressure or temperature"):
            SaturationPropertiesParams()

    def test_phase_liquid(self):
        p = SaturationPropertiesParams(pressure=101325.0, phase="liquid")
        assert p.phase == "liquid"


# ===========================================================================
# 29-30. saturation_properties
# ===========================================================================


class TestSaturationProperties:
    def test_saturated_vapor_1atm(self):
        df = pd.DataFrame({"p": [101325.0]})
        ctx = _make_ctx(df)
        params = SaturationPropertiesParams(
            fluid="Water",
            pressure_col="p",
            phase="vapor",
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg", output_column="hg"),
                PropertyOutputConfig(property="T", unit="degC", output_column="t_sat"),
            ],
        )
        result = saturation_properties(ctx, params)
        assert result.df["hg"].iloc[0] == pytest.approx(2675, rel=0.01)
        assert result.df["t_sat"].iloc[0] == pytest.approx(100.0, rel=0.01)

    def test_saturated_liquid_1atm(self):
        df = pd.DataFrame({"p": [101325.0]})
        ctx = _make_ctx(df)
        params = SaturationPropertiesParams(
            fluid="Water",
            pressure_col="p",
            phase="liquid",
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg", output_column="hf"),
            ],
        )
        result = saturation_properties(ctx, params)
        assert result.df["hf"].iloc[0] == pytest.approx(419, rel=0.01)


# ===========================================================================
# 31-33. PsychrometricsParams validation
# ===========================================================================


class TestPsychrometricsParamsValidation:
    def test_valid_with_drybulb_rh(self):
        p = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
        )
        assert p.dry_bulb_col == "tdb"

    def test_missing_humidity_raises(self):
        with pytest.raises(ValidationError, match="one of"):
            PsychrometricsParams(
                dry_bulb_col="tdb",
                pressure=101325.0,
            )

    def test_missing_pressure_source_raises(self):
        with pytest.raises(ValidationError, match="pressure"):
            PsychrometricsParams(
                dry_bulb_col="tdb",
                relative_humidity=0.5,
            )


# ===========================================================================
# 34-42. _compute_psychrometrics_row
# ===========================================================================


class TestComputePsychrometricsRowCoverage:
    def test_drybulb_25c_rh50_atmospheric(self):
        """25°C, 50% RH, 101325 Pa → humidity ratio ≈ 0.01."""
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] == pytest.approx(0.01, rel=0.1)

    def test_rh_is_percent_true(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            rh_is_percent=True,
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0, "rh": 50.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] == pytest.approx(0.01, rel=0.1)

    def test_fixed_pressure_scalar(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
            pressure_unit="Pa",
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] is not None
        assert result["w"] > 0

    def test_elevation_ft(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            elevation_ft=1000.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] is not None
        assert result["w"] > 0

    def test_elevation_m(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            elevation_m=300.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] is not None
        assert result["w"] > 0

    def test_humidity_ratio_col_input(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            humidity_ratio_col="w_in",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="R", output_column="rh_out")],
        )
        row = {"tdb": 25.0, "w_in": 0.01}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["rh_out"] is not None
        assert 0 < result["rh_out"] < 1

    def test_wet_bulb_col_input(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            wet_bulb_col="twb",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": 25.0, "twb": 18.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] is not None
        assert result["w"] > 0

    def test_null_drybulb_returns_none(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        row = {"tdb": None}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["w"] is None

    def test_output_unit_conversions(self):
        """degC for temps, g/kg for W, kJ/kg for H."""
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[
                PsychrometricOutputConfig(property="B", unit="degC", output_column="wb"),
                PsychrometricOutputConfig(property="W", unit="g/kg", output_column="w_gkg"),
                PsychrometricOutputConfig(property="H", unit="kJ/kg", output_column="h_kjkg"),
            ],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, CP)
        assert result["wb"] == pytest.approx(18.0, abs=2.0)
        assert result["w_gkg"] == pytest.approx(10.0, rel=0.2)
        assert result["h_kjkg"] is not None
        assert result["h_kjkg"] > 0


# ===========================================================================
# 43-44. psychrometrics (engine paths)
# ===========================================================================


class TestPsychrometricsEngine:
    def test_pandas_engine(self):
        df = pd.DataFrame({"tdb": [25.0, 30.0], "rh": [0.5, 0.6]})
        ctx = _make_ctx(df)
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        result = psychrometrics(ctx, params)
        assert "w" in result.df.columns
        assert len(result.df) == 2
        assert result.df["w"].notna().all()

    def test_polars_engine(self):
        pl = pytest.importorskip("polars")
        df = pd.DataFrame({"tdb": [25.0], "rh": [0.5]})
        ctx = _make_polars_ctx(df)
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w")],
        )
        result = psychrometrics(ctx, params)
        assert isinstance(result.df, pl.DataFrame)
        assert "w" in result.df.columns
