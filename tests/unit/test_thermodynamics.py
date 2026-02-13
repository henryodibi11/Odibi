"""Unit tests for odibi/transformers/thermodynamics.py."""

import math
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.transformers.thermodynamics import (
    ATM_PA,
    FluidPropertiesParams,
    PropertyOutputConfig,
    PsychrometricOutputConfig,
    PsychrometricsParams,
    _compute_psychrometrics_row,
    _pressure_from_pa,
    _pressure_to_pa,
    _temp_from_kelvin,
    _temp_to_kelvin,
    fluid_properties,
    psychrometrics,
)


# =============================================================================
# _ensure_coolprop
# =============================================================================


class TestEnsureCoolprop:
    def test_raises_when_coolprop_missing(self):
        with patch.dict("sys.modules", {"CoolProp": None, "CoolProp.CoolProp": None}):
            from odibi.transformers.thermodynamics import _ensure_coolprop

            with pytest.raises(ImportError, match="CoolProp is required"):
                _ensure_coolprop()


# =============================================================================
# Temperature conversions
# =============================================================================


class TestTempToKelvin:
    def test_kelvin_passthrough(self):
        assert _temp_to_kelvin(300.0, "K") == 300.0

    def test_celsius(self):
        assert _temp_to_kelvin(0.0, "degC") == pytest.approx(273.15)
        assert _temp_to_kelvin(100.0, "degC") == pytest.approx(373.15)

    def test_fahrenheit(self):
        assert _temp_to_kelvin(32.0, "degF") == pytest.approx(273.15)
        assert _temp_to_kelvin(212.0, "degF") == pytest.approx(373.15)

    def test_rankine(self):
        assert _temp_to_kelvin(491.67, "degR") == pytest.approx(273.15, rel=1e-4)

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown temperature unit"):
            _temp_to_kelvin(100.0, "degX")


class TestTempFromKelvin:
    def test_kelvin_passthrough(self):
        assert _temp_from_kelvin(300.0, "K") == 300.0

    def test_celsius(self):
        assert _temp_from_kelvin(273.15, "degC") == pytest.approx(0.0)
        assert _temp_from_kelvin(373.15, "degC") == pytest.approx(100.0)

    def test_fahrenheit(self):
        assert _temp_from_kelvin(273.15, "degF") == pytest.approx(32.0)
        assert _temp_from_kelvin(373.15, "degF") == pytest.approx(212.0)

    def test_rankine(self):
        assert _temp_from_kelvin(273.15, "degR") == pytest.approx(491.67)

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown temperature unit"):
            _temp_from_kelvin(300.0, "degX")


# =============================================================================
# Pressure conversions
# =============================================================================


class TestPressureToPa:
    def test_pa_passthrough(self):
        assert _pressure_to_pa(101325.0, "Pa") == 101325.0

    def test_kpa(self):
        assert _pressure_to_pa(101.325, "kPa") == pytest.approx(101325.0)

    def test_bar(self):
        assert _pressure_to_pa(1.0, "bar") == pytest.approx(100_000.0)

    def test_psia(self):
        assert _pressure_to_pa(14.696, "psia") == pytest.approx(101325.0, rel=1e-3)

    def test_psig_with_default_offset(self):
        # psig: value + 14.696 (default), then * psia factor
        result = _pressure_to_pa(0.0, "psig")
        expected = 14.696 * 6894.757293168
        assert result == pytest.approx(expected)

    def test_psig_with_custom_offset(self):
        result = _pressure_to_pa(10.0, "psig", gauge_offset=14.7)
        expected = (10.0 + 14.7) * 6894.757293168
        assert result == pytest.approx(expected)

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown pressure unit"):
            _pressure_to_pa(100.0, "mmHg")


class TestPressureFromPa:
    def test_pa_passthrough(self):
        assert _pressure_from_pa(101325.0, "Pa") == 101325.0

    def test_kpa(self):
        assert _pressure_from_pa(101325.0, "kPa") == pytest.approx(101.325)

    def test_bar(self):
        assert _pressure_from_pa(100_000.0, "bar") == pytest.approx(1.0)

    def test_psia(self):
        assert _pressure_from_pa(101325.0, "psia") == pytest.approx(14.696, rel=1e-3)

    def test_psig_with_default_offset(self):
        pa_input = 14.696 * 6894.757293168
        result = _pressure_from_pa(pa_input, "psig")
        assert result == pytest.approx(0.0, abs=0.01)

    def test_psig_with_custom_offset(self):
        pa_input = 24.7 * 6894.757293168
        result = _pressure_from_pa(pa_input, "psig", gauge_offset=14.7)
        assert result == pytest.approx(10.0, abs=0.01)

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError, match="Unknown pressure unit"):
            _pressure_from_pa(100.0, "mmHg")


# =============================================================================
# Pydantic model validation
# =============================================================================


class TestPropertyOutputConfig:
    def test_minimal(self):
        cfg = PropertyOutputConfig(property="H")
        assert cfg.property == "H"
        assert cfg.unit is None
        assert cfg.output_column is None

    def test_with_all_fields(self):
        cfg = PropertyOutputConfig(property="S", unit="kJ/(kg·K)", output_column="entropy")
        assert cfg.property == "S"
        assert cfg.unit == "kJ/(kg·K)"
        assert cfg.output_column == "entropy"


class TestFluidPropertiesParams:
    def test_valid_pt(self):
        params = FluidPropertiesParams(pressure=100.0, temperature=300.0)
        assert params.fluid == "Water"
        assert params.pressure == 100.0
        assert params.temperature == 300.0

    def test_valid_pq(self):
        params = FluidPropertiesParams(pressure_col="p", quality=1.0)
        assert params.quality == 1.0

    def test_too_few_state_props_raises(self):
        with pytest.raises(ValidationError, match="2 independent properties"):
            FluidPropertiesParams(pressure=100.0)

    def test_defaults(self):
        params = FluidPropertiesParams(pressure=100.0, temperature=300.0)
        assert params.pressure_unit == "Pa"
        assert params.temperature_unit == "K"
        assert params.gauge_offset == 14.696
        assert len(params.outputs) == 1
        assert params.outputs[0].property == "H"


class TestPsychrometricsParams:
    def test_valid_with_rh(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
        )
        assert params.dry_bulb_col == "tdb"

    def test_missing_humidity_input_raises(self):
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

    def test_valid_with_elevation_ft(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            elevation_ft=1000.0,
        )
        assert params.elevation_ft == 1000.0

    def test_valid_with_humidity_ratio_col(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            humidity_ratio_col="w",
            pressure=101325.0,
        )
        assert params.humidity_ratio_col == "w"

    def test_valid_with_wet_bulb_col(self):
        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            wet_bulb_col="twb",
            elevation_m=300.0,
        )
        assert params.wet_bulb_col == "twb"


# =============================================================================
# _compute_psychrometrics_row
# =============================================================================


class TestComputePsychrometricsRow:
    def _make_params(self, **overrides):
        defaults = dict(
            dry_bulb_col="tdb",
            relative_humidity=0.5,
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w_out")],
        )
        defaults.update(overrides)
        return PsychrometricsParams(**defaults)

    def test_success_case(self):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.0088
        params = self._make_params()
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, mock_cp)
        assert "w_out" in result
        assert result["w_out"] == pytest.approx(0.0088)
        mock_cp.HAPropsSI.assert_called_once()

    def test_null_dry_bulb_returns_none(self):
        mock_cp = MagicMock()
        params = self._make_params()
        row = {"tdb": None}
        result = _compute_psychrometrics_row(row, params, mock_cp)
        assert result["w_out"] is None
        mock_cp.HAPropsSI.assert_not_called()

    def test_nan_dry_bulb_returns_none(self):
        mock_cp = MagicMock()
        params = self._make_params()
        row = {"tdb": float("nan")}
        result = _compute_psychrometrics_row(row, params, mock_cp)
        assert result["w_out"] is None

    def test_humidity_ratio_gkg_conversion(self):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.010  # kg/kg
        params = self._make_params(
            relative_humidity=None,
            humidity_ratio=10.0,
            humidity_ratio_unit="g/kg",
            outputs=[PsychrometricOutputConfig(property="W", unit="g/kg", output_column="w_out")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, mock_cp)
        # Output should be raw_value * 1000 = 10.0
        assert result["w_out"] == pytest.approx(10.0)

    def test_elevation_ft_pressure_calc(self):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.008
        params = self._make_params(
            pressure=None,
            elevation_ft=875.0,
        )
        row = {"tdb": 25.0}
        _compute_psychrometrics_row(row, params, mock_cp)
        # Verify pressure arg passed to HAPropsSI
        call_args = mock_cp.HAPropsSI.call_args
        p_pa_arg = call_args[0][2]  # "P", p_pa is index 1,2
        expected_p = ATM_PA * math.exp(-0.0000366 * 875.0)
        assert p_pa_arg == pytest.approx(expected_p)

    def test_prefix_used_when_no_output_column(self):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.008
        params = self._make_params(
            prefix="psych",
            outputs=[PsychrometricOutputConfig(property="W")],
        )
        row = {"tdb": 25.0}
        result = _compute_psychrometrics_row(row, params, mock_cp)
        assert "psych_W" in result

    def test_rh_col_input(self):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.008
        params = self._make_params(
            relative_humidity=None,
            relative_humidity_col="rh",
            rh_is_percent=True,
        )
        row = {"tdb": 25.0, "rh": 50.0}
        _compute_psychrometrics_row(row, params, mock_cp)
        call_args = mock_cp.HAPropsSI.call_args
        # args: (prop, "P", p_pa, "T", tdb_k, "R", rh)
        assert call_args[0][5] == "R"
        assert call_args[0][6] == pytest.approx(0.5)


# =============================================================================
# psychrometrics (Pandas path)
# =============================================================================


class TestPsychrometricsPandas:
    @patch("odibi.transformers.thermodynamics._ensure_coolprop")
    def test_pandas_end_to_end(self, mock_ensure):
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.0088
        mock_ensure.return_value = mock_cp

        df = pd.DataFrame({"tdb": [25.0, 30.0], "rh": [0.5, 0.6]})
        ctx_mock = MagicMock(spec=["context"])
        engine_ctx = EngineContext(
            context=ctx_mock,
            df=df,
            engine_type=EngineType.PANDAS,
        )

        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w_out")],
        )
        result_ctx = psychrometrics(engine_ctx, params)
        result_df = result_ctx.df
        assert "w_out" in result_df.columns
        assert len(result_df) == 2
        assert list(result_df["w_out"]) == pytest.approx([0.0088, 0.0088])


# =============================================================================
# psychrometrics (Polars path)
# =============================================================================


class TestPsychrometricsPolars:
    @patch("odibi.transformers.thermodynamics._ensure_coolprop")
    def test_polars_delegates_to_pandas(self, mock_ensure):
        pl = pytest.importorskip("polars")
        mock_cp = MagicMock()
        mock_cp.HAPropsSI.return_value = 0.0088
        mock_ensure.return_value = mock_cp

        df = pl.DataFrame({"tdb": [25.0, 30.0], "rh": [0.5, 0.6]})
        ctx_mock = MagicMock(spec=["context"])
        engine_ctx = EngineContext(
            context=ctx_mock,
            df=df,
            engine_type=EngineType.POLARS,
        )

        params = PsychrometricsParams(
            dry_bulb_col="tdb",
            relative_humidity_col="rh",
            pressure=101325.0,
            temperature_unit="degC",
            outputs=[PsychrometricOutputConfig(property="W", output_column="w_out")],
        )
        result_ctx = psychrometrics(engine_ctx, params)
        assert isinstance(result_ctx.df, pl.DataFrame)
        assert "w_out" in result_ctx.df.columns


# =============================================================================
# fluid_properties (Pandas path)
# =============================================================================


class TestFluidPropertiesPandas:
    @patch("odibi.transformers.thermodynamics._ensure_coolprop")
    def test_pandas_end_to_end(self, mock_ensure):
        mock_cp = MagicMock()
        mock_cp.PropsSI.return_value = 2675000.0  # enthalpy in J/kg
        mock_ensure.return_value = mock_cp

        df = pd.DataFrame({"pressure": [101325.0], "temperature": [373.15]})
        ctx_mock = MagicMock(spec=["context"])
        engine_ctx = EngineContext(
            context=ctx_mock,
            df=df,
            engine_type=EngineType.PANDAS,
        )

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="pressure",
            temperature_col="temperature",
            outputs=[PropertyOutputConfig(property="H", output_column="enthalpy")],
        )
        result_ctx = fluid_properties(engine_ctx, params)
        result_df = result_ctx.df
        assert "enthalpy" in result_df.columns
        assert result_df["enthalpy"].iloc[0] == pytest.approx(2675000.0)
        mock_cp.PropsSI.assert_called_once()
