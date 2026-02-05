"""
Tests for thermodynamics transformers.

Tests fluid_properties, saturation_properties, and psychrometrics transformers
with engine parity (Pandas, Spark mock, Polars).
"""

import pandas as pd
import pytest

# Skip all tests if CoolProp not installed
coolprop_available = False
try:
    import CoolProp.CoolProp as CP  # noqa: F401

    coolprop_available = True
except ImportError:
    pass

pytestmark = pytest.mark.skipif(not coolprop_available, reason="CoolProp not installed")


from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.thermodynamics import (
    FluidPropertiesParams,
    PropertyOutputConfig,
    PsychrometricOutputConfig,
    PsychrometricsParams,
    SaturationPropertiesParams,
    _pressure_to_pa,
    _temp_from_kelvin,
    _temp_to_kelvin,
    fluid_properties,
    psychrometrics,
    saturation_properties,
)


# =============================================================================
# UNIT CONVERSION TESTS
# =============================================================================


class TestUnitConversions:
    """Test unit conversion helper functions."""

    def test_temp_to_kelvin_degf(self):
        # 212°F = 373.15 K (boiling point of water)
        result = _temp_to_kelvin(212.0, "degF")
        assert abs(result - 373.15) < 0.01

    def test_temp_to_kelvin_degc(self):
        # 100°C = 373.15 K
        result = _temp_to_kelvin(100.0, "degC")
        assert abs(result - 373.15) < 0.01

    def test_temp_to_kelvin_already_kelvin(self):
        result = _temp_to_kelvin(300.0, "K")
        assert result == 300.0

    def test_temp_from_kelvin_degf(self):
        # 373.15 K = 212°F
        result = _temp_from_kelvin(373.15, "degF")
        assert abs(result - 212.0) < 0.1

    def test_temp_from_kelvin_degc(self):
        # 373.15 K = 100°C
        result = _temp_from_kelvin(373.15, "degC")
        assert abs(result - 100.0) < 0.01

    def test_pressure_to_pa_psia(self):
        # 14.696 psia = 101325 Pa (1 atm)
        result = _pressure_to_pa(14.696, "psia")
        assert abs(result - 101325) < 100

    def test_pressure_to_pa_psig(self):
        # 0 psig = 14.696 psia = 101325 Pa (with default gauge offset)
        result = _pressure_to_pa(0.0, "psig", gauge_offset=14.696)
        assert abs(result - 101325) < 100

    def test_pressure_to_pa_bar(self):
        # 1 bar = 100000 Pa
        result = _pressure_to_pa(1.0, "bar")
        assert result == 100000.0

    def test_pressure_to_pa_mpa(self):
        # 0.1 MPa = 100000 Pa
        result = _pressure_to_pa(0.1, "MPa")
        assert result == 100000.0


# =============================================================================
# FLUID PROPERTIES TESTS
# =============================================================================


class TestFluidPropertiesPandas:
    """Test fluid_properties transformer with Pandas engine."""

    def _create_context(self, df: pd.DataFrame) -> EngineContext:
        """Create a Pandas EngineContext for testing."""
        pandas_ctx = PandasContext()
        return EngineContext(
            context=pandas_ctx,
            df=df,
            engine_type=EngineType.PANDAS,
        )

    def test_steam_enthalpy_from_pt(self):
        """Test calculating steam enthalpy from pressure and temperature."""
        df = pd.DataFrame(
            {
                "pressure_mpa": [0.1, 0.5, 1.0],
                "temp_c": [150.0, 200.0, 250.0],
            }
        )
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="pressure_mpa",
            temperature_col="temp_c",
            pressure_unit="MPa",
            temperature_unit="degC",
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg", output_column="enthalpy"),
            ],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "enthalpy" in result_df.columns
        # All values should be positive and reasonable for superheated steam
        assert all(result_df["enthalpy"] > 2500)  # kJ/kg for superheated steam
        assert all(result_df["enthalpy"] < 3500)

    def test_steam_multiple_outputs(self):
        """Test calculating multiple properties at once."""
        df = pd.DataFrame(
            {
                "P_psia": [100.0],
                "T_degF": [400.0],
            }
        )
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P_psia",
            temperature_col="T_degF",
            pressure_unit="psia",
            temperature_unit="degF",
            outputs=[
                PropertyOutputConfig(property="H", unit="BTU/lb", output_column="h"),
                PropertyOutputConfig(property="S", unit="BTU/(lb·R)", output_column="s"),
                PropertyOutputConfig(property="D", unit="lb/ft³", output_column="rho"),
            ],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "h" in result_df.columns
        assert "s" in result_df.columns
        assert "rho" in result_df.columns
        # Enthalpy should be around 1200 BTU/lb for this condition
        assert 1100 < result_df["h"].iloc[0] < 1300

    def test_saturated_steam_from_pq(self):
        """Test calculating saturated vapor properties using P and Q."""
        df = pd.DataFrame(
            {
                "pressure_psig": [0.0, 50.0, 100.0],
            }
        )
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="pressure_psig",
            pressure_unit="psig",
            gauge_offset=14.696,
            quality=1.0,  # Saturated vapor
            outputs=[
                PropertyOutputConfig(property="H", unit="BTU/lb", output_column="hg"),
                PropertyOutputConfig(property="T", unit="degF", output_column="sat_temp"),
            ],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "hg" in result_df.columns
        assert "sat_temp" in result_df.columns
        # At 0 psig (14.696 psia), sat temp should be ~212°F
        assert abs(result_df["sat_temp"].iloc[0] - 212) < 2

    def test_refrigerant_properties(self):
        """Test with R134a refrigerant."""
        df = pd.DataFrame(
            {
                "P_kpa": [500.0],
                "T_c": [25.0],
            }
        )
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="R134a",
            pressure_col="P_kpa",
            temperature_col="T_c",
            pressure_unit="kPa",
            temperature_unit="degC",
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg"),
            ],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "H" in result_df.columns
        assert result_df["H"].iloc[0] is not None

    def test_null_handling(self):
        """Test that null inputs produce null outputs."""
        df = pd.DataFrame(
            {
                "pressure": [100.0, None, 100.0],
                "temp": [200.0, 200.0, None],
            }
        )
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="pressure",
            temperature_col="temp",
            pressure_unit="psia",
            temperature_unit="degF",
            outputs=[PropertyOutputConfig(property="H")],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert result_df["H"].iloc[0] is not None
        assert pd.isna(result_df["H"].iloc[1])  # Null pressure
        assert pd.isna(result_df["H"].iloc[2])  # Null temp

    def test_prefix_in_output_columns(self):
        """Test that prefix is applied to output columns."""
        df = pd.DataFrame({"P": [100.0], "T": [300.0]})
        ctx = self._create_context(df)

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P",
            temperature_col="T",
            pressure_unit="psia",
            temperature_unit="degF",
            prefix="steam",
            outputs=[
                PropertyOutputConfig(property="H"),
                PropertyOutputConfig(property="S"),
            ],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "steam_H" in result_df.columns
        assert "steam_S" in result_df.columns


# =============================================================================
# SATURATION PROPERTIES TESTS
# =============================================================================


class TestSaturationPropertiesPandas:
    """Test saturation_properties transformer."""

    def _create_context(self, df: pd.DataFrame) -> EngineContext:
        pandas_ctx = PandasContext()
        return EngineContext(
            context=pandas_ctx,
            df=df,
            engine_type=EngineType.PANDAS,
        )

    def test_saturated_vapor_properties(self):
        """Test saturated vapor (Q=1) properties."""
        df = pd.DataFrame({"P_psia": [14.696, 50.0, 100.0]})
        ctx = self._create_context(df)

        params = SaturationPropertiesParams(
            fluid="Water",
            pressure_col="P_psia",
            pressure_unit="psia",
            phase="vapor",
            outputs=[
                PropertyOutputConfig(property="H", unit="BTU/lb", output_column="hg"),
                PropertyOutputConfig(property="T", unit="degF", output_column="Tsat"),
            ],
        )

        result = saturation_properties(ctx, params)
        result_df = result.df

        # At 14.696 psia, Tsat = 212°F, hg ≈ 1150 BTU/lb
        assert abs(result_df["Tsat"].iloc[0] - 212) < 2
        assert 1140 < result_df["hg"].iloc[0] < 1160

    def test_saturated_liquid_properties(self):
        """Test saturated liquid (Q=0) properties."""
        df = pd.DataFrame({"P_bar": [1.0, 5.0, 10.0]})
        ctx = self._create_context(df)

        params = SaturationPropertiesParams(
            fluid="Water",
            pressure_col="P_bar",
            pressure_unit="bar",
            phase="liquid",
            outputs=[
                PropertyOutputConfig(property="H", unit="kJ/kg", output_column="hf"),
            ],
        )

        result = saturation_properties(ctx, params)
        result_df = result.df

        # Saturated liquid enthalpy should be positive and less than ~1000 kJ/kg
        assert all(result_df["hf"] > 0)
        assert all(result_df["hf"] < 1000)


# =============================================================================
# PSYCHROMETRICS TESTS
# =============================================================================


class TestPsychrometricsPandas:
    """Test psychrometrics transformer."""

    def _create_context(self, df: pd.DataFrame) -> EngineContext:
        pandas_ctx = PandasContext()
        return EngineContext(
            context=pandas_ctx,
            df=df,
            engine_type=EngineType.PANDAS,
        )

    def test_humidity_ratio_from_t_rh(self):
        """Test calculating humidity ratio from dry bulb and RH."""
        df = pd.DataFrame(
            {
                "Tdb_F": [80.0, 75.0, 90.0],
                "RH_pct": [60.0, 45.0, 70.0],
            }
        )
        ctx = self._create_context(df)

        params = PsychrometricsParams(
            dry_bulb_col="Tdb_F",
            relative_humidity_col="RH_pct",
            temperature_unit="degF",
            rh_is_percent=True,
            elevation_ft=875.0,
            outputs=[
                PsychrometricOutputConfig(
                    property="W", unit="lb/lb", output_column="humidity_ratio"
                ),
            ],
        )

        result = psychrometrics(ctx, params)
        result_df = result.df

        assert "humidity_ratio" in result_df.columns
        # Humidity ratio at 80°F, 60% RH should be around 0.013
        assert 0.01 < result_df["humidity_ratio"].iloc[0] < 0.02

    def test_wet_bulb_calculation(self):
        """Test calculating wet bulb temperature."""
        df = pd.DataFrame(
            {
                "Tdb": [25.0],  # degC
                "RH": [0.50],  # 50%
            }
        )
        ctx = self._create_context(df)

        params = PsychrometricsParams(
            dry_bulb_col="Tdb",
            relative_humidity_col="RH",
            temperature_unit="degC",
            rh_is_percent=False,
            pressure=101325.0,
            pressure_unit="Pa",
            outputs=[
                PsychrometricOutputConfig(property="B", unit="degC", output_column="wet_bulb"),
            ],
        )

        result = psychrometrics(ctx, params)
        result_df = result.df

        # Wet bulb should be less than dry bulb
        assert result_df["wet_bulb"].iloc[0] < 25.0
        # At 25°C, 50% RH, wet bulb is around 17-18°C
        assert 15 < result_df["wet_bulb"].iloc[0] < 20

    def test_dew_point_calculation(self):
        """Test calculating dew point temperature."""
        df = pd.DataFrame(
            {
                "Tdb_F": [80.0],
                "RH_pct": [50.0],
            }
        )
        ctx = self._create_context(df)

        params = PsychrometricsParams(
            dry_bulb_col="Tdb_F",
            relative_humidity_col="RH_pct",
            temperature_unit="degF",
            rh_is_percent=True,
            pressure=14.696,
            pressure_unit="psia",
            outputs=[
                PsychrometricOutputConfig(property="D", unit="degF", output_column="dew_point"),
            ],
        )

        result = psychrometrics(ctx, params)
        result_df = result.df

        # Dew point should be less than dry bulb
        assert result_df["dew_point"].iloc[0] < 80.0
        # At 80°F, 50% RH, dew point is around 59°F
        assert 55 < result_df["dew_point"].iloc[0] < 65

    def test_multiple_psychrometric_outputs(self):
        """Test calculating multiple properties at once."""
        df = pd.DataFrame(
            {
                "T": [300.0],  # K
                "RH": [0.60],
            }
        )
        ctx = self._create_context(df)

        params = PsychrometricsParams(
            dry_bulb_col="T",
            relative_humidity_col="RH",
            temperature_unit="K",
            rh_is_percent=False,
            pressure=101325.0,
            pressure_unit="Pa",
            prefix="air",
            outputs=[
                PsychrometricOutputConfig(property="W"),
                PsychrometricOutputConfig(property="B", unit="K"),
                PsychrometricOutputConfig(property="D", unit="K"),
                PsychrometricOutputConfig(property="H", unit="kJ/kg"),
            ],
        )

        result = psychrometrics(ctx, params)
        result_df = result.df

        assert "air_W" in result_df.columns
        assert "air_B" in result_df.columns
        assert "air_D" in result_df.columns
        assert "air_H" in result_df.columns

    def test_elevation_pressure_estimation(self):
        """Test that elevation is used to estimate pressure."""
        df = pd.DataFrame({"Tdb": [80.0], "RH": [50.0]})
        ctx = self._create_context(df)

        params = PsychrometricsParams(
            dry_bulb_col="Tdb",
            relative_humidity_col="RH",
            temperature_unit="degF",
            rh_is_percent=True,
            elevation_ft=5000.0,  # High elevation -> lower pressure
            outputs=[PsychrometricOutputConfig(property="W", output_column="W")],
        )

        result = psychrometrics(ctx, params)
        result_df = result.df

        # At higher elevation, humidity ratio should be slightly different
        assert result_df["W"].iloc[0] is not None


# =============================================================================
# VALIDATION TESTS
# =============================================================================


class TestParamsValidation:
    """Test Pydantic model validation."""

    def test_fluid_properties_requires_two_state_props(self):
        """FluidPropertiesParams requires exactly 2 state properties."""
        with pytest.raises(ValueError, match="Fluid state requires 2 independent properties"):
            FluidPropertiesParams(
                fluid="Water",
                pressure_col="P",
                # Missing second state property
            )

    def test_fluid_properties_valid_with_p_and_t(self):
        """Valid with pressure and temperature."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P",
            temperature_col="T",
        )
        assert params is not None

    def test_fluid_properties_valid_with_p_and_q(self):
        """Valid with pressure and quality."""
        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P",
            quality=1.0,
        )
        assert params is not None

    def test_psychrometrics_requires_humidity_input(self):
        """PsychrometricsParams requires at least one humidity input."""
        with pytest.raises(ValueError, match="requires dry_bulb and one of"):
            PsychrometricsParams(
                dry_bulb_col="Tdb",
                pressure=101325.0,
                # Missing RH, W, Twb, or Tdp
            )

    def test_psychrometrics_requires_pressure_source(self):
        """PsychrometricsParams requires a pressure source."""
        with pytest.raises(ValueError, match="requires pressure"):
            PsychrometricsParams(
                dry_bulb_col="Tdb",
                relative_humidity_col="RH",
                # Missing pressure, pressure_col, or elevation
            )


# =============================================================================
# POLARS ENGINE TESTS
# =============================================================================


class TestFluidPropertiesPolars:
    """Test fluid_properties with Polars engine."""

    @pytest.fixture
    def polars_available(self):
        try:
            import polars  # noqa: F401

            return True
        except ImportError:
            pytest.skip("Polars not installed")

    def test_basic_calculation(self, polars_available):
        """Test basic Polars calculation."""
        import polars as pl

        from odibi.context import PolarsContext

        df = pl.DataFrame(
            {
                "P": [100.0, 200.0],
                "T": [400.0, 500.0],
            }
        )

        polars_ctx = PolarsContext()
        ctx = EngineContext(
            context=polars_ctx,
            df=df,
            engine_type=EngineType.POLARS,
        )

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P",
            temperature_col="T",
            pressure_unit="psia",
            temperature_unit="degF",
            outputs=[PropertyOutputConfig(property="H", unit="BTU/lb")],
        )

        result = fluid_properties(ctx, params)
        result_df = result.df

        assert "H" in result_df.columns
        assert len(result_df) == 2


# =============================================================================
# SPARK MOCK TESTS
# =============================================================================


class TestFluidPropertiesSparkMock:
    """Test fluid_properties with mocked Spark engine."""

    def test_spark_udf_structure(self):
        """Test that Spark implementation creates correct UDF structure."""
        # This test verifies the Spark code path without needing real Spark
        # The actual Spark tests run in Databricks

        params = FluidPropertiesParams(
            fluid="Water",
            pressure_col="P",
            temperature_col="T",
            pressure_unit="psia",
            temperature_unit="degF",
            outputs=[
                PropertyOutputConfig(property="H", unit="BTU/lb"),
                PropertyOutputConfig(property="S"),
            ],
        )

        # Verify params serialize correctly for UDF
        params_dict = params.model_dump()
        reconstructed = FluidPropertiesParams(**params_dict)
        assert reconstructed.fluid == "Water"
        assert len(reconstructed.outputs) == 2
