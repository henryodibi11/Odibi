"""
Tests for unit conversion transformer.

Tests unit_convert transformer with engine parity (Pandas, Spark mock, Polars).
"""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.units import (
    ConversionSpec,
    UnitConvertParams,
    convert,
    get_unit_registry,
    list_units,
    unit_convert,
)


# =============================================================================
# UTILITY FUNCTION TESTS
# =============================================================================


class TestConvertFunction:
    """Test the standalone convert() function."""

    def test_temperature_f_to_c(self):
        result = convert(212, "degF", "degC")
        assert abs(result - 100.0) < 0.01

    def test_temperature_c_to_k(self):
        result = convert(100, "degC", "K")
        assert abs(result - 373.15) < 0.01

    def test_pressure_psia_to_bar(self):
        result = convert(14.696, "psia", "bar")
        assert abs(result - 1.01325) < 0.001

    def test_pressure_bar_to_kpa(self):
        result = convert(1, "bar", "kPa")
        assert result == 100.0

    def test_enthalpy_btu_lb_to_kj_kg(self):
        result = convert(1000, "BTU/lb", "kJ/kg")
        assert abs(result - 2326.0) < 1.0

    def test_flow_gpm_to_m3s(self):
        result = convert(100, "gpm", "m³/s")
        assert abs(result - 0.006309) < 0.0001

    def test_energy_mmbtu_to_gj(self):
        result = convert(1, "mmbtu", "GJ")
        assert abs(result - 1.055) < 0.01

    def test_mass_klb_to_kg(self):
        result = convert(1, "klb", "kg")
        assert abs(result - 453.59) < 0.1


class TestListUnits:
    """Test the list_units() function."""

    def test_list_pressure_units(self):
        units = list_units("pressure")
        assert "psi" in units
        assert "bar" in units
        assert "kPa" in units

    def test_list_temperature_units(self):
        units = list_units("temperature")
        assert "degF" in units
        assert "degC" in units
        assert "K" in units

    def test_list_all_categories(self):
        categories = list_units()
        assert "pressure" in categories
        assert "temperature" in categories
        assert "energy" in categories
        assert "flow_rate" in categories


class TestUnitRegistry:
    """Test the unit registry with custom engineering units."""

    def test_mcf_defined(self):
        ureg = get_unit_registry()
        quantity = 1 * ureg.mcf
        result = quantity.to("ft³").magnitude
        assert result == 1000.0

    def test_mmbtu_defined(self):
        ureg = get_unit_registry()
        quantity = 1 * ureg.mmbtu
        result = quantity.to("BTU").magnitude
        assert result == 1e6

    def test_klb_defined(self):
        ureg = get_unit_registry()
        quantity = 1 * ureg.klb
        result = quantity.to("lb").magnitude
        assert result == 1000.0

    def test_gpm_defined(self):
        ureg = get_unit_registry()
        quantity = 1 * ureg.gpm
        # Just check it parses without error
        assert quantity.magnitude == 1


# =============================================================================
# TRANSFORMER TESTS - PANDAS
# =============================================================================


class TestUnitConvertPandas:
    """Test unit_convert transformer with Pandas engine."""

    def _create_context(self, df: pd.DataFrame) -> EngineContext:
        pandas_ctx = PandasContext()
        return EngineContext(
            context=pandas_ctx,
            df=df,
            engine_type=EngineType.PANDAS,
        )

    def test_single_conversion(self):
        """Test converting a single column."""
        df = pd.DataFrame({"temp_f": [32.0, 212.0, 98.6]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        assert "temp_c" in result_df.columns
        assert abs(result_df["temp_c"].iloc[0] - 0.0) < 0.01  # 32°F = 0°C
        assert abs(result_df["temp_c"].iloc[1] - 100.0) < 0.01  # 212°F = 100°C
        assert abs(result_df["temp_c"].iloc[2] - 37.0) < 0.1  # 98.6°F ≈ 37°C

    def test_multiple_conversions(self):
        """Test converting multiple columns at once."""
        df = pd.DataFrame(
            {
                "pressure_psia": [14.696, 100.0, 200.0],
                "temp_f": [212.0, 400.0, 500.0],
                "flow_gpm": [100.0, 200.0, 300.0],
            }
        )
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "pressure_psia": ConversionSpec(from_unit="psia", to="bar", output="pressure_bar"),
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
                "flow_gpm": ConversionSpec(from_unit="gpm", to="L/min", output="flow_lpm"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        assert "pressure_bar" in result_df.columns
        assert "temp_c" in result_df.columns
        assert "flow_lpm" in result_df.columns

        # 14.696 psia ≈ 1.01325 bar
        assert abs(result_df["pressure_bar"].iloc[0] - 1.01325) < 0.001

    def test_in_place_conversion(self):
        """Test overwriting source column when no output specified."""
        df = pd.DataFrame({"temperature": [32.0, 212.0]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "temperature": ConversionSpec(from_unit="degF", to="degC"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        # Should overwrite original column
        assert abs(result_df["temperature"].iloc[0] - 0.0) < 0.01
        assert abs(result_df["temperature"].iloc[1] - 100.0) < 0.01

    def test_gauge_pressure_to_absolute(self):
        """Test converting gauge pressure to absolute."""
        df = pd.DataFrame({"pressure_psig": [0.0, 50.0, 100.0]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            gauge_pressure_offset="14.696 psia",
            conversions={
                "pressure_psig": ConversionSpec(
                    from_unit="psig", to="psia", output="pressure_psia"
                ),
            },
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        # 0 psig = 14.696 psia
        assert abs(result_df["pressure_psia"].iloc[0] - 14.696) < 0.001
        # 50 psig = 64.696 psia
        assert abs(result_df["pressure_psia"].iloc[1] - 64.696) < 0.001
        # 100 psig = 114.696 psia
        assert abs(result_df["pressure_psia"].iloc[2] - 114.696) < 0.001

    def test_gauge_pressure_to_bar(self):
        """Test converting gauge pressure directly to bar."""
        df = pd.DataFrame({"pressure_psig": [0.0]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "pressure_psig": ConversionSpec(from_unit="psig", to="bar", output="pressure_bar"),
            },
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        # 0 psig = 14.696 psia ≈ 1.01325 bar
        assert abs(result_df["pressure_bar"].iloc[0] - 1.01325) < 0.001

    def test_complex_engineering_units(self):
        """Test compound engineering units."""
        df = pd.DataFrame(
            {
                "htc_imperial": [10.0],  # BTU/(hr·ft²·°F)
            }
        )
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "htc_imperial": ConversionSpec(
                    from_unit="BTU/(hr * ft² * degF)",
                    to="W/(m² * K)",
                    output="htc_si",
                ),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        # 10 BTU/(hr·ft²·°F) ≈ 56.78 W/(m²·K)
        assert 50 < result_df["htc_si"].iloc[0] < 60

    def test_null_handling(self):
        """Test that null values remain null."""
        df = pd.DataFrame({"temp_f": [32.0, None, 212.0]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        assert abs(result_df["temp_c"].iloc[0] - 0.0) < 0.01
        assert pd.isna(result_df["temp_c"].iloc[1])
        assert abs(result_df["temp_c"].iloc[2] - 100.0) < 0.01

    def test_preserves_original_columns(self):
        """Test that original columns are preserved when output is different."""
        df = pd.DataFrame({"temp_f": [212.0], "other_col": ["keep me"]})
        ctx = self._create_context(df)

        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        assert "temp_f" in result_df.columns
        assert "temp_c" in result_df.columns
        assert "other_col" in result_df.columns
        assert result_df["other_col"].iloc[0] == "keep me"


# =============================================================================
# POLARS TESTS
# =============================================================================


class TestUnitConvertPolars:
    """Test unit_convert with Polars engine."""

    @pytest.fixture
    def polars_available(self):
        try:
            import polars  # noqa: F401

            return True
        except ImportError:
            pytest.skip("Polars not installed")

    def test_basic_conversion(self, polars_available):
        """Test basic Polars conversion."""
        import polars as pl

        from odibi.context import PolarsContext

        df = pl.DataFrame({"temp_f": [32.0, 212.0]})

        polars_ctx = PolarsContext()
        ctx = EngineContext(
            context=polars_ctx,
            df=df,
            engine_type=EngineType.POLARS,
        )

        params = UnitConvertParams(
            conversions={
                "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
            }
        )

        result = unit_convert(ctx, params)
        result_df = result.df

        assert "temp_c" in result_df.columns
        temps = result_df["temp_c"].to_list()
        assert abs(temps[0] - 0.0) < 0.01
        assert abs(temps[1] - 100.0) < 0.01


# =============================================================================
# VALIDATION TESTS
# =============================================================================


class TestParamsValidation:
    """Test Pydantic model validation."""

    def test_valid_params(self):
        params = UnitConvertParams(
            conversions={
                "temp": ConversionSpec(from_unit="degF", to="degC"),
            }
        )
        assert params is not None

    def test_errors_validation(self):
        with pytest.raises(ValueError, match="errors must be"):
            UnitConvertParams(
                conversions={"temp": ConversionSpec(from_unit="degF", to="degC")},
                errors="invalid",
            )

    def test_alias_from_unit(self):
        """Test that 'from' alias works for from_unit."""
        spec = ConversionSpec(**{"from": "degF", "to": "degC"})
        assert spec.from_unit == "degF"


# =============================================================================
# YAML COMPATIBILITY TESTS
# =============================================================================


class TestYamlCompatibility:
    """Test that params can be constructed from YAML-like dicts."""

    def test_from_yaml_dict(self):
        """Test constructing params from a YAML-parsed dict."""
        yaml_dict = {
            "conversions": {
                "pressure_psig": {"from": "psig", "to": "bar", "output": "pressure_bar"},
                "temp_f": {"from": "degF", "to": "degC", "output": "temp_c"},
            },
            "gauge_pressure_offset": "14.696 psia",
        }

        # This should work without errors
        params = UnitConvertParams(**yaml_dict)

        assert len(params.conversions) == 2
        assert params.conversions["pressure_psig"].from_unit == "psig"
        assert params.conversions["temp_f"].to == "degC"
