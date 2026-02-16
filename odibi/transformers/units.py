"""
Unit Conversion Transformer

Provides declarative unit conversion for data engineering pipelines using Pint.
Supports 1000+ unit definitions out of the box, including all common engineering units.

Example:
    ```yaml
    transforms:
      - unit_convert:
          conversions:
            pressure_psig:
              from: psig
              to: bar
              output: pressure_bar
            temperature_f:
              from: degF
              to: degC
              output: temperature_c
    ```
"""

import time
from typing import Dict, List, Optional

import pint
from pydantic import BaseModel, Field, field_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context

# Initialize unit registry with default definitions
# This includes SI, imperial, and many engineering units
_ureg = pint.UnitRegistry()

# Add common engineering aliases that Pint doesn't have by default
# Note: Don't redefine units that already exist (causes recursion)
_ureg.define("psig = psi")  # Gauge pressure (handled specially in code)
_ureg.define("psia = psi")  # Absolute pressure (alias)
_ureg.define("mcf = 1000 * foot ** 3")  # Thousand cubic feet
_ureg.define("mmcf = 1e6 * foot ** 3")  # Million cubic feet
_ureg.define("mscf = 1000 * foot ** 3")  # Thousand standard cubic feet
_ureg.define("mmscf = 1e6 * foot ** 3")  # Million standard cubic feet
_ureg.define("mbtu = 1000 * BTU")  # Thousand BTU
_ureg.define("mmbtu = 1e6 * BTU")  # Million BTU
_ureg.define("klb = 1000 * pound")  # Thousand pounds
_ureg.define("mlb = 1e6 * pound")  # Million pounds
_ureg.define("kgal = 1000 * gallon")  # Thousand gallons
_ureg.define("mgd = 1e6 * gallon / day")  # Million gallons per day
_ureg.define("gpm = gallon / minute")  # Gallons per minute
_ureg.define("cfm = foot ** 3 / minute")  # Cubic feet per minute
_ureg.define("scfm = foot ** 3 / minute")  # Standard CFM (same dim, different conditions)
_ureg.define("acfm = foot ** 3 / minute")  # Actual CFM


def get_unit_registry() -> pint.UnitRegistry:
    """Get the odibi unit registry with engineering units defined."""
    return _ureg


# =============================================================================
# CONFIGURATION MODELS
# =============================================================================


class ConversionSpec(BaseModel):
    """Specification for a single unit conversion."""

    from_unit: str = Field(
        ...,
        alias="from",
        description="Source unit (e.g., 'psig', 'degF', 'BTU/lb')",
    )
    to: str = Field(
        ...,
        description="Target unit (e.g., 'bar', 'degC', 'kJ/kg')",
    )
    output: Optional[str] = Field(
        None,
        description="Output column name. If not specified, overwrites the source column.",
    )

    model_config = {"populate_by_name": True}


class UnitConvertParams(BaseModel):
    """
    Configuration for unit conversion transformer.

    Converts columns from one unit to another using Pint's comprehensive
    unit database. Supports all SI units, imperial units, and common
    engineering units.

    Scenario: Normalize sensor data to SI units
    ```yaml
    unit_convert:
      conversions:
        pressure_psig:
          from: psig
          to: bar
          output: pressure_bar
        temperature_f:
          from: degF
          to: degC
          output: temperature_c
        flow_gpm:
          from: gpm
          to: m³/s
          output: flow_si
    ```

    Scenario: Convert in-place (overwrite original columns)
    ```yaml
    unit_convert:
      conversions:
        pressure:
          from: psia
          to: kPa
        temperature:
          from: degF
          to: K
    ```

    Scenario: Handle gauge pressure with custom atmospheric reference
    ```yaml
    unit_convert:
      gauge_pressure_offset: 14.696 psia
      conversions:
        steam_pressure:
          from: psig
          to: psia
          output: steam_pressure_abs
    ```

    Scenario: Complex engineering units
    ```yaml
    unit_convert:
      conversions:
        heat_transfer_coeff:
          from: BTU/(hr * ft² * degF)
          to: W/(m² * K)
          output: htc_si
        thermal_conductivity:
          from: BTU/(hr * ft * degF)
          to: W/(m * K)
          output: k_si
        specific_heat:
          from: BTU/(lb * degF)
          to: kJ/(kg * K)
          output: cp_si
    ```
    """

    conversions: Dict[str, ConversionSpec] = Field(
        ...,
        description="Mapping of source column names to conversion specifications",
    )
    gauge_pressure_offset: Optional[str] = Field(
        default="14.696 psia",
        description="Atmospheric pressure for gauge-to-absolute conversions. "
        "Default is sea level (14.696 psia).",
    )
    errors: str = Field(
        default="null",
        description="How to handle conversion errors: 'null' (default), 'raise', or 'ignore'",
    )

    @field_validator("errors")
    @classmethod
    def validate_errors(cls, v):
        if v not in ("null", "raise", "ignore"):
            raise ValueError("errors must be 'null', 'raise', or 'ignore'")
        return v


# =============================================================================
# CONVERSION LOGIC
# =============================================================================


def _parse_gauge_offset(offset_str: str) -> float:
    """Parse gauge pressure offset string to psia value."""
    try:
        quantity = _ureg(offset_str)
        return quantity.to("psi").magnitude
    except Exception as e:
        logger = get_logging_context()
        logger.debug(
            f"Failed to parse gauge offset '{offset_str}', using default 14.696 psia: {type(e).__name__}: {e}"
        )
        return 14.696  # Default to sea level


def _convert_value(
    value: float,
    from_unit: str,
    to_unit: str,
    gauge_offset: float = 14.696,
) -> float:
    """Convert a single value from one unit to another."""
    if value is None or (isinstance(value, float) and value != value):  # NaN check
        return None

    # Handle gauge pressure specially
    from_unit_lower = from_unit.lower().strip()
    to_unit_lower = to_unit.lower().strip()

    # psig -> anything: add atmospheric pressure first
    if from_unit_lower == "psig":
        value = value + gauge_offset
        from_unit = "psi"

    # anything -> psig: convert to psi then subtract atmospheric
    if to_unit_lower == "psig":
        quantity = value * _ureg(from_unit)
        psi_value = quantity.to("psi").magnitude
        return psi_value - gauge_offset

    # Standard conversion
    quantity = value * _ureg(from_unit)
    return quantity.to(to_unit).magnitude


def _convert_column_pandas(
    df,
    source_col: str,
    from_unit: str,
    to_unit: str,
    output_col: str,
    gauge_offset: float,
    errors: str,
) -> None:
    """Convert a column in a Pandas DataFrame."""
    import numpy as np

    try:
        # Get values
        values = df[source_col].values.copy()

        # Handle gauge pressure
        from_unit_lower = from_unit.lower().strip()
        to_unit_lower = to_unit.lower().strip()

        if from_unit_lower == "psig":
            values = values + gauge_offset
            from_unit = "psi"

        # Use Quantity constructor for proper offset unit handling (temperatures)
        quantity = _ureg.Quantity(values, from_unit)
        converted = quantity.to(to_unit).magnitude

        if to_unit_lower == "psig":
            converted = converted - gauge_offset

        df[output_col] = converted

    except Exception as e:
        if errors == "raise":
            raise ValueError(f"Failed to convert {source_col} from {from_unit} to {to_unit}: {e}")
        elif errors == "null":
            df[output_col] = np.nan
        # else 'ignore': leave column unchanged


def _convert_column_spark(
    df,
    source_col: str,
    from_unit: str,
    to_unit: str,
    output_col: str,
    gauge_offset: float,
    errors: str,
):
    """Convert a column in a Spark DataFrame using a UDF."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import DoubleType

    from_unit_captured = from_unit
    to_unit_captured = to_unit
    gauge_offset_captured = gauge_offset
    errors_captured = errors

    @pandas_udf(DoubleType())
    def convert_udf(values: pd.Series) -> pd.Series:
        import numpy as np

        try:
            # Re-import inside UDF for Spark executors
            import pint

            ureg = pint.UnitRegistry()

            vals = values.values.copy()
            from_u = from_unit_captured
            to_u = to_unit_captured

            # Handle gauge pressure
            if from_u.lower().strip() == "psig":
                vals = vals + gauge_offset_captured
                from_u = "psi"

            # Use Quantity constructor for proper offset unit handling
            quantity = ureg.Quantity(vals, from_u)
            converted = quantity.to(to_u).magnitude

            if to_u.lower().strip() == "psig":
                converted = converted - gauge_offset_captured

            return pd.Series(converted)

        except Exception as e:
            logger = get_logging_context()
            logger.debug(
                f"Failed to convert values from '{from_unit_captured}' to '{to_unit_captured}': {type(e).__name__}: {e}"
            )
            if errors_captured == "raise":
                raise
            elif errors_captured == "null":
                return pd.Series([np.nan] * len(values))
            else:
                return values

    return df.withColumn(output_col, convert_udf(df[source_col]))


def _convert_column_polars(
    df,
    source_col: str,
    from_unit: str,
    to_unit: str,
    output_col: str,
    gauge_offset: float,
    errors: str,
):
    """Convert a column in a Polars DataFrame."""
    import polars as pl

    try:
        values = df[source_col].to_numpy().copy()

        from_unit_lower = from_unit.lower().strip()
        to_unit_lower = to_unit.lower().strip()

        if from_unit_lower == "psig":
            values = values + gauge_offset
            from_unit = "psi"

        # Use Quantity constructor for proper offset unit handling
        quantity = _ureg.Quantity(values, from_unit)
        converted = quantity.to(to_unit).magnitude

        if to_unit_lower == "psig":
            converted = converted - gauge_offset

        return df.with_columns(pl.Series(name=output_col, values=converted))

    except Exception as e:
        if errors == "raise":
            raise ValueError(f"Failed to convert {source_col} from {from_unit} to {to_unit}: {e}")
        elif errors == "null":
            return df.with_columns(pl.lit(None).alias(output_col))
        else:
            return df


# =============================================================================
# MAIN TRANSFORMER
# =============================================================================


def unit_convert(context: EngineContext, params: UnitConvertParams) -> EngineContext:
    """
    Convert columns from one unit to another.

    Uses Pint for comprehensive unit conversion support. Handles all SI units,
    imperial units, and common engineering units out of the box.

    Engine parity: Pandas, Spark, Polars
    """
    ctx = get_logging_context()
    start_time = time.time()

    conversions = params.conversions
    gauge_offset = _parse_gauge_offset(params.gauge_pressure_offset or "14.696 psia")
    errors = params.errors

    ctx.debug(
        "UnitConvert starting",
        num_conversions=len(conversions),
        engine=str(context.engine_type),
    )

    if context.engine_type == EngineType.PANDAS:
        df = context.df.copy()
        for source_col, spec in conversions.items():
            output_col = spec.output or source_col
            _convert_column_pandas(
                df, source_col, spec.from_unit, spec.to, output_col, gauge_offset, errors
            )
        result_df = df

    elif context.engine_type == EngineType.SPARK:
        result_df = context.df
        for source_col, spec in conversions.items():
            output_col = spec.output or source_col
            result_df = _convert_column_spark(
                result_df, source_col, spec.from_unit, spec.to, output_col, gauge_offset, errors
            )

    elif context.engine_type == EngineType.POLARS:
        result_df = context.df
        for source_col, spec in conversions.items():
            output_col = spec.output or source_col
            result_df = _convert_column_polars(
                result_df, source_col, spec.from_unit, spec.to, output_col, gauge_offset, errors
            )

    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "UnitConvert completed",
        conversions=list(conversions.keys()),
        elapsed_ms=round(elapsed_ms, 2),
    )

    return context.with_df(result_df)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def convert(value: float, from_unit: str, to_unit: str) -> float:
    """
    Convenience function for one-off unit conversions.

    Args:
        value: Numeric value to convert
        from_unit: Source unit string (e.g., 'degF', 'psia', 'BTU/lb')
        to_unit: Target unit string (e.g., 'degC', 'bar', 'kJ/kg')

    Returns:
        Converted value

    Example:
        >>> from odibi.transformers.units import convert
        >>> convert(212, 'degF', 'degC')
        100.0
        >>> convert(14.696, 'psia', 'bar')
        1.01325
        >>> convert(1000, 'BTU/lb', 'kJ/kg')
        2326.0
    """
    # Use Quantity constructor for proper offset unit handling (temperatures)
    quantity = _ureg.Quantity(value, from_unit)
    return quantity.to(to_unit).magnitude


def list_units(category: Optional[str] = None) -> List[str]:
    """
    List available units, optionally filtered by category.

    Args:
        category: Optional category like 'pressure', 'temperature', 'energy'

    Returns:
        List of unit names

    Example:
        >>> from odibi.transformers.units import list_units
        >>> list_units('pressure')[:5]
        ['Pa', 'kPa', 'MPa', 'bar', 'psi']
    """
    # Common engineering units by category
    categories = {
        "pressure": [
            "Pa",
            "kPa",
            "MPa",
            "bar",
            "mbar",
            "psi",
            "psia",
            "psig",
            "atm",
            "torr",
            "mmHg",
            "inHg",
            "inH2O",
        ],
        "temperature": ["K", "degC", "degF", "degR"],
        "energy": [
            "J",
            "kJ",
            "MJ",
            "GJ",
            "BTU",
            "mbtu",
            "mmbtu",
            "therm",
            "cal",
            "kcal",
            "Wh",
            "kWh",
            "MWh",
        ],
        "power": ["W", "kW", "MW", "GW", "hp", "BTU/hr", "BTU/s", "ton_of_refrigeration"],
        "mass": ["kg", "g", "mg", "lb", "klb", "oz", "ton", "tonne"],
        "length": ["m", "cm", "mm", "km", "ft", "inch", "mile", "yard"],
        "volume": ["m³", "L", "mL", "gallon", "kgal", "ft³", "mcf", "barrel"],
        "flow_rate": [
            "m³/s",
            "m³/hr",
            "L/s",
            "L/min",
            "gpm",
            "cfm",
            "scfm",
            "mgd",
            "kg/s",
            "kg/hr",
            "lb/hr",
            "klb/hr",
        ],
        "density": ["kg/m³", "g/cm³", "lb/ft³", "lb/gallon"],
        "viscosity": ["Pa*s", "poise", "centipoise", "lb/(ft*s)"],
        "thermal_conductivity": ["W/(m*K)", "BTU/(hr*ft*degF)"],
        "heat_transfer": ["W/(m²*K)", "BTU/(hr*ft²*degF)"],
        "specific_heat": ["J/(kg*K)", "kJ/(kg*K)", "BTU/(lb*degF)"],
        "enthalpy": ["J/kg", "kJ/kg", "BTU/lb"],
    }

    if category:
        return categories.get(category.lower(), [])
    return categories
