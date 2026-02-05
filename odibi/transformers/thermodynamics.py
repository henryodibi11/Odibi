"""
Thermodynamics Transformers

General-purpose thermodynamic property calculations for engineering applications.
Uses CoolProp as the primary backend (IAPWS-IF97 for water/steam, 122+ fluids).

Key transformers:
- fluid_properties: General CoolProp wrapper for any fluid
- psychrometrics: Humid air calculations using HAPropsSI
- saturation_properties: Convenience wrapper for saturated liquid/vapor

All transformers support:
- Pandas, Spark, and Polars engines
- Configurable input/output units
- Vectorized calculations for performance
"""

import time
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context

# =============================================================================
# UNIT CONVERSION CONSTANTS
# =============================================================================

# Pressure conversions TO Pascals (SI base for CoolProp)
PRESSURE_TO_PA: Dict[str, float] = {
    "Pa": 1.0,
    "kPa": 1000.0,
    "MPa": 1_000_000.0,
    "bar": 100_000.0,
    "psia": 6894.757293168,
    "psig": 6894.757293168,  # Will add gauge_offset
    "atm": 101325.0,
}

# Temperature conversions TO Kelvin (SI base for CoolProp)
# These are offset-based, handled in functions
TEMPERATURE_UNITS = {"K", "degC", "degF", "degR"}

# Enthalpy conversions FROM J/kg (SI base from CoolProp)
ENTHALPY_FROM_JKG: Dict[str, float] = {
    "J/kg": 1.0,
    "kJ/kg": 0.001,
    "BTU/lb": 0.000429923,  # 1 J/kg = 0.000429923 BTU/lb
}

# Entropy conversions FROM J/(kg·K) (SI base from CoolProp)
ENTROPY_FROM_JKGK: Dict[str, float] = {
    "J/(kg·K)": 1.0,
    "kJ/(kg·K)": 0.001,
    "BTU/(lb·R)": 0.000238846,
}

# Density conversions FROM kg/m³ (SI base from CoolProp)
DENSITY_FROM_KGM3: Dict[str, float] = {
    "kg/m³": 1.0,
    "kg/m3": 1.0,
    "lb/ft³": 0.062428,
    "lb/ft3": 0.062428,
}

# Specific heat conversions FROM J/(kg·K)
SPECIFIC_HEAT_FROM_JKGK: Dict[str, float] = {
    "J/(kg·K)": 1.0,
    "kJ/(kg·K)": 0.001,
    "BTU/(lb·R)": 0.000238846,
}

# Viscosity conversions FROM Pa·s
VISCOSITY_FROM_PAS: Dict[str, float] = {
    "Pa·s": 1.0,
    "cP": 1000.0,  # centipoise
    "lb/(ft·s)": 0.671969,
}

# Thermal conductivity conversions FROM W/(m·K)
CONDUCTIVITY_FROM_WMK: Dict[str, float] = {
    "W/(m·K)": 1.0,
    "BTU/(hr·ft·R)": 0.577789,
}

# Humidity ratio conversions (dimensionless, but can be kg/kg or lb/lb)
HUMIDITY_RATIO_UNITS = {"kg/kg", "lb/lb", "g/kg", "gr/lb"}

# Standard atmospheric pressure
ATM_PA = 101325.0
ATM_PSIA = 14.696


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _ensure_coolprop():
    """Import CoolProp and raise helpful error if not installed."""
    try:
        import CoolProp.CoolProp as CP

        return CP
    except ImportError:
        raise ImportError(
            "CoolProp is required for thermodynamics transformers. "
            "Install with: pip install CoolProp>=6.4.0"
        )


def _temp_to_kelvin(value: float, unit: str) -> float:
    """Convert temperature to Kelvin."""
    if unit == "K":
        return value
    elif unit == "degC":
        return value + 273.15
    elif unit == "degF":
        return (value - 32) * 5 / 9 + 273.15
    elif unit == "degR":
        return value * 5 / 9
    else:
        raise ValueError(f"Unknown temperature unit: {unit}")


def _temp_from_kelvin(value: float, unit: str) -> float:
    """Convert temperature from Kelvin."""
    if unit == "K":
        return value
    elif unit == "degC":
        return value - 273.15
    elif unit == "degF":
        return (value - 273.15) * 9 / 5 + 32
    elif unit == "degR":
        return value * 9 / 5
    else:
        raise ValueError(f"Unknown temperature unit: {unit}")


def _pressure_to_pa(value: float, unit: str, gauge_offset: float = 0.0) -> float:
    """Convert pressure to Pascals."""
    if unit == "psig":
        # psig = psia - atmospheric, so add gauge offset (default 14.696 psia)
        value = value + (gauge_offset if gauge_offset > 0 else 14.696)
        return value * PRESSURE_TO_PA["psia"]
    factor = PRESSURE_TO_PA.get(unit)
    if factor is None:
        raise ValueError(f"Unknown pressure unit: {unit}")
    return value * factor


def _pressure_from_pa(value: float, unit: str, gauge_offset: float = 0.0) -> float:
    """Convert pressure from Pascals."""
    if unit == "psig":
        psia = value / PRESSURE_TO_PA["psia"]
        return psia - (gauge_offset if gauge_offset > 0 else 14.696)
    factor = PRESSURE_TO_PA.get(unit)
    if factor is None:
        raise ValueError(f"Unknown pressure unit: {unit}")
    return value / factor


# =============================================================================
# FLUID PROPERTIES TRANSFORMER
# =============================================================================


class PropertyOutputConfig(BaseModel):
    """Configuration for a single output property."""

    property: str = Field(
        ...,
        description="CoolProp property key: H (enthalpy), S (entropy), D (density), "
        "C (specific heat Cp), CVMASS (Cv), V (viscosity), L (conductivity), "
        "T (temperature), P (pressure), Q (quality)",
    )
    unit: Optional[str] = Field(
        None,
        description="Output unit for this property. If not specified, uses SI units.",
    )
    output_column: Optional[str] = Field(
        None,
        description="Custom output column name. Defaults to {prefix}_{property}.",
    )


class FluidPropertiesParams(BaseModel):
    """
    Configuration for fluid property calculations using CoolProp.

    Supports 122+ fluids including Water, Ammonia, R134a, Air, CO2, etc.
    See CoolProp documentation for full list: http://www.coolprop.org/fluid_properties/PurePseudoPure.html

    State is defined by two independent properties. Common combinations:
    - P + T (pressure + temperature) - subcooled/superheated regions
    - P + Q (pressure + quality) - two-phase region
    - P + H (pressure + enthalpy) - when enthalpy is known

    Scenario: Calculate steam properties from pressure and temperature
    ```yaml
    fluid_properties:
      fluid: Water
      pressure_col: steam_pressure
      temperature_col: steam_temp
      pressure_unit: psig
      temperature_unit: degF
      gauge_offset: 14.696
      outputs:
        - property: H
          unit: BTU/lb
          output_column: steam_enthalpy
        - property: S
          unit: BTU/(lb·R)
          output_column: steam_entropy
    ```

    Scenario: Calculate saturated steam properties from pressure only
    ```yaml
    fluid_properties:
      fluid: Water
      pressure_col: steam_pressure
      quality: 1.0  # Saturated vapor
      pressure_unit: psia
      outputs:
        - property: H
          unit: BTU/lb
        - property: T
          unit: degF
    ```
    """

    fluid: str = Field(
        default="Water",
        description="CoolProp fluid name (e.g., Water, Ammonia, R134a, Air, CO2)",
    )

    # Input columns - at least 2 required to define state
    pressure_col: Optional[str] = Field(None, description="Column containing pressure values")
    temperature_col: Optional[str] = Field(None, description="Column containing temperature values")
    enthalpy_col: Optional[str] = Field(None, description="Column containing enthalpy values")
    quality_col: Optional[str] = Field(None, description="Column containing quality values (0-1)")

    # Fixed values (alternative to columns)
    pressure: Optional[float] = Field(None, description="Fixed pressure value")
    temperature: Optional[float] = Field(None, description="Fixed temperature value")
    enthalpy: Optional[float] = Field(None, description="Fixed enthalpy value (in input unit)")
    quality: Optional[float] = Field(
        None, description="Fixed quality value (0=sat liquid, 1=sat vapor)"
    )

    # Input units
    pressure_unit: str = Field(
        default="Pa",
        description="Pressure unit: Pa, kPa, MPa, bar, psia, psig, atm",
    )
    temperature_unit: str = Field(
        default="K",
        description="Temperature unit: K, degC, degF, degR",
    )
    enthalpy_unit: str = Field(
        default="J/kg",
        description="Input enthalpy unit: J/kg, kJ/kg, BTU/lb",
    )
    gauge_offset: float = Field(
        default=14.696,
        description="Atmospheric pressure offset for psig (default 14.696 psia)",
    )

    # Outputs
    outputs: List[PropertyOutputConfig] = Field(
        default_factory=lambda: [PropertyOutputConfig(property="H")],
        description="List of properties to calculate with their units",
    )
    prefix: str = Field(
        default="",
        description="Prefix for output column names (e.g., 'steam' -> 'steam_H')",
    )

    @model_validator(mode="after")
    def validate_state_definition(self) -> "FluidPropertiesParams":
        """Ensure exactly 2 state properties are defined."""
        state_props = []
        if self.pressure_col or self.pressure is not None:
            state_props.append("P")
        if self.temperature_col or self.temperature is not None:
            state_props.append("T")
        if self.enthalpy_col or self.enthalpy is not None:
            state_props.append("H")
        if self.quality_col or self.quality is not None:
            state_props.append("Q")

        if len(state_props) < 2:
            raise ValueError(
                f"Fluid state requires 2 independent properties, got {len(state_props)}: {state_props}. "
                "Provide pressure + temperature, pressure + quality, etc."
            )
        return self


def _compute_fluid_properties_row(
    row: Dict[str, Any],
    params: FluidPropertiesParams,
    CP: Any,
) -> Dict[str, Optional[float]]:
    """Compute fluid properties for a single row."""
    result = {}

    try:
        # Build input state
        inputs = []

        # Pressure
        if params.pressure_col:
            p_val = row.get(params.pressure_col)
            if p_val is None or (isinstance(p_val, float) and p_val != p_val):  # NaN check
                raise ValueError("Null pressure value")
            p_pa = _pressure_to_pa(float(p_val), params.pressure_unit, params.gauge_offset)
            inputs.extend(["P", p_pa])
        elif params.pressure is not None:
            p_pa = _pressure_to_pa(params.pressure, params.pressure_unit, params.gauge_offset)
            inputs.extend(["P", p_pa])

        # Temperature
        if params.temperature_col:
            t_val = row.get(params.temperature_col)
            if t_val is None or (isinstance(t_val, float) and t_val != t_val):
                raise ValueError("Null temperature value")
            t_k = _temp_to_kelvin(float(t_val), params.temperature_unit)
            inputs.extend(["T", t_k])
        elif params.temperature is not None:
            t_k = _temp_to_kelvin(params.temperature, params.temperature_unit)
            inputs.extend(["T", t_k])

        # Quality
        if params.quality_col:
            q_val = row.get(params.quality_col)
            if q_val is None or (isinstance(q_val, float) and q_val != q_val):
                raise ValueError("Null quality value")
            inputs.extend(["Q", float(q_val)])
        elif params.quality is not None:
            inputs.extend(["Q", params.quality])

        # Enthalpy
        if params.enthalpy_col:
            h_val = row.get(params.enthalpy_col)
            if h_val is None or (isinstance(h_val, float) and h_val != h_val):
                raise ValueError("Null enthalpy value")
            # Convert to J/kg for CoolProp
            h_factor = 1.0 / ENTHALPY_FROM_JKG.get(params.enthalpy_unit, 1.0)
            h_jkg = float(h_val) * h_factor
            inputs.extend(["H", h_jkg])
        elif params.enthalpy is not None:
            h_factor = 1.0 / ENTHALPY_FROM_JKG.get(params.enthalpy_unit, 1.0)
            h_jkg = params.enthalpy * h_factor
            inputs.extend(["H", h_jkg])

        # Calculate each output property
        for out_cfg in params.outputs:
            prop = out_cfg.property
            if out_cfg.output_column:
                col_name = out_cfg.output_column
            elif params.prefix:
                col_name = f"{params.prefix}_{prop}"
            else:
                col_name = prop

            # Call CoolProp: PropsSI(output, input1, val1, input2, val2, fluid)
            raw_value = CP.PropsSI(prop, inputs[0], inputs[1], inputs[2], inputs[3], params.fluid)

            # Convert output units
            if out_cfg.unit:
                if prop == "H":
                    factor = ENTHALPY_FROM_JKG.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop == "S":
                    factor = ENTROPY_FROM_JKGK.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop == "D":
                    factor = DENSITY_FROM_KGM3.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop in ("C", "CVMASS", "CPMASS"):
                    factor = SPECIFIC_HEAT_FROM_JKGK.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop == "V":
                    factor = VISCOSITY_FROM_PAS.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop == "L":
                    factor = CONDUCTIVITY_FROM_WMK.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor
                elif prop == "T":
                    raw_value = _temp_from_kelvin(raw_value, out_cfg.unit)
                elif prop == "P":
                    raw_value = _pressure_from_pa(raw_value, out_cfg.unit, params.gauge_offset)

            result[col_name] = raw_value

    except Exception:
        # Return nulls for all outputs on error
        for out_cfg in params.outputs:
            if out_cfg.output_column:
                col_name = out_cfg.output_column
            elif params.prefix:
                col_name = f"{params.prefix}_{out_cfg.property}"
            else:
                col_name = out_cfg.property
            result[col_name] = None

    return result


def fluid_properties(context: EngineContext, params: FluidPropertiesParams) -> EngineContext:
    """
    Calculate thermodynamic properties for any fluid using CoolProp.

    Supports 122+ fluids including Water, Ammonia, R134a, Air, CO2, etc.
    Uses IAPWS-IF97 formulation for water/steam calculations.

    Engine parity: Pandas, Spark (via Pandas UDF), Polars
    """
    ctx = get_logging_context()
    start_time = time.time()
    CP = _ensure_coolprop()

    output_cols = [
        out.output_column or (f"{params.prefix}_{out.property}" if params.prefix else out.property)
        for out in params.outputs
    ]

    ctx.debug(
        "FluidProperties starting",
        fluid=params.fluid,
        outputs=output_cols,
        engine=str(context.engine_type),
    )

    if context.engine_type == EngineType.PANDAS:
        result_df = _fluid_properties_pandas(context.df, params, CP)
    elif context.engine_type == EngineType.SPARK:
        result_df = _fluid_properties_spark(context.df, params, CP)
    elif context.engine_type == EngineType.POLARS:
        result_df = _fluid_properties_polars(context.df, params, CP)
    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "FluidProperties completed",
        elapsed_ms=round(elapsed_ms, 2),
    )
    return context.with_df(result_df)


def _fluid_properties_pandas(df, params: FluidPropertiesParams, CP) -> Any:
    """Pandas implementation using vectorized apply."""

    df = df.copy()

    # Determine required input columns
    input_cols = []
    if params.pressure_col:
        input_cols.append(params.pressure_col)
    if params.temperature_col:
        input_cols.append(params.temperature_col)
    if params.quality_col:
        input_cols.append(params.quality_col)
    if params.enthalpy_col:
        input_cols.append(params.enthalpy_col)

    def compute_row(row):
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        return _compute_fluid_properties_row(row_dict, params, CP)

    results = df.apply(compute_row, axis=1, result_type="expand")
    for col in results.columns:
        df[col] = results[col]

    return df


def _fluid_properties_spark(df, params: FluidPropertiesParams, CP) -> Any:
    """Spark implementation using Pandas UDF."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, DoubleType

    # Build output schema
    output_cols = []
    for out in params.outputs:
        col_name = out.output_column or (
            f"{params.prefix}_{out.property}" if params.prefix else out.property
        )
        output_cols.append(col_name)

    schema = StructType([StructField(c, DoubleType(), True) for c in output_cols])

    # Collect input column names
    input_cols = []
    if params.pressure_col:
        input_cols.append(params.pressure_col)
    if params.temperature_col:
        input_cols.append(params.temperature_col)
    if params.quality_col:
        input_cols.append(params.quality_col)
    if params.enthalpy_col:
        input_cols.append(params.enthalpy_col)

    # Serialize params for UDF
    params_dict = params.model_dump()

    @pandas_udf(schema)
    def compute_props_udf(*cols: pd.Series) -> pd.DataFrame:
        # Re-import inside UDF for Spark executors
        import CoolProp.CoolProp as CP_local

        # Reconstruct params
        local_params = FluidPropertiesParams(**params_dict)

        # Build DataFrame from input columns
        input_df = pd.concat(cols, axis=1)
        input_df.columns = input_cols

        results = []
        for _, row in input_df.iterrows():
            row_dict = row.to_dict()
            res = _compute_fluid_properties_row(row_dict, local_params, CP_local)
            results.append(res)

        return pd.DataFrame(results)

    # Apply UDF
    struct_col = compute_props_udf(*[df[c] for c in input_cols])
    result = df.withColumn("__fluid_props__", struct_col)

    # Extract individual columns
    for col_name in output_cols:
        result = result.withColumn(col_name, result["__fluid_props__"][col_name])

    return result.drop("__fluid_props__")


def _fluid_properties_polars(df, params: FluidPropertiesParams, CP) -> Any:
    """Polars implementation - converts through Pandas for CoolProp compatibility."""
    import polars as pl

    # Convert to Pandas, process, convert back
    pdf = df.to_pandas()
    result_pdf = _fluid_properties_pandas(pdf, params, CP)
    return pl.from_pandas(result_pdf)


# =============================================================================
# SATURATION PROPERTIES TRANSFORMER
# =============================================================================


class SaturationPropertiesParams(BaseModel):
    """
    Convenience wrapper for saturated liquid (Q=0) or saturated vapor (Q=1) properties.

    This is a simplified interface for common saturation calculations.

    Scenario: Get saturated steam properties at a given pressure
    ```yaml
    saturation_properties:
      fluid: Water
      pressure_col: steam_pressure
      pressure_unit: psig
      phase: vapor
      outputs:
        - property: H
          unit: BTU/lb
          output_column: hg
        - property: T
          unit: degF
          output_column: sat_temp
    ```

    Scenario: Get saturated liquid enthalpy (hf)
    ```yaml
    saturation_properties:
      pressure_col: pressure_psia
      pressure_unit: psia
      phase: liquid
      outputs:
        - property: H
          unit: BTU/lb
          output_column: hf
    ```
    """

    fluid: str = Field(default="Water", description="CoolProp fluid name")
    pressure_col: Optional[str] = Field(None, description="Column containing pressure values")
    pressure: Optional[float] = Field(None, description="Fixed pressure value")
    temperature_col: Optional[str] = Field(None, description="Column containing saturation temp")
    temperature: Optional[float] = Field(None, description="Fixed saturation temperature")
    pressure_unit: str = Field(default="Pa", description="Pressure unit")
    temperature_unit: str = Field(default="K", description="Temperature unit")
    gauge_offset: float = Field(default=14.696, description="Gauge pressure offset for psig")

    phase: Literal["liquid", "vapor"] = Field(
        default="vapor",
        description="Phase: 'liquid' (Q=0) or 'vapor' (Q=1)",
    )

    outputs: List[PropertyOutputConfig] = Field(
        default_factory=lambda: [PropertyOutputConfig(property="H")],
        description="Properties to calculate",
    )
    prefix: str = Field(default="", description="Prefix for output columns")

    @model_validator(mode="after")
    def validate_single_state_prop(self) -> "SaturationPropertiesParams":
        """Ensure exactly one state property is defined (P or T)."""
        has_p = self.pressure_col is not None or self.pressure is not None
        has_t = self.temperature_col is not None or self.temperature is not None
        if not has_p and not has_t:
            raise ValueError("Either pressure or temperature must be provided for saturation")
        return self


def saturation_properties(
    context: EngineContext, params: SaturationPropertiesParams
) -> EngineContext:
    """
    Calculate saturated liquid or vapor properties.

    Convenience wrapper that sets Q=0 (liquid) or Q=1 (vapor) automatically.
    """
    # Convert to FluidPropertiesParams with quality set
    quality = 0.0 if params.phase == "liquid" else 1.0

    fluid_params = FluidPropertiesParams(
        fluid=params.fluid,
        pressure_col=params.pressure_col,
        pressure=params.pressure,
        temperature_col=params.temperature_col,
        temperature=params.temperature,
        pressure_unit=params.pressure_unit,
        temperature_unit=params.temperature_unit,
        gauge_offset=params.gauge_offset,
        quality=quality,
        outputs=params.outputs,
        prefix=params.prefix,
    )

    return fluid_properties(context, fluid_params)


# =============================================================================
# PSYCHROMETRICS TRANSFORMER
# =============================================================================


class PsychrometricOutputConfig(BaseModel):
    """Configuration for psychrometric output property."""

    property: str = Field(
        ...,
        description="Property: W (humidity ratio), B (wet bulb), D (dew point), "
        "H (enthalpy), V (specific volume), R (relative humidity)",
    )
    unit: Optional[str] = Field(None, description="Output unit")
    output_column: Optional[str] = Field(None, description="Custom output column name")


class PsychrometricsParams(BaseModel):
    """
    Psychrometric (humid air) calculations using CoolProp HAPropsSI.

    Calculates moist air properties from dry bulb temperature and one other
    property (typically relative humidity or humidity ratio).

    CoolProp property keys for humid air:
    - W: Humidity ratio (kg water / kg dry air)
    - B: Wet bulb temperature
    - D: Dew point temperature
    - H: Enthalpy (per kg dry air)
    - V: Specific volume (per kg dry air)
    - R: Relative humidity (0-1)
    - T: Dry bulb temperature
    - P: Total pressure

    Scenario: Calculate humidity ratio from T and RH
    ```yaml
    psychrometrics:
      dry_bulb_col: ambient_temp
      relative_humidity_col: rh_percent
      temperature_unit: degF
      rh_is_percent: true
      pressure_unit: psia
      elevation_ft: 875
      outputs:
        - property: W
          unit: lb/lb
          output_column: humidity_ratio
        - property: B
          unit: degF
          output_column: wet_bulb
        - property: D
          unit: degF
          output_column: dew_point
    ```

    Scenario: Calculate RH from temperature and humidity ratio
    ```yaml
    psychrometrics:
      dry_bulb_col: temp_f
      humidity_ratio_col: w
      temperature_unit: degF
      pressure: 14.696
      pressure_unit: psia
      outputs:
        - property: R
          output_column: relative_humidity
    ```
    """

    # Input columns
    dry_bulb_col: str = Field(..., description="Column containing dry bulb temperature")
    relative_humidity_col: Optional[str] = Field(
        None, description="Column containing relative humidity"
    )
    humidity_ratio_col: Optional[str] = Field(
        None, description="Column containing humidity ratio (kg/kg or lb/lb)"
    )
    wet_bulb_col: Optional[str] = Field(None, description="Column containing wet bulb temperature")
    dew_point_col: Optional[str] = Field(
        None, description="Column containing dew point temperature"
    )

    # Fixed values
    relative_humidity: Optional[float] = Field(None, description="Fixed relative humidity")
    humidity_ratio: Optional[float] = Field(None, description="Fixed humidity ratio")

    # Pressure input (required for psychrometrics)
    pressure_col: Optional[str] = Field(None, description="Column containing pressure")
    pressure: Optional[float] = Field(None, description="Fixed pressure value")
    elevation_ft: Optional[float] = Field(
        None,
        description="Elevation in feet (used to estimate pressure if not provided)",
    )
    elevation_m: Optional[float] = Field(
        None,
        description="Elevation in meters (used to estimate pressure if not provided)",
    )

    # Units
    temperature_unit: str = Field(default="K", description="Temperature unit for all temps")
    pressure_unit: str = Field(default="Pa", description="Pressure unit")
    humidity_ratio_unit: str = Field(
        default="kg/kg",
        description="Humidity ratio unit (kg/kg, lb/lb, g/kg)",
    )
    rh_is_percent: bool = Field(
        default=False,
        description="If True, RH input is 0-100%, otherwise 0-1",
    )

    # Outputs
    outputs: List[PsychrometricOutputConfig] = Field(
        default_factory=lambda: [PsychrometricOutputConfig(property="W")],
        description="Properties to calculate",
    )
    prefix: str = Field(default="", description="Prefix for output columns")

    @model_validator(mode="after")
    def validate_inputs(self) -> "PsychrometricsParams":
        """Validate that we have enough inputs to define the state."""
        # Need T + one of: RH, W, Twb, Tdp
        has_second = (
            self.relative_humidity_col is not None
            or self.relative_humidity is not None
            or self.humidity_ratio_col is not None
            or self.humidity_ratio is not None
            or self.wet_bulb_col is not None
            or self.dew_point_col is not None
        )
        if not has_second:
            raise ValueError(
                "Psychrometrics requires dry_bulb and one of: "
                "relative_humidity, humidity_ratio, wet_bulb, or dew_point"
            )

        # Need pressure source
        has_pressure = (
            self.pressure_col is not None
            or self.pressure is not None
            or self.elevation_ft is not None
            or self.elevation_m is not None
        )
        if not has_pressure:
            raise ValueError(
                "Psychrometrics requires pressure via: pressure, pressure_col, "
                "elevation_ft, or elevation_m"
            )

        return self


def _compute_psychrometrics_row(
    row: Dict[str, Any],
    params: PsychrometricsParams,
    CP: Any,
) -> Dict[str, Optional[float]]:
    """Compute psychrometric properties for a single row."""
    import math

    result = {}

    try:
        # Get dry bulb temperature
        tdb_val = row.get(params.dry_bulb_col)
        if tdb_val is None or (isinstance(tdb_val, float) and tdb_val != tdb_val):
            raise ValueError("Null dry bulb temperature")
        tdb_k = _temp_to_kelvin(float(tdb_val), params.temperature_unit)

        # Get pressure
        if params.pressure_col:
            p_val = row.get(params.pressure_col)
            if p_val is None:
                raise ValueError("Null pressure")
            p_pa = _pressure_to_pa(float(p_val), params.pressure_unit)
        elif params.pressure is not None:
            p_pa = _pressure_to_pa(params.pressure, params.pressure_unit)
        elif params.elevation_ft is not None:
            # Barometric formula: P = P0 * exp(-0.0000366 * elevation_ft)
            p_pa = ATM_PA * math.exp(-0.0000366 * params.elevation_ft)
        elif params.elevation_m is not None:
            # P = P0 * exp(-elevation_m / 8500)
            p_pa = ATM_PA * math.exp(-params.elevation_m / 8500)
        else:
            p_pa = ATM_PA

        # Build CoolProp HAPropsSI inputs
        # HAPropsSI(output, input1_name, input1_val, input2_name, input2_val, input3_name, input3_val)
        # Always need P and T, plus one humidity parameter

        inputs = ["P", p_pa, "T", tdb_k]

        # Determine the humidity input
        if params.relative_humidity_col:
            rh_val = row.get(params.relative_humidity_col)
            if rh_val is None:
                raise ValueError("Null RH")
            rh = float(rh_val)
            if params.rh_is_percent:
                rh = rh / 100.0
            inputs.extend(["R", rh])
        elif params.relative_humidity is not None:
            rh = params.relative_humidity
            if params.rh_is_percent:
                rh = rh / 100.0
            inputs.extend(["R", rh])
        elif params.humidity_ratio_col:
            w_val = row.get(params.humidity_ratio_col)
            if w_val is None:
                raise ValueError("Null humidity ratio")
            w = float(w_val)
            # Convert if needed (g/kg -> kg/kg)
            if params.humidity_ratio_unit == "g/kg":
                w = w / 1000.0
            elif params.humidity_ratio_unit == "gr/lb":
                w = w / 7000.0  # grains to lb
            inputs.extend(["W", w])
        elif params.humidity_ratio is not None:
            w = params.humidity_ratio
            if params.humidity_ratio_unit == "g/kg":
                w = w / 1000.0
            elif params.humidity_ratio_unit == "gr/lb":
                w = w / 7000.0
            inputs.extend(["W", w])
        elif params.wet_bulb_col:
            twb_val = row.get(params.wet_bulb_col)
            if twb_val is None:
                raise ValueError("Null wet bulb")
            twb_k = _temp_to_kelvin(float(twb_val), params.temperature_unit)
            inputs.extend(["B", twb_k])
        elif params.dew_point_col:
            tdp_val = row.get(params.dew_point_col)
            if tdp_val is None:
                raise ValueError("Null dew point")
            tdp_k = _temp_to_kelvin(float(tdp_val), params.temperature_unit)
            inputs.extend(["D", tdp_k])

        # Calculate each output
        for out_cfg in params.outputs:
            prop = out_cfg.property
            col_name = out_cfg.output_column or (
                f"{params.prefix}_{prop}" if params.prefix else prop
            )

            raw_value = CP.HAPropsSI(
                prop, inputs[0], inputs[1], inputs[2], inputs[3], inputs[4], inputs[5]
            )

            # Convert output units
            if out_cfg.unit:
                if prop in ("T", "B", "D"):  # Temperatures
                    raw_value = _temp_from_kelvin(raw_value, out_cfg.unit)
                elif prop == "W":  # Humidity ratio
                    if out_cfg.unit == "g/kg":
                        raw_value *= 1000.0
                    elif out_cfg.unit == "gr/lb":
                        raw_value *= 7000.0
                    # lb/lb and kg/kg are equivalent (mass ratio)
                elif prop == "H":  # Enthalpy
                    factor = ENTHALPY_FROM_JKG.get(out_cfg.unit)
                    if factor:
                        raw_value *= factor

            result[col_name] = raw_value

    except Exception:
        for out_cfg in params.outputs:
            col_name = out_cfg.output_column or (
                f"{params.prefix}_{out_cfg.property}" if params.prefix else out_cfg.property
            )
            result[col_name] = None

    return result


def psychrometrics(context: EngineContext, params: PsychrometricsParams) -> EngineContext:
    """
    Calculate psychrometric (humid air) properties using CoolProp HAPropsSI.

    Engine parity: Pandas, Spark (via Pandas UDF), Polars
    """
    ctx = get_logging_context()
    start_time = time.time()
    CP = _ensure_coolprop()

    output_cols = [
        out.output_column or (f"{params.prefix}_{out.property}" if params.prefix else out.property)
        for out in params.outputs
    ]

    ctx.debug(
        "Psychrometrics starting",
        outputs=output_cols,
        engine=str(context.engine_type),
    )

    if context.engine_type == EngineType.PANDAS:
        result_df = _psychrometrics_pandas(context.df, params, CP)
    elif context.engine_type == EngineType.SPARK:
        result_df = _psychrometrics_spark(context.df, params, CP)
    elif context.engine_type == EngineType.POLARS:
        result_df = _psychrometrics_polars(context.df, params, CP)
    else:
        raise ValueError(f"Unsupported engine: {context.engine_type}")

    elapsed_ms = (time.time() - start_time) * 1000
    ctx.debug(
        "Psychrometrics completed",
        elapsed_ms=round(elapsed_ms, 2),
    )
    return context.with_df(result_df)


def _psychrometrics_pandas(df, params: PsychrometricsParams, CP) -> Any:
    """Pandas implementation."""

    df = df.copy()

    def compute_row(row):
        row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        return _compute_psychrometrics_row(row_dict, params, CP)

    results = df.apply(compute_row, axis=1, result_type="expand")
    for col in results.columns:
        df[col] = results[col]

    return df


def _psychrometrics_spark(df, params: PsychrometricsParams, CP) -> Any:
    """Spark implementation using Pandas UDF."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, DoubleType

    # Build output schema
    output_cols = []
    for out in params.outputs:
        col_name = out.output_column or (
            f"{params.prefix}_{out.property}" if params.prefix else out.property
        )
        output_cols.append(col_name)

    schema = StructType([StructField(c, DoubleType(), True) for c in output_cols])

    # Collect input columns
    input_cols = [params.dry_bulb_col]
    if params.relative_humidity_col:
        input_cols.append(params.relative_humidity_col)
    if params.humidity_ratio_col:
        input_cols.append(params.humidity_ratio_col)
    if params.wet_bulb_col:
        input_cols.append(params.wet_bulb_col)
    if params.dew_point_col:
        input_cols.append(params.dew_point_col)
    if params.pressure_col:
        input_cols.append(params.pressure_col)

    params_dict = params.model_dump()

    @pandas_udf(schema)
    def compute_psychro_udf(*cols: pd.Series) -> pd.DataFrame:
        import CoolProp.CoolProp as CP_local

        local_params = PsychrometricsParams(**params_dict)

        input_df = pd.concat(cols, axis=1)
        input_df.columns = input_cols

        results = []
        for _, row in input_df.iterrows():
            row_dict = row.to_dict()
            res = _compute_psychrometrics_row(row_dict, local_params, CP_local)
            results.append(res)

        return pd.DataFrame(results)

    struct_col = compute_psychro_udf(*[df[c] for c in input_cols])
    result = df.withColumn("__psychro_props__", struct_col)

    for col_name in output_cols:
        result = result.withColumn(col_name, result["__psychro_props__"][col_name])

    return result.drop("__psychro_props__")


def _psychrometrics_polars(df, params: PsychrometricsParams, CP) -> Any:
    """Polars implementation."""
    import polars as pl

    pdf = df.to_pandas()
    result_pdf = _psychrometrics_pandas(pdf, params, CP)
    return pl.from_pandas(result_pdf)
