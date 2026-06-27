# Thermodynamics Transformers Guide

This guide provides complete documentation for odibi's thermodynamics transformers, which enable engineering calculations for steam, refrigerants, and humid air directly in your data pipelines.

---

## Overview

The thermodynamics module provides three transformers for engineering calculations:

| Transformer | Purpose | Use Cases |
|-------------|---------|-----------|
| `fluid_properties` | Calculate thermodynamic properties for any fluid | Steam tables, refrigerant cycles, gas properties |
| `saturation_properties` | Shortcut for saturated liquid/vapor properties | Steam drums, evaporators, condensers |
| `psychrometrics` | Humid air (psychrometric) calculations | Dryers, HVAC, weather corrections |

### Key Features

- **122+ fluids supported** via CoolProp (Water, Ammonia, R134a, CO2, etc.)
- **IAPWS-IF97 formulation** for water/steam (industry standard)
- **Flexible unit conversion** - work in your preferred units (psig, BTU/lb, °F, etc.)
- **Engine parity** - works with Pandas, Spark, and Polars
- **YAML-native** - configure in pipelines like any other transformer

---

## Installation

```bash
pip install odibi[thermodynamics]
```

This installs:
- **CoolProp** ≥6.4.0 - Primary calculation engine
- **iapws** - Optional fallback for steam tables
- **psychrolib** - Optional fallback for psychrometrics

### Verify Installation

```python
import CoolProp
print(f"CoolProp version: {CoolProp.__version__}")
```

---

## Quick Start

### Example: Steam Enthalpy from Pressure and Temperature

```yaml
nodes:
  - name: steam_with_properties
    source: boiler_readings
    transforms:
      - fluid_properties:
          fluid: Water
          pressure_col: steam_pressure_psig
          temperature_col: steam_temp_f
          pressure_unit: psig
          temperature_unit: degF
          outputs:
            - property: H
              unit: BTU/lb
              output_column: steam_enthalpy
```

**Input:**
| steam_pressure_psig | steam_temp_f |
|---------------------|--------------|
| 100 | 400 |
| 150 | 450 |

**Output:**
| steam_pressure_psig | steam_temp_f | steam_enthalpy |
|---------------------|--------------|----------------|
| 100 | 400 | 1225.5 |
| 150 | 450 | 1253.2 |

---

## Transformer Reference

## `fluid_properties`

The general-purpose thermodynamic property calculator. Works with any CoolProp-supported fluid.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `fluid` | string | No | `"Water"` | Fluid name (see [Supported Fluids](#supported-fluids)) |
| `pressure_col` | string | Conditional | - | Column containing pressure values |
| `temperature_col` | string | Conditional | - | Column containing temperature values |
| `quality_col` | string | Conditional | - | Column containing vapor quality (0-1) |
| `enthalpy_col` | string | Conditional | - | Column containing enthalpy values |
| `pressure` | float | Conditional | - | Fixed pressure value (instead of column) |
| `temperature` | float | Conditional | - | Fixed temperature value (instead of column) |
| `quality` | float | Conditional | - | Fixed quality value (0=liquid, 1=vapor) |
| `enthalpy` | float | Conditional | - | Fixed enthalpy value |
| `pressure_unit` | string | No | `"Pa"` | Pressure unit (see [Units](#supported-units)) |
| `temperature_unit` | string | No | `"K"` | Temperature unit |
| `enthalpy_unit` | string | No | `"J/kg"` | Input enthalpy unit |
| `gauge_offset` | float | No | `14.696` | Atmospheric pressure for psig conversion |
| `outputs` | list | No | `[{property: "H"}]` | Properties to calculate |
| `prefix` | string | No | `""` | Prefix for output column names |

!!! important "State Definition"
    You must provide **exactly 2 independent properties** to define the thermodynamic state. Common combinations:

    - **P + T**: Pressure + Temperature (subcooled or superheated)
    - **P + Q**: Pressure + Quality (two-phase/saturation region)
    - **P + H**: Pressure + Enthalpy (when enthalpy is known)
    - **T + Q**: Temperature + Quality (saturation at known temp)

### Output Properties

| Property Key | Description | SI Unit |
|--------------|-------------|---------|
| `H` | Specific enthalpy | J/kg |
| `S` | Specific entropy | J/(kg·K) |
| `D` | Density | kg/m³ |
| `C` or `CPMASS` | Specific heat at constant pressure (Cp) | J/(kg·K) |
| `CVMASS` | Specific heat at constant volume (Cv) | J/(kg·K) |
| `V` | Dynamic viscosity | Pa·s |
| `L` | Thermal conductivity | W/(m·K) |
| `T` | Temperature | K |
| `P` | Pressure | Pa |
| `Q` | Vapor quality | - (0 to 1) |

### Examples

#### Basic Steam Properties

```yaml
- fluid_properties:
    fluid: Water
    pressure_col: P_psia
    temperature_col: T_degF
    pressure_unit: psia
    temperature_unit: degF
    outputs:
      - property: H
        unit: BTU/lb
        output_column: enthalpy
      - property: S
        unit: BTU/(lb·R)
        output_column: entropy
      - property: D
        unit: lb/ft³
        output_column: density
```

#### Saturated Steam (using fixed quality)

```yaml
- fluid_properties:
    fluid: Water
    pressure_col: steam_pressure
    pressure_unit: psig
    quality: 1.0  # Saturated vapor
    outputs:
      - property: H
        unit: BTU/lb
        output_column: hg
      - property: T
        unit: degF
        output_column: saturation_temp
```

#### Refrigerant Properties

```yaml
- fluid_properties:
    fluid: R134a
    pressure_col: evap_pressure_kpa
    temperature_col: evap_temp_c
    pressure_unit: kPa
    temperature_unit: degC
    prefix: refrig
    outputs:
      - property: H
        unit: kJ/kg
      - property: S
        unit: kJ/(kg·K)
      - property: D
        unit: kg/m³
```

#### Multiple Columns with Prefix

```yaml
- fluid_properties:
    fluid: Water
    pressure_col: P
    temperature_col: T
    pressure_unit: bar
    temperature_unit: degC
    prefix: steam
    outputs:
      - property: H
        unit: kJ/kg
      - property: S
      - property: D
```

Output columns: `steam_H`, `steam_S`, `steam_D`

---

## `saturation_properties`

A convenience wrapper for saturated liquid (Q=0) or saturated vapor (Q=1) properties. Simpler than `fluid_properties` when you only need saturation data.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `fluid` | string | No | `"Water"` | Fluid name |
| `pressure_col` | string | Conditional | - | Column containing pressure |
| `pressure` | float | Conditional | - | Fixed pressure value |
| `temperature_col` | string | Conditional | - | Column containing saturation temp |
| `temperature` | float | Conditional | - | Fixed saturation temperature |
| `pressure_unit` | string | No | `"Pa"` | Pressure unit |
| `temperature_unit` | string | No | `"K"` | Temperature unit |
| `gauge_offset` | float | No | `14.696` | Gauge pressure offset |
| `phase` | string | No | `"vapor"` | `"liquid"` (Q=0) or `"vapor"` (Q=1) |
| `outputs` | list | No | `[{property: "H"}]` | Properties to calculate |
| `prefix` | string | No | `""` | Output column prefix |

### Examples

#### Saturated Vapor Properties (hg, Tsat)

```yaml
- saturation_properties:
    fluid: Water
    pressure_col: steam_drum_pressure
    pressure_unit: psig
    phase: vapor
    outputs:
      - property: H
        unit: BTU/lb
        output_column: hg
      - property: T
        unit: degF
        output_column: Tsat
```

#### Saturated Liquid Enthalpy (hf)

```yaml
- saturation_properties:
    pressure_col: P_psia
    pressure_unit: psia
    phase: liquid
    outputs:
      - property: H
        unit: BTU/lb
        output_column: hf
```

#### Calculating Latent Heat (hfg)

Combine two saturation transforms with a derive:

```yaml
transforms:
  - saturation_properties:
      pressure_col: P
      pressure_unit: psia
      phase: vapor
      outputs:
        - property: H
          unit: BTU/lb
          output_column: hg

  - saturation_properties:
      pressure_col: P
      pressure_unit: psia
      phase: liquid
      outputs:
        - property: H
          unit: BTU/lb
          output_column: hf

  - derive_columns:
      expressions:
        hfg: "hg - hf"
```

---

## `psychrometrics`

Calculate humid air (psychrometric) properties using CoolProp's HAPropsSI function.

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `dry_bulb_col` | string | **Yes** | - | Column containing dry bulb temperature |
| `relative_humidity_col` | string | Conditional | - | Column containing relative humidity |
| `humidity_ratio_col` | string | Conditional | - | Column containing humidity ratio |
| `wet_bulb_col` | string | Conditional | - | Column containing wet bulb temperature |
| `dew_point_col` | string | Conditional | - | Column containing dew point |
| `relative_humidity` | float | Conditional | - | Fixed relative humidity value |
| `humidity_ratio` | float | Conditional | - | Fixed humidity ratio value |
| `pressure_col` | string | Conditional | - | Column containing pressure |
| `pressure` | float | Conditional | - | Fixed pressure value |
| `elevation_ft` | float | Conditional | - | Elevation in feet (estimates pressure) |
| `elevation_m` | float | Conditional | - | Elevation in meters (estimates pressure) |
| `temperature_unit` | string | No | `"K"` | Temperature unit for all temps |
| `pressure_unit` | string | No | `"Pa"` | Pressure unit |
| `humidity_ratio_unit` | string | No | `"kg/kg"` | Humidity ratio unit |
| `rh_is_percent` | bool | No | `false` | If true, RH is 0-100 instead of 0-1 |
| `outputs` | list | No | `[{property: "W"}]` | Properties to calculate |
| `prefix` | string | No | `""` | Output column prefix |

!!! important "Input Requirements"
    Psychrometrics requires:

    1. **Dry bulb temperature** (always required)
    2. **One humidity parameter**: relative humidity, humidity ratio, wet bulb, OR dew point
    3. **Pressure source**: fixed pressure, pressure column, OR elevation

### Output Properties

| Property Key | Description |
|--------------|-------------|
| `W` | Humidity ratio (kg water / kg dry air) |
| `B` | Wet bulb temperature |
| `D` | Dew point temperature |
| `H` | Enthalpy (per kg dry air) |
| `V` | Specific volume (per kg dry air) |
| `R` | Relative humidity (0-1) |

### Examples

#### Humidity Ratio from Temperature and RH

```yaml
- psychrometrics:
    dry_bulb_col: ambient_temp_f
    relative_humidity_col: rh_percent
    temperature_unit: degF
    rh_is_percent: true
    elevation_ft: 875  # Estimate pressure from plant elevation
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

#### Using Fixed Pressure

```yaml
- psychrometrics:
    dry_bulb_col: Tdb
    relative_humidity_col: RH
    temperature_unit: degC
    rh_is_percent: false  # RH is 0-1
    pressure: 101325
    pressure_unit: Pa
    prefix: air
    outputs:
      - property: W
      - property: H
        unit: kJ/kg
```

#### Calculate RH from Humidity Ratio

```yaml
- psychrometrics:
    dry_bulb_col: temp_f
    humidity_ratio_col: W
    temperature_unit: degF
    humidity_ratio_unit: lb/lb
    pressure: 14.696
    pressure_unit: psia
    outputs:
      - property: R
        output_column: relative_humidity
```

---

## Supported Units

### Pressure Units

| Unit | Description | Example |
|------|-------------|---------|
| `Pa` | Pascals (SI) | 101325 Pa = 1 atm |
| `kPa` | Kilopascals | 101.325 kPa = 1 atm |
| `MPa` | Megapascals | 0.101325 MPa = 1 atm |
| `bar` | Bar | 1.01325 bar = 1 atm |
| `psia` | Pounds per square inch absolute | 14.696 psia = 1 atm |
| `psig` | Pounds per square inch gauge | 0 psig = 14.696 psia |
| `atm` | Atmospheres | 1 atm = 101325 Pa |

!!! note "Gauge Pressure (psig)"
    When using `psig`, the `gauge_offset` parameter specifies atmospheric pressure. Default is 14.696 psia (sea level).

    ```yaml
    pressure_unit: psig
    gauge_offset: 14.696  # Sea level (default)
    ```

    For high-elevation plants, adjust accordingly:
    ```yaml
    gauge_offset: 12.23  # ~5000 ft elevation
    ```

### Temperature Units

| Unit | Description | Conversion |
|------|-------------|------------|
| `K` | Kelvin (SI) | K = °C + 273.15 |
| `degC` | Celsius | °C = K - 273.15 |
| `degF` | Fahrenheit | °F = °C × 9/5 + 32 |
| `degR` | Rankine | °R = K × 9/5 |

### Enthalpy Units

| Unit | Description |
|------|-------------|
| `J/kg` | Joules per kilogram (SI) |
| `kJ/kg` | Kilojoules per kilogram |
| `BTU/lb` | BTU per pound |

### Entropy / Specific Heat Units

| Unit | Description |
|------|-------------|
| `J/(kg·K)` | Joules per kg-Kelvin (SI) |
| `kJ/(kg·K)` | Kilojoules per kg-Kelvin |
| `BTU/(lb·R)` | BTU per pound-Rankine |

### Density Units

| Unit | Description |
|------|-------------|
| `kg/m³` | Kilograms per cubic meter (SI) |
| `lb/ft³` | Pounds per cubic foot |

### Humidity Ratio Units

| Unit | Description |
|------|-------------|
| `kg/kg` | kg water / kg dry air (SI) |
| `lb/lb` | lb water / lb dry air |
| `g/kg` | grams water / kg dry air |
| `gr/lb` | grains water / lb dry air |

---

## Supported Fluids

CoolProp supports 122+ fluids. Common ones include:

### Water and Steam
- `Water` - Uses IAPWS-IF97 formulation (industry standard)

### Refrigerants
- `R134a` - HFC-134a (automotive AC)
- `R410A` - Common HVAC refrigerant
- `R22` - HCFC-22 (legacy AC)
- `Ammonia` / `NH3` - Industrial refrigeration
- `R744` / `CO2` - Carbon dioxide
- `R290` / `Propane` - Hydrocarbon refrigerant

### Industrial Gases
- `Air` - Dry air
- `Nitrogen` - N2
- `Oxygen` - O2
- `Hydrogen` - H2
- `CarbonDioxide` / `CO2`
- `Methane` - CH4
- `Argon` - Ar
- `Helium` - He

### Hydrocarbons
- `Methane`
- `Ethane`
- `Propane`
- `Butane`
- `Pentane`
- `Hexane`

For the complete list, see [CoolProp Fluid Properties](http://www.coolprop.org/fluid_properties/PurePseudoPure.html).

---

## Real-World Examples

### Boiler Efficiency Calculation

Calculate boiler efficiency by comparing steam output to fuel input:

```yaml
nodes:
  - name: boiler_with_enthalpies
    source: boiler_readings
    transforms:
      # Steam enthalpy (superheated or saturated)
      - fluid_properties:
          fluid: Water
          pressure_col: steam_pressure_psig
          temperature_col: steam_temp_f
          pressure_unit: psig
          temperature_unit: degF
          outputs:
            - property: H
              unit: BTU/lb
              output_column: h_steam

      # Feedwater enthalpy (subcooled liquid)
      - fluid_properties:
          fluid: Water
          pressure_col: feedwater_pressure_psig
          temperature_col: feedwater_temp_f
          pressure_unit: psig
          temperature_unit: degF
          outputs:
            - property: H
              unit: BTU/lb
              output_column: h_feedwater

      # Calculate efficiency
      - derive_columns:
          expressions:
            heat_added_btu_lb: "h_steam - h_feedwater"
            heat_output_btu_hr: "steam_flow_klb_hr * 1000 * heat_added_btu_lb"
            heat_input_btu_hr: "gas_flow_mcf_hr * 1000 * gas_hhv_btu_cf"
            boiler_efficiency: "heat_output_btu_hr / heat_input_btu_hr * 100"
```

### Dryer Moisture Load

Calculate moisture removed by a dryer from inlet/outlet air conditions:

```yaml
nodes:
  - name: dryer_moisture_analysis
    source: dryer_readings
    transforms:
      # Inlet air humidity ratio
      - psychrometrics:
          dry_bulb_col: inlet_temp_f
          relative_humidity_col: inlet_rh_pct
          temperature_unit: degF
          rh_is_percent: true
          elevation_ft: 875
          outputs:
            - property: W
              unit: lb/lb
              output_column: W_inlet

      # Outlet air humidity ratio
      - psychrometrics:
          dry_bulb_col: outlet_temp_f
          relative_humidity_col: outlet_rh_pct
          temperature_unit: degF
          rh_is_percent: true
          elevation_ft: 875
          outputs:
            - property: W
              unit: lb/lb
              output_column: W_outlet

      # Calculate moisture load
      - derive_columns:
          expressions:
            delta_W: "W_outlet - W_inlet"
            moisture_removed_lb_hr: "dry_air_flow_lb_hr * delta_W"
            moisture_removed_klb_hr: "moisture_removed_lb_hr / 1000"
```

### Evaporator Performance

Calculate evaporator duty using saturation properties:

```yaml
nodes:
  - name: evaporator_analysis
    source: evaporator_readings
    transforms:
      # Get saturated steam properties at evaporator pressure
      - saturation_properties:
          pressure_col: evap_pressure_psig
          pressure_unit: psig
          phase: vapor
          outputs:
            - property: H
              unit: BTU/lb
              output_column: hg
            - property: T
              unit: degF
              output_column: Tsat

      - saturation_properties:
          pressure_col: evap_pressure_psig
          pressure_unit: psig
          phase: liquid
          outputs:
            - property: H
              unit: BTU/lb
              output_column: hf

      # Calculate condensate and evaporation
      - derive_columns:
          expressions:
            hfg: "hg - hf"  # Latent heat
            steam_duty_btu_hr: "steam_flow_klb_hr * 1000 * (hg - condensate_h)"
            evaporation_lb_hr: "steam_duty_btu_hr / hfg"
```

### Refrigeration Cycle Analysis

Analyze a simple vapor compression cycle:

```yaml
nodes:
  - name: refrigeration_cycle
    source: chiller_readings
    transforms:
      # Evaporator outlet (superheated vapor)
      - fluid_properties:
          fluid: R134a
          pressure_col: evap_pressure_kpa
          temperature_col: evap_outlet_temp_c
          pressure_unit: kPa
          temperature_unit: degC
          prefix: evap
          outputs:
            - property: H
              unit: kJ/kg
            - property: S
              unit: kJ/(kg·K)

      # Condenser inlet (superheated vapor from compressor)
      - fluid_properties:
          fluid: R134a
          pressure_col: cond_pressure_kpa
          temperature_col: cond_inlet_temp_c
          pressure_unit: kPa
          temperature_unit: degC
          prefix: cond_in
          outputs:
            - property: H
              unit: kJ/kg

      # Condenser outlet (subcooled liquid)
      - fluid_properties:
          fluid: R134a
          pressure_col: cond_pressure_kpa
          temperature_col: cond_outlet_temp_c
          pressure_unit: kPa
          temperature_unit: degC
          prefix: cond_out
          outputs:
            - property: H
              unit: kJ/kg

      # Calculate cycle performance
      - derive_columns:
          expressions:
            compressor_work_kj_kg: "cond_in_H - evap_H"
            condenser_duty_kj_kg: "cond_in_H - cond_out_H"
            evaporator_duty_kj_kg: "evap_H - cond_out_H"  # After expansion
            COP: "evaporator_duty_kj_kg / compressor_work_kj_kg"
```

---

## Engine Parity

All thermodynamics transformers work identically across engines:

| Engine | Implementation | Performance |
|--------|----------------|-------------|
| **Pandas** | Vectorized apply with row-wise CoolProp calls | Good for <100k rows |
| **Spark** | Pandas UDF for distributed processing | Scales to billions of rows |
| **Polars** | Converts through Pandas | Good for medium datasets |

### Spark Usage

```python
from pyspark.sql import SparkSession
from odibi.context import SparkContext, EngineContext
from odibi.enums import EngineType
from odibi.transformers.thermodynamics import fluid_properties, FluidPropertiesParams

spark = SparkSession.builder.getOrCreate()
spark_ctx = SparkContext(spark)

# Your Spark DataFrame with pressure and temperature columns
df = spark.table("bronze.boiler_readings")

ctx = EngineContext(
    context=spark_ctx,
    df=df,
    engine_type=EngineType.SPARK,
)

params = FluidPropertiesParams(
    fluid="Water",
    pressure_col="steam_pressure_psig",
    temperature_col="steam_temp_f",
    pressure_unit="psig",
    temperature_unit="degF",
    outputs=[{"property": "H", "unit": "BTU/lb", "output_column": "enthalpy"}],
)

result = fluid_properties(ctx, params)
result_df = result.df  # Spark DataFrame with new columns
```

---

## Error Handling

### Null Values

Null or NaN inputs produce null outputs (graceful degradation):

```python
# Input with nulls
df = pd.DataFrame({
    "P": [100.0, None, 100.0],
    "T": [400.0, 400.0, None],
})

# Output
#    P      T        H
# 0  100.0  400.0    1225.5
# 1  NaN    400.0    NaN     <- Null pressure
# 2  100.0  NaN      NaN     <- Null temperature
```

### Invalid States

CoolProp returns errors for physically impossible states (e.g., liquid at superheated conditions). These are caught and converted to null outputs.

### Missing CoolProp

If CoolProp is not installed:

```
ImportError: CoolProp is required for thermodynamics transformers.
Install with: pip install CoolProp>=6.4.0
```

---

## Python API

For programmatic use outside YAML pipelines:

```python
import pandas as pd
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.thermodynamics import (
    fluid_properties,
    saturation_properties,
    psychrometrics,
    FluidPropertiesParams,
    SaturationPropertiesParams,
    PsychrometricsParams,
    PropertyOutputConfig,
    PsychrometricOutputConfig,
)

# Create context
df = pd.DataFrame({"P_psig": [100.0, 150.0], "T_F": [400.0, 450.0]})
ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)

# Define parameters
params = FluidPropertiesParams(
    fluid="Water",
    pressure_col="P_psig",
    temperature_col="T_F",
    pressure_unit="psig",
    temperature_unit="degF",
    outputs=[
        PropertyOutputConfig(property="H", unit="BTU/lb", output_column="enthalpy"),
        PropertyOutputConfig(property="S", unit="BTU/(lb·R)", output_column="entropy"),
    ],
)

# Execute
result = fluid_properties(ctx, params)
print(result.df)
```

---

## Troubleshooting

### "Fluid 'X' is not in the list of fluids"

Check the exact fluid name. CoolProp is case-sensitive for some fluids:
- ✅ `Water` (not `water`)
- ✅ `R134a` (not `r134a`)
- ✅ `Ammonia` or `NH3`

### "PropsSI failed" or all nulls

This usually means an invalid thermodynamic state:
- Pressure and temperature combination is in the two-phase region (use P+Q instead)
- Values are outside the valid range for the fluid
- Units are incorrect (check pressure_unit, temperature_unit)

### CoolProp installation fails

See [Installation Troubleshooting](installation.md#coolprop-installation-issues).

---

## References

- [CoolProp Documentation](http://www.coolprop.org/)
- [CoolProp Fluid List](http://www.coolprop.org/fluid_properties/PurePseudoPure.html)
- [CoolProp Humid Air](http://www.coolprop.org/fluid_properties/HumidAir.html)
- [IAPWS-IF97](http://www.iapws.org/relguide/IF97-Rev.html) - Water/steam formulation

---

## Next Steps

- [Installation Guide](installation.md) - All pip install options
- [Writing Transformations](writing_transformations.md) - General transformer guide
- [Recipes](recipes.md) - Common patterns and examples
