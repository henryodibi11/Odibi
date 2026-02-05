# Unit Conversion Guide

Odibi includes **Pint** as a core dependency, giving you access to 1000+ unit definitions for seamless unit conversion in your data pipelines. No extra installation required.

---

## Overview

The `unit_convert` transformer converts columns from one unit to another declaratively in YAML. It handles:

- **All SI units** (meters, kilograms, seconds, etc.)
- **Imperial units** (feet, pounds, BTU, etc.)
- **Engineering units** (psig, gpm, cfm, mmbtu, etc.)
- **Compound units** (BTU/(hr·ft²·°F), kJ/(kg·K), etc.)
- **Temperature offsets** (°F ↔ °C ↔ K correctly)
- **Gauge pressure** (psig → psia with configurable atmospheric offset)

---

## Quick Start

```yaml
nodes:
  - name: normalized_data
    source: raw_sensor_data
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
            flow_gpm:
              from: gpm
              to: L/min
              output: flow_lpm
```

**Input:**
| pressure_psig | temperature_f | flow_gpm |
|---------------|---------------|----------|
| 100 | 400 | 50 |

**Output:**
| pressure_psig | pressure_bar | temperature_f | temperature_c | flow_gpm | flow_lpm |
|---------------|--------------|---------------|---------------|----------|----------|
| 100 | 7.91 | 400 | 204.4 | 50 | 189.3 |

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `conversions` | dict | **Yes** | - | Map of column names to conversion specs |
| `gauge_pressure_offset` | string | No | `"14.696 psia"` | Atmospheric pressure for psig conversions |
| `errors` | string | No | `"null"` | Error handling: `"null"`, `"raise"`, or `"ignore"` |

### Conversion Spec

Each entry in `conversions` is a mapping with:

| Field | Required | Description |
|-------|----------|-------------|
| `from` | **Yes** | Source unit (e.g., `"psig"`, `"degF"`, `"BTU/lb"`) |
| `to` | **Yes** | Target unit (e.g., `"bar"`, `"degC"`, `"kJ/kg"`) |
| `output` | No | Output column name. If omitted, overwrites the source column. |

---

## Common Unit Conversions

### Pressure

```yaml
- unit_convert:
    conversions:
      # Gauge to absolute
      P_psig:
        from: psig
        to: psia
        output: P_psia

      # Gauge to metric
      P_psig:
        from: psig
        to: bar
        output: P_bar

      # Absolute conversions
      P_psia:
        from: psia
        to: kPa
        output: P_kpa

      P_bar:
        from: bar
        to: MPa
        output: P_mpa
```

### Temperature

```yaml
- unit_convert:
    conversions:
      # Fahrenheit to Celsius
      T_f:
        from: degF
        to: degC
        output: T_c

      # Celsius to Kelvin
      T_c:
        from: degC
        to: K
        output: T_k

      # Fahrenheit to Kelvin
      T_f:
        from: degF
        to: K
        output: T_k
```

### Flow Rate

```yaml
- unit_convert:
    conversions:
      # Gallons per minute to liters per minute
      flow_gpm:
        from: gpm
        to: L/min
        output: flow_lpm

      # GPM to cubic meters per second
      flow_gpm:
        from: gpm
        to: m³/s
        output: flow_m3s

      # CFM to cubic meters per hour
      air_cfm:
        from: cfm
        to: m³/hour
        output: air_m3h

      # Million gallons per day
      water_mgd:
        from: mgd
        to: m³/day
        output: water_m3d
```

### Energy

```yaml
- unit_convert:
    conversions:
      # BTU to kilojoules
      energy_btu:
        from: BTU
        to: kJ
        output: energy_kj

      # Million BTU to gigajoules
      energy_mmbtu:
        from: mmbtu
        to: GJ
        output: energy_gj

      # Therms to kWh
      gas_therms:
        from: therm
        to: kWh
        output: gas_kwh
```

### Mass Flow

```yaml
- unit_convert:
    conversions:
      # Thousand pounds per hour to kg/s
      steam_klb_hr:
        from: klb/hour
        to: kg/s
        output: steam_kgs

      # Pounds per hour to kg/hr
      flow_lb_hr:
        from: lb/hour
        to: kg/hour
        output: flow_kgh
```

### Specific Properties (Enthalpy, Specific Heat)

```yaml
- unit_convert:
    conversions:
      # Enthalpy
      h_btu_lb:
        from: BTU/lb
        to: kJ/kg
        output: h_kj_kg

      # Specific heat
      cp_btu_lb_f:
        from: BTU/(lb * degF)
        to: kJ/(kg * K)
        output: cp_si
```

### Heat Transfer Coefficients

```yaml
- unit_convert:
    conversions:
      # Overall heat transfer coefficient
      U_imperial:
        from: BTU/(hour * ft² * degF)
        to: W/(m² * K)
        output: U_si

      # Thermal conductivity
      k_imperial:
        from: BTU/(hour * ft * degF)
        to: W/(m * K)
        output: k_si
```

---

## Gauge Pressure Handling

Gauge pressure (psig) is relative to atmospheric pressure. The transformer automatically handles the offset:

```yaml
- unit_convert:
    gauge_pressure_offset: "14.696 psia"  # Sea level (default)
    conversions:
      steam_pressure:
        from: psig
        to: psia
        output: steam_pressure_abs
```

### High Elevation Plants

At higher elevations, atmospheric pressure is lower:

```yaml
- unit_convert:
    gauge_pressure_offset: "12.23 psia"  # ~5000 ft elevation
    conversions:
      pressure_psig:
        from: psig
        to: bar
        output: pressure_bar
```

| Elevation | Approx. Atmospheric Pressure |
|-----------|------------------------------|
| Sea level | 14.696 psia |
| 1000 ft | 14.18 psia |
| 2000 ft | 13.66 psia |
| 5000 ft | 12.23 psia |
| 10000 ft | 10.11 psia |

---

## In-Place Conversion

Omit `output` to overwrite the source column:

```yaml
- unit_convert:
    conversions:
      temperature:
        from: degF
        to: degC
        # No output = overwrites "temperature" column
      pressure:
        from: psia
        to: bar
        # No output = overwrites "pressure" column
```

---

## Supported Units Reference

### Pressure

| Unit | Description |
|------|-------------|
| `Pa` | Pascal (SI) |
| `kPa` | Kilopascal |
| `MPa` | Megapascal |
| `bar` | Bar |
| `mbar` | Millibar |
| `psi`, `psia` | Pounds per square inch (absolute) |
| `psig` | Pounds per square inch (gauge) |
| `atm` | Atmosphere |
| `torr` | Torr |
| `mmHg` | Millimeters of mercury |
| `inHg` | Inches of mercury |
| `inH2O` | Inches of water |

### Temperature

| Unit | Description |
|------|-------------|
| `K` | Kelvin (SI) |
| `degC` | Degrees Celsius |
| `degF` | Degrees Fahrenheit |
| `degR` | Degrees Rankine |

### Energy

| Unit | Description |
|------|-------------|
| `J` | Joule (SI) |
| `kJ` | Kilojoule |
| `MJ` | Megajoule |
| `GJ` | Gigajoule |
| `BTU` | British Thermal Unit |
| `mbtu` | Thousand BTU |
| `mmbtu` | Million BTU |
| `therm` | Therm (100,000 BTU) |
| `cal` | Calorie |
| `kcal` | Kilocalorie |
| `Wh` | Watt-hour |
| `kWh` | Kilowatt-hour |
| `MWh` | Megawatt-hour |

### Power

| Unit | Description |
|------|-------------|
| `W` | Watt (SI) |
| `kW` | Kilowatt |
| `MW` | Megawatt |
| `hp` | Horsepower |
| `BTU/hr` | BTU per hour |
| `BTU/s` | BTU per second |
| `ton_of_refrigeration` | Ton of refrigeration |

### Mass

| Unit | Description |
|------|-------------|
| `kg` | Kilogram (SI) |
| `g` | Gram |
| `mg` | Milligram |
| `lb`, `pound` | Pound |
| `klb` | Thousand pounds |
| `oz` | Ounce |
| `ton` | Short ton (2000 lb) |
| `tonne` | Metric ton (1000 kg) |

### Volume

| Unit | Description |
|------|-------------|
| `m³`, `m**3` | Cubic meter (SI) |
| `L` | Liter |
| `mL` | Milliliter |
| `gallon` | US gallon |
| `kgal` | Thousand gallons |
| `ft³`, `ft**3` | Cubic foot |
| `mcf` | Thousand cubic feet |
| `mmcf` | Million cubic feet |
| `barrel` | Barrel (42 gallons) |

### Flow Rate

| Unit | Description |
|------|-------------|
| `m³/s` | Cubic meters per second (SI) |
| `m³/hr`, `m³/hour` | Cubic meters per hour |
| `L/s` | Liters per second |
| `L/min` | Liters per minute |
| `gpm` | Gallons per minute |
| `cfm` | Cubic feet per minute |
| `scfm` | Standard cubic feet per minute |
| `mgd` | Million gallons per day |
| `kg/s` | Kilograms per second |
| `kg/hr` | Kilograms per hour |
| `lb/hr` | Pounds per hour |
| `klb/hr` | Thousand pounds per hour |

### Density

| Unit | Description |
|------|-------------|
| `kg/m³` | Kilograms per cubic meter (SI) |
| `g/cm³` | Grams per cubic centimeter |
| `lb/ft³` | Pounds per cubic foot |
| `lb/gallon` | Pounds per gallon |

### Specific Properties

| Unit | Description |
|------|-------------|
| `J/kg` | Joules per kilogram (enthalpy) |
| `kJ/kg` | Kilojoules per kilogram |
| `BTU/lb` | BTU per pound |
| `J/(kg*K)` | Specific heat (SI) |
| `kJ/(kg*K)` | Specific heat |
| `BTU/(lb*degF)` | Specific heat (imperial) |

### Heat Transfer

| Unit | Description |
|------|-------------|
| `W/(m²*K)` | Heat transfer coefficient (SI) |
| `BTU/(hour*ft²*degF)` | Heat transfer coefficient (imperial) |
| `W/(m*K)` | Thermal conductivity (SI) |
| `BTU/(hour*ft*degF)` | Thermal conductivity (imperial) |

---

## Python API

For programmatic use:

```python
from odibi.transformers.units import convert, list_units

# One-off conversions
temp_c = convert(212, "degF", "degC")  # 100.0
pressure_bar = convert(100, "psig", "bar")  # 7.91

# List available units by category
print(list_units("pressure"))
# ['Pa', 'kPa', 'MPa', 'bar', 'psi', 'psia', 'psig', ...]

print(list_units("temperature"))
# ['K', 'degC', 'degF', 'degR']

# List all categories
print(list_units())
# {'pressure': [...], 'temperature': [...], 'energy': [...], ...}
```

### Using the Transformer Directly

```python
import pandas as pd
from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.transformers.units import unit_convert, UnitConvertParams, ConversionSpec

df = pd.DataFrame({
    "pressure_psig": [100, 150, 200],
    "temp_f": [400, 450, 500],
})

ctx = EngineContext(PandasContext(), df, EngineType.PANDAS)

params = UnitConvertParams(
    conversions={
        "pressure_psig": ConversionSpec(from_unit="psig", to="bar", output="pressure_bar"),
        "temp_f": ConversionSpec(from_unit="degF", to="degC", output="temp_c"),
    }
)

result = unit_convert(ctx, params)
print(result.df)
```

---

## Error Handling

### Null Values

Null/NaN values pass through as null:

```python
df = pd.DataFrame({"temp_f": [212, None, 32]})
# Result: temp_c = [100, NaN, 0]
```

### Invalid Units

By default, invalid units result in null values. Use `errors: raise` to get exceptions:

```yaml
- unit_convert:
    errors: raise  # Throw exception on invalid units
    conversions:
      temp:
        from: invalid_unit
        to: degC
```

Error modes:
- `null` (default): Invalid conversions produce null
- `raise`: Throw an exception
- `ignore`: Leave the column unchanged

---

## Engine Parity

Works identically on Pandas, Spark, and Polars:

| Engine | Implementation |
|--------|----------------|
| Pandas | Vectorized NumPy operations |
| Spark | Pandas UDF for distributed processing |
| Polars | NumPy-based conversion |

---

## Real-World Examples

### Normalize Boiler Data

```yaml
nodes:
  - name: boiler_normalized
    source: raw_boiler_data
    transforms:
      - unit_convert:
          gauge_pressure_offset: "14.696 psia"
          conversions:
            steam_pressure_psig:
              from: psig
              to: MPa
              output: steam_pressure_mpa
            feedwater_temp_f:
              from: degF
              to: K
              output: feedwater_temp_k
            steam_flow_klb_hr:
              from: klb/hour
              to: kg/s
              output: steam_flow_kgs
            gas_flow_mcf_hr:
              from: mcf/hour
              to: m³/hour
              output: gas_flow_m3h
```

### Prepare Data for Thermodynamics Calculations

Convert to SI units before calling `fluid_properties`:

```yaml
transforms:
  # Step 1: Convert to SI
  - unit_convert:
      conversions:
        P_psig:
          from: psig
          to: Pa
          output: P_pa
        T_f:
          from: degF
          to: K
          output: T_k

  # Step 2: Calculate properties (SI in, SI out)
  - fluid_properties:
      fluid: Water
      pressure_col: P_pa
      temperature_col: T_k
      pressure_unit: Pa
      temperature_unit: K
      outputs:
        - property: H
          output_column: h_j_kg

  # Step 3: Convert back to imperial for reports
  - unit_convert:
      conversions:
        h_j_kg:
          from: J/kg
          to: BTU/lb
          output: h_btu_lb
```

### Multi-Unit Sensor Data

When sensors report in different units:

```yaml
- unit_convert:
    conversions:
      # Pressure sensors in different units
      PT001_bar:
        from: bar
        to: psia
        output: PT001_psia
      PT002_kpa:
        from: kPa
        to: psia
        output: PT002_psia
      PT003_psig:
        from: psig
        to: psia
        output: PT003_psia

      # Now all pressures are in psia for calculations
```

---

## Next Steps

- [Thermodynamics Guide](thermodynamics.md) - Steam tables, refrigerants, psychrometrics
- [Installation Guide](installation.md) - All pip install options
- [Writing Transformations](writing_transformations.md) - General transformer guide
