# Simulation Pattern Examples

Standalone YAML configs for all 38 simulation patterns documented in [Patterns & Recipes](../../docs/simulation/patterns/index.md).

## Folder Structure

### `oneshot/`
Single-run simulation configs. Each file generates data once using **overwrite** mode and **Parquet** format.

- Run any pattern: `odibi run oneshot/01_sales_pipeline.yaml`
- Output lands in `./data/bronze/` (or silver/gold for multi-node patterns)
- Great for learning, experimentation, and one-off data generation

### `datalake/`
Incremental simulation configs for continuous data feeds. Each file uses:

- **`incremental: {mode: stateful, column: timestamp}`** on the read node — each run picks up where the last one left off
- **Delta** format — supports ACID transactions, time travel, and efficient appends
- **Append** mode — new data is added to existing tables, building a growing dataset

Run daily (via cron or Airflow) to create a continuously updating data lake:

```bash
# First run generates initial data
odibi run datalake/01_sales_pipeline.yaml

# Subsequent runs append the next batch
odibi run datalake/01_sales_pipeline.yaml
```

## Pattern Index

| # | File | Industry | Key Feature |
|---|------|----------|-------------|
| 1 | `01_sales_pipeline` | General | Full medallion pipeline (bronze→silver→gold) |
| 2 | `02_production_line` | Manufacturing | Entity overrides, scheduled events, chaos |
| 3 | `03_iot_sensors` | Building Mgmt | Daily profile, derived columns, null_rate |
| 3b | `03b_hvac_feedback` | Building Mgmt | Derived column chaining, feedback modeling |
| 4 | `04_order_stream` | E-commerce | Named entities, derived expressions |
| 5 | `05_degradation` | Maintenance | Trend, recurrence, cleaning cycles |
| 6 | `06_stress_test` | Data Engineering | High-volume generation |
| 7 | `07_dashboard_feed` | Analytics | Dashboard-ready KPIs |
| 8 | `08_multi_system` | Data Engineering | Cross-entity references, prev() |
| 9 | `09_wastewater` | Environmental | Cross-entity cascade (stage→stage) |
| 10 | `10_compressor` | Oil & Gas | Shock rate / shock bias |
| 11 | `11_cstr_pid` | Chemical Engineering | PID control |
| 12 | `12_distillation` | Chemical Engineering | mean_reversion_to dynamic column |
| 13 | `13_cooling_tower` | Utilities | EMA smoothing |
| 14 | `14_batch_reactor` | Pharma / ChemE | Scheduled setpoint changes |
| 15 | `15_tank_farm` | Oil & Gas | prev() for level integration |
| 16 | `16_solar_farm` | Renewables | Boolean generator, weather coupling |
| 17 | `17_wind_turbines` | Renewables | Geo generator, entity overrides |
| 18 | `18_bess` | Energy Storage | State of charge with prev() |
| 19 | `19_smart_meters` | Utilities | IPv4 generator, high entity count |
| 20 | `20_ev_charging` | Transportation | UUID v5 deterministic IDs |
| 21 | `21_packaging_spc` | Food & Bev | Validation on simulated data |
| 22 | `22_cnc_shop` | Manufacturing | Downtime events in chaos |
| 23 | `23_warehouse_ops` | Logistics | Multi-pipeline project |
| 24 | `24_cold_chain` | Food & Bev | Email generator, alert thresholds |
| 25 | `25_assembly_line` | Automotive | Cross-entity station flow |
| 26 | `26_weather_stations` | Meteorology | Geo bbox, multi-sensor |
| 27 | `27_air_quality` | Environmental | Trend for seasonal drift |
| 28 | `28_greenhouse` | Agriculture | PID + dynamic setpoints |
| 29 | `29_icu_vitals` | Healthcare | High-frequency data, alarms |
| 30 | `30_pharma_batch` | Pharma | Sequential batch IDs, recipe phases |
| 31 | `31_retail_pos` | Retail | Weighted categoricals, derived totals |
| 32 | `32_call_center` | Customer Service | prev() for queue depth |
| 33 | `33_server_monitor` | IT Ops | IPv4 + email, memory leak trend |
| 34 | `34_api_perf_logs` | SaaS | Latency distributions, error rates |
| 35 | `35_supply_chain` | Logistics | Geo, UUID, multi-leg tracking |
| 36 | `36_late_data` | Data Engineering | Chaos + duplicates |
| 37 | `37_schema_evolution` | Data Engineering | Simulation → transform → validate |
| 38 | `38_multi_source_merge` | Data Engineering | Multiple sim nodes → one silver |

## Documentation

Full documentation for each pattern (with explanations, charts, and "Try this" exercises) is in [docs/simulation/patterns/](../../docs/simulation/patterns/index.md).
