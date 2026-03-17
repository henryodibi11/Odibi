# L15: CSTR Digital Twin (Capstone Project)

**Prerequisites:** L00-L14 | **Effort:** 2-3 hours | **Seborg:** Chapters 1-20

---

## Learning Objectives

1. ✅ Build full Bronze/Silver/Gold data pipeline
2. ✅ Implement CSTR reactor simulation with temperature control
3. ✅ Add validation, quarantine, SCD2 tracking
4. ✅ Calculate KPIs and control performance metrics
5. ✅ Create production-ready digital twin

---

## Project Overview

**CSTR (Continuous Stirred Tank Reactor) Digital Twin**

**Bronze Layer:** Raw sensor simulation
- Reactor temperature, concentration, flow rates
- Controller outputs
- Disturbances (feed temp, flow rate)
- Realistic sensor noise

**Silver Layer:** Validated, cleaned data
- Data quality tests (range, rate-of-change)
- Quarantine bad readings
- SCD2 tracking for controller tuning changes

**Gold Layer:** Business metrics
- Reaction conversion %
- Energy efficiency (kJ/kg product)
- Controller performance (IAE, settling time)
- Alarm events

---

## CSTR Dynamics

**Mass Balance:**
```
dC/dt = F/V·(C_in - C) - k·C
```

**Energy Balance:**
```
dT/dt = F/V·(T_in - T) + (-ΔH)·k·C / (ρ·Cp) + Q_jacket / (ρ·Cp·V)
```

**Reaction Rate:**
```
k = k0·exp(-E/(R·T))  (Arrhenius)
```

**Controller:**
- PID controls jacket heat duty (Q_jacket) to maintain reactor temp
- PI ratio control maintains feed ratio

---

## Odibi Implementation

### **Bronze: Raw Sensor Data**

```yaml
# cstr_digital_twin.yaml
nodes:
  - name: bronze_cstr_sensors
    read:
      format: simulation
      options:
        simulation:
          scope:
            start_time: "2024-01-01T00:00:00Z"
            timestep: "30sec"
            row_count: 2880  # 24 hours
            seed: 42

          entities:
            count: 1
            id_prefix: "CSTR-"

          columns:
            # (Full CSTR model with PID control)
            # See working example for complete implementation

          chaos:
            outlier_rate: 0.01
            duplicate_rate: 0.005

    write:
      connection: local
      format: parquet
      path: data/output/bronze_cstr_sensors.parquet
      mode: overwrite
```

### **Silver: Validated Data**

```yaml
  - name: silver_cstr_validated
    read:
      connection: local
      format: parquet
      path: data/output/bronze_cstr_sensors.parquet

    validate:
      - name: temp_range_check
        test: range
        column: reactor_temp_degc
        min: 50
        max: 100
        on_fail: fail
        quarantine: true

      - name: flow_rate_check
        test: rate_of_change
        column: feed_flow_gpm
        max_change: 10
        on_fail: warn

    transform:
      - name: scd2_controller_tuning
        transformer: scd2
        natural_key: entity_id
        tracked_columns:
          - Kp
          - Ki
          - Kd

    write:
      connection: local
      format: parquet
      path: data/output/silver_cstr_validated.parquet
      mode: overwrite
```

### **Gold: KPIs**

```yaml
  - name: gold_cstr_kpis
    read:
      connection: local
      format: parquet
      path: data/output/silver_cstr_validated.parquet

    transform:
      - sql: >
          SELECT
            DATE_TRUNC('hour', timestamp) as hour,
            AVG(conversion_pct) as avg_conversion,
            AVG(abs(temp_sp_degc - reactor_temp_degc)) as avg_temp_error,
            SUM(energy_kwh) as total_energy_kwh,
            SUM(CASE WHEN fault_detected THEN 1 ELSE 0 END) as fault_count
          FROM __this__
          GROUP BY hour
          ORDER BY hour

    write:
      connection: local
      format: parquet
      path: data/output/gold_cstr_kpis.parquet
      mode: overwrite
```

**Working example:** [/examples/cheme_course/L15_cstr_digital_twin/cstr_digital_twin.yaml](file:///d:/odibi/examples/cheme_course/L15_cstr_digital_twin/cstr_digital_twin.yaml)

---

## Project Extensions

1. **Add cascade control:** Inner loop on jacket temp
2. **Feedforward:** Compensate for feed temp changes
3. **Alarm management:** High temp, low conversion
4. **Batch tracking:** SCD2 on batch ID
5. **OpenLineage:** Track data lineage

---

## Summary

**Key Achievements:**
- ✅ Built full Bronze/Silver/Gold pipeline
- ✅ Simulated realistic CSTR with control
- ✅ Applied validation, quarantine, SCD2
- ✅ Calculated business KPIs
- ✅ Production-ready digital twin

**You've completed the ChemE Data Engineering course!**

You now can:
- Simulate process control systems
- Build data pipelines for process data
- Apply validation and quality gates
- Generate synthetic data for ML
- Understand control engineering from a data perspective

---

## Additional Resources

- **Full course:** [ChemE Data Engineering](file:///d:/odibi/docs/learning/cheme_data_course/)
- **Odibi docs:** [ODIBI_DEEP_CONTEXT.md](file:///d:/odibi/docs/ODIBI_DEEP_CONTEXT.md)
- **Examples:** [/examples/cheme_course/](file:///d:/odibi/examples/cheme_course/)
