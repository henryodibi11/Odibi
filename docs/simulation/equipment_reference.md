# Manufacturing Equipment Reference

**Look up your equipment → get the pattern, equation, and starting parameters.**

This table maps common manufacturing equipment to Odibi simulation patterns. Instead of deriving equations from textbooks, find your equipment, copy the expression, and tune the 1–2 numbers that matter.

!!! tip "Not listed?"
    Ask one question: *what does the output do when I change the input?*

    - **Settles to a new value** → `first_order`
    - **Keeps ramping** → `integrator`
    - **Bounces before settling** → `second_order`
    - **Nothing, then responds** → `dead_time`
    - **Changes instantly** → `gain`

    Find the closest equipment below and copy its equation. The pattern is what matters, not the exact equipment name.

---

## Quick Reference: Key Terms

Before reading the table, here's what each column means:

- **gain** = max output ÷ 100 (when input is 0–100%). It converts input units to output units. Example: a valve with max flow 15 m³/hr → gain = 15 ÷ 100 = **0.15**
- **alpha** = how fast it responds. Each timestep, alpha × the remaining gap gets closed. Only applies to `first_order`. Low (0.02) = sluggish. High (0.3) = snappy
- **dt** = timestep unit conversion so rates accumulate correctly. Example: flow in m³/hr with a 5-min timestep → dt = 5/60

See [Parameter Intuition Guide](simulation_playbook.md#3-parameter-intuition-guide) for the full breakdown of alpha, gain, and dt.

---

## Valves

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Globe valve** (throttling) | `first_order` | `prev(flow) + alpha × (gain × valve_pct - prev(flow))` | Max flow ÷ 100. Example: 15 m³/hr max → 0.15 | 0.05–0.15 | Slow — designed for precise flow control |
| **Ball valve** (on/off) | `first_order` | `prev(flow) + alpha × (gain × valve_pct - prev(flow))` | Max flow ÷ 100 | 0.2–0.5 | Fast — quarter-turn, snaps open/closed |
| **Butterfly valve** | `first_order` | `prev(flow) + alpha × (gain × valve_pct - prev(flow))` | Max flow ÷ 100 | 0.1–0.3 | Moderate speed. Nonlinear at low openings |
| **Relief / safety valve** | `gain` | `max_flow if pressure > setpoint else 0` | Binary — fully open or closed | — | Threshold behavior, not proportional |

---

## Pumps

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Centrifugal pump** | `first_order` | `prev(flow) + alpha × (gain × speed_pct - prev(flow))` | Max flow ÷ 100. Example: 20 m³/hr → 0.20 | 0.2–0.5 | Fast response. Flow varies with speed² at extremes, but linear enough for simulation |
| **Positive displacement pump** | `gain` | `gain × speed_pct` | Max flow ÷ 100 | — | Flow is directly proportional to speed. Nearly instant response |
| **Peristaltic / diaphragm pump** | `gain` | `gain × speed_pct` | Max flow ÷ 100 | — | Like PD pumps — flow tracks speed with very little lag |

---

## Vessels & Tanks

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Storage tank** (liquid level) | `integrator` | `max(0, min(cap, prev(level) + (inflow - outflow) × dt))` | — | — | Always clamp to [0, capacity]. Get dt units right |
| **Tank with gravity drain** | `integrator` (self-regulating) | Level: `max(0, min(cap, prev(level) + (inflow - drain_coeff × prev(level)) × dt))` | — | — | Self-regulates — outflow increases with level. No controller needed |
| **Pressure vessel** (gas) | `second_order` | Velocity: `prev(vel) + beta × (target - prev(pressure)) - damping × prev(vel)` then Pressure: `prev(pressure) + velocity` | — | — | Gas compresses/expands with overshoot. Use beta=0.04, damping=0.3 as starting point |
| **Mixing tank** (concentration) | `first_order` | `prev(conc) + alpha × (inlet_conc - prev(conc))` | — | 0.05–0.2 | Alpha depends on tank volume vs flow rate. Bigger tank = lower alpha |
| **Reactor (CSTR)** | `first_order` | `prev(conc) + alpha × (feed_conc - prev(conc))` | — | 0.02–0.1 | Similar to mixing tank but slower. Reaction kinetics add complexity — start simple |

---

## Heat Transfer

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Shell & tube heat exchanger** | `first_order` | `prev(t_out) + alpha × (gain × t_in - prev(t_out))` | 0.6–0.9 (efficiency) | 0.02–0.08 | Large thermal mass = slow. Gain < 1.0 because heat transfer isn't perfect |
| **Plate heat exchanger** | `first_order` | `prev(t_out) + alpha × (gain × t_in - prev(t_out))` | 0.7–0.95 | 0.1–0.3 | Much faster than shell & tube — less thermal mass, more surface area |
| **Jacketed vessel** | `first_order` | `prev(t_vessel) + alpha × (gain × t_jacket - prev(t_vessel))` | 0.3–0.7 | 0.01–0.05 | Very slow — heating/cooling a large batch through a wall |
| **Cooling tower** | `first_order` | `prev(t_out) + alpha × (t_wet_bulb - prev(t_out))` | — | 0.02–0.1 | Target is wet-bulb temp. Gain ≈ 1.0 in ideal conditions |
| **Electric heater** | `first_order` | `prev(temp) + alpha × (gain × heater_pct - prev(temp))` | Max temp rise ÷ 100 | 0.05–0.2 | Gain = max temp above ambient per % power |
| **Boiler / steam generator** | `first_order` | `prev(steam_pressure) + alpha × (gain × fuel_rate - prev(steam_pressure))` | System-specific | 0.01–0.05 | Very slow, large thermal mass. Steam drum adds lag |

---

## Conveyors & Transport

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Belt conveyor** | `dead_time` | `delay(input, steps, default)` | — | — | steps = belt length ÷ belt speed ÷ timestep. Pure transport delay |
| **Screw conveyor** | `dead_time` | `delay(input, steps, default)` | — | — | Shorter delay than belt. steps = screw length ÷ feed rate ÷ timestep |
| **Pipeline** (liquid) | `dead_time` + `first_order` | `prev(output) + alpha × (delay(input, steps, default) × gain - prev(output))` | Flow gain | 0.1–0.3 | Dead time = pipe volume ÷ flow rate. First-order for pressure dynamics |
| **Pneumatic transport** | `dead_time` | `delay(input, steps, default)` | — | — | Air velocity is fast, so fewer delay steps than liquid pipeline |

---

## Motors & Drives

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **VFD motor** (variable frequency drive) | `first_order` | `prev(speed) + alpha × (command_pct - prev(speed))` | 1.0 (command = speed in %) | 0.2–0.5 | Fast response. VFD ramp rate determines alpha |
| **Direct-on-line motor** | `gain` | `rated_speed if running else 0` | — | — | On/off — no speed control. Full speed or stopped |
| **Compressor** | `first_order` | `prev(pressure) + alpha × (gain × speed_pct - prev(pressure))` | Max pressure ÷ 100 | 0.05–0.15 | Slow — compressed gas has thermal mass |

---

## Instruments & Sensors

| Equipment | Pattern | Equation | Gain | Alpha | Notes |
|---|---|---|---|---|---|
| **Thermocouple** | `first_order` | `prev(reading) + alpha × (true_temp - prev(reading))` | 1.0 (reads what it sees) | 0.1–0.3 | Sensor lag — doesn't read instantly. Add noise: `+ (random() - 0.5) * band` |
| **RTD** (resistance temp detector) | `first_order` | `prev(reading) + alpha × (true_temp - prev(reading))` | 1.0 | 0.05–0.15 | Slower than thermocouple — larger thermal mass in the sensor |
| **Pressure transmitter** | `first_order` | `prev(reading) + alpha × (true_pressure - prev(reading))` | 1.0 | 0.3–0.5 | Very fast — nearly instant. Often modeled as `gain` |
| **Flow meter** (magnetic / Coriolis) | `gain` | `true_flow + (random() - 0.5) * noise_band` | 1.0 | — | Essentially instant. Main imperfection is noise, not lag |
| **Level sensor** (ultrasonic) | `first_order` | `prev(reading) + alpha × (true_level - prev(reading))` | 1.0 | 0.2–0.4 | Moderate lag. Surface turbulence adds noise |
| **pH probe** | `first_order` | `prev(reading) + alpha × (true_ph - prev(reading))` | 1.0 | 0.02–0.1 | Slow — electrochemical response time. Gets slower as probe ages |

---

## How to Use This Table

**Step 1:** Find your equipment (or the closest match).

**Step 2:** Copy the equation into your YAML `expression:` field. Replace variable names with your column names.

**Step 3:** Pick a gain and alpha from the ranges shown. Use the middle of the range to start.

**Step 4:** Run the simulation and look at the chart. Does it respond too fast? Lower alpha. Too slow? Raise alpha. Wrong magnitude? Adjust gain.

**Example — Globe valve controlling cooling water flow:**

```yaml
columns:
  - name: valve_position_pct
    data_type: float
    generator:
      type: random_walk
      start: 50.0
      min: 0.0
      max: 100.0
      step_size: 3.0

  - name: cooling_flow_m3_hr
    data_type: float
    generator:
      type: derived
      expression: "max(0, prev('cooling_flow_m3_hr', 7.5) + 0.1 * (0.15 * valve_position_pct - prev('cooling_flow_m3_hr', 7.5)))"
      # gain = 0.15 → 100% valve = 15 m³/hr max flow
      # alpha = 0.1 → globe valve, moderate speed
      # default = 7.5 → starts at 50% valve = 7.5 m³/hr
```

---

## Related Documentation

- **[Simulation Playbook](simulation_playbook.md)** — Complete execution system for building simulations
- **[Core Patterns](simulation_playbook.md#2-core-pattern-library)** — Detailed explanation of each pattern with row-by-row traces
- **[Parameter Intuition Guide](simulation_playbook.md#3-parameter-intuition-guide)** — How to tune alpha, gain, dt, and PID parameters
- **[Stateful Functions](stateful_functions.md)** — `prev()`, `ema()`, `pid()`, `delay()` complete reference
