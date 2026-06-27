# Scheduled Events

**Status:** ✅ IMPLEMENTED (v3.3+)

---

## Overview

Time-based events that modify simulation behavior at specific timestamps.
Critical for realistic process simulation with planned maintenance, grid curtailment, cleaning cycles, etc.

---

## Use Cases

### 1. Maintenance Windows
```yaml
scheduled_events:
  - type: forced_value
    entity: Turbine_01
    column: power_kw
    value: 0.0
    start_time: "2026-03-11T14:00:00Z"
    end_time: "2026-03-11T18:00:00Z"
```

### 2. Setpoint Changes
```yaml
scheduled_events:
  - type: setpoint_change
    entity: Reactor_01
    column: temp_setpoint_c
    value: 370.0
    start_time: "2026-03-11T08:00:00Z"
    # No end_time = permanent change
```

### 3. Grid Curtailment
```yaml
scheduled_events:
  - type: forced_value
    entity: BESS_Module_01
    column: available_capacity_pct
    value: 50.0  # Reduced to 50% during peak
    start_time: "2026-03-11T16:00:00Z"
    end_time: "2026-03-11T19:00:00Z"
```

### 4. Cleaning Cycles
```yaml
scheduled_events:
  - type: parameter_override
    entity: Solar_Array_01
    column: efficiency_pct
    value: 95.0  # Restored after cleaning
    start_time: "2026-03-11T06:00:00Z"
    end_time: "2026-03-12T06:00:00Z"  # Degrades again after 24hr
```

---

## Event Types

### 1. `forced_value`
Overrides column value during event window.

**Use:** Maintenance (power=0), constraints (max flow), outages

### 2. `setpoint_change`
Changes a setpoint column value (permanent or temporary).

**Use:** Process optimization, load following, seasonal changes

### 3. `parameter_override`
Temporarily modifies a generator parameter.

**Use:** Efficiency changes, degradation, seasonal effects

---

## Config Schema

```yaml
simulation:
  # ... existing config ...
  
scheduled_events:
  - type: forced_value | setpoint_change | parameter_override
    entity: entity_name  # or null for all entities
    column: column_name
    value: new_value
    start_time: "ISO8601 timestamp"
    end_time: "ISO8601 timestamp"  # Optional (null = permanent)
    priority: 1  # Optional (higher = applied last if overlapping)
```

---

## Implementation Plan

### 1. Config Model
Add to `config.py`:
```python
class ScheduledEventType(str, Enum):
    FORCED_VALUE = "forced_value"
    SETPOINT_CHANGE = "setpoint_change"  
    PARAMETER_OVERRIDE = "parameter_override"

class ScheduledEvent(BaseModel):
    type: ScheduledEventType
    entity: Optional[str] = None  # None = all entities
    column: str
    value: Any
    start_time: str
    end_time: Optional[str] = None  # None = permanent
    priority: int = 0
```

### 2. Event Application
In `_generate_value()`, check if current timestamp has active event:
```python
def _generate_value(..., timestamp):
    # Check for active events
    active_event = self._get_active_event(entity_name, col_name, timestamp)
    if active_event:
        if active_event.type == "forced_value":
            return active_event.value
        # ... other event types
    
    # Normal generation
    return self._generate_<type>(...)
```

### 3. Event Index
Build event index at init for fast lookup:
```python
self.events_by_column = defaultdict(list)
for event in config.scheduled_events:
    self.events_by_column[(event.entity, event.column)].append(event)
```

---

## Validation Rules

1. **Column exists:** Event column must be defined
2. **Entity exists:** If entity specified, must exist
3. **Valid timestamps:** start_time <= end_time
4. **Value type:** Event value must match column data_type

---

## Minimal Implementation (Phase 3)

Focus on `forced_value` type only - simplest and most useful:
- Override column value during time window
- Applied after normal generation
- Handles null entity (applies to all)

Later phases can add:
- `setpoint_change` (modifies random_walk start during event)
- `parameter_override` (modifies generator config parameters)

---

## Example: Wind Farm Curtailment

```yaml
entities:
  - name: Wind_Turbine_01
  - name: Wind_Turbine_02

columns:
  - name: power_kw
    generator:
      type: random_walk
      start: 2500.0
      min: 0.0
      max: 3000.0

scheduled_events:
  # Grid curtailment event
  - type: forced_value
    entity: null  # All turbines
    column: power_kw
    value: 0.0
    start_time: "2026-03-11T14:00:00Z"
    end_time: "2026-03-11T15:00:00Z"
```

Result: All turbines drop to 0 kW from 14:00-15:00, then resume normal operation.

---

Scheduled events are fully implemented. The `forced_value` event type is supported.
