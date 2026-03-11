# Cross-Entity References

**Status:** 🚧 IN DEVELOPMENT  
**Sprint:** 2 Phase 2  
**Estimated Effort:** 2-3 days  
**Complexity:** HIGH

---

## Overview

Enable entities to reference other entities' column values at the same timestamp.
Critical for flowsheet simulation where process units are connected (streams between tanks, heat exchangers, reactors).

---

## Use Cases

### 1. Tank-to-Tank Flow
```yaml
entities:
  - name: Tank_A
  - name: Tank_B

columns:
  - name: level_pct
    generator:
      type: random_walk
      start: 50.0
      min: 0.0
      max: 100.0
      
  - name: flow_in_from_tank_a  # Only in Tank_B
    generator:
      type: derived
      expression: "Tank_A.level_pct * 0.05"  # Flow proportional to Tank_A level
```

### 2. Heat Exchanger Network
```yaml
entities:
  - name: HX_01_Hot_Side
  - name: HX_01_Cold_Side

columns:
  - name: temp_in_c
    ...
    
  - name: heat_duty_kw
    generator:
      type: derived
      expression: "UA * (HX_01_Hot_Side.temp_in_c - HX_01_Cold_Side.temp_in_c)"
```

### 3. Series Reactors
```yaml
entities:
  - name: Reactor_1
  - name: Reactor_2
  - name: Reactor_3

columns:
  - name: feed_concentration
    generator:
      type: derived
      # Reactor_2 feed is Reactor_1 effluent
      expression: "Reactor_1.effluent_concentration if entity_id == 'Reactor_2' else fresh_feed"
```

---

## Syntax Design

### Dot Notation
```python
expression: "OtherEntity.column_name"
```

### With Arithmetic
```python
expression: "Tank_A.level * 0.1 + Tank_B.level * 0.05"
```

### Combined with prev()
```python
expression: "prev('level', 100) + Tank_A.flow_out - flow_in"
```

### Conditional Logic
```python
expression: "Tank_A.level if Tank_A.level > 10 else 0"
```

---

## Implementation Strategy

### Phase 1: Entity Proxy Objects
Create lightweight proxy objects that allow attribute access:

```python
class EntityProxy:
    def __init__(self, entity_name, entity_row_data):
        self.entity_name = entity_name
        self._data = entity_row_data
    
    def __getattr__(self, column_name):
        if column_name in self._data:
            return self._data[column_name]
        raise AttributeError(f"Entity '{self.entity_name}' has no column '{column_name}'")
```

### Phase 2: Namespace Enhancement
Pass all entities' current row data to derived expression evaluator:

```python
# Before (current):
namespace = {**row_data, **safe_builtins}

# After (with cross-entity):
entity_proxies = {
    entity_name: EntityProxy(entity_name, entity_rows[entity_name])
    for entity_name in entity_rows
}
namespace = {**row_data, **safe_builtins, **entity_proxies}
```

### Phase 3: Dependency Ordering
Extend dependency analyzer to handle cross-entity dependencies:

```python
def _extract_entity_references(expression: str) -> Set[str]:
    """Extract entity names from expression like 'Tank_A.level'."""
    return set(re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\.[A-Za-z_]', expression))
```

Then topologically sort entities based on cross-entity dependencies.

### Phase 4: Generation Order
Generate entities in dependency order at each timestamp:

```
For each timestamp:
    For each entity (in topological order):
        Generate all columns for this entity
        Store complete row
    Combine all entity rows into output
```

---

## Challenges & Solutions

### Challenge 1: Circular Dependencies
**Problem:** Tank_A references Tank_B, Tank_B references Tank_A  
**Solution:** Detect cycles during dependency analysis and raise clear error

### Challenge 2: Entity Not Yet Generated
**Problem:** Tank_B.level referenced before Tank_B row generated  
**Solution:** Topological sort ensures Tank_B generated before any entity that references it

### Challenge 3: Performance
**Problem:** Creating EntityProxy objects for every row might be slow  
**Solution:** Reuse proxy objects, just update internal `_data` reference

### Challenge 4: prev() with Cross-Entity
**Problem:** What does `Tank_A.prev('level', 100)` mean?  
**Solution:** Don't support this initially - prev() only works within same entity

---

## Validation Rules

1. **Entity Exists:** Referenced entity must exist in entities list
2. **Column Exists:** Referenced column must be defined
3. **No Cycles:** No circular cross-entity dependencies
4. **Deterministic Order:** Must be able to generate entities in deterministic order

---

## Example: Two-Tank System

```yaml
entities:
  - name: Tank_A
    count: 1
  - name: Tank_B
    count: 1

start_time: "2026-03-11T00:00:00Z"
end_time: "2026-03-11T01:00:00Z"
timestep_seconds: 60

columns:
  # Tank level (both tanks)
  - name: level_m3
    generator:
      type: random_walk
      start: 50.0
      min: 0.0
      max: 100.0
      volatility: 1.0
      mean_reversion: 0.05
      
  # Inlet flow (Tank_A only - from external source)
  - name: flow_in_m3_hr
    generator:
      type: random_walk
      start: 10.0
      min: 0.0
      max: 20.0
    entity_overrides:
      Tank_B:
        type: constant
        value: 0.0  # Tank_B has no external inlet
      
  # Outlet flow (proportional to level)
  - name: flow_out_m3_hr
    generator:
      type: derived
      expression: "level_m3 * 0.1"
      
  # Cross-entity reference: Tank_B receives Tank_A's outlet
  - name: flow_from_tank_a_m3_hr
    generator:
      type: derived
      expression: "Tank_A.flow_out_m3_hr if entity_id == 'Tank_B' else 0.0"
      
  # Material balance (integration)
  - name: level_calculated_m3
    generator:
      type: derived
      expression: >
        prev('level_calculated_m3', 50) + 
        (flow_in_m3_hr + flow_from_tank_a_m3_hr - flow_out_m3_hr) * (1/60)
```

**Result:**
- Tank_A: Receives external flow, drains proportionally
- Tank_B: Receives Tank_A's outlet (cross-entity!), drains proportionally
- Levels integrate based on mass balance
- Realistic coupled tank dynamics

---

## Testing Strategy

1. **Basic cross-reference:** Tank_B.level = Tank_A.level + 5
2. **Arithmetic:** Flow = Tank_A.level * 0.1 + Tank_B.level * 0.05
3. **Conditional:** Value = Tank_A.level if condition else 0
4. **Multiple entities:** 3+ tanks in series
5. **Circular dependency detection:** A → B → C → A should error
6. **Missing entity:** Reference to undefined entity should error
7. **Missing column:** Tank_A.nonexistent should error

---

## Documentation Updates Needed

1. Update `yaml_schema.md` with cross-entity syntax
2. Add examples to `simulation_guide.md`
3. Create flowsheet simulation tutorial
4. Add to AGENTS.md as completed feature

---

## Timeline

- **Day 1:** Design + implement EntityProxy + namespace enhancement
- **Day 2:** Dependency ordering + generation order + validation
- **Day 3:** Tests + examples + documentation

---

## Status: NOT YET IMPLEMENTED

This is a design document. Implementation starts now.
