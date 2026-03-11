"""Tests for scheduled events in simulation.

Tests time-based events that modify simulation behavior:
- Maintenance windows (forced power=0)
- Grid curtailment (forced output limits)
- Setpoint changes (process optimization)
"""

import pandas as pd
import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    EntityConfig,
    RandomWalkGeneratorConfig,
    ScheduledEvent,
    ScheduledEventType,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


def test_forced_value_single_entity():
    """Test forced_value event on single entity during time window."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=20,
            seed=42,
        ),
        entities=EntityConfig(names=["Turbine_01"]),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="power_kw",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=1000.0,
                    min=800.0,
                    max=1200.0,
                    volatility=20.0,
                ),
            ),
        ],
        scheduled_events=[
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="Turbine_01",
                column="power_kw",
                value=0.0,
                start_time="2026-03-11T00:10:00Z",
                end_time="2026-03-11T00:15:00Z",
            )
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 20 rows
    assert len(df) == 20

    # Before event (rows 0-9): power should be ~1000
    before = df[df["timestamp"] < "2026-03-11T00:10:00Z"]
    assert before["power_kw"].min() > 800
    assert before["power_kw"].max() < 1200

    # During event (rows 10-15): power should be 0
    during = df[
        (df["timestamp"] >= "2026-03-11T00:10:00Z") & (df["timestamp"] <= "2026-03-11T00:15:00Z")
    ]
    assert len(during) == 6  # 10, 11, 12, 13, 14, 15
    assert (during["power_kw"] == 0.0).all(), "Power should be forced to 0 during event"

    # After event (rows 16-19): power should resume
    after = df[df["timestamp"] > "2026-03-11T00:15:00Z"]
    assert after["power_kw"].min() > 0, "Power should resume after event"


def test_forced_value_all_entities():
    """Test forced_value event on all entities (entity=null)."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(count=3, id_prefix="Turbine"),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="power_kw",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=500.0,
                    min=0.0,
                    max=1000.0,
                ),
            ),
        ],
        scheduled_events=[
            # Grid curtailment affecting ALL turbines
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity=None,  # All entities
                column="power_kw",
                value=0.0,
                start_time="2026-03-11T00:05:00Z",
                end_time="2026-03-11T00:07:00Z",
            )
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 3 entities × 10 rows = 30 rows
    assert len(df) == 30

    # During event (timestamps 00:05, 00:06, 00:07), all turbines should be 0
    during = df[
        (df["timestamp"] >= "2026-03-11T00:05:00Z") & (df["timestamp"] <= "2026-03-11T00:07:00Z")
    ]
    assert len(during) == 9  # 3 entities × 3 rows each
    assert (during["power_kw"] == 0.0).all(), "All turbines should be curtailed"

    # After event, all should resume
    after = df[df["timestamp"] > "2026-03-11T00:07:00Z"]
    assert after["power_kw"].sum() > 0, "Power should resume after curtailment"


def test_permanent_event_no_end_time():
    """Test permanent event with no end_time."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=20,
            seed=42,
        ),
        entities=EntityConfig(names=["Reactor"]),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="setpoint_c",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=350.0),
            ),
        ],
        scheduled_events=[
            # Permanent setpoint change at t=10
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="Reactor",
                column="setpoint_c",
                value=370.0,
                start_time="2026-03-11T00:10:00Z",
                end_time=None,  # Permanent
            )
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Before event (rows 0-9): setpoint = 350
    before = df[df["timestamp"] < "2026-03-11T00:10:00Z"]
    assert (before["setpoint_c"] == 350.0).all()

    # After event starts (rows 10+): setpoint = 370 permanently
    after = df[df["timestamp"] >= "2026-03-11T00:10:00Z"]
    assert (after["setpoint_c"] == 370.0).all()


def test_overlapping_events_priority():
    """Test priority when multiple events overlap."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(names=["Tank"]),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="level",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=50.0),
            ),
        ],
        scheduled_events=[
            # Lower priority event (applied first)
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="Tank",
                column="level",
                value=60.0,
                start_time="2026-03-11T00:05:00Z",
                end_time="2026-03-11T00:07:00Z",
                priority=1,
            ),
            # Higher priority event (applied last, overrides)
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="Tank",
                column="level",
                value=80.0,
                start_time="2026-03-11T00:05:00Z",
                end_time="2026-03-11T00:07:00Z",
                priority=2,
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # During overlap (00:05-00:07), higher priority (80) should win
    during = df[
        (df["timestamp"] >= "2026-03-11T00:05:00Z") & (df["timestamp"] <= "2026-03-11T00:07:00Z")
    ]
    assert (during["level"] == 80.0).all(), "Higher priority event should override"


def test_multiple_events_different_columns():
    """Test multiple events on different columns."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=15,
            seed=42,
        ),
        entities=EntityConfig(names=["BESS"]),
        columns=[
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="power_kw",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk", start=500.0, min=0.0, max=1000.0
                ),
            ),
            ColumnGeneratorConfig(
                name="temp_c",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk", start=28.0, min=20.0, max=35.0
                ),
            ),
        ],
        scheduled_events=[
            # Power curtailment 5-8
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="BESS",
                column="power_kw",
                value=0.0,
                start_time="2026-03-11T00:05:00Z",
                end_time="2026-03-11T00:08:00Z",
            ),
            # Temperature spike 10-12
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity="BESS",
                column="temp_c",
                value=40.0,
                start_time="2026-03-11T00:10:00Z",
                end_time="2026-03-11T00:12:00Z",
            ),
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # During power event (5-8), power=0 but temp normal
    power_event = df[
        (df["timestamp"] >= "2026-03-11T00:05:00Z") & (df["timestamp"] <= "2026-03-11T00:08:00Z")
    ]
    assert (power_event["power_kw"] == 0.0).all()
    assert (power_event["temp_c"] != 40.0).all()  # Temp unaffected

    # During temp event (10-12), temp=40 but power normal
    temp_event = df[
        (df["timestamp"] >= "2026-03-11T00:10:00Z") & (df["timestamp"] <= "2026-03-11T00:12:00Z")
    ]
    assert (temp_event["temp_c"] == 40.0).all()
    assert (temp_event["power_kw"] != 0.0).all()  # Power unaffected


def test_renewable_energy_example_wind_curtailment():
    """Realistic renewable energy example: wind farm curtailment."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="5m",
            row_count=24,  # 2 hours
            seed=2026,
        ),
        entities=EntityConfig(count=2, id_prefix="Wind_Turbine_"),
        columns=[
            ColumnGeneratorConfig(
                name="entity_id",
                data_type=SimulationDataType.STRING,
                generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
            ),
            ColumnGeneratorConfig(
                name="timestamp",
                data_type=SimulationDataType.TIMESTAMP,
                generator=TimestampGeneratorConfig(type="timestamp"),
            ),
            ColumnGeneratorConfig(
                name="power_kw",
                data_type=SimulationDataType.FLOAT,
                generator=RandomWalkGeneratorConfig(
                    type="random_walk",
                    start=2500.0,
                    min=0.0,
                    max=3000.0,
                    volatility=100.0,
                ),
            ),
        ],
        scheduled_events=[
            # Grid curtailment: all turbines to zero for 30 minutes
            ScheduledEvent(
                type=ScheduledEventType.FORCED_VALUE,
                entity=None,  # All turbines
                column="power_kw",
                value=0.0,
                start_time="2026-03-11T00:45:00Z",
                end_time="2026-03-11T01:15:00Z",
            )
        ],
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # Should have 2 turbines × 24 rows = 48 rows
    assert len(df) == 48

    # During curtailment (00:45-01:15), all turbines at 0
    during = df[
        (df["timestamp"] >= "2026-03-11T00:45:00Z") & (df["timestamp"] <= "2026-03-11T01:15:00Z")
    ]
    assert (during["power_kw"] == 0.0).all(), "All turbines should be curtailed"

    # Before and after, power should be non-zero
    before = df[df["timestamp"] < "2026-03-11T00:45:00Z"]
    after = df[df["timestamp"] > "2026-03-11T01:15:00Z"]

    assert before["power_kw"].mean() > 100, "Turbines should generate before curtailment"
    assert after["power_kw"].mean() > 100, "Turbines should resume after curtailment"


def test_no_events_no_impact():
    """Test that simulations without scheduled events work normally."""
    config = SimulationConfig(
        scope=SimulationScope(
            start_time="2026-03-11T00:00:00Z",
            timestep="1m",
            row_count=10,
            seed=42,
        ),
        entities=EntityConfig(names=["Entity"]),
        columns=[
            ColumnGeneratorConfig(
                name="value",
                data_type=SimulationDataType.FLOAT,
                generator=ConstantGeneratorConfig(type="constant", value=100.0),
            ),
        ],
        # No scheduled_events
    )

    engine = SimulationEngine(config)
    rows = engine.generate()
    df = pd.DataFrame(rows)

    # All values should be 100 (no events to modify)
    assert (df["value"] == 100.0).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
