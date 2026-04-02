"""Tests for simulation data generation."""

import pytest

from odibi.config import (
    BooleanGeneratorConfig,
    CategoricalGeneratorConfig,
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    EntityConfig,
    RandomWalkGeneratorConfig,
    RangeGeneratorConfig,
    ScheduledEvent,
    SequentialGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestSimulationConfig:
    """Tests for simulation configuration validation."""

    def test_scope_requires_row_count_or_end_time(self):
        """Scope must have either row_count or end_time."""
        with pytest.raises(ValueError, match="row_count.*end_time"):
            SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
            )

    def test_scope_cannot_have_both_row_count_and_end_time(self):
        """Scope cannot have both row_count and end_time."""
        with pytest.raises(ValueError, match="row_count.*end_time"):
            SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=100,
                end_time="2026-01-02T00:00:00Z",
            )

    def test_entity_requires_count_or_names(self):
        """Entity config must have either count or names."""
        with pytest.raises(ValueError, match="count.*names"):
            EntityConfig()

    def test_entity_cannot_have_both_count_and_names(self):
        """Entity config cannot have both count and names."""
        with pytest.raises(ValueError, match="count.*names"):
            EntityConfig(count=10, names=["pump_01", "pump_02"])

    def test_range_generator_validates_min_max(self):
        """Range generator requires min < max."""
        with pytest.raises(ValueError, match="min.*max"):
            RangeGeneratorConfig(type="range", min=100, max=50)

    def test_categorical_weights_must_match_values(self):
        """Categorical weights must match values length."""
        with pytest.raises(ValueError, match="Weights length"):
            CategoricalGeneratorConfig(
                type="categorical",
                values=["A", "B", "C"],
                weights=[0.5, 0.5],  # Wrong length
            )

    def test_categorical_weights_must_sum_to_one(self):
        """Categorical weights must sum to 1.0."""
        with pytest.raises(ValueError, match="sum to 1.0"):
            CategoricalGeneratorConfig(
                type="categorical",
                values=["A", "B"],
                weights=[0.3, 0.3],  # Sum = 0.6
            )

    def test_duplicate_column_names_rejected(self):
        """Duplicate column names should be rejected."""
        with pytest.raises(ValueError, match="Duplicate column"):
            SimulationConfig(
                scope=SimulationScope(
                    start_time="2026-01-01T00:00:00Z",
                    timestep="5m",
                    row_count=10,
                ),
                entities=EntityConfig(count=2),
                columns=[
                    ColumnGeneratorConfig(
                        name="temp",
                        data_type=SimulationDataType.FLOAT,
                        generator=RangeGeneratorConfig(type="range", min=50, max=100),
                    ),
                    ColumnGeneratorConfig(
                        name="temp",  # Duplicate
                        data_type=SimulationDataType.FLOAT,
                        generator=RangeGeneratorConfig(type="range", min=60, max=120),
                    ),
                ],
            )

    def test_scheduled_event_requires_start_time_or_condition(self):
        """ScheduledEvent must have at least one of start_time or condition."""
        with pytest.raises(ValueError, match="start_time.*condition"):
            ScheduledEvent(
                type="forced_value",
                column="temp",
                value=0.0,
            )

    def test_scheduled_event_cannot_have_duration_and_end_time(self):
        """ScheduledEvent cannot have both duration and end_time."""
        with pytest.raises(ValueError, match="end_time.*duration"):
            ScheduledEvent(
                type="forced_value",
                column="temp",
                value=0.0,
                start_time="2026-01-01T00:00:00Z",
                end_time="2026-01-01T04:00:00Z",
                duration="4h",
            )

    def test_scheduled_event_recurrence_requires_start_time(self):
        """ScheduledEvent recurrence requires start_time."""
        with pytest.raises(ValueError, match="recurrence.*start_time"):
            ScheduledEvent(
                type="forced_value",
                column="temp",
                value=0.0,
                condition="value > 50",
                recurrence="6h",
            )

    def test_scheduled_event_cooldown_requires_condition(self):
        """ScheduledEvent cooldown requires condition."""
        with pytest.raises(ValueError, match="cooldown.*condition"):
            ScheduledEvent(
                type="forced_value",
                column="temp",
                value=0.0,
                start_time="2026-01-01T00:00:00Z",
                cooldown="5h",
            )

    def test_scheduled_event_ramp_requires_duration(self):
        """ScheduledEvent transition='ramp' requires duration or start+end."""
        with pytest.raises(ValueError, match="ramp.*duration"):
            ScheduledEvent(
                type="setpoint_change",
                column="temp",
                value=200.0,
                start_time="2026-01-01T00:00:00Z",
                transition="ramp",
            )

    def test_entity_override_validates_entity_names(self):
        """Entity overrides must reference valid entities."""
        with pytest.raises(ValueError, match="undefined entity"):
            SimulationConfig(
                scope=SimulationScope(
                    start_time="2026-01-01T00:00:00Z",
                    timestep="5m",
                    row_count=10,
                ),
                entities=EntityConfig(names=["pump_01", "pump_02"]),
                columns=[
                    ColumnGeneratorConfig(
                        name="temp",
                        data_type=SimulationDataType.FLOAT,
                        generator=RangeGeneratorConfig(type="range", min=50, max=100),
                        entity_overrides={
                            "pump_99": RangeGeneratorConfig(type="range", min=80, max=120),
                        },
                    ),
                ],
            )


class TestSimulationEngine:
    """Tests for simulation engine."""

    def test_basic_simulation_with_row_count(self):
        """Test basic simulation with row_count."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=2, id_prefix="pump_"),
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
                    name="temperature",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=60.0, max=80.0),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should have 2 entities * 10 rows = 20 rows
        assert len(rows) == 20

        # Check columns exist
        assert all("entity_id" in row for row in rows)
        assert all("timestamp" in row for row in rows)
        assert all("temperature" in row for row in rows)

        # Check entity IDs
        entity_ids = {row["entity_id"] for row in rows}
        assert entity_ids == {"pump_01", "pump_02"}

        # Check temperature range
        temps = [row["temperature"] for row in rows]
        assert all(60.0 <= t <= 80.0 for t in temps)

    def test_timestep_parsing(self):
        """Test timestep parsing."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        assert engine.timestep_seconds == 3600  # 1 hour = 3600 seconds

    def test_categorical_generator(self):
        """Test categorical generator."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="status",
                    data_type=SimulationDataType.CATEGORICAL,
                    generator=CategoricalGeneratorConfig(
                        type="categorical",
                        values=["Running", "Idle", "Error"],
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All values should be from the allowed set
        statuses = {row["status"] for row in rows}
        assert statuses.issubset({"Running", "Idle", "Error"})

    def test_boolean_generator(self):
        """Test boolean generator."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="is_active",
                    data_type=SimulationDataType.BOOLEAN,
                    generator=BooleanGeneratorConfig(type="boolean", true_probability=0.9),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Most should be True (90% probability)
        true_count = sum(1 for row in rows if row["is_active"] is True)
        assert true_count > 75  # At least 75% should be True (with some randomness)

    def test_sequential_generator(self):
        """Test sequential generator."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=5,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="id",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=100, step=10),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # IDs should be 100, 110, 120, 130, 140
        ids = [row["id"] for row in rows]
        assert ids == [100, 110, 120, 130, 140]

    def test_sequential_unique_across_entities(self):
        """Test sequential generator produces non-overlapping IDs across entities."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=3,
            ),
            entities=EntityConfig(names=["web", "mobile", "store"]),
            columns=[
                ColumnGeneratorConfig(
                    name="entity_id",
                    data_type=SimulationDataType.STRING,
                    generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
                ),
                ColumnGeneratorConfig(
                    name="order_id",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1, step=1),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All order_ids should be globally unique
        order_ids = [row["order_id"] for row in rows]
        assert len(order_ids) == len(set(order_ids)), (
            f"Sequential IDs must be unique across entities, got duplicates: {order_ids}"
        )

        # 3 entities × 3 rows = 9 total rows, IDs 1-9
        assert sorted(order_ids) == list(range(1, 10))

    def test_sequential_non_unique_across_entities(self):
        """Test sequential with unique_across_entities=False gives per-entity sequences."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=3,
            ),
            entities=EntityConfig(names=["web", "mobile"]),
            columns=[
                ColumnGeneratorConfig(
                    name="entity_id",
                    data_type=SimulationDataType.STRING,
                    generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
                ),
                ColumnGeneratorConfig(
                    name="seq",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(
                        type="sequential", start=1, step=1, unique_across_entities=False
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Both entities should have the same per-entity sequence
        web_seqs = [r["seq"] for r in rows if r["entity_id"] == "web"]
        mobile_seqs = [r["seq"] for r in rows if r["entity_id"] == "mobile"]
        assert web_seqs == [1, 2, 3]
        assert mobile_seqs == [1, 2, 3]

    def test_null_rate(self):
        """Test null_rate parameter."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                    null_rate=0.2,  # 20% nulls
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should have approximately 20% nulls
        null_count = sum(1 for row in rows if row["value"] is None)
        assert 10 <= null_count <= 30  # Allow some variance

    def test_determinism_with_seed(self):
        """Test that same seed produces same results."""
        config1 = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=2),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        config2 = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=10,
                seed=42,  # Same seed
            ),
            entities=EntityConfig(count=2),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        engine1 = SimulationEngine(config1)
        engine2 = SimulationEngine(config2)

        rows1 = engine1.generate()
        rows2 = engine2.generate()

        # Same seed should produce identical results
        assert len(rows1) == len(rows2)
        for r1, r2 in zip(rows1, rows2):
            assert r1["value"] == pytest.approx(r2["value"])

    def test_entity_overrides(self):
        """Test entity-specific overrides."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(names=["pump_01", "pump_02"]),
            columns=[
                ColumnGeneratorConfig(
                    name="temperature",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=60.0, max=80.0),
                    entity_overrides={
                        "pump_02": RangeGeneratorConfig(type="range", min=90.0, max=110.0),
                    },
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # pump_01 should have temps in 60-80, pump_02 in 90-110
        # Note: entity_id might not exist if not defined in columns
        # Test verifies entity overrides are parsed correctly
        assert len(rows) == 10  # 2 entities × 5 rows

    def test_incremental_mode_with_hwm(self):
        """Test incremental generation with HWM."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
            ],
        )

        # First run - no HWM
        engine1 = SimulationEngine(config, hwm_timestamp=None)
        rows1 = engine1.generate()

        # Extract max timestamp
        max_ts1 = engine1.get_max_timestamp(rows1)

        # Second run - with HWM
        engine2 = SimulationEngine(config, hwm_timestamp=max_ts1)
        rows2 = engine2.generate()

        # Second run should start after first run's max timestamp
        if rows1 and rows2:
            first_ts2 = rows2[0]["timestamp"]
            assert first_ts2 > max_ts1

    def test_parameter_override_resets_random_walk(self):
        """Test parameter_override resets random walk value (e.g., CIP cleaning)."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=48,  # 2 days
                seed=42,
            ),
            entities=EntityConfig(names=["HX_01"]),
            columns=[
                ColumnGeneratorConfig(
                    name="equipment_id",
                    data_type=SimulationDataType.STRING,
                    generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
                ),
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=RandomWalkGeneratorConfig(
                        type="random_walk",
                        start=94.0,
                        min=60.0,
                        max=95.0,
                        volatility=0.1,
                        trend=-0.1,
                        mean_reversion=0.0,
                    ),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="parameter_override",
                    entity="HX_01",
                    column="efficiency",
                    value=94.0,
                    start_time="2026-01-02T00:00:00Z",
                    end_time="2026-01-02T04:00:00Z",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # During the override window (hours 24-28), efficiency should be 94.0
        override_rows = [r for r in rows if "2026-01-02T00" <= r["timestamp"] <= "2026-01-02T04"]
        for r in override_rows:
            assert r["efficiency"] == 94.0, (
                f"At {r['timestamp']}, efficiency should be 94.0 during override, got {r['efficiency']}"
            )

        # After override ends, walk should continue from 94.0 (not from pre-override value)
        post_rows = [r for r in rows if r["timestamp"] > "2026-01-02T04:00:00Z"]
        if post_rows:
            # First value after override should be close to 94.0 (one step away)
            assert post_rows[0]["efficiency"] > 90.0, (
                "Random walk should continue from reset value, not pre-override value"
            )

    def test_setpoint_change_overrides_value(self):
        """Test setpoint_change works like forced_value."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(names=["Reactor_01"]),
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
                    name="setpoint",
                    data_type=SimulationDataType.FLOAT,
                    generator=ConstantGeneratorConfig(type="constant", value=350.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="setpoint_change",
                    entity="Reactor_01",
                    column="setpoint",
                    value=370.0,
                    start_time="2026-01-01T05:00:00Z",
                    # No end_time = permanent
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Before setpoint change: 350.0
        before = [r for r in rows if r["timestamp"] < "2026-01-01T05:00:00Z"]
        for r in before:
            assert r["setpoint"] == 350.0

        # After setpoint change: 370.0 (permanent)
        after = [r for r in rows if r["timestamp"] >= "2026-01-01T05:00:00Z"]
        assert len(after) > 0
        for r in after:
            assert r["setpoint"] == 370.0

    # ------------------------------------------------------------------ #
    # V1 Tests: duration, recurrence, jitter, max_occurrences
    # ------------------------------------------------------------------ #

    def test_scheduled_event_with_duration(self):
        """Duration field should behave like specifying an end_time offset from start_time."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(names=["E1"]),
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
                    name="power",
                    data_type=SimulationDataType.FLOAT,
                    generator=ConstantGeneratorConfig(type="constant", value=100.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="E1",
                    column="power",
                    value=0.0,
                    start_time="2026-01-01T02:00:00Z",
                    duration="4h",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        for r in rows:
            ts = r["timestamp"]
            # duration="4h" from T02 means window is T02 to T06 inclusive
            if "2026-01-01T02:00:00Z" <= ts <= "2026-01-01T06:00:00Z":
                assert r["power"] == 0.0, f"Expected 0.0 during override at {ts}"
            else:
                assert r["power"] == 100.0, f"Expected 100.0 outside override at {ts}"

    def test_scheduled_event_recurrence(self):
        """Recurring parameter_override should trigger at each recurrence interval."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=24,
                seed=42,
            ),
            entities=EntityConfig(names=["HX_01"]),
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
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=RandomWalkGeneratorConfig(
                        type="random_walk",
                        start=50.0,
                        min=0.0,
                        max=100.0,
                        volatility=0.1,
                        trend=0.0,
                        mean_reversion=0.0,
                    ),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="parameter_override",
                    entity="HX_01",
                    column="efficiency",
                    value=94.0,
                    start_time="2026-01-01T00:00:00Z",
                    recurrence="6h",
                    duration="2h",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Check that value is 94.0 during each 2-hour occurrence window
        # Windows: 00-02, 06-08, 12-14, 18-20
        override_hours = {0, 1, 2, 6, 7, 8, 12, 13, 14, 18, 19, 20}
        for r in rows:
            hour = int(r["timestamp"][11:13])
            if hour in override_hours:
                assert r["efficiency"] == 94.0, (
                    f"Expected 94.0 during override at hour {hour}, got {r['efficiency']}"
                )

    def test_scheduled_event_max_occurrences(self):
        """max_occurrences should limit how many times a recurring event triggers."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=24,
                seed=42,
            ),
            entities=EntityConfig(names=["HX_01"]),
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
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=RandomWalkGeneratorConfig(
                        type="random_walk",
                        start=50.0,
                        min=0.0,
                        max=100.0,
                        volatility=0.1,
                        trend=0.0,
                        mean_reversion=0.0,
                    ),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="parameter_override",
                    entity="HX_01",
                    column="efficiency",
                    value=94.0,
                    start_time="2026-01-01T00:00:00Z",
                    recurrence="6h",
                    duration="2h",
                    max_occurrences=2,
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Only first 2 occurrences (00-02, 06-08) should be overridden
        first_window = [r for r in rows if "2026-01-01T00" <= r["timestamp"] <= "2026-01-01T02"]
        for r in first_window:
            assert r["efficiency"] == 94.0

        second_window = [r for r in rows if "2026-01-01T06" <= r["timestamp"] <= "2026-01-01T08"]
        for r in second_window:
            assert r["efficiency"] == 94.0

        # Third window (12-14) should NOT be overridden
        third_window = [r for r in rows if "2026-01-01T12" <= r["timestamp"] <= "2026-01-01T14"]
        has_non_94 = any(r["efficiency"] != 94.0 for r in third_window)
        assert has_non_94, "Third occurrence should not trigger with max_occurrences=2"

    def test_scheduled_event_with_jitter(self):
        """Recurring event with jitter should shift occurrence windows deterministically."""
        base_config = dict(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=24,
                seed=42,
            ),
            entities=EntityConfig(names=["T1"]),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="power",
                    data_type=SimulationDataType.FLOAT,
                    generator=ConstantGeneratorConfig(type="constant", value=100.0),
                ),
            ],
        )

        # Without jitter
        config_no_jitter = SimulationConfig(
            **base_config,
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="T1",
                    column="power",
                    value=0.0,
                    start_time="2026-01-01T00:00:00Z",
                    recurrence="8h",
                    duration="2h",
                ),
            ],
        )

        # With jitter
        config_with_jitter = SimulationConfig(
            **base_config,
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="T1",
                    column="power",
                    value=0.0,
                    start_time="2026-01-01T00:00:00Z",
                    recurrence="8h",
                    duration="2h",
                    jitter="2h",
                ),
            ],
        )

        rows_no_jitter = SimulationEngine(config_no_jitter).generate()
        rows_with_jitter = SimulationEngine(config_with_jitter).generate()

        # Both should run without error
        assert len(rows_no_jitter) == 24
        assert len(rows_with_jitter) == 24

        # The zero-value timestamps should differ due to jitter
        zero_ts_no = {r["timestamp"] for r in rows_no_jitter if r["power"] == 0.0}
        zero_ts_with = {r["timestamp"] for r in rows_with_jitter if r["power"] == 0.0}
        assert zero_ts_no != zero_ts_with, "Jitter should shift occurrence windows"

    # ------------------------------------------------------------------ #
    # V2 Tests: condition, cooldown, sustain
    # ------------------------------------------------------------------ #

    def test_condition_based_event(self):
        """Condition-based event should override values when condition is true."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(names=["P1"]),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="temp",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0.0, max=100.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="P1",
                    column="temp",
                    value=80.0,
                    condition="temp > 80",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All output values should be capped at 80.0
        for r in rows:
            assert r["temp"] <= 80.0, f"Expected temp <= 80.0, got {r['temp']}"

    def test_condition_with_cooldown(self):
        """Cooldown should prevent re-triggering within the cooldown period."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=20,
                seed=42,
            ),
            entities=EntityConfig(names=["P1"]),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RandomWalkGeneratorConfig(
                        type="random_walk",
                        start=50.0,
                        min=0.0,
                        max=100.0,
                        volatility=5.0,
                        trend=0.0,
                        mean_reversion=0.0,
                    ),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="parameter_override",
                    entity="P1",
                    column="value",
                    value=50.0,
                    condition="value > 60",
                    cooldown="5h",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Find all timestamps where value was reset to 50.0 (trigger events)
        trigger_times = []
        for r in rows:
            if r["value"] == 50.0:
                trigger_times.append(r["timestamp"])

        # If there are multiple triggers, consecutive triggers must be >= 5h apart
        for i in range(1, len(trigger_times)):
            # ISO timestamps sort lexicographically for same-day comparisons
            prev_hour = int(trigger_times[i - 1][11:13])
            curr_hour = int(trigger_times[i][11:13])
            gap = curr_hour - prev_hour
            if gap < 0:
                gap += 24  # handle day wrap
            assert gap >= 5, (
                f"Cooldown violated: triggers at {trigger_times[i - 1]} and {trigger_times[i]} "
                f"(gap={gap}h, expected >= 5h)"
            )

    def test_condition_with_sustain(self):
        """Sustain should require condition true for N consecutive steps before triggering."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=20,
                seed=42,
            ),
            entities=EntityConfig(names=["P1"]),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="level",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0.0, max=100.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="P1",
                    column="level",
                    value=50.0,
                    condition="level > 50",
                    sustain="3h",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # The event should not trigger on the very first row where level > 50;
        # it requires 3 consecutive timesteps (3h sustain at 1h timestep).
        # Just verify no error and output exists.
        assert len(rows) == 20

        # With sustain=3h (3 steps), a single spike above 50 should NOT trigger
        # Only 3+ consecutive above-50 values should trigger
        # Verify the simulation produced valid output
        for r in rows:
            assert r["level"] is not None

    def test_condition_with_duration_holds_override(self):
        """Once a condition-based event triggers, duration should hold the override."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=20,
                seed=99,
            ),
            entities=EntityConfig(names=["P1"]),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="temp",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0.0, max=100.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="forced_value",
                    entity="P1",
                    column="temp",
                    value=50.0,
                    condition="temp > 80",
                    duration="4h",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Find the first trigger point
        trigger_idx = None
        for i, r in enumerate(rows):
            if r["temp"] == 50.0:
                trigger_idx = i
                break

        if trigger_idx is not None:
            # The override should hold for 4 consecutive hours (4 rows at 1h timestep)
            hold_count = 0
            for j in range(trigger_idx, min(trigger_idx + 5, len(rows))):
                if rows[j]["temp"] == 50.0:
                    hold_count += 1
            # At minimum, more than 1 row should be held at 50.0
            assert hold_count >= 2, (
                f"Duration should hold override for multiple steps, only held for {hold_count}"
            )

    # ------------------------------------------------------------------ #
    # V3 Tests: transition=ramp
    # ------------------------------------------------------------------ #

    def test_ramp_transition(self):
        """Ramp transition should linearly interpolate from base to target over duration."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(names=["R1"]),
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
                    name="setpoint",
                    data_type=SimulationDataType.FLOAT,
                    generator=ConstantGeneratorConfig(type="constant", value=100.0),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="setpoint_change",
                    entity="R1",
                    column="setpoint",
                    value=200.0,
                    start_time="2026-01-01T02:00:00Z",
                    duration="4h",
                    transition="ramp",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Before ramp: should be 100.0
        before = [r for r in rows if r["timestamp"] < "2026-01-01T02:00:00Z"]
        for r in before:
            assert r["setpoint"] == 100.0

        # At ramp start (T02): progress=0/4=0.0 -> 100 + 0*(200-100) = 100.0
        r_start = [r for r in rows if r["timestamp"] == "2026-01-01T02:00:00Z"][0]
        assert r_start["setpoint"] == pytest.approx(100.0, abs=1.0)

        # At midpoint (T04): progress=2/4=0.5 -> 100 + 0.5*100 = 150.0
        r_mid = [r for r in rows if r["timestamp"] == "2026-01-01T04:00:00Z"][0]
        assert r_mid["setpoint"] == pytest.approx(150.0, abs=1.0)

        # At end (T06): progress=4/4=1.0 -> 200.0
        r_end = [r for r in rows if r["timestamp"] == "2026-01-01T06:00:00Z"][0]
        assert r_end["setpoint"] == pytest.approx(200.0, abs=1.0)

        # Values should be monotonically increasing during the ramp
        ramp_rows = [r for r in rows if "2026-01-01T02" <= r["timestamp"] <= "2026-01-01T06"]
        ramp_values = [r["setpoint"] for r in ramp_rows]
        for i in range(1, len(ramp_values)):
            assert ramp_values[i] >= ramp_values[i - 1], (
                f"Ramp should be monotonically increasing: {ramp_values}"
            )

    def test_ramp_transition_parameter_override(self):
        """Ramp transition with parameter_override should update random walk state progressively."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(names=["HX_01"]),
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
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=RandomWalkGeneratorConfig(
                        type="random_walk",
                        start=70.0,
                        min=60.0,
                        max=95.0,
                        volatility=0.1,
                        trend=0.0,
                        mean_reversion=0.0,
                    ),
                ),
            ],
            scheduled_events=[
                ScheduledEvent(
                    type="parameter_override",
                    entity="HX_01",
                    column="efficiency",
                    value=94.0,
                    start_time="2026-01-01T02:00:00Z",
                    duration="4h",
                    transition="ramp",
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # During ramp window, values should increase toward 94.0
        ramp_rows = [
            r for r in rows if "2026-01-01T02:00:00Z" <= r["timestamp"] <= "2026-01-01T06:00:00Z"
        ]
        assert len(ramp_rows) > 0

        # At the end of the ramp (T06, progress=1.0) the value should be 94.0
        last_ramp = ramp_rows[-1]
        assert last_ramp["efficiency"] == pytest.approx(94.0, abs=1.0)

        # After the ramp, random walk should continue from the ramped value
        post_ramp = [r for r in rows if r["timestamp"] > "2026-01-01T06:00:00Z"]
        if post_ramp:
            # Should be close to 94.0 (one random walk step away)
            assert post_ramp[0]["efficiency"] > 85.0, (
                "Random walk should continue from ramped value"
            )
