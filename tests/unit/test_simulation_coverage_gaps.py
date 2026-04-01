"""Tests addressing coverage gaps identified in gap analysis."""

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    DerivedGeneratorConfig,
    EntityConfig,
    RangeGeneratorConfig,
    SequentialGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
    UUIDGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestWriteModeCompatibility:
    """Test simulation works with various write modes."""

    def test_uuid_generator_for_upsert_mode(self):
        """Test UUID generator produces unique keys suitable for upsert."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=5),
            columns=[
                ColumnGeneratorConfig(
                    name="record_id",
                    data_type=SimulationDataType.STRING,
                    generator=UUIDGeneratorConfig(type="uuid", version=4),
                ),
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All UUIDs must be unique for upsert
        record_ids = [row["record_id"] for row in rows]
        assert len(record_ids) == len(set(record_ids)), "UUIDs must be unique for upsert mode"

        # Check total rows (5 entities × 100 rows)
        assert len(rows) == 500

    def test_sequential_composite_key_for_append_once(self):
        """Test sequential + entity_id creates composite key for append_once."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
            ),
            entities=EntityConfig(count=3, id_prefix="device_"),
            columns=[
                ColumnGeneratorConfig(
                    name="device_id",
                    data_type=SimulationDataType.STRING,
                    generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
                ),
                ColumnGeneratorConfig(
                    name="sequence",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1, step=1),
                ),
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # With unique_across_entities=True (default), each entity gets a non-overlapping range
        # 3 entities × 10 rows: entity 0 → 1-10, entity 1 → 11-20, entity 2 → 21-30
        all_sequences = [r["sequence"] for r in rows]
        assert sorted(all_sequences) == list(range(1, 31))
        assert len(all_sequences) == len(set(all_sequences)), "All IDs must be globally unique"


class TestLargeScalePerformance:
    """Test memory and performance with large datasets."""

    def test_100k_rows_generation(self):
        """Test generation of 100K rows completes without OOM."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1m",
                row_count=100_000,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        assert len(rows) == 100_000
        # Spot check first and last rows
        assert rows[0]["value"] >= 0
        assert rows[-1]["value"] <= 100

    def test_500k_rows_multi_entity(self):
        """Test 500K rows across 10 entities."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1s",
                row_count=50_000,
                seed=42,
            ),
            entities=EntityConfig(count=10, id_prefix="sensor_"),
            columns=[
                ColumnGeneratorConfig(
                    name="sensor_id",
                    data_type=SimulationDataType.STRING,
                    generator=ConstantGeneratorConfig(type="constant", value="{entity_id}"),
                ),
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # 10 entities × 50K rows = 500K total
        assert len(rows) == 500_000

        # Each entity should have exactly 50K rows
        sensor_counts = {}
        for row in rows:
            sid = row["sensor_id"]
            sensor_counts[sid] = sensor_counts.get(sid, 0) + 1

        assert len(sensor_counts) == 10
        for count in sensor_counts.values():
            assert count == 50_000

    def test_derived_columns_at_scale(self):
        """Test derived columns with 100K rows."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1s",
                row_count=100_000,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="input",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=100),
                ),
                ColumnGeneratorConfig(
                    name="output",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
                ColumnGeneratorConfig(
                    name="efficiency",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="(output / input * 100) if input > 0 else 0",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        assert len(rows) == 100_000
        # Spot check calculations
        for i in [0, 50000, 99999]:
            row = rows[i]
            expected = (row["output"] / row["input"] * 100) if row["input"] > 0 else 0
            assert row["efficiency"] == pytest.approx(expected)


class TestEdgeCaseCombinations:
    """Test edge case combinations not covered elsewhere."""

    def test_chaos_parameters_with_derived_columns(self):
        """Test that chaos and derived columns both work (chaos applied after derived)."""
        from odibi.config import ChaosConfig

        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="base_value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(
                        type="range",
                        min=50,
                        max=100,
                        distribution="uniform",
                    ),
                ),
                ColumnGeneratorConfig(
                    name="doubled",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="base_value * 2",
                    ),
                ),
            ],
            chaos=ChaosConfig(
                outlier_rate=0.1,  # 10% outliers
                outlier_factor=10.0,
            ),
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Check for outliers (chaos is applied AFTER derived columns)
        # Both base_value and doubled are numeric, so both can get outliers
        outliers = [r for r in rows if r["base_value"] > 100]
        assert len(outliers) > 0, "Should have some outliers"

        # Chaos applies per-cell independently, so we can only check the
        # derived relationship on rows where neither column was outlier-affected.
        # base_value range is [50,100], doubled range is [100,200] pre-chaos.
        normal_rows = [r for r in rows if r["base_value"] <= 100 and r["doubled"] <= 200]
        for row in normal_rows:
            assert row["doubled"] == pytest.approx(row["base_value"] * 2)

    def test_entity_overrides_with_incremental_mode(self):
        """Test entity overrides work correctly across incremental runs."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(names=["normal", "hot"]),
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
                    generator=RangeGeneratorConfig(type="range", min=60, max=80),
                    entity_overrides={
                        "hot": RangeGeneratorConfig(type="range", min=90, max=110),
                    },
                ),
            ],
        )

        # First run
        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()

        normal_temps1 = [r["temperature"] for r in rows1 if r["entity_id"] == "normal"]
        hot_temps1 = [r["temperature"] for r in rows1 if r["entity_id"] == "hot"]

        # Check ranges in first run
        assert all(60 <= t <= 80 for t in normal_temps1)
        assert all(90 <= t <= 110 for t in hot_temps1)

        max_ts = engine1.get_max_timestamp(rows1)

        # Second run (incremental)
        engine2 = SimulationEngine(config, hwm_timestamp=max_ts)
        rows2 = engine2.generate()

        normal_temps2 = [r["temperature"] for r in rows2 if r["entity_id"] == "normal"]
        hot_temps2 = [r["temperature"] for r in rows2 if r["entity_id"] == "hot"]

        # Check ranges maintained in incremental run
        assert all(60 <= t <= 80 for t in normal_temps2)
        assert all(90 <= t <= 110 for t in hot_temps2)

    def test_empty_dataset_with_derived_columns(self):
        """Test that exhausted end_time works with derived columns."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-02T00:00:00Z",  # After end_time
                end_time="2026-01-01T00:00:00Z",
                timestep="1h",
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="base",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1),
                ),
                ColumnGeneratorConfig(
                    name="derived",
                    data_type=SimulationDataType.INT,
                    generator=DerivedGeneratorConfig(type="derived", expression="base * 2"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should be empty (not crash)
        assert len(rows) == 0

    def test_null_rate_with_chaos_and_derived(self):
        """Test null_rate + outliers + derived columns all work together."""
        from odibi.config import ChaosConfig

        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=100,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="raw_value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(
                        type="range",
                        min=10,
                        max=100,
                    ),
                    null_rate=0.2,  # 20% nulls
                ),
                ColumnGeneratorConfig(
                    name="processed",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="safe_div(raw_value, 10, 0) if raw_value else -1",
                    ),
                ),
            ],
            chaos=ChaosConfig(
                outlier_rate=0.1,
                outlier_factor=5.0,
            ),
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should have nulls
        null_count = sum(1 for r in rows if r["raw_value"] is None)
        assert null_count > 0

        # Should have outliers (values multiplied by 5, chaos applied after derived)
        # Both raw_value and processed are numeric, so both can get outliers
        outliers = [r for r in rows if r["raw_value"] and r["raw_value"] > 100]
        assert len(outliers) > 0

        # Verify derived column correctly handles nulls
        # Chaos can multiply the -1 sentinel by the outlier_factor (5.0),
        # so null rows may have processed == -1 or -5.0
        null_rows = [r for r in rows if r["raw_value"] is None]
        for row in null_rows:
            assert row["processed"] in (-1, -1 * 5.0)

        # Chaos applies per-cell independently to both columns, so we can't
        # reliably check the derived relationship. Just verify both features
        # coexist without errors and that non-null processed values are numeric.
        non_null_rows = [r for r in rows if r["raw_value"] is not None]
        assert len(non_null_rows) > 0
        for row in non_null_rows:
            assert isinstance(row["processed"], (int, float))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
