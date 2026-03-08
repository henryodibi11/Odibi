"""Tests for simulation P0 critical fixes."""

import subprocess
import sys

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    EntityConfig,
    RangeGeneratorConfig,
    SequentialGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    UUIDGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestCriticalFixes:
    """Tests for P0 critical bug fixes."""

    def test_deterministic_entity_rng_across_processes(self):
        """Test that entity RNG is deterministic across Python processes.

        This was broken in v3 because hash() is process-randomized.
        Fixed by using hashlib.md5 for stable hashing.
        """
        import json

        # Script to run in subprocess
        script = """
import sys
import json
from odibi.config import (
    SimulationConfig, SimulationScope, EntityConfig,
    ColumnGeneratorConfig, RangeGeneratorConfig, SimulationDataType
)
from odibi.simulation import SimulationEngine

config = SimulationConfig(
    scope=SimulationScope(
        start_time="2026-01-01T00:00:00Z",
        timestep="1h",
        row_count=5,
        seed=42,
    ),
    entities=EntityConfig(names=["entity_a", "entity_b"]),
    columns=[
        ColumnGeneratorConfig(
            name="value",
            data_type=SimulationDataType.FLOAT,
            generator=RangeGeneratorConfig(type="range", min=0, max=100),
        ),
    ],
)

engine = SimulationEngine(config)
rows = engine.generate()
# Output just the values as JSON
values = [row["value"] for row in rows]
print(json.dumps(values))
"""

        # Generate in this process
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(names=["entity_a", "entity_b"]),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
            ],
        )

        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()
        values1 = [row["value"] for row in rows1]

        # Generate in subprocess (different hash() seed)
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, f"Subprocess failed: {result.stderr}"
        values2 = json.loads(result.stdout.strip())

        # Should be identical despite different process
        assert values1 == pytest.approx(values2), (
            f"Cross-process determinism failed. Process 1: {values1[:3]}, Process 2: {values2[:3]}"
        )

    def test_zero_timestep_rejected(self):
        """Test that timestep of 0 is rejected with clear error."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="0s",  # Invalid
                row_count=10,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1),
                ),
            ],
        )

        # Engine initialization should fail
        with pytest.raises(ValueError, match="Timestep must be positive"):
            SimulationEngine(config)

    def test_negative_timestep_rejected(self):
        """Test that negative timestep is rejected."""
        # Note: Regex doesn't support negative numbers, but this tests validation
        with pytest.raises(ValueError):
            config = SimulationConfig(
                scope=SimulationScope(
                    start_time="2026-01-01T00:00:00Z",
                    timestep="-5m",  # Invalid
                    row_count=10,
                ),
                entities=EntityConfig(count=1),
                columns=[],
            )
            SimulationEngine(config)

    def test_end_time_before_start_produces_empty_dataset(self):
        """Test that end_time < start_time produces 0 rows, not 1.

        This was broken in v3 (used max(1, ...)) causing phantom row.
        Fixed by using max(0, ...).
        """
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-02T00:00:00Z",  # Later
                end_time="2026-01-01T00:00:00Z",  # Earlier
                timestep="1h",
                seed=42,
            ),
            entities=EntityConfig(count=2),
            columns=[
                ColumnGeneratorConfig(
                    name="value",
                    data_type=SimulationDataType.INT,
                    generator=SequentialGeneratorConfig(type="sequential", start=1),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should be empty, not 1 phantom row
        assert len(rows) == 0, f"Expected 0 rows when end < start, got {len(rows)}"

    def test_uuid_generator_version4(self):
        """Test UUID4 generator (random but deterministic with seed)."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=2),
            columns=[
                ColumnGeneratorConfig(
                    name="id",
                    data_type=SimulationDataType.STRING,
                    generator=UUIDGeneratorConfig(type="uuid", version=4),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All UUIDs should be unique
        uuids = [row["id"] for row in rows]
        assert len(uuids) == len(set(uuids)), "UUIDs must be unique"

        # All should be valid UUID format
        import uuid

        for uuid_str in uuids:
            parsed = uuid.UUID(uuid_str)
            assert parsed.version == 4, f"UUID should be version 4: {uuid_str}"

        # Deterministic: same seed should produce same UUIDs
        engine2 = SimulationEngine(config)
        rows2 = engine2.generate()
        uuids2 = [row["id"] for row in rows2]

        assert uuids == uuids2, "Same seed should produce identical UUIDs"

    def test_uuid_generator_version5(self):
        """Test UUID5 generator (deterministic from entity+row)."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=2, id_prefix="device_"),
            columns=[
                ColumnGeneratorConfig(
                    name="id",
                    data_type=SimulationDataType.STRING,
                    generator=UUIDGeneratorConfig(type="uuid", version=5),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All UUIDs should be unique
        uuids = [row["id"] for row in rows]
        assert len(uuids) == len(set(uuids)), "UUIDs must be unique"

        # All should be valid UUID format
        import uuid

        for uuid_str in uuids:
            parsed = uuid.UUID(uuid_str)
            assert parsed.version == 5, f"UUID should be version 5: {uuid_str}"

        # Fully deterministic (no seed dependency)
        engine2 = SimulationEngine(
            SimulationConfig(
                scope=SimulationScope(
                    start_time="2026-01-01T00:00:00Z",
                    timestep="1h",
                    row_count=5,
                    seed=999,  # Different seed
                ),
                entities=EntityConfig(count=2, id_prefix="device_"),
                columns=[
                    ColumnGeneratorConfig(
                        name="id",
                        data_type=SimulationDataType.STRING,
                        generator=UUIDGeneratorConfig(type="uuid", version=5),
                    ),
                ],
            )
        )
        rows2 = engine2.generate()
        uuids2 = [row["id"] for row in rows2]

        # UUID5 should be identical regardless of seed (based on entity+row)
        assert uuids == uuids2, "UUID5 should be seed-independent"

    def test_uuid_generator_with_custom_namespace(self):
        """Test UUID5 with custom namespace."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=3,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="id",
                    data_type=SimulationDataType.STRING,
                    generator=UUIDGeneratorConfig(
                        type="uuid",
                        version=5,
                        namespace="com.example.odibi",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should generate valid UUIDs
        assert len(rows) == 3
        import uuid

        for row in rows:
            parsed = uuid.UUID(row["id"])
            assert parsed.version == 5

    def test_incremental_empty_dataset_with_end_time(self):
        """Test that incremental mode with end_time handles exhausted datasets correctly."""
        from odibi.config import TimestampGeneratorConfig

        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                end_time="2026-01-01T10:00:00Z",  # 10 hours total
                timestep="1h",
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

        # First run
        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()
        assert len(rows1) == 11  # 0:00, 1:00, ..., 10:00 = 11 rows

        max_ts = engine1.get_max_timestamp(rows1)
        assert max_ts == "2026-01-01T10:00:00Z"  # Zulu format

        # Second run with HWM at end_time
        # effective_start = 2026-01-01T10:00:00 + 1h = 11:00
        # end_time = 2026-01-01T10:00:00
        # Since effective_start > end_time, should produce 0 rows
        engine2 = SimulationEngine(config, hwm_timestamp=max_ts)
        rows2 = engine2.generate()

        # Should be empty (not 1 phantom row) - dataset exhausted
        assert len(rows2) == 0, f"Expected 0 rows when HWM >= end_time, got {len(rows2)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
