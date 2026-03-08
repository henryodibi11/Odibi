"""Tests for P1 feature improvements."""

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    DerivedGeneratorConfig,
    EmailGeneratorConfig,
    EntityConfig,
    GeoGeneratorConfig,
    IPGeneratorConfig,
    RangeGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestIncrementalRNGAdvancement:
    """Test that RNG advances correctly in incremental mode."""

    def test_incremental_produces_different_values(self):
        """Test that incremental runs produce different random values."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="sensor_"),
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

        # First run
        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()
        values1 = [row["value"] for row in rows1]
        max_ts1 = engine1.get_max_timestamp(rows1)

        # Second run (incremental)
        engine2 = SimulationEngine(config, hwm_timestamp=max_ts1)
        rows2 = engine2.generate()
        values2 = [row["value"] for row in rows2]

        # Values should be different (RNG advanced)
        assert values1 != pytest.approx(values2), (
            f"Incremental run should produce different random values. "
            f"Run 1: {values1[:3]}, Run 2: {values2[:3]}"
        )


class TestNullSafeDerivedColumns:
    """Test null-safe helpers in derived columns."""

    def test_safe_div_with_none(self):
        """Test safe_div handles None inputs."""
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
                    name="numerator",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                    null_rate=0.5,  # 50% nulls
                ),
                ColumnGeneratorConfig(
                    name="denominator",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=10),
                ),
                ColumnGeneratorConfig(
                    name="ratio",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="safe_div(numerator, denominator, 0)",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Should not crash with None values
        assert len(rows) == 5

        for row in rows:
            if row["numerator"] is None:
                assert row["ratio"] == 0  # Default value
            else:
                expected = row["numerator"] / row["denominator"]
                assert row["ratio"] == pytest.approx(expected)

    def test_coalesce_function(self):
        """Test coalesce returns first non-None value."""
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
                    name="primary",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                    null_rate=0.5,
                ),
                ColumnGeneratorConfig(
                    name="fallback",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=0, max=100),
                ),
                ColumnGeneratorConfig(
                    name="result",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="coalesce(primary, fallback, 0)",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            if row["primary"] is not None:
                assert row["result"] == row["primary"]
            else:
                assert row["result"] == row["fallback"]

    def test_multi_level_null_propagation(self):
        """Test null propagation through 3-level derived chain."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=20,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                # Level 0: Base column with nulls
                ColumnGeneratorConfig(
                    name="base",
                    data_type=SimulationDataType.FLOAT,
                    generator=RangeGeneratorConfig(type="range", min=1, max=100),
                    null_rate=0.3,  # 30% nulls
                ),
                # Level 1: Derived from base (null-safe)
                ColumnGeneratorConfig(
                    name="level1",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="safe_div(base, 2, 0) if base is not None else -1",
                    ),
                ),
                # Level 2: Derived from level1 (null-safe)
                ColumnGeneratorConfig(
                    name="level2",
                    data_type=SimulationDataType.FLOAT,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="level1 * 10 if level1 >= 0 else -999",
                    ),
                ),
                # Level 3: Derived from level2
                ColumnGeneratorConfig(
                    name="level3",
                    data_type=SimulationDataType.STRING,
                    generator=DerivedGeneratorConfig(
                        type="derived",
                        expression="'INVALID' if level2 < 0 else 'VALID'",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Verify null handling through all levels
        for row in rows:
            if row["base"] is None:
                # Null base should cascade as -1 → -999 → 'INVALID'
                assert row["level1"] == -1
                assert row["level2"] == -999
                assert row["level3"] == "INVALID"
            else:
                # Valid base should produce valid chain
                expected_l1 = row["base"] / 2
                expected_l2 = expected_l1 * 10
                assert row["level1"] == pytest.approx(expected_l1)
                assert row["level2"] == pytest.approx(expected_l2)
                assert row["level3"] == "VALID"


class TestTimestampStandardization:
    """Test timestamp format standardization."""

    def test_timestamp_uses_zulu_format(self):
        """Test that timestamps use Z suffix, not +00:00."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=3,
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
        rows = engine.generate()

        for row in rows:
            ts = row["timestamp"]
            # Should end with Z, not +00:00
            assert ts.endswith("Z"), f"Timestamp should use Zulu format: {ts}"
            assert "+00:00" not in ts, f"Timestamp should not contain +00:00: {ts}"


class TestCommonGenerators:
    """Test email, IP, and geo generators."""

    def test_email_generator(self):
        """Test email address generation."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=3,
            ),
            entities=EntityConfig(count=2, id_prefix="user_"),
            columns=[
                ColumnGeneratorConfig(
                    name="email",
                    data_type=SimulationDataType.STRING,
                    generator=EmailGeneratorConfig(
                        type="email",
                        domain="company.com",
                        pattern="{entity}_{row}",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Check email format
        for row in rows:
            email = row["email"]
            assert "@company.com" in email
            assert "user." in email  # Underscores converted to dots

    def test_ipv4_generator_full_range(self):
        """Test IPv4 generation without subnet."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="ip",
                    data_type=SimulationDataType.STRING,
                    generator=IPGeneratorConfig(type="ipv4"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # Check IP format
        import re

        ip_pattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"

        for row in rows:
            ip = row["ip"]
            assert re.match(ip_pattern, ip), f"Invalid IP format: {ip}"

    def test_ipv4_generator_with_subnet(self):
        """Test IPv4 generation within subnet."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="ip",
                    data_type=SimulationDataType.STRING,
                    generator=IPGeneratorConfig(type="ipv4", subnet="192.168.1.0/24"),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        # All IPs should be in 192.168.1.x range
        for row in rows:
            ip = row["ip"]
            assert ip.startswith("192.168.1."), f"IP should be in subnet: {ip}"

    def test_geo_generator(self):
        """Test geographic coordinate generation."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="1h",
                row_count=10,
                seed=42,
            ),
            entities=EntityConfig(count=1),
            columns=[
                ColumnGeneratorConfig(
                    name="location",
                    data_type=SimulationDataType.STRING,
                    generator=GeoGeneratorConfig(
                        type="geo",
                        bbox=[40.0, -75.0, 41.0, -74.0],  # NYC area
                        format="tuple",
                    ),
                ),
            ],
        )

        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            coords = row["location"]
            assert isinstance(coords, tuple)
            lat, lon = coords

            # Check bounds
            assert 40.0 <= lat <= 41.0, f"Latitude out of bounds: {lat}"
            assert -75.0 <= lon <= -74.0, f"Longitude out of bounds: {lon}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
