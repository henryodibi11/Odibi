"""Tests for daily profile simulation generator."""

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    DailyProfileGeneratorConfig,
    EntityConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestDailyProfileConfig:
    """Tests for daily profile configuration validation."""

    def test_min_must_be_less_than_max(self):
        """min must be less than max."""
        with pytest.raises(ValueError, match="min.*max"):
            DailyProfileGeneratorConfig(
                type="daily_profile",
                min=100,
                max=50,
                profile={"08:00": 75},
            )

    def test_profile_must_not_be_empty(self):
        """Profile must have at least one anchor point."""
        with pytest.raises(ValueError, match="at least one anchor"):
            DailyProfileGeneratorConfig(
                type="daily_profile",
                min=0,
                max=100,
                profile={},
            )

    def test_invalid_time_format_rejected(self):
        """Profile keys must be valid HH:MM format."""
        with pytest.raises(ValueError, match="HH:MM"):
            DailyProfileGeneratorConfig(
                type="daily_profile",
                min=0,
                max=100,
                profile={"25:00": 50},
            )

    def test_invalid_time_format_no_colon(self):
        """Profile keys without colon are rejected."""
        with pytest.raises(ValueError, match="HH:MM"):
            DailyProfileGeneratorConfig(
                type="daily_profile",
                min=0,
                max=100,
                profile={"0800": 50},
            )

    def test_valid_config(self):
        """Valid config should parse."""
        config = DailyProfileGeneratorConfig(
            type="daily_profile",
            min=0,
            max=25,
            precision=0,
            noise=1.5,
            profile={"00:00": 1, "08:00": 19, "17:00": 14},
        )
        assert config.min == 0
        assert config.max == 25
        assert config.precision == 0
        assert config.noise == 1.5
        assert len(config.profile) == 3

    def test_defaults(self):
        """Default values should be sensible."""
        config = DailyProfileGeneratorConfig(
            type="daily_profile",
            min=0,
            max=100,
            profile={"12:00": 50},
        )
        assert config.noise == 0.0
        assert config.interpolation == "linear"
        assert config.precision is None
        assert config.weekend_scale is None


class TestDailyProfileGeneration:
    """Tests for daily profile value generation."""

    def _make_config(self, profile, **kwargs):
        """Helper to create a simulation config with daily profile column."""
        defaults = {
            "min": 0,
            "max": 100,
            "noise": 0.0,
            "precision": None,
        }
        defaults.update(kwargs)

        return SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T00:00:00Z",  # Monday
                timestep="1h",
                row_count=24,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        profile=profile,
                        **defaults,
                    ),
                ),
            ],
        )

    def test_values_follow_profile(self):
        """Values should follow the defined profile curve."""
        config = self._make_config(
            profile={"00:00": 5, "08:00": 80, "12:00": 60, "17:00": 90, "22:00": 10},
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # At midnight (row 0), should be near 5
        assert rows[0]["occupancy"] == pytest.approx(5.0, abs=1)
        # At 08:00 (row 8), should be near 80
        assert rows[8]["occupancy"] == pytest.approx(80.0, abs=1)
        # At 12:00 (row 12), should be near 60
        assert rows[12]["occupancy"] == pytest.approx(60.0, abs=1)

    def test_interpolation_between_anchors(self):
        """Values between anchors should be interpolated."""
        config = self._make_config(
            profile={"00:00": 0, "12:00": 100},
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # At 06:00 (row 6), should be halfway → ~50
        assert rows[6]["occupancy"] == pytest.approx(50.0, abs=1)
        # At 03:00 (row 3), should be ~25
        assert rows[3]["occupancy"] == pytest.approx(25.0, abs=1)

    def test_step_interpolation(self):
        """Step interpolation should hold previous value until next anchor."""
        config = self._make_config(
            profile={"00:00": 10, "12:00": 90},
            interpolation="step",
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # Between 00:00 and 12:00, should hold at 10
        assert rows[6]["occupancy"] == 10.0
        # At 12:00, jumps to 90
        assert rows[12]["occupancy"] == 90.0

    def test_precision_zero_gives_integers(self):
        """Precision 0 should produce integer values."""
        config = self._make_config(
            profile={"00:00": 1, "12:00": 19},
            precision=0,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            assert isinstance(row["occupancy"], int), f"Expected int, got {type(row['occupancy'])}"

    def test_precision_one_gives_one_decimal(self):
        """Precision 1 should round to one decimal place."""
        config = self._make_config(
            profile={"00:00": 1.0, "12:00": 19.5},
            precision=1,
            noise=2.0,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            value = row["occupancy"]
            assert value == round(value, 1)

    def test_values_clamped_to_min_max(self):
        """Values must not exceed min/max bounds."""
        config = self._make_config(
            profile={"00:00": 0, "12:00": 100},
            min=10,
            max=80,
            noise=50.0,  # High noise to push past bounds
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            assert 10 <= row["occupancy"] <= 80

    def test_noise_adds_variation(self):
        """Noise should produce different values from pure profile."""
        config_no_noise = self._make_config(
            profile={"00:00": 50, "12:00": 50},
            noise=0.0,
        )
        config_noise = self._make_config(
            profile={"00:00": 50, "12:00": 50},
            noise=10.0,
        )

        rows_clean = SimulationEngine(config_no_noise).generate()
        rows_noisy = SimulationEngine(config_noise).generate()

        # All clean values should be exactly 50
        clean_values = [r["occupancy"] for r in rows_clean]
        assert all(v == pytest.approx(50.0, abs=0.1) for v in clean_values)

        # Noisy values should have variation
        noisy_values = [r["occupancy"] for r in rows_noisy]
        assert max(noisy_values) - min(noisy_values) > 1.0

    def test_weekend_scale(self):
        """Weekend values should be scaled down."""
        # Saturday start
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-10T08:00:00Z",  # Saturday
                timestep="1h",
                row_count=1,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        min=0,
                        max=100,
                        profile={"08:00": 80},
                        weekend_scale=0.2,
                    ),
                ),
            ],
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # 80 * 0.2 = 16
        assert rows[0]["occupancy"] == pytest.approx(16.0, abs=1)

    def test_weekend_scale_none_no_effect(self):
        """No weekend_scale means weekends get same values as weekdays."""
        # Saturday
        config_sat = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-10T08:00:00Z",  # Saturday
                timestep="1h",
                row_count=1,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        min=0,
                        max=100,
                        profile={"08:00": 80},
                        weekend_scale=None,
                    ),
                ),
            ],
        )
        rows = SimulationEngine(config_sat).generate()
        assert rows[0]["occupancy"] == pytest.approx(80.0, abs=1)

    def test_deterministic_with_seed(self):
        """Same seed should produce identical results."""
        config = self._make_config(
            profile={"00:00": 5, "08:00": 80, "17:00": 20},
            noise=5.0,
        )

        rows1 = SimulationEngine(config).generate()
        rows2 = SimulationEngine(config).generate()

        for r1, r2 in zip(rows1, rows2):
            assert r1["occupancy"] == r2["occupancy"]

    def test_volatility_varies_between_days(self):
        """Volatility should make different days produce different values at the same time."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T08:00:00Z",  # Monday
                timestep="24h",
                row_count=7,  # 7 days, all at 08:00
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        min=0,
                        max=100,
                        noise=0.0,
                        volatility=5.0,
                        profile={"08:00": 50},
                    ),
                ),
            ],
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        values = [r["occupancy"] for r in rows]

        # With volatility=5 and no noise, different days should have different values
        unique_values = set(values)
        assert len(unique_values) > 1, f"Expected variation across days, got {values}"

    def test_volatility_zero_same_every_day(self):
        """Zero volatility should produce same value at same time each day."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T08:00:00Z",  # Monday
                timestep="24h",
                row_count=5,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        min=0,
                        max=100,
                        noise=0.0,
                        volatility=0.0,
                        profile={"08:00": 50},
                    ),
                ),
            ],
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        values = [r["occupancy"] for r in rows]

        # All values should be identical (same time, no noise, no volatility)
        assert all(v == values[0] for v in values)

    def test_volatility_deterministic_same_day(self):
        """Same day should always get the same volatility shift (deterministic)."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T00:00:00Z",  # Monday
                timestep="1h",
                row_count=24,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="building_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="occupancy",
                    data_type=SimulationDataType.FLOAT,
                    generator=DailyProfileGeneratorConfig(
                        type="daily_profile",
                        min=0,
                        max=100,
                        noise=0.0,
                        volatility=5.0,
                        profile={"00:00": 10, "12:00": 80},
                    ),
                ),
            ],
        )
        # Run twice — same seed, same day → same results
        rows1 = SimulationEngine(config).generate()
        rows2 = SimulationEngine(config).generate()

        for r1, r2 in zip(rows1, rows2):
            assert r1["occupancy"] == r2["occupancy"]

    def test_volatility_respects_min_max(self):
        """Volatility shifts should still be clamped to [min, max]."""
        config = self._make_config(
            profile={"08:00": 5},  # Near min, high volatility could push below
            min=0,
            max=100,
            volatility=20.0,
            noise=0.0,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            assert 0 <= row["occupancy"] <= 100

    def test_single_anchor_point(self):
        """Single anchor point should produce constant value."""
        config = self._make_config(
            profile={"12:00": 50},
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        for row in rows:
            assert row["occupancy"] == pytest.approx(50.0, abs=1)

    def test_wrapping_across_midnight(self):
        """Profile should wrap smoothly across midnight."""
        config = self._make_config(
            profile={"22:00": 10, "06:00": 10},
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # At 00:00 (between 22:00=10 and 06:00=10), should be ~10
        assert rows[0]["occupancy"] == pytest.approx(10.0, abs=1)


class TestDerivedTimestamp:
    """Tests for _timestamp access in derived expressions."""

    def test_timestamp_accessible_in_derived(self):
        """Derived expressions should have access to _timestamp."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T08:00:00Z",
                timestep="1h",
                row_count=3,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="test_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="hour",
                    data_type=SimulationDataType.INT,
                    generator={
                        "type": "derived",
                        "expression": "_timestamp.hour",
                    },
                ),
            ],
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        assert rows[0]["hour"] == 8
        assert rows[1]["hour"] == 9
        assert rows[2]["hour"] == 10

    def test_timestamp_used_for_time_based_logic(self):
        """Derived expressions can use _timestamp for time-of-day logic."""
        config = SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-05T06:00:00Z",
                timestep="6h",
                row_count=4,
                seed=42,
            ),
            entities=EntityConfig(count=1, id_prefix="test_"),
            columns=[
                ColumnGeneratorConfig(
                    name="timestamp",
                    data_type=SimulationDataType.TIMESTAMP,
                    generator=TimestampGeneratorConfig(type="timestamp"),
                ),
                ColumnGeneratorConfig(
                    name="shift",
                    data_type=SimulationDataType.STRING,
                    generator={
                        "type": "derived",
                        "expression": "'day' if 6 <= _timestamp.hour < 18 else 'night'",
                    },
                ),
            ],
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # 06:00 → day, 12:00 → day, 18:00 → night, 00:00 → night
        assert rows[0]["shift"] == "day"
        assert rows[1]["shift"] == "day"
        assert rows[2]["shift"] == "night"
        assert rows[3]["shift"] == "night"
