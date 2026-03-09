"""Tests for random walk simulation generator."""

import warnings

import pytest

from odibi.config import (
    ColumnGeneratorConfig,
    ConstantGeneratorConfig,
    EntityConfig,
    RandomWalkGeneratorConfig,
    SimulationConfig,
    SimulationDataType,
    SimulationScope,
    TimestampGeneratorConfig,
)
from odibi.simulation import SimulationEngine


class TestRandomWalkConfig:
    """Tests for random walk configuration validation."""

    def test_min_must_be_less_than_max(self):
        """min must be less than max."""
        with pytest.raises(ValueError, match="min.*max"):
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=100, max=50)

    def test_start_must_be_in_range(self):
        """start must be between min and max."""
        with pytest.raises(ValueError, match="start.*between"):
            RandomWalkGeneratorConfig(type="random_walk", start=500, min=0, max=100)

    def test_valid_config(self):
        """Valid config should parse."""
        config = RandomWalkGeneratorConfig(
            type="random_walk",
            start=50.0,
            min=0.0,
            max=100.0,
            volatility=1.0,
            mean_reversion=0.1,
            trend=0.001,
            precision=2,
        )
        assert config.start == 50.0
        assert config.volatility == 1.0
        assert config.mean_reversion == 0.1
        assert config.trend == 0.001
        assert config.precision == 2

    def test_defaults(self):
        """Default values should be sensible."""
        config = RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100)
        assert config.volatility == 1.0
        assert config.mean_reversion == 0.0
        assert config.trend == 0.0
        assert config.precision is None

    def test_shock_rate_defaults(self):
        """Shock parameters should have sensible defaults."""
        config = RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100)
        assert config.shock_rate == 0.0
        assert config.shock_magnitude == 10.0
        assert config.shock_bias == 0.0

    def test_shock_rate_validation(self):
        """shock_rate must be between 0.0 and 1.0."""
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100, shock_rate=-0.1)
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100, shock_rate=1.1)

    def test_shock_bias_validation(self):
        """shock_bias must be between -1.0 and 1.0."""
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100, shock_bias=-1.5)
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100, shock_bias=1.5)

    def test_shock_magnitude_must_be_positive(self):
        """shock_magnitude must be greater than 0."""
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(
                type="random_walk", start=50, min=0, max=100, shock_magnitude=0.0
            )
        with pytest.raises(ValueError):
            RandomWalkGeneratorConfig(
                type="random_walk", start=50, min=0, max=100, shock_magnitude=-5.0
            )

    def test_shock_without_mean_reversion_warns(self):
        """Warning should be issued when shock_rate > 0 and mean_reversion == 0."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            RandomWalkGeneratorConfig(type="random_walk", start=50, min=0, max=100, shock_rate=0.5)
            assert len(w) == 1
            assert "shock_rate" in str(w[0].message)
            assert "mean_reversion" in str(w[0].message)


class TestRandomWalkGeneration:
    """Tests for random walk data generation."""

    def _make_config(self, rw_config, row_count=100, seed=42, entity_count=1):
        """Helper to build a SimulationConfig with a random walk column."""
        return SimulationConfig(
            scope=SimulationScope(
                start_time="2026-01-01T00:00:00Z",
                timestep="5m",
                row_count=row_count,
                seed=seed,
            ),
            entities=EntityConfig(count=entity_count, id_prefix="sensor_"),
            columns=[
                ColumnGeneratorConfig(
                    name="sensor_id",
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
                    generator=rw_config,
                ),
            ],
        )

    def test_values_stay_within_bounds(self):
        """All generated values must be within min/max bounds."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk", start=50.0, min=0.0, max=100.0, volatility=5.0
            ),
            row_count=500,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        temps = [r["temperature"] for r in rows]
        assert all(0.0 <= t <= 100.0 for t in temps), f"Out of bounds: {min(temps)}-{max(temps)}"

    def test_values_are_smooth(self):
        """Consecutive values should be close (not random jumps)."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk", start=50.0, min=0.0, max=100.0, volatility=0.5
            ),
            row_count=100,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        temps = [r["temperature"] for r in rows]
        # Check that average step size is small relative to range
        steps = [abs(temps[i + 1] - temps[i]) for i in range(len(temps) - 1)]
        avg_step = sum(steps) / len(steps)
        assert avg_step < 5.0, f"Average step {avg_step} too large for volatility=0.5"

    def test_mean_reversion_pulls_toward_start(self):
        """With strong mean reversion, values should stay near start."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=1.0,
                mean_reversion=0.5,
            ),
            row_count=500,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        temps = [r["temperature"] for r in rows]
        avg = sum(temps) / len(temps)
        # With strong mean reversion, average should be close to start (50)
        assert 40.0 < avg < 60.0, f"Average {avg} not near start=50 with mean_reversion=0.5"

    def test_trend_shifts_values(self):
        """Positive trend should cause values to drift upward over time."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.1,
                trend=0.1,
            ),
            row_count=200,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        temps = [r["temperature"] for r in rows]
        # Last values should be higher than first values on average
        first_quarter = sum(temps[:50]) / 50
        last_quarter = sum(temps[-50:]) / 50
        assert last_quarter > first_quarter, (
            f"Trend not visible: first_quarter={first_quarter}, last_quarter={last_quarter}"
        )

    def test_precision_rounding(self):
        """Precision parameter should round values."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=1.0,
                precision=1,
            ),
            row_count=50,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        temps = [r["temperature"] for r in rows]
        for t in temps:
            assert t == round(t, 1), f"Value {t} not rounded to 1 decimal"

    def test_deterministic_with_seed(self):
        """Same seed should produce identical results."""
        rw_config = RandomWalkGeneratorConfig(
            type="random_walk", start=50.0, min=0.0, max=100.0, volatility=1.0
        )
        config = self._make_config(rw_config, seed=42)

        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()

        engine2 = SimulationEngine(config)
        rows2 = engine2.generate()

        temps1 = [r["temperature"] for r in rows1]
        temps2 = [r["temperature"] for r in rows2]
        assert temps1 == temps2

    def test_multiple_entities_independent(self):
        """Each entity should have independent random walk state."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk", start=50.0, min=0.0, max=100.0, volatility=2.0
            ),
            row_count=50,
            entity_count=3,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()

        # Group by entity
        entities = {}
        for row in rows:
            eid = row["sensor_id"]
            entities.setdefault(eid, []).append(row["temperature"])

        # Each entity should have different values (extremely unlikely to be identical)
        values = list(entities.values())
        assert len(values) == 3
        assert values[0] != values[1]

    def test_get_random_walk_final_state(self):
        """Final state should capture last value per entity per column."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk", start=50.0, min=0.0, max=100.0, volatility=1.0
            ),
            row_count=10,
            entity_count=2,
        )
        engine = SimulationEngine(config)
        rows = engine.generate()
        state = engine.get_random_walk_final_state(rows)

        assert len(state) == 2  # 2 entities
        for entity_name, cols in state.items():
            assert "temperature" in cols
            assert isinstance(cols["temperature"], float)

    def test_incremental_continuity(self):
        """Second run with state should continue from where first run left off."""
        rw_config = RandomWalkGeneratorConfig(
            type="random_walk",
            start=50.0,
            min=0.0,
            max=100.0,
            volatility=0.5,
            mean_reversion=0.1,
        )
        config = self._make_config(rw_config, row_count=10, entity_count=1)

        # Run 1
        engine1 = SimulationEngine(config)
        rows1 = engine1.generate()
        max_ts = engine1.get_max_timestamp(rows1)
        rw_state = engine1.get_random_walk_final_state(rows1)
        last_temp_run1 = rows1[-1]["temperature"]

        # Run 2: with HWM and random walk state
        engine2 = SimulationEngine(config, hwm_timestamp=max_ts, random_walk_state=rw_state)
        rows2 = engine2.generate()
        first_temp_run2 = rows2[0]["temperature"]

        # The first value of run 2 should be close to the last value of run 1
        # (not jumping back to start=50)
        diff = abs(first_temp_run2 - last_temp_run1)
        assert diff < 5.0, (
            f"Continuity broken: run1 ended at {last_temp_run1}, "
            f"run2 started at {first_temp_run2} (diff={diff})"
        )

    def test_shock_creates_large_jumps(self):
        """With shock_rate=1.0 and high magnitude, steps should be much larger."""
        config_shock = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.1,
                shock_rate=1.0,
                shock_magnitude=20.0,
                mean_reversion=0.1,
            ),
            row_count=500,
            seed=42,
        )
        config_no_shock = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.1,
                shock_rate=0.0,
            ),
            row_count=500,
            seed=42,
        )
        rows_shock = SimulationEngine(config_shock).generate()
        rows_no_shock = SimulationEngine(config_no_shock).generate()

        temps_shock = [r["temperature"] for r in rows_shock]
        temps_no_shock = [r["temperature"] for r in rows_no_shock]

        avg_step_shock = sum(
            abs(temps_shock[i + 1] - temps_shock[i]) for i in range(len(temps_shock) - 1)
        ) / (len(temps_shock) - 1)
        avg_step_no_shock = sum(
            abs(temps_no_shock[i + 1] - temps_no_shock[i]) for i in range(len(temps_no_shock) - 1)
        ) / (len(temps_no_shock) - 1)

        assert avg_step_shock > avg_step_no_shock * 2, (
            f"Shocks should create larger steps: shock={avg_step_shock}, "
            f"no_shock={avg_step_no_shock}"
        )

    def test_shock_with_mean_reversion_recovers(self):
        """With mean_reversion, values should cluster around start despite shocks."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.5,
                shock_rate=0.5,
                shock_magnitude=30.0,
                mean_reversion=0.3,
            ),
            row_count=1000,
            seed=42,
        )
        rows = SimulationEngine(config).generate()
        temps = [r["temperature"] for r in rows]
        avg = sum(temps) / len(temps)
        assert 25.0 < avg < 75.0, (
            f"Mean {avg} not within reasonable range of start=50 with mean_reversion=0.3"
        )

    def test_shock_bias_positive(self):
        """With shock_bias=1.0, shocks always go up so average should be above start."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.1,
                shock_rate=1.0,
                shock_magnitude=5.0,
                shock_bias=1.0,
                mean_reversion=0.05,
            ),
            row_count=200,
            seed=42,
        )
        rows = SimulationEngine(config).generate()
        temps = [r["temperature"] for r in rows]
        avg = sum(temps) / len(temps)
        assert avg > 50.0, f"Positive bias should push average above start=50, got {avg}"

    def test_shock_bias_negative(self):
        """With shock_bias=-1.0, shocks always go down so average should be below start."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=0.1,
                shock_rate=1.0,
                shock_magnitude=5.0,
                shock_bias=-1.0,
                mean_reversion=0.05,
            ),
            row_count=200,
            seed=42,
        )
        rows = SimulationEngine(config).generate()
        temps = [r["temperature"] for r in rows]
        avg = sum(temps) / len(temps)
        assert avg < 50.0, f"Negative bias should push average below start=50, got {avg}"

    def test_shock_respects_bounds(self):
        """Even with extreme shocks, values must stay within min/max bounds."""
        config = self._make_config(
            RandomWalkGeneratorConfig(
                type="random_walk",
                start=50.0,
                min=0.0,
                max=100.0,
                volatility=1.0,
                shock_rate=1.0,
                shock_magnitude=50.0,
                mean_reversion=0.1,
            ),
            row_count=500,
            seed=42,
        )
        rows = SimulationEngine(config).generate()
        temps = [r["temperature"] for r in rows]
        assert all(0.0 <= t <= 100.0 for t in temps), (
            f"Out of bounds: min={min(temps)}, max={max(temps)}"
        )

    def test_shock_zero_rate_no_effect(self):
        """With shock_rate=0.0, shock_magnitude should have no effect on output."""
        rw_no_shock = RandomWalkGeneratorConfig(
            type="random_walk", start=50.0, min=0.0, max=100.0, volatility=1.0
        )
        rw_shock_zero_rate = RandomWalkGeneratorConfig(
            type="random_walk",
            start=50.0,
            min=0.0,
            max=100.0,
            volatility=1.0,
            shock_rate=0.0,
            shock_magnitude=100.0,
        )
        config1 = self._make_config(rw_no_shock, seed=42)
        config2 = self._make_config(rw_shock_zero_rate, seed=42)

        rows1 = SimulationEngine(config1).generate()
        rows2 = SimulationEngine(config2).generate()

        temps1 = [r["temperature"] for r in rows1]
        temps2 = [r["temperature"] for r in rows2]
        assert temps1 == temps2
