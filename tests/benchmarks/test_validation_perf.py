"""
Performance benchmarks for validation engine.

Run with: pytest tests/benchmarks/test_validation_perf.py -v --benchmark-only

These benchmarks test:
1. Baseline validation with many contracts (10+) on large DataFrames (100K+ rows)
2. Fail-fast mode performance gains
3. Polars lazy vs eager performance
4. Quarantine split performance
5. Memory efficiency of optimizations

Benchmark naming convention:
- test_bench_<engine>_<scenario>_<row_count>
"""

import time
from typing import Any, List

import numpy as np
import pandas as pd
import pytest

from odibi.config import (
    AcceptedValuesTest,
    ContractSeverity,
    NotNullTest,
    RangeTest,
    RegexMatchTest,
    RowCountTest,
    TestType,
    UniqueTest,
    ValidationConfig,
)
from odibi.validation.engine import Validator
from odibi.validation.quarantine import split_valid_invalid


def generate_pandas_df(n_rows: int, seed: int = 42) -> pd.DataFrame:
    """Generate a test DataFrame with realistic data patterns."""
    np.random.seed(seed)

    statuses = ["active", "inactive", "pending", "deleted", "archived"]
    categories = ["A", "B", "C", "D", "E", "F", "G", "H"]

    df = pd.DataFrame(
        {
            "id": range(1, n_rows + 1),
            "customer_id": np.random.randint(1, 10000, n_rows),
            "name": [f"Customer_{i}" for i in range(n_rows)],
            "email": [f"user_{i}@example.com" if i % 10 != 0 else "invalid" for i in range(n_rows)],
            "age": np.random.randint(18, 80, n_rows),
            "status": np.random.choice(statuses, n_rows),
            "category": np.random.choice(categories, n_rows),
            "amount": np.random.uniform(0, 1000, n_rows),
            "created_at": pd.date_range("2020-01-01", periods=n_rows, freq="min"),
            "score": np.random.uniform(0, 100, n_rows),
        }
    )

    null_indices = np.random.choice(n_rows, size=n_rows // 100, replace=False)
    df.loc[null_indices, "name"] = None
    df.loc[null_indices[:10], "customer_id"] = None

    df.loc[np.random.choice(n_rows, 5), "age"] = 150
    df.loc[np.random.choice(n_rows, 5), "amount"] = -100

    return df


def create_many_contracts(n_tests: int = 10) -> List[Any]:
    """Create a realistic set of validation contracts."""
    contracts = [
        NotNullTest(type=TestType.NOT_NULL, columns=["id", "customer_id"]),
        NotNullTest(type=TestType.NOT_NULL, columns=["name", "email"]),
        UniqueTest(type=TestType.UNIQUE, columns=["id"]),
        AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="status",
            values=["active", "inactive", "pending"],
        ),
        AcceptedValuesTest(
            type=TestType.ACCEPTED_VALUES,
            column="category",
            values=["A", "B", "C", "D", "E"],
        ),
        RangeTest(type=TestType.RANGE, column="age", min=0, max=120),
        RangeTest(type=TestType.RANGE, column="amount", min=0, max=10000),
        RangeTest(type=TestType.RANGE, column="score", min=0, max=100),
        RegexMatchTest(
            type=TestType.REGEX_MATCH,
            column="email",
            pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        ),
        RowCountTest(type=TestType.ROW_COUNT, min=1),
    ]

    if n_tests > 10:
        for i in range(n_tests - 10):
            contracts.append(
                RangeTest(
                    type=TestType.RANGE,
                    column="score",
                    min=i,
                    max=100,
                    name=f"score_range_{i}",
                )
            )

    return contracts[:n_tests]


class MockEngine:
    """Mock engine for testing quarantine without full engine setup."""

    def count_rows(self, df: Any) -> int:
        return len(df)


class TestValidationPerformance:
    """Performance benchmarks for validation engine."""

    @pytest.fixture
    def validator(self):
        return Validator()

    @pytest.fixture
    def small_df(self):
        """10K rows - quick tests"""
        return generate_pandas_df(10_000)

    @pytest.fixture
    def medium_df(self):
        """100K rows - standard benchmarks"""
        return generate_pandas_df(100_000)

    @pytest.fixture
    def large_df(self):
        """500K rows - stress tests"""
        return generate_pandas_df(500_000)

    def test_bench_pandas_10_contracts_100k_rows(self, validator, medium_df):
        """Benchmark: 10 contracts on 100K rows with Pandas."""
        contracts = create_many_contracts(10)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures = validator.validate(medium_df, config)
        elapsed = time.perf_counter() - start

        print(f"\n[Pandas 100K/10 contracts] Time: {elapsed:.3f}s, Failures: {len(failures)}")
        assert elapsed < 10.0, f"Validation took too long: {elapsed:.2f}s"

    def test_bench_pandas_15_contracts_100k_rows(self, validator, medium_df):
        """Benchmark: 15 contracts on 100K rows with Pandas."""
        contracts = create_many_contracts(15)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures = validator.validate(medium_df, config)
        elapsed = time.perf_counter() - start

        print(f"\n[Pandas 100K/15 contracts] Time: {elapsed:.3f}s, Failures: {len(failures)}")
        assert elapsed < 15.0, f"Validation took too long: {elapsed:.2f}s"

    def test_bench_pandas_fail_fast_vs_full(self, validator, medium_df):
        """Benchmark: Compare fail-fast vs full validation."""
        contracts = create_many_contracts(10)

        config_full = ValidationConfig(tests=contracts, fail_fast=False)
        start = time.perf_counter()
        failures_full = validator.validate(medium_df, config_full)
        elapsed_full = time.perf_counter() - start

        config_fast = ValidationConfig(tests=contracts, fail_fast=True)
        start = time.perf_counter()
        failures_fast = validator.validate(medium_df, config_fast)
        elapsed_fast = time.perf_counter() - start

        print("\n[Pandas 100K fail-fast comparison]")
        print(f"  Full: {elapsed_full:.3f}s, {len(failures_full)} failures")
        print(f"  Fast: {elapsed_fast:.3f}s, {len(failures_fast)} failures")
        print(f"  Speedup: {elapsed_full / elapsed_fast:.2f}x" if elapsed_fast > 0 else "  N/A")

        assert len(failures_fast) >= 1, "Fail-fast should capture at least one failure"
        assert len(failures_fast) <= len(failures_full), "Fail-fast should not find more failures"

    def test_bench_pandas_quarantine_split_100k(self, medium_df):
        """Benchmark: Quarantine split on 100K rows."""
        contracts = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["name"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
            RangeTest(
                type=TestType.RANGE,
                column="age",
                min=0,
                max=120,
                on_fail=ContractSeverity.QUARANTINE,
            ),
            AcceptedValuesTest(
                type=TestType.ACCEPTED_VALUES,
                column="status",
                values=["active", "inactive", "pending"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]

        engine = MockEngine()

        start = time.perf_counter()
        result = split_valid_invalid(medium_df, contracts, engine)
        elapsed = time.perf_counter() - start

        print(f"\n[Pandas 100K quarantine split] Time: {elapsed:.3f}s")
        print(f"  Valid: {result.rows_valid}, Quarantined: {result.rows_quarantined}")
        assert elapsed < 5.0, f"Quarantine split took too long: {elapsed:.2f}s"

    def test_bench_pandas_500k_rows(self, validator, large_df):
        """Benchmark: 10 contracts on 500K rows with Pandas."""
        contracts = create_many_contracts(10)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures = validator.validate(large_df, config)
        elapsed = time.perf_counter() - start

        print(f"\n[Pandas 500K/10 contracts] Time: {elapsed:.3f}s, Failures: {len(failures)}")
        assert elapsed < 30.0, f"Validation took too long: {elapsed:.2f}s"


class TestPolarsValidationPerformance:
    """Performance benchmarks for Polars validation engine."""

    @pytest.fixture
    def validator(self):
        return Validator()

    @pytest.fixture
    def medium_df(self):
        """100K rows as Polars DataFrame"""
        try:
            import polars as pl

            pdf = generate_pandas_df(100_000)
            return pl.from_pandas(pdf)
        except ImportError:
            pytest.skip("Polars not installed")

    @pytest.fixture
    def medium_lazy_df(self):
        """100K rows as Polars LazyFrame"""
        try:
            import polars as pl

            pdf = generate_pandas_df(100_000)
            return pl.from_pandas(pdf).lazy()
        except ImportError:
            pytest.skip("Polars not installed")

    def test_bench_polars_eager_10_contracts(self, validator, medium_df):
        """Benchmark: 10 contracts on 100K rows with Polars (eager)."""
        contracts = create_many_contracts(10)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures = validator.validate(medium_df, config)
        elapsed = time.perf_counter() - start

        print(f"\n[Polars eager 100K/10 contracts] Time: {elapsed:.3f}s, Failures: {len(failures)}")
        assert elapsed < 10.0, f"Validation took too long: {elapsed:.2f}s"

    def test_bench_polars_lazy_10_contracts(self, validator, medium_lazy_df):
        """Benchmark: 10 contracts on 100K rows with Polars (lazy)."""
        contracts = create_many_contracts(10)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures = validator.validate(medium_lazy_df, config)
        elapsed = time.perf_counter() - start

        print(f"\n[Polars lazy 100K/10 contracts] Time: {elapsed:.3f}s, Failures: {len(failures)}")
        assert elapsed < 10.0, f"Validation took too long: {elapsed:.2f}s"

    def test_bench_polars_lazy_vs_eager_comparison(self, validator, medium_df, medium_lazy_df):
        """Benchmark: Compare lazy vs eager Polars performance."""
        contracts = create_many_contracts(10)
        config = ValidationConfig(tests=contracts)

        start = time.perf_counter()
        failures_eager = validator.validate(medium_df, config)
        elapsed_eager = time.perf_counter() - start

        start = time.perf_counter()
        failures_lazy = validator.validate(medium_lazy_df, config)
        elapsed_lazy = time.perf_counter() - start

        print("\n[Polars 100K eager vs lazy]")
        print(f"  Eager: {elapsed_eager:.3f}s, {len(failures_eager)} failures")
        print(f"  Lazy:  {elapsed_lazy:.3f}s, {len(failures_lazy)} failures")

        assert len(failures_eager) == len(failures_lazy), "Should find same failures"


class TestQuarantinePerformance:
    """Performance benchmarks for quarantine operations."""

    @pytest.fixture
    def medium_df(self):
        return generate_pandas_df(100_000)

    def test_bench_quarantine_memory_efficiency(self, medium_df):
        """Benchmark: Memory usage of quarantine split."""

        contracts = [
            NotNullTest(
                type=TestType.NOT_NULL,
                columns=["name"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
            RangeTest(
                type=TestType.RANGE,
                column="age",
                min=0,
                max=120,
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]

        engine = MockEngine()

        result = split_valid_invalid(medium_df, contracts, engine)

        has_per_row_lists = any(isinstance(v, list) for v in result.test_results.values())
        assert not has_per_row_lists, "test_results should not contain per-row lists"

        if result.test_results:
            for name, stats in result.test_results.items():
                assert isinstance(stats, dict), f"Expected dict for {name}, got {type(stats)}"
                assert "pass_count" in stats or stats == {}, f"Expected pass_count in {name}"

        print(f"\n[Memory efficiency test] test_results type: {type(result.test_results)}")
        print(f"  test_results content: {result.test_results}")

    def test_bench_quarantine_high_failure_rate(self, medium_df):
        """Benchmark: Quarantine performance with high failure rate (~20%)."""
        contracts = [
            AcceptedValuesTest(
                type=TestType.ACCEPTED_VALUES,
                column="status",
                values=["active", "inactive"],
                on_fail=ContractSeverity.QUARANTINE,
            ),
        ]

        engine = MockEngine()

        start = time.perf_counter()
        result = split_valid_invalid(medium_df, contracts, engine)
        elapsed = time.perf_counter() - start

        failure_rate = result.rows_quarantined / (result.rows_valid + result.rows_quarantined)
        print(f"\n[High failure quarantine] Time: {elapsed:.3f}s")
        print(f"  Failure rate: {failure_rate:.1%}")
        print(f"  Quarantined: {result.rows_quarantined}")

        assert elapsed < 5.0, f"Quarantine took too long: {elapsed:.2f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
