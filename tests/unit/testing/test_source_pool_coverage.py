"""Comprehensive tests for odibi.testing.source_pool models and enums."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from odibi.testing.source_pool import (
    ColumnSchema,
    DataCharacteristics,
    DataQuality,
    FileFormat,
    IntegrityManifest,
    PoolStatus,
    SourcePoolConfig,
    SourcePoolIndex,
    SourceType,
    TableSchema,
)


# ============================================
# Helper factories
# ============================================


def _make_columns(*names: str, pk_names: tuple[str, ...] = ()) -> list[ColumnSchema]:
    return [ColumnSchema(name=n, dtype="string", primary_key=(n in pk_names)) for n in names]


def _make_table_schema(
    col_names: tuple[str, ...] = ("id", "name"),
    pk_names: tuple[str, ...] | None = None,
    partition_columns: list[str] | None = None,
) -> TableSchema:
    pks = list(pk_names) if pk_names else None
    return TableSchema(
        columns=_make_columns(*col_names, pk_names=pk_names or ()),
        primary_keys=pks,
        partition_columns=partition_columns,
    )


def _make_characteristics(**overrides) -> DataCharacteristics:
    defaults = {"row_count": 100}
    defaults.update(overrides)
    return DataCharacteristics(**defaults)


def _make_integrity(**overrides) -> IntegrityManifest:
    defaults = {
        "file_hashes": {"data/part-00000.parquet": "abc123"},
        "manifest_hash": "deadbeef",
        "frozen_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return IntegrityManifest(**defaults)


def _make_pool_config(**overrides) -> SourcePoolConfig:
    defaults = {
        "pool_id": "test_pool",
        "name": "Test Pool",
        "description": "A test pool",
        "file_format": FileFormat.CSV,
        "source_type": SourceType.LOCAL,
        "data_quality": DataQuality.CLEAN,
        "schema": _make_table_schema(),
        "cache_path": "test/csv/clean/",
        "characteristics": _make_characteristics(),
    }
    defaults.update(overrides)
    return SourcePoolConfig(**defaults)


# ============================================
# Enum Tests
# ============================================


class TestFileFormat:
    def test_values(self):
        expected = {"csv", "json", "parquet", "avro", "delta"}
        assert {f.value for f in FileFormat} == expected

    def test_string_access(self):
        assert FileFormat("csv") is FileFormat.CSV
        assert FileFormat("delta") is FileFormat.DELTA


class TestSourceType:
    def test_values(self):
        expected = {"local", "adls_emulated", "azure_blob_emulated", "sql_jdbc_local", "cloudfiles"}
        assert {s.value for s in SourceType} == expected

    def test_string_access(self):
        assert SourceType("local") is SourceType.LOCAL


class TestDataQuality:
    def test_values(self):
        expected = {"clean", "messy", "mixed"}
        assert {q.value for q in DataQuality} == expected


class TestPoolStatus:
    def test_values(self):
        expected = {"draft", "frozen", "deprecated"}
        assert {s.value for s in PoolStatus} == expected

    def test_members(self):
        assert PoolStatus.DRAFT.value == "draft"
        assert PoolStatus.FROZEN.value == "frozen"
        assert PoolStatus.DEPRECATED.value == "deprecated"


# ============================================
# ColumnSchema Tests
# ============================================


class TestColumnSchema:
    def test_basic_creation_with_defaults(self):
        col = ColumnSchema(name="id", dtype="int64")
        assert col.name == "id"
        assert col.dtype == "int64"
        assert col.nullable is False
        assert col.primary_key is False
        assert col.description is None
        assert col.sample_values is None

    def test_all_fields_populated(self):
        col = ColumnSchema(
            name="amount",
            dtype="float64",
            nullable=True,
            primary_key=True,
            description="Transaction amount",
            sample_values=[1.5, 2.0, 3.99],
        )
        assert col.name == "amount"
        assert col.dtype == "float64"
        assert col.nullable is True
        assert col.primary_key is True
        assert col.description == "Transaction amount"
        assert col.sample_values == [1.5, 2.0, 3.99]

    def test_primary_key_defaults_to_false(self):
        col = ColumnSchema(name="x", dtype="string")
        assert col.primary_key is False


# ============================================
# TableSchema Tests
# ============================================


class TestTableSchema:
    def test_basic_creation(self):
        schema = _make_table_schema()
        assert len(schema.columns) == 2
        assert schema.primary_keys is None
        assert schema.partition_columns is None

    def test_valid_primary_keys(self):
        schema = _make_table_schema(col_names=("id", "name"), pk_names=("id",))
        assert schema.primary_keys == ["id"]

    def test_invalid_primary_key_raises(self):
        with pytest.raises(ValueError, match="Primary key column 'missing' not in schema"):
            TableSchema(
                columns=_make_columns("id", "name"),
                primary_keys=["missing"],
            )

    def test_partition_columns_optional(self):
        schema = _make_table_schema(
            col_names=("id", "date"),
            partition_columns=["date"],
        )
        assert schema.partition_columns == ["date"]

    def test_multiple_primary_keys(self):
        schema = _make_table_schema(
            col_names=("tenant_id", "record_id", "value"),
            pk_names=("tenant_id", "record_id"),
        )
        assert schema.primary_keys == ["tenant_id", "record_id"]


# ============================================
# DataCharacteristics Tests
# ============================================


class TestDataCharacteristics:
    def test_basic_creation_with_defaults(self):
        dc = _make_characteristics()
        assert dc.row_count == 100
        assert dc.has_nulls is False
        assert dc.has_duplicates is False
        assert dc.has_unicode is False
        assert dc.has_special_chars is False
        assert dc.has_empty_strings is False
        assert dc.has_whitespace_issues is False
        assert dc.has_type_coercion_cases is False
        assert dc.date_range is None
        assert dc.numeric_ranges is None

    def test_row_count_must_be_non_negative(self):
        with pytest.raises(ValueError):
            DataCharacteristics(row_count=-1)

    def test_row_count_zero_is_valid(self):
        dc = DataCharacteristics(row_count=0)
        assert dc.row_count == 0

    def test_date_range_optional(self):
        dc = _make_characteristics(date_range={"min": "2024-01-01", "max": "2024-12-31"})
        assert dc.date_range == {"min": "2024-01-01", "max": "2024-12-31"}

    def test_numeric_ranges_optional(self):
        dc = _make_characteristics(numeric_ranges={"amount": {"min": 0.0, "max": 999.99}})
        assert dc.numeric_ranges == {"amount": {"min": 0.0, "max": 999.99}}


# ============================================
# IntegrityManifest Tests
# ============================================


class TestIntegrityManifest:
    def test_algorithm_is_sha256(self):
        manifest = _make_integrity()
        assert manifest.algorithm == "sha256"

    def test_basic_creation(self):
        manifest = _make_integrity(frozen_by="ci_bot")
        assert manifest.file_hashes == {"data/part-00000.parquet": "abc123"}
        assert manifest.manifest_hash == "deadbeef"
        assert manifest.frozen_by == "ci_bot"
        assert manifest.frozen_at == datetime(2025, 1, 1, tzinfo=timezone.utc)

    def test_frozen_by_defaults_to_system(self):
        manifest = _make_integrity()
        assert manifest.frozen_by == "system"


# ============================================
# SourcePoolConfig Tests
# ============================================


class TestSourcePoolConfig:
    def test_basic_valid_creation(self):
        pool = _make_pool_config()
        assert pool.pool_id == "test_pool"
        assert pool.status == PoolStatus.DRAFT
        assert pool.integrity is None
        assert pool.version == "1.0.0"

    def test_frozen_without_integrity_raises(self):
        with pytest.raises(ValueError, match="frozen status requires integrity manifest"):
            _make_pool_config(status=PoolStatus.FROZEN, integrity=None)

    def test_frozen_with_integrity_succeeds(self):
        pool = _make_pool_config(
            status=PoolStatus.FROZEN,
            integrity=_make_integrity(),
        )
        assert pool.status == PoolStatus.FROZEN
        assert pool.integrity is not None
        assert pool.integrity.algorithm == "sha256"

    def test_cache_path_relative_valid(self):
        pool = _make_pool_config(cache_path="nyc_taxi/csv/clean/")
        assert pool.cache_path == "nyc_taxi/csv/clean/"

    def test_cache_path_absolute_raises(self):
        with pytest.raises(ValueError, match="cache_path must be relative"):
            _make_pool_config(cache_path="/absolute/path")

    def test_cache_path_backslash_absolute_raises(self):
        with pytest.raises(ValueError, match="cache_path must be relative"):
            _make_pool_config(cache_path="\\absolute\\path")

    def test_cache_path_dotdot_raises(self):
        with pytest.raises(ValueError, match="cache_path must be relative"):
            _make_pool_config(cache_path="some/../escape")

    def test_cache_path_backslashes_converted(self):
        pool = _make_pool_config(cache_path="nyc_taxi\\csv\\clean")
        assert pool.cache_path == "nyc_taxi/csv/clean"

    def test_pool_id_valid_patterns(self):
        for valid_id in ("a", "abc", "a1", "test_pool_v2", "x0_y1_z2"):
            pool = _make_pool_config(pool_id=valid_id)
            assert pool.pool_id == valid_id

    def test_pool_id_starts_with_uppercase_raises(self):
        with pytest.raises(ValueError):
            _make_pool_config(pool_id="Abc")

    def test_pool_id_starts_with_digit_raises(self):
        with pytest.raises(ValueError):
            _make_pool_config(pool_id="1abc")

    def test_pool_id_special_chars_raises(self):
        with pytest.raises(ValueError):
            _make_pool_config(pool_id="test-pool")

    def test_deprecated_status_no_integrity_ok(self):
        pool = _make_pool_config(status=PoolStatus.DEPRECATED)
        assert pool.status == PoolStatus.DEPRECATED

    def test_tests_coverage_default_empty(self):
        pool = _make_pool_config()
        assert pool.tests_coverage == []

    def test_compatible_pipelines_default_empty(self):
        pool = _make_pool_config()
        assert pool.compatible_pipelines == []


# ============================================
# SourcePoolIndex Tests
# ============================================


class TestSourcePoolIndex:
    def test_basic_creation(self):
        index = SourcePoolIndex()
        assert index.version == "1.0.0"
        assert index.pools == {}

    def test_add_pool(self):
        index = SourcePoolIndex()
        before = index.updated_at
        index.add_pool("taxi_csv", "pools/taxi_csv.yaml")
        assert index.pools["taxi_csv"] == "pools/taxi_csv.yaml"
        assert index.updated_at >= before

    def test_add_pool_overwrites_existing(self):
        index = SourcePoolIndex()
        index.add_pool("taxi_csv", "pools/taxi_csv_v1.yaml")
        index.add_pool("taxi_csv", "pools/taxi_csv_v2.yaml")
        assert index.pools["taxi_csv"] == "pools/taxi_csv_v2.yaml"
        assert len(index.pools) == 1

    def test_remove_pool(self):
        index = SourcePoolIndex()
        index.add_pool("taxi_csv", "pools/taxi_csv.yaml")
        before = index.updated_at
        index.remove_pool("taxi_csv")
        assert "taxi_csv" not in index.pools
        assert index.updated_at >= before

    def test_remove_pool_nonexistent_is_noop(self):
        index = SourcePoolIndex()
        before = index.updated_at
        index.remove_pool("nonexistent")
        assert index.pools == {}
        assert index.updated_at == before

    def test_add_multiple_pools(self):
        index = SourcePoolIndex()
        index.add_pool("pool_a", "a.yaml")
        index.add_pool("pool_b", "b.yaml")
        assert len(index.pools) == 2
        assert index.pools["pool_a"] == "a.yaml"
        assert index.pools["pool_b"] == "b.yaml"
