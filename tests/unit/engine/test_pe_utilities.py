"""Unit tests for PandasEngine utility methods."""

import hashlib
from types import SimpleNamespace

import pandas as pd
import pytest

from odibi.config import (
    OnMissingColumns,
    OnNewColumns,
    SchemaMode,
    WriteMetadataConfig,
)
from odibi.engine.pandas_engine import LazyDataset, PandasEngine


@pytest.fixture
def engine():
    return PandasEngine(config={})


# ── add_write_metadata ──────────────────────────────────────────────


class TestAddWriteMetadata:
    def test_true_adds_default_columns(self, engine):
        df = pd.DataFrame({"a": [1, 2]})
        result = engine.add_write_metadata(
            df,
            metadata_config=True,
            source_connection="conn1",
            source_path="/f.csv",
            is_file_source=True,
        )
        assert "_extracted_at" in result.columns
        assert "_source_file" in result.columns
        # source_connection defaults to False in WriteMetadataConfig
        assert "_source_connection" not in result.columns

    def test_config_object(self, engine):
        cfg = WriteMetadataConfig(
            extracted_at=True, source_file=False, source_connection=True, source_table=True
        )
        df = pd.DataFrame({"x": [1]})
        result = engine.add_write_metadata(
            df, metadata_config=cfg, source_connection="myconn", source_table="tbl"
        )
        assert "_extracted_at" in result.columns
        assert "_source_file" not in result.columns
        assert result["_source_connection"].iloc[0] == "myconn"
        assert result["_source_table"].iloc[0] == "tbl"

    def test_none_is_noop(self, engine):
        df = pd.DataFrame({"a": [1]})
        result = engine.add_write_metadata(df, metadata_config=None)
        assert "_extracted_at" not in result.columns
        pd.testing.assert_frame_equal(result, df)

    def test_file_source_adds_source_file(self, engine):
        df = pd.DataFrame({"v": [10]})
        result = engine.add_write_metadata(
            df, metadata_config=True, source_path="/data/file.csv", is_file_source=True
        )
        assert result["_source_file"].iloc[0] == "/data/file.csv"

    def test_sql_source_no_source_file(self, engine):
        df = pd.DataFrame({"v": [10]})
        result = engine.add_write_metadata(
            df, metadata_config=True, source_table="orders", is_file_source=False
        )
        assert "_source_file" not in result.columns

    def test_does_not_mutate_original(self, engine):
        df = pd.DataFrame({"a": [1]})
        engine.add_write_metadata(df, metadata_config=True)
        assert "_extracted_at" not in df.columns


# ── harmonize_schema ────────────────────────────────────────────────


class TestHarmonizeSchema:
    def _policy(self, mode, on_missing, on_new):
        return SimpleNamespace(mode=mode, on_missing_columns=on_missing, on_new_columns=on_new)

    def test_evolve_add_nullable(self, engine):
        df = pd.DataFrame({"a": [1], "extra": [2]})
        target = {"a": "int64", "b": "int64"}
        policy = self._policy(
            SchemaMode.EVOLVE, OnMissingColumns.FILL_NULL, OnNewColumns.ADD_NULLABLE
        )
        result = engine.harmonize_schema(df, target, policy)
        assert "a" in result.columns
        assert "extra" in result.columns  # kept
        assert "b" in result.columns  # added
        assert result["b"].isna().all()

    def test_enforce_reindex(self, engine):
        df = pd.DataFrame({"a": [1], "extra": [2]})
        target = {"a": "int64", "b": "int64"}
        policy = self._policy(SchemaMode.ENFORCE, OnMissingColumns.FILL_NULL, OnNewColumns.IGNORE)
        result = engine.harmonize_schema(df, target, policy)
        assert list(result.columns) == ["a", "b"]
        assert "extra" not in result.columns

    def test_fail_on_missing(self, engine):
        df = pd.DataFrame({"a": [1]})
        target = {"a": "int64", "b": "int64"}
        policy = self._policy(SchemaMode.ENFORCE, OnMissingColumns.FAIL, OnNewColumns.IGNORE)
        with pytest.raises(ValueError, match="Missing columns"):
            engine.harmonize_schema(df, target, policy)

    def test_fail_on_new(self, engine):
        df = pd.DataFrame({"a": [1], "extra": [2]})
        target = {"a": "int64"}
        policy = self._policy(SchemaMode.ENFORCE, OnMissingColumns.FILL_NULL, OnNewColumns.FAIL)
        with pytest.raises(ValueError, match="New columns"):
            engine.harmonize_schema(df, target, policy)


# ── anonymize ───────────────────────────────────────────────────────


class TestAnonymize:
    def test_hash_without_salt(self, engine):
        df = pd.DataFrame({"name": ["alice", "bob"]})
        result = engine.anonymize(df, columns=["name"], method="hash")
        expected_alice = hashlib.sha256("alice".encode()).hexdigest()
        assert result["name"].iloc[0] == expected_alice

    def test_hash_with_salt(self, engine):
        df = pd.DataFrame({"name": ["alice"]})
        result = engine.anonymize(df, columns=["name"], method="hash", salt="s")
        expected = hashlib.sha256("alices".encode()).hexdigest()
        assert result["name"].iloc[0] == expected

    def test_mask(self, engine):
        df = pd.DataFrame({"ssn": ["123456789"]})
        result = engine.anonymize(df, columns=["ssn"], method="mask")
        val = result["ssn"].iloc[0]
        assert val.endswith("6789")
        assert val.startswith("*")

    def test_redact(self, engine):
        df = pd.DataFrame({"email": ["a@b.com"]})
        result = engine.anonymize(df, columns=["email"], method="redact")
        assert result["email"].iloc[0] == "[REDACTED]"

    def test_skip_nonexistent_column(self, engine):
        df = pd.DataFrame({"a": [1]})
        result = engine.anonymize(df, columns=["missing"], method="redact")
        pd.testing.assert_frame_equal(result, df)

    def test_preserves_nulls_hash(self, engine):
        df = pd.DataFrame({"name": ["alice", None]})
        result = engine.anonymize(df, columns=["name"], method="hash")
        assert pd.isna(result["name"].iloc[1])

    def test_preserves_nulls_mask(self, engine):
        df = pd.DataFrame({"name": ["alice", None]})
        result = engine.anonymize(df, columns=["name"], method="mask")
        assert pd.isna(result["name"].iloc[1])


# ── count_nulls ─────────────────────────────────────────────────────


class TestCountNulls:
    def test_valid_columns(self, engine):
        df = pd.DataFrame({"a": [1, None, 3], "b": [None, None, 3]})
        result = engine.count_nulls(df, ["a", "b"])
        assert result == {"a": 1, "b": 2}

    def test_missing_column_raises(self, engine):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Column 'z' not found"):
            engine.count_nulls(df, ["z"])


# ── validate_schema ─────────────────────────────────────────────────


class TestValidateSchema:
    def test_required_columns_missing(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_schema(df, {"required_columns": ["a", "b"]})
        assert any("Missing required columns" in f for f in failures)

    def test_required_columns_pass(self, engine):
        df = pd.DataFrame({"a": [1], "b": [2]})
        failures = engine.validate_schema(df, {"required_columns": ["a", "b"]})
        assert failures == []

    def test_type_validation_pass(self, engine):
        df = pd.DataFrame({"a": [1], "b": [1.0]})
        failures = engine.validate_schema(df, {"types": {"a": "int", "b": "float"}})
        assert failures == []

    def test_type_validation_fail(self, engine):
        df = pd.DataFrame({"a": ["x"]})
        failures = engine.validate_schema(df, {"types": {"a": "int"}})
        assert any("has type" in f for f in failures)

    def test_type_validation_column_missing(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_schema(df, {"types": {"missing_col": "int"}})
        assert any("not found" in f for f in failures)

    def test_pyarrow_type_stripping(self, engine):
        df = pd.DataFrame({"a": pd.array([1, 2], dtype="int64")})
        # Simulate pyarrow-backed type by renaming dtype string
        # Since we can't easily create pyarrow types without pyarrow, test the mapping directly
        failures = engine.validate_schema(df, {"types": {"a": "int"}})
        assert failures == []


# ── _infer_avro_schema ──────────────────────────────────────────────


class TestInferAvroSchema:
    def test_basic_types(self, engine):
        df = pd.DataFrame({"i": [1], "f": [1.0], "s": ["x"], "b": [True]})
        schema = engine._infer_avro_schema(df)
        assert schema["type"] == "record"
        fields = {f["name"]: f["type"] for f in schema["fields"]}
        assert fields["i"] == "long"
        assert fields["f"] == "double"
        assert fields["s"] == "string"
        assert fields["b"] == "boolean"

    def test_datetime_column(self, engine):
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01"])})
        schema = engine._infer_avro_schema(df)
        field = schema["fields"][0]
        assert field["type"]["logicalType"] == "timestamp-micros"

    def test_nullable_column(self, engine):
        df = pd.DataFrame({"a": [1, None]})
        schema = engine._infer_avro_schema(df)
        field = schema["fields"][0]
        assert isinstance(field["type"], list)
        assert "null" in field["type"]


# ── validate_data ───────────────────────────────────────────────────


class TestValidateData:
    def _config(self, **kwargs):
        defaults = {
            "not_empty": False,
            "no_nulls": None,
            "schema_validation": None,
            "ranges": None,
            "allowed_values": None,
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def test_not_empty_pass(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_data(df, self._config(not_empty=True))
        assert failures == []

    def test_not_empty_fail(self, engine):
        df = pd.DataFrame({"a": []})
        failures = engine.validate_data(df, self._config(not_empty=True))
        assert any("empty" in f for f in failures)

    def test_no_nulls_fail(self, engine):
        df = pd.DataFrame({"a": [1, None]})
        failures = engine.validate_data(df, self._config(no_nulls=["a"]))
        assert any("null" in f for f in failures)

    def test_no_nulls_pass(self, engine):
        df = pd.DataFrame({"a": [1, 2]})
        failures = engine.validate_data(df, self._config(no_nulls=["a"]))
        assert failures == []

    def test_schema_validation(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_data(
            df, self._config(schema_validation={"required_columns": ["a", "b"]})
        )
        assert any("Missing" in f for f in failures)

    def test_range_violation(self, engine):
        df = pd.DataFrame({"a": [1, 10, 100]})
        failures = engine.validate_data(df, self._config(ranges={"a": {"min": 0, "max": 50}}))
        assert any("> 50" in f for f in failures)
        assert not any("< 0" in f for f in failures)

    def test_range_missing_col(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_data(df, self._config(ranges={"missing": {"min": 0}}))
        assert any("not found" in f for f in failures)

    def test_allowed_values_violation(self, engine):
        df = pd.DataFrame({"status": ["A", "B", "C"]})
        failures = engine.validate_data(df, self._config(allowed_values={"status": ["A", "B"]}))
        assert any("invalid values" in f for f in failures)

    def test_allowed_values_pass(self, engine):
        df = pd.DataFrame({"status": ["A", "B"]})
        failures = engine.validate_data(df, self._config(allowed_values={"status": ["A", "B"]}))
        assert failures == []

    def test_allowed_values_missing_col(self, engine):
        df = pd.DataFrame({"a": [1]})
        failures = engine.validate_data(df, self._config(allowed_values={"missing": ["x"]}))
        assert any("not found" in f for f in failures)


# ── get_source_files ────────────────────────────────────────────────


class TestGetSourceFiles:
    def test_lazy_dataset_list_path(self, engine):
        ds = LazyDataset(path=["/a.csv", "/b.csv"], format="csv", options={})
        assert engine.get_source_files(ds) == ["/a.csv", "/b.csv"]

    def test_lazy_dataset_single_path(self, engine):
        ds = LazyDataset(path="/single.parquet", format="parquet", options={})
        assert engine.get_source_files(ds) == ["/single.parquet"]

    def test_dataframe_with_attrs(self, engine):
        df = pd.DataFrame({"a": [1]})
        df.attrs["odibi_source_files"] = ["/src.csv"]
        assert engine.get_source_files(df) == ["/src.csv"]

    def test_dataframe_without_attrs(self, engine):
        df = pd.DataFrame({"a": [1]})
        assert engine.get_source_files(df) == []


# ── profile_nulls ──────────────────────────────────────────────────


class TestProfileNulls:
    def test_null_percentage(self, engine):
        df = pd.DataFrame({"a": [1, None, None, 4], "b": [None, None, None, None]})
        result = engine.profile_nulls(df)
        assert result["a"] == pytest.approx(0.5)
        assert result["b"] == pytest.approx(1.0)

    def test_no_nulls(self, engine):
        df = pd.DataFrame({"a": [1, 2]})
        result = engine.profile_nulls(df)
        assert result["a"] == pytest.approx(0.0)


# ── filter_greater_than ────────────────────────────────────────────


class TestFilterGreaterThan:
    def test_numeric_filter(self, engine):
        df = pd.DataFrame({"val": [1, 5, 10]})
        result = engine.filter_greater_than(df, "val", 4)
        assert list(result["val"]) == [5, 10]

    def test_string_to_datetime_cast(self, engine):
        df = pd.DataFrame({"d": ["2024-01-01", "2024-06-01", "2024-12-01"]})
        result = engine.filter_greater_than(df, "d", pd.Timestamp("2024-05-01"))
        assert len(result) == 2

    def test_datetime_column_with_string_value(self, engine):
        df = pd.DataFrame({"d": pd.to_datetime(["2024-01-01", "2024-06-01", "2024-12-01"])})
        result = engine.filter_greater_than(df, "d", "2024-05-01")
        assert len(result) == 2

    def test_missing_column_raises(self, engine):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="not found"):
            engine.filter_greater_than(df, "missing", 0)


# ── filter_coalesce ────────────────────────────────────────────────


class TestFilterCoalesce:
    def test_ge_operator(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", ">=", 5)
        assert list(result["a"]) == [5, 10]

    def test_gt_operator(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", ">", 5)
        assert list(result["a"]) == [10]

    def test_le_operator(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", "<=", 5)
        assert list(result["a"]) == [1, 5]

    def test_lt_operator(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", "<", 5)
        assert list(result["a"]) == [1]

    def test_eq_double_equals(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", "==", 5)
        assert list(result["a"]) == [5]

    def test_eq_single_equals(self, engine):
        df = pd.DataFrame({"a": [1, 5, 10]})
        result = engine.filter_coalesce(df, "a", "b", "=", 5)
        assert list(result["a"]) == [5]

    def test_missing_col2_fallback(self, engine):
        df = pd.DataFrame({"a": [None, 5, None], "val": [1, 2, 3]})
        result = engine.filter_coalesce(df, "a", "missing_col", ">=", 5)
        assert len(result) == 1
        assert result["a"].iloc[0] == 5

    def test_coalesce_with_col2(self, engine):
        df = pd.DataFrame({"a": [None, 5], "b": [10, 20]})
        result = engine.filter_coalesce(df, "a", "b", ">=", 10)
        # row 0: coalesce(None, 10) = 10 >= 10 => True
        # row 1: coalesce(5, 20) = 5 >= 10 => False
        assert len(result) == 1

    def test_unsupported_operator(self, engine):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Unsupported operator"):
            engine.filter_coalesce(df, "a", "b", "!=", 1)

    def test_missing_col1_raises(self, engine):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="not found"):
            engine.filter_coalesce(df, "missing", "a", ">", 0)

    def test_string_to_datetime_cast(self, engine):
        df = pd.DataFrame(
            {
                "d1": ["2024-01-01", None],
                "d2": [None, "2024-06-01"],
            }
        )
        result = engine.filter_coalesce(df, "d1", "d2", ">=", "2024-03-01")
        # row0: coalesce("2024-01-01", None) -> 2024-01-01 >= 2024-03-01 => False
        # row1: coalesce(None, "2024-06-01") -> 2024-06-01 >= 2024-03-01 => True
        assert len(result) == 1
