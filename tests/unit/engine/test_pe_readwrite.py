"""Unit tests for PandasEngine read/write methods - targeting uncovered lines."""

import os
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# read() tests
# ---------------------------------------------------------------------------


class TestReadStreaming:
    def test_streaming_raises_value_error(self):
        engine = PandasEngine(config={})
        with pytest.raises(ValueError, match="Streaming is not supported"):
            engine.read(connection=None, format="csv", path="/fake.csv", streaming=True)


class TestReadSimulation:
    def test_simulation_format_dispatches_to_read_simulation(self):
        engine = PandasEngine(config={})
        fake_df = pd.DataFrame({"sim": [1, 2, 3]})
        with patch.object(engine, "_read_simulation", return_value=fake_df) as mock_sim:
            result = engine.read(connection=None, format="simulation", options={"simulation": {}})
        mock_sim.assert_called_once()
        pd.testing.assert_frame_equal(result, fake_df)


class TestReadTableWithoutConnection:
    def test_table_without_connection_raises(self):
        engine = PandasEngine(config={})
        with patch.object(engine, "_resolve_path", side_effect=ValueError("resolve failed")):
            with pytest.raises(ValueError, match="connection is required"):
                engine.read(connection=None, format="csv", table="my_table")

    def test_no_path_no_table_raises(self):
        engine = PandasEngine(config={})
        with pytest.raises(ValueError, match="neither 'path' nor 'table'"):
            engine.read(connection=None, format="csv")


class TestReadHeaderSanitization:
    def test_header_true_becomes_zero(self, tmp_path):
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"a": [1, 2]}).to_csv(csv_path, index=False)
        engine = PandasEngine(config={})
        result = engine.read(connection=None, format="csv", path=csv_path, options={"header": True})
        assert list(result.columns) == ["a"]
        assert len(result) == 2

    def test_header_false_becomes_none(self, tmp_path):
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"a": [1, 2]}).to_csv(csv_path, index=False, header=False)
        engine = PandasEngine(config={})
        result = engine.read(
            connection=None, format="csv", path=csv_path, options={"header": False}
        )
        # header=None means no header row, so all lines become data rows
        assert len(result) == 2


class TestReadTimeTravel:
    def test_version_as_of_passed_in_options(self):
        engine = PandasEngine(config={})
        with patch.object(engine, "_read_file", return_value=pd.DataFrame({"x": [1]})) as mock_rf:
            engine.read(connection=None, format="delta", path="/fake/delta", as_of_version=5)
        opts_passed = mock_rf.call_args[0][2]
        assert opts_passed["versionAsOf"] == 5

    def test_timestamp_as_of_passed_in_options(self):
        engine = PandasEngine(config={})
        with patch.object(engine, "_read_file", return_value=pd.DataFrame({"x": [1]})) as mock_rf:
            engine.read(
                connection=None,
                format="delta",
                path="/fake/delta",
                as_of_timestamp="2024-01-01",
            )
        opts_passed = mock_rf.call_args[0][2]
        assert opts_passed["timestampAsOf"] == "2024-01-01"


class TestReadMetrics:
    def test_metrics_logged_for_dataframe(self, tmp_path):
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"a": [1, 2, 3]}).to_csv(csv_path, index=False)
        engine = PandasEngine(config={})
        result = engine.read(connection=None, format="csv", path=csv_path)
        # Ensure we get a proper DataFrame back (metrics path is exercised)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# write() tests
# ---------------------------------------------------------------------------


class TestWriteIteratorDispatch:
    def test_iterator_dispatches_to_write_iterator(self, tmp_path):
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "iter_out.csv")

        def gen():
            yield pd.DataFrame({"a": [1]})
            yield pd.DataFrame({"a": [2]})

        engine.write(df=gen(), connection=None, format="csv", path=csv_path, mode="overwrite")
        result = pd.read_csv(csv_path)
        assert list(result["a"]) == [1, 2]


class TestWriteSqlDispatch:
    def test_sql_format_dispatches(self):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1]})
        with patch.object(engine, "_write_sql", return_value=None) as mock_ws:
            engine.write(df=df, connection=Mock(), format="sql", table="tbl", mode="append")
        mock_ws.assert_called_once()


class TestWriteNoPathRaises:
    def test_no_path_no_table_raises(self):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Either path or table must be provided"):
            engine.write(df=df, connection=None, format="csv", mode="overwrite")

    def test_table_without_connection_raises(self):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1]})
        with patch.object(engine, "_resolve_path", side_effect=ValueError("resolve failed")):
            with pytest.raises(ValueError, match="Connection is required"):
                engine.write(df=df, connection=None, format="csv", table="tbl", mode="overwrite")


class TestWriteCustomWriter:
    def test_custom_writer_called(self, tmp_path):
        engine = PandasEngine(config={})
        custom_fn = Mock()
        PandasEngine.register_format("myformat", writer=custom_fn)
        try:
            df = pd.DataFrame({"a": [1]})
            out_path = str(tmp_path / "out.myformat")
            engine.write(df=df, connection=None, format="myformat", path=out_path, mode="overwrite")
            custom_fn.assert_called_once()
            call_kwargs = custom_fn.call_args
            assert call_kwargs[1]["mode"] == "overwrite"
        finally:
            PandasEngine._custom_writers.pop("myformat", None)


class TestWriteDelta:
    def test_delta_write_returns_result_and_logs_metrics(self, tmp_path):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1, 2]})
        delta_result = {"version": 1, "timestamp": pd.Timestamp.now()}

        with patch.object(engine, "_write_delta", return_value=delta_result) as mock_wd:
            with patch.object(engine, "_ensure_directory"):
                with patch.object(engine, "_check_partitioning"):
                    result = engine.write(
                        df=df,
                        connection=None,
                        format="delta",
                        path=str(tmp_path / "delta_tbl"),
                        mode="overwrite",
                    )
        mock_wd.assert_called_once()
        assert result == delta_result


class TestWriteGenericUpsert:
    def test_non_delta_upsert_calls_handle_generic_upsert(self, tmp_path):
        engine = PandasEngine(config={})
        existing = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
        csv_path = str(tmp_path / "data.csv")
        existing.to_csv(csv_path, index=False)

        new_data = pd.DataFrame({"id": [2, 3], "val": [200, 300]})
        engine.write(
            df=new_data,
            connection=None,
            format="csv",
            path=csv_path,
            mode="upsert",
            options={"keys": ["id"]},
        )
        result = pd.read_csv(csv_path)
        assert set(result["id"]) == {1, 2, 3}


class TestWriteMetrics:
    def test_write_csv_logs_metrics(self, tmp_path):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1, 2, 3]})
        csv_path = str(tmp_path / "metrics.csv")
        result = engine.write(df=df, connection=None, format="csv", path=csv_path, mode="overwrite")
        # Standard writes return None
        assert result is None
        # File was written
        assert os.path.exists(csv_path)


# ---------------------------------------------------------------------------
# _write_iterator() tests
# ---------------------------------------------------------------------------


class TestWriteIterator:
    def test_csv_header_false_for_subsequent_chunks(self, tmp_path):
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "chunked.csv")

        chunks = [
            pd.DataFrame({"a": [1], "b": [10]}),
            pd.DataFrame({"a": [2], "b": [20]}),
            pd.DataFrame({"a": [3], "b": [30]}),
        ]
        engine._write_iterator(
            iter(chunks),
            connection=None,
            format="csv",
            table=None,
            path=csv_path,
            mode="overwrite",
            options={},
        )
        result = pd.read_csv(csv_path)
        assert len(result) == 3
        assert list(result["a"]) == [1, 2, 3]

    def test_iterator_returns_none(self, tmp_path):
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "out.csv")

        chunks = [pd.DataFrame({"x": [1]})]
        result = engine._write_iterator(
            iter(chunks),
            connection=None,
            format="csv",
            table=None,
            path=csv_path,
            mode="overwrite",
            options={},
        )
        assert result is None

    def test_iterator_preserves_header_false_option(self, tmp_path):
        """When header is already False in options, subsequent chunks keep it."""
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "nohead.csv")

        chunks = [
            pd.DataFrame({"a": [1]}),
            pd.DataFrame({"a": [2]}),
        ]
        engine._write_iterator(
            iter(chunks),
            connection=None,
            format="csv",
            table=None,
            path=csv_path,
            mode="overwrite",
            options={"header": False},
        )
        # With header=False on first chunk too, the file has no header at all
        with open(csv_path) as f:
            lines = f.read().strip().split("\n")
        # First chunk writes without header, so we have raw data lines only
        assert len(lines) == 2


# ---------------------------------------------------------------------------
# _handle_generic_upsert() tests
# ---------------------------------------------------------------------------


class TestHandleGenericUpsert:
    def test_upsert_csv(self, tmp_path):
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_csv(csv_path, index=False)

        new = pd.DataFrame({"id": [2, 3], "val": [200, 300]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, csv_path, "csv", "upsert", {"keys": ["id"]}
        )
        assert result_mode == "overwrite"
        assert set(result_df["id"]) == {1, 2, 3}

    def test_upsert_json_existing(self, tmp_path):
        engine = PandasEngine(config={})
        json_path = str(tmp_path / "data.json")
        pd.DataFrame({"id": [1], "val": [10]}).to_json(json_path)

        new = pd.DataFrame({"id": [1, 2], "val": [100, 200]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, json_path, "json", "upsert", {"keys": ["id"]}
        )
        assert result_mode == "overwrite"
        assert set(result_df["id"]) == {1, 2}

    def test_excel_read_for_existing(self, tmp_path):
        engine = PandasEngine(config={})
        xls_path = str(tmp_path / "data.xlsx")
        pd.DataFrame({"id": [1], "val": [10]}).to_excel(xls_path, index=False)

        new = pd.DataFrame({"id": [1, 2], "val": [100, 200]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, xls_path, "excel", "upsert", {"keys": ["id"]}
        )
        assert result_mode == "overwrite"
        assert set(result_df["id"]) == {1, 2}

    def test_existing_file_not_found_returns_overwrite(self, tmp_path):
        engine = PandasEngine(config={})
        missing = str(tmp_path / "nonexistent.csv")
        df = pd.DataFrame({"id": [1], "val": [10]})
        result_df, result_mode = engine._handle_generic_upsert(
            df, missing, "csv", "upsert", {"keys": ["id"]}
        )
        assert result_mode == "overwrite"
        pd.testing.assert_frame_equal(result_df, df)

    def test_existing_df_none_returns_overwrite(self):
        """When format is unsupported for read, existing_df stays None."""
        engine = PandasEngine(config={})
        df = pd.DataFrame({"id": [1], "val": [10]})
        # Use a format that's not handled (e.g., "avro") so existing_df remains None
        result_df, result_mode = engine._handle_generic_upsert(
            df, "/nonexistent/path.avro", "avro", "upsert", {"keys": ["id"]}
        )
        # File doesn't exist => exception => returns overwrite
        assert result_mode == "overwrite"

    def test_append_once_csv_returns_append(self, tmp_path):
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_csv(csv_path, index=False)

        new = pd.DataFrame({"id": [2, 3], "val": [200, 300]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, csv_path, "csv", "append_once", {"keys": ["id"]}
        )
        # CSV append_once returns "append" mode
        assert result_mode == "append"
        # Only the new row (id=3) should be in the result
        assert list(result_df["id"]) == [3]

    def test_append_once_json_returns_append(self, tmp_path):
        engine = PandasEngine(config={})
        json_path = str(tmp_path / "data.json")
        pd.DataFrame({"id": [1], "val": [10]}).to_json(json_path)

        new = pd.DataFrame({"id": [1, 2], "val": [100, 200]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, json_path, "json", "append_once", {"keys": ["id"]}
        )
        assert result_mode == "append"
        assert list(result_df["id"]) == [2]

    def test_append_once_parquet_returns_overwrite(self, tmp_path):
        """Non csv/json formats with append_once rewrite everything."""
        engine = PandasEngine(config={})
        pq_path = str(tmp_path / "data.parquet")
        pd.DataFrame({"id": [1, 2], "val": [10, 20]}).to_parquet(pq_path, index=False)

        new = pd.DataFrame({"id": [2, 3], "val": [200, 300]})
        result_df, result_mode = engine._handle_generic_upsert(
            new, pq_path, "parquet", "append_once", {"keys": ["id"]}
        )
        assert result_mode == "overwrite"
        assert set(result_df["id"]) == {1, 2, 3}

    def test_unknown_mode_returns_unchanged(self, tmp_path):
        """An unrecognized mode passes through unchanged."""
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "data.csv")
        pd.DataFrame({"id": [1]}).to_csv(csv_path, index=False)

        df = pd.DataFrame({"id": [2], "val": [20]})
        result_df, result_mode = engine._handle_generic_upsert(
            df, csv_path, "csv", "some_unknown_mode", {"keys": ["id"]}
        )
        assert result_mode == "some_unknown_mode"

    def test_missing_keys_raises(self):
        engine = PandasEngine(config={})
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="requires 'keys'"):
            engine._handle_generic_upsert(df, "/fake", "csv", "upsert", {})


# ---------------------------------------------------------------------------
# _write_to_target() tests
# ---------------------------------------------------------------------------


class TestWriteToTargetCsv:
    def test_csv_append_new_file_gets_header(self, tmp_path):
        """When appending to a non-existent CSV, header should be True."""
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "new.csv")
        engine._write_to_target(
            pd.DataFrame({"a": [1, 2]}), csv_path, csv_path, "csv", "append", {}
        )
        result = pd.read_csv(csv_path)
        assert list(result.columns) == ["a"]
        assert len(result) == 2

    def test_csv_append_existing_gets_no_header(self, tmp_path):
        """Appending to existing CSV: header defaults to False."""
        engine = PandasEngine(config={})
        csv_path = str(tmp_path / "existing.csv")
        pd.DataFrame({"a": [1]}).to_csv(csv_path, index=False)

        engine._write_to_target(pd.DataFrame({"a": [2]}), csv_path, csv_path, "csv", "append", {})
        result = pd.read_csv(csv_path)
        assert list(result["a"]) == [1, 2]


class TestWriteToTargetParquet:
    def test_parquet_overwrite(self, tmp_path):
        engine = PandasEngine(config={})
        pq_path = str(tmp_path / "data.parquet")
        df = pd.DataFrame({"x": [10, 20]})
        engine._write_to_target(df, pq_path, pq_path, "parquet", "overwrite", {})
        result = pd.read_parquet(pq_path)
        assert list(result["x"]) == [10, 20]

    def test_parquet_append_merges_with_pyarrow(self, tmp_path):
        engine = PandasEngine(config={})
        pq_path = str(tmp_path / "data.parquet")
        pd.DataFrame({"x": [1, 2]}).to_parquet(pq_path, index=False)

        engine._write_to_target(
            pd.DataFrame({"x": [3, 4]}), pq_path, pq_path, "parquet", "append", {}
        )
        result = pd.read_parquet(pq_path)
        assert list(result["x"]) == [1, 2, 3, 4]

    def test_parquet_append_different_columns(self, tmp_path):
        """Appending with extra column fills with null for existing rows."""
        engine = PandasEngine(config={})
        pq_path = str(tmp_path / "data.parquet")
        pd.DataFrame({"a": [1]}).to_parquet(pq_path, index=False)

        engine._write_to_target(
            pd.DataFrame({"a": [2], "b": [20]}), pq_path, pq_path, "parquet", "append", {}
        )
        result = pd.read_parquet(pq_path)
        assert len(result) == 2
        assert "b" in result.columns
        # First row should have null for 'b'
        assert pd.isna(result.loc[0, "b"])

    def test_parquet_append_cleans_temp_on_failure(self, tmp_path):
        """If pyarrow merge fails, temp file is cleaned up."""
        engine = PandasEngine(config={})
        pq_path = str(tmp_path / "data.parquet")
        pd.DataFrame({"x": [1]}).to_parquet(pq_path, index=False)

        import pyarrow.parquet as pq

        with patch.object(pq, "ParquetWriter", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                engine._write_to_target(
                    pd.DataFrame({"x": [2]}), pq_path, pq_path, "parquet", "append", {}
                )
        # No temp files left
        remaining = [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
        assert remaining == []


class TestWriteToTargetJson:
    def test_json_overwrite(self, tmp_path):
        engine = PandasEngine(config={})
        json_path = str(tmp_path / "data.json")
        df = pd.DataFrame({"a": [1, 2]})
        engine._write_to_target(df, json_path, json_path, "json", "overwrite", {})
        result = pd.read_json(json_path, orient="records")
        assert list(result["a"]) == [1, 2]

    def test_json_append_mode(self, tmp_path):
        engine = PandasEngine(config={})
        json_path = str(tmp_path / "data.json")
        df = pd.DataFrame({"a": [1]})
        # JSON append requires lines=True with orient=records
        engine._write_to_target(df, json_path, json_path, "json", "append", {"lines": True})
        # File should exist
        assert os.path.exists(json_path)

    def test_json_orient_defaults_to_records(self, tmp_path):
        engine = PandasEngine(config={})
        json_path = str(tmp_path / "data.json")
        df = pd.DataFrame({"a": [1]})
        with patch.object(df, "to_json") as mock_to_json:
            engine._write_to_target(df, json_path, json_path, "json", "overwrite", {})
        call_kwargs = mock_to_json.call_args[1]
        assert call_kwargs["orient"] == "records"


class TestWriteToTargetExcel:
    def test_excel_overwrite(self, tmp_path):
        engine = PandasEngine(config={})
        xls_path = str(tmp_path / "data.xlsx")
        df = pd.DataFrame({"a": [1, 2]})
        engine._write_to_target(df, xls_path, xls_path, "excel", "overwrite", {})
        result = pd.read_excel(xls_path)
        assert list(result["a"]) == [1, 2]

    def test_excel_append_with_overlay(self, tmp_path):
        engine = PandasEngine(config={})
        xls_path = str(tmp_path / "data.xlsx")
        pd.DataFrame({"a": [1]}).to_excel(xls_path, index=False)

        engine._write_to_target(pd.DataFrame({"a": [2]}), xls_path, xls_path, "excel", "append", {})
        # File was written (overlay mode used)
        assert os.path.exists(xls_path)


class TestWriteToTargetAvro:
    def test_avro_overwrite_local(self, tmp_path):
        engine = PandasEngine(config={})
        avro_path = str(tmp_path / "data.avro")
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        engine._write_to_target(df, avro_path, avro_path, "avro", "overwrite", {})
        assert os.path.exists(avro_path)

        import fastavro

        with open(avro_path, "rb") as f:
            reader = fastavro.reader(f)
            records = list(reader)
        assert len(records) == 2

    def test_avro_append_local(self, tmp_path):
        engine = PandasEngine(config={})
        avro_path = str(tmp_path / "data.avro")

        df1 = pd.DataFrame({"id": [1], "name": ["a"]})
        engine._write_to_target(df1, avro_path, avro_path, "avro", "overwrite", {})

        size_before = os.path.getsize(avro_path)

        df2 = pd.DataFrame({"id": [2], "name": ["b"]})
        engine._write_to_target(df2, avro_path, avro_path, "avro", "append", {})

        size_after = os.path.getsize(avro_path)
        # Append should make the file larger
        assert size_after > size_before

    def test_avro_unsupported_format_raises(self, tmp_path):
        engine = PandasEngine(config={})
        with pytest.raises(ValueError, match="Unsupported format"):
            engine._write_to_target(
                pd.DataFrame({"a": [1]}),
                str(tmp_path / "x.xml"),
                str(tmp_path / "x.xml"),
                "xml",
                "overwrite",
                {},
            )
