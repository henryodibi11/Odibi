"""Unit tests for PandasEngine format-handling branches in _read_file."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from odibi.engine.pandas_engine import PandasEngine


# ---------------------------------------------------------------------------
# CSV: glob parallel read
# ---------------------------------------------------------------------------
class TestCsvGlobParallelRead:
    def test_csv_glob_reads_multiple_files(self, tmp_path):
        """Lines 1090-1096: parallel CSV read when full_path is a list (glob)."""
        for i in range(3):
            df = pd.DataFrame({"a": [i], "b": [i * 10]})
            df.to_csv(tmp_path / f"part_{i}.csv", index=False)

        pattern = str(tmp_path / "part_*.csv")
        engine = PandasEngine(config={})
        result = engine._read_file(pattern, "csv", {})
        assert len(result) == 3
        assert set(result["a"].tolist()) == {0, 1, 2}


# ---------------------------------------------------------------------------
# CSV: UnicodeDecodeError → retry with latin1
# ---------------------------------------------------------------------------
class TestCsvUnicodeRetry:
    def test_csv_unicode_error_retries_with_latin1(self, tmp_path):
        """Lines 1103-1117: UnicodeDecodeError on first try, succeed on latin1."""
        csv_path = str(tmp_path / "data.csv")
        ok_df = pd.DataFrame({"x": [1, 2]})

        call_count = 0

        def fake_read_csv(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise UnicodeDecodeError("utf-8", b"", 0, 1, "bad byte")
            return ok_df

        engine = PandasEngine(config={})
        with patch("pandas.read_csv", side_effect=fake_read_csv):
            result = engine._read_file(csv_path, "csv", {})

        assert len(result) == 2
        assert call_count == 2


# ---------------------------------------------------------------------------
# CSV: ParserError → retry with on_bad_lines='skip'
# ---------------------------------------------------------------------------
class TestCsvParserErrorRetry:
    def test_csv_parser_error_retries_with_skip(self, tmp_path):
        """Lines 1134-1147: ParserError on first try, succeed with on_bad_lines='skip'."""
        csv_path = str(tmp_path / "data.csv")
        ok_df = pd.DataFrame({"x": [1]})

        call_count = 0

        def fake_read_csv(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise pd.errors.ParserError("bad line")
            return ok_df

        engine = PandasEngine(config={})
        with patch("pandas.read_csv", side_effect=fake_read_csv):
            result = engine._read_file(csv_path, "csv", {})

        assert len(result) == 1
        assert call_count == 2


# ---------------------------------------------------------------------------
# CSV: UnicodeDecodeError → then ParserError on latin1 retry → succeed
# ---------------------------------------------------------------------------
class TestCsvUnicodeThenParserRetry:
    def test_csv_unicode_then_parser_error_then_success(self, tmp_path):
        """Lines 1118-1132: UnicodeDecodeError first, then ParserError on latin1,
        then succeed with on_bad_lines='skip'."""
        csv_path = str(tmp_path / "data.csv")
        ok_df = pd.DataFrame({"y": [42]})

        call_count = 0

        def fake_read_csv(path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise UnicodeDecodeError("utf-8", b"", 0, 1, "bad")
            if call_count == 2:
                raise pd.errors.ParserError("still bad")
            return ok_df

        engine = PandasEngine(config={})
        with patch("pandas.read_csv", side_effect=fake_read_csv):
            result = engine._read_file(csv_path, "csv", {})

        assert len(result) == 1
        assert call_count == 3


# ---------------------------------------------------------------------------
# Parquet: list of files
# ---------------------------------------------------------------------------
class TestParquetListOfFiles:
    def test_parquet_list_of_files_sets_attrs(self, tmp_path):
        """Line 1152: full_path is already a list of parquet files."""
        paths = []
        for i in range(2):
            p = str(tmp_path / f"part_{i}.parquet")
            pd.DataFrame({"v": [i]}).to_parquet(p, index=False)
            paths.append(p)

        engine = PandasEngine(config={})
        result = engine._read_file(paths, "parquet", {})
        assert len(result) == 2
        assert result.attrs["odibi_source_files"] == paths


# ---------------------------------------------------------------------------
# JSON: glob parallel read
# ---------------------------------------------------------------------------
class TestJsonGlobParallelRead:
    def test_json_glob_reads_multiple_files(self, tmp_path):
        """Lines 1158-1164: parallel JSON read via glob."""
        for i in range(3):
            pd.DataFrame({"k": [i]}).to_json(tmp_path / f"part_{i}.json")

        pattern = str(tmp_path / "part_*.json")
        engine = PandasEngine(config={})
        result = engine._read_file(pattern, "json", {})
        assert len(result) == 3
        assert set(result["k"].tolist()) == {0, 1, 2}


# ---------------------------------------------------------------------------
# Delta: DeletionVectors fallback to DuckDB
# ---------------------------------------------------------------------------
class TestDvFallbackToDuckDB:
    def test_dv_error_falls_back_to_duckdb(self, tmp_path):
        """Lines 1251-1270: DeletionVectors error triggers DuckDB fallback."""
        table_path = str(tmp_path / "tbl")

        mock_dt_class = Mock(side_effect=Exception("DeletionVectors are not yet supported"))

        fallback_df = pd.DataFrame({"col": [1, 2, 3]})

        engine = PandasEngine(config={})

        with patch.dict(
            "sys.modules",
            {"deltalake": Mock(DeltaTable=mock_dt_class)},
        ):
            with patch.object(
                engine,
                "_read_delta_with_duckdb",
                return_value=fallback_df,
            ) as mock_duckdb:
                result = engine._read_file(table_path, "delta", {})

        mock_duckdb.assert_called_once()
        assert len(result) == 3

    def test_non_dv_error_reraises(self, tmp_path):
        """Lines 1271-1272: non-DV error is re-raised."""
        table_path = str(tmp_path / "tbl")

        mock_dt_class = Mock(side_effect=RuntimeError("disk is on fire"))

        engine = PandasEngine(config={})

        with patch.dict(
            "sys.modules",
            {"deltalake": Mock(DeltaTable=mock_dt_class)},
        ):
            with pytest.raises(RuntimeError, match="disk is on fire"):
                engine._read_file(table_path, "delta", {})


# ---------------------------------------------------------------------------
# Avro: local read
# ---------------------------------------------------------------------------
class TestAvroLocalRead:
    def test_avro_local_read(self, tmp_path):
        """Lines 1296-1300: local avro file read via fastavro."""
        avro_path = str(tmp_path / "data.avro")
        records = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        # Create a mock fastavro reader that acts as an iterator
        mock_reader = MagicMock()
        mock_reader.__iter__ = Mock(return_value=iter(records))

        mock_fastavro = Mock()
        mock_fastavro.reader = Mock(return_value=mock_reader)

        engine = PandasEngine(config={})

        # Write a dummy file so open() succeeds
        with open(avro_path, "wb") as f:
            f.write(b"\x00")

        with patch.dict("sys.modules", {"fastavro": mock_fastavro}):
            result = engine._read_file(avro_path, "avro", {})

        assert len(result) == 2
        assert list(result.columns) == ["id", "name"]


# ---------------------------------------------------------------------------
# Avro: remote read with fsspec
# ---------------------------------------------------------------------------
class TestAvroRemoteRead:
    def test_avro_remote_read_uses_fsspec(self):
        """Lines 1288-1295: remote avro read uses fsspec."""
        remote_path = "abfss://container@account.dfs.core.windows.net/data.avro"
        records = [{"x": 10}]

        mock_reader = MagicMock()
        mock_reader.__iter__ = Mock(return_value=iter(records))

        mock_fastavro = Mock()
        mock_fastavro.reader = Mock(return_value=mock_reader)

        mock_fh = MagicMock()
        mock_fsspec_open = MagicMock()
        mock_fsspec_open.__enter__ = Mock(return_value=mock_fh)
        mock_fsspec_open.__exit__ = Mock(return_value=False)

        mock_fsspec = Mock()
        mock_fsspec.open = Mock(return_value=mock_fsspec_open)

        engine = PandasEngine(config={})

        with patch.dict(
            "sys.modules",
            {"fastavro": mock_fastavro, "fsspec": mock_fsspec},
        ):
            result = engine._read_file(remote_path, "avro", {})

        mock_fsspec.open.assert_called_once()
        assert len(result) == 1
        assert result["x"].iloc[0] == 10


# ---------------------------------------------------------------------------
# Excel: _read_excel_with_patterns – single file, no sheet pattern
# ---------------------------------------------------------------------------
class TestExcelReadPatterns:
    def test_single_file_no_pattern(self, tmp_path):
        """Single local Excel file, no sheet pattern → reads first sheet."""
        xlsx_path = str(tmp_path / "book.xlsx")
        pd.DataFrame({"a": [1, 2, 3]}).to_excel(xlsx_path, index=False)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(xlsx_path)
        assert len(result) == 3
        assert "a" in result.columns

    def test_sheet_pattern_filtering(self, tmp_path):
        """Sheet pattern filtering → only matching sheets returned."""
        xlsx_path = str(tmp_path / "multi.xlsx")
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            pd.DataFrame({"v": [1]}).to_excel(writer, sheet_name="Report_Jan", index=False)
            pd.DataFrame({"v": [2]}).to_excel(writer, sheet_name="Report_Feb", index=False)
            pd.DataFrame({"v": [3]}).to_excel(writer, sheet_name="Summary", index=False)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(xlsx_path, sheet_pattern="Report_*")
        assert len(result) == 2
        assert set(result["v"].tolist()) == {1, 2}

    def test_add_source_file_column(self, tmp_path):
        """add_source_file=True → adds _source_file column."""
        xlsx_path = str(tmp_path / "source.xlsx")
        pd.DataFrame({"a": [1]}).to_excel(xlsx_path, index=False)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(xlsx_path, add_source_file=True)
        assert "_source_file" in result.columns
        assert result["_source_file"].iloc[0] == "source.xlsx"

    def test_list_of_paths_input(self, tmp_path):
        """List of paths input → reads all files."""
        paths = []
        for i in range(2):
            p = str(tmp_path / f"file_{i}.xlsx")
            pd.DataFrame({"n": [i]}).to_excel(p, index=False)
            paths.append(p)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(paths)
        assert len(result) == 2

    def test_no_matching_files_raises(self):
        """No matching files → raises FileNotFoundError."""
        engine = PandasEngine(config={})
        with pytest.raises(FileNotFoundError):
            engine._read_excel_with_patterns("/nonexistent/path/*.xlsx", is_glob=True)

    def test_empty_result_returns_empty_df(self, tmp_path):
        """Empty result from all files → returns empty DataFrame."""
        xlsx_path = str(tmp_path / "empty.xlsx")
        pd.DataFrame({"a": []}).to_excel(xlsx_path, index=False)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(xlsx_path)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_sheet_pattern_no_match_returns_empty(self, tmp_path):
        """Sheet pattern that matches nothing returns empty DataFrame."""
        xlsx_path = str(tmp_path / "book.xlsx")
        pd.DataFrame({"a": [1]}).to_excel(xlsx_path, sheet_name="Data", index=False)

        engine = PandasEngine(config={})
        result = engine._read_excel_with_patterns(xlsx_path, sheet_pattern="NoMatch_*")
        assert isinstance(result, pd.DataFrame)
        assert result.empty
