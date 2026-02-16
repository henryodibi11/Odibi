"""Tests for encoding detection utilities."""

from unittest.mock import MagicMock, patch

import pytest

from odibi.utils.encoding import (
    CANDIDATE_ENCODINGS,
    _is_valid_encoding,
    _read_sample_bytes,
    detect_encoding,
)


class TestCandidateEncodings:
    def test_contains_expected_encodings(self):
        assert "utf-8" in CANDIDATE_ENCODINGS
        assert "latin1" in CANDIDATE_ENCODINGS
        assert "utf-8-sig" in CANDIDATE_ENCODINGS
        assert "cp1252" in CANDIDATE_ENCODINGS


class TestIsValidEncoding:
    def test_valid_utf8(self):
        sample = "hello world".encode("utf-8")
        assert _is_valid_encoding(sample, "utf-8") is True

    def test_valid_utf8_with_multibyte(self):
        sample = "café résumé".encode("utf-8")
        assert _is_valid_encoding(sample, "utf-8") is True

    def test_valid_latin1(self):
        sample = "café".encode("latin1")
        assert _is_valid_encoding(sample, "latin1") is True

    def test_invalid_utf8_bytes(self):
        sample = b"\xff\xfe"
        assert _is_valid_encoding(sample, "utf-8") is False

    def test_latin1_accepts_all_bytes(self):
        sample = bytes(range(256))
        assert _is_valid_encoding(sample, "latin1") is True

    def test_invalid_encoding_name(self):
        sample = b"hello"
        with pytest.raises(LookupError):
            _is_valid_encoding(sample, "not-a-real-encoding")

    def test_empty_bytes(self):
        assert _is_valid_encoding(b"", "utf-8") is True


class TestReadSampleBytes:
    def test_local_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        conn = MagicMock()
        result = _read_sample_bytes(conn, str(f), 1024)
        assert result == b"hello world"

    def test_local_file_respects_size(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        conn = MagicMock()
        result = _read_sample_bytes(conn, str(f), 5)
        assert result == b"hello"

    def test_file_prefix_stripped(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"prefixed")
        conn = MagicMock()
        path_with_prefix = "file://" + str(f)
        with patch.dict("sys.modules", {"fsspec": None}):
            result = _read_sample_bytes(conn, path_with_prefix, 1024)
        assert result == b"prefixed"

    def test_fsspec_path(self):
        mock_file = MagicMock()
        mock_file.read.return_value = b"remote data"
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)

        mock_fsspec = MagicMock()
        mock_fsspec.open.return_value = mock_file

        conn = MagicMock()
        conn.pandas_storage_options.return_value = {"key": "val"}

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            result = _read_sample_bytes(conn, "abfss://container/file.csv", 1024)
        assert result == b"remote data"
        mock_fsspec.open.assert_called_once_with("abfss://container/file.csv", "rb", key="val")

    def test_fsspec_exception_falls_through(self, tmp_path):
        f = tmp_path / "fallback.txt"
        f.write_bytes(b"local fallback")

        mock_fsspec = MagicMock()
        mock_fsspec.open.side_effect = OSError("connection failed")

        conn = MagicMock(spec=[])

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            result = _read_sample_bytes(conn, str(f), 1024)
        assert result == b"local fallback"

    def test_remote_path_no_fsspec_returns_none(self):
        conn = MagicMock(spec=[])
        with patch.dict("sys.modules", {"fsspec": None}):
            result = _read_sample_bytes(conn, "https://example.com/file.csv", 1024)
        assert result is None

    def test_nonexistent_local_file_returns_none(self, tmp_path):
        conn = MagicMock()
        with patch.dict("sys.modules", {"fsspec": None}):
            result = _read_sample_bytes(conn, str(tmp_path / "nonexistent.txt"), 1024)
        assert result is None

    def test_no_storage_options_method(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")

        mock_file = MagicMock()
        mock_file.read.return_value = b"data"
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)

        mock_fsspec = MagicMock()
        mock_fsspec.open.return_value = mock_file

        conn = MagicMock(spec=[])

        with patch.dict("sys.modules", {"fsspec": mock_fsspec}):
            result = _read_sample_bytes(conn, str(f), 1024)
        assert result == b"data"
        mock_fsspec.open.assert_called_once_with(str(f), "rb")


class TestDetectEncoding:
    def _make_connection(self, full_path):
        conn = MagicMock()
        conn.get_path.return_value = full_path
        return conn

    def test_utf8_file(self, tmp_path):
        f = tmp_path / "utf8.csv"
        f.write_bytes("id,name\n1,café".encode("utf-8"))
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "utf8.csv")
        assert result == "utf-8"
        conn.get_path.assert_called_once_with("utf8.csv")

    def test_latin1_file(self, tmp_path):
        f = tmp_path / "latin1.csv"
        content = "id,name\n1,caf\xe9"
        f.write_bytes(content.encode("latin1"))
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "latin1.csv", candidates=["utf-8", "latin1"])
        assert result in ("utf-8", "latin1")

    def test_latin1_only_file(self, tmp_path):
        f = tmp_path / "latin1_only.csv"
        raw = b"hello \xff\xfe world"
        f.write_bytes(raw)
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "latin1_only.csv", candidates=["utf-8", "latin1"])
        assert result == "latin1"

    def test_empty_file_returns_none(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_bytes(b"")
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "empty.csv")
        assert result is None

    def test_custom_candidates(self, tmp_path):
        f = tmp_path / "cp1252.csv"
        f.write_bytes("data".encode("cp1252"))
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "cp1252.csv", candidates=["cp1252"])
        assert result == "cp1252"

    def test_no_matching_encoding_returns_none(self, tmp_path):
        f = tmp_path / "binary.bin"
        f.write_bytes(b"\x80\x81\x82\x83")
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "binary.bin", candidates=["ascii"])
        assert result is None

    def test_sample_bytes_parameter(self, tmp_path):
        f = tmp_path / "large.csv"
        f.write_bytes(b"a" * 100)
        conn = self._make_connection(str(f))
        result = detect_encoding(conn, "large.csv", sample_bytes=10)
        assert result == "utf-8"

    def test_unreadable_file_returns_none(self):
        conn = MagicMock()
        conn.get_path.return_value = "/nonexistent/path/file.csv"
        result = detect_encoding(conn, "file.csv")
        assert result is None

    def test_uses_default_candidates_when_none(self, tmp_path):
        f = tmp_path / "default.csv"
        f.write_bytes(b"hello")
        conn = self._make_connection(str(f))
        with patch(
            "odibi.utils.encoding._is_valid_encoding", side_effect=lambda s, e: e == "utf-8"
        ) as mock_valid:
            result = detect_encoding(conn, "default.csv", candidates=None)
        called_encodings = [call.args[1] for call in mock_valid.call_args_list]
        assert called_encodings[0] == "utf-8"
        assert result == "utf-8"
