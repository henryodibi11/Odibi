"""Unit tests for odibi/utils/encoding.py."""

import os
import tempfile

import pytest

from odibi.utils import encoding


# Dummy connection to simulate the connection object with a get_path method.
class DummyConnection:
    def get_path(self, path):
        return "dummy_path"


@pytest.fixture
def dummy_connection():
    return DummyConnection()


# =============================================================================
# Tests for detect_encoding()
# =============================================================================


def test_detect_encoding_utf8_valid_returns_utf8(monkeypatch, dummy_connection):
    """
    When the sample bytes are valid UTF-8, detect_encoding should return "utf-8".
    """
    sample_bytes = "Hello, world!".encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result == "utf-8"


def test_detect_encoding_empty_sample_returns_none(monkeypatch, dummy_connection):
    """
    When _read_sample_bytes returns empty bytes, detect_encoding should return None.
    """
    sample_bytes = b""
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result is None


def test_detect_encoding_invalid_for_utf8_returns_latin1(monkeypatch, dummy_connection):
    """
    When the sample bytes are invalid for UTF-8 but valid for Latin1,
    detect_encoding should return "latin1" (following default candidate order).
    """
    # b'\xff' is invalid for "utf-8" and "utf-8-sig" but valid for "latin1" and "cp1252".
    sample_bytes = b"\xff"
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    # Default candidates: ["utf-8", "utf-8-sig", "latin1", "cp1252"]
    assert result == "latin1"


def test_detect_encoding_custom_candidates_returns_cp1252(monkeypatch, dummy_connection):
    """
    When a custom candidate list is provided and the sample bytes are encoded in cp1252,
    detect_encoding should return "cp1252" if prioritized in the custom list.
    """
    sample_bytes = "Café".encode("cp1252")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    custom_candidates = ["cp1252", "utf-8"]
    result = encoding.detect_encoding(
        dummy_connection, "irrelevant_path", candidates=custom_candidates
    )
    assert result == "cp1252"


def test_detect_encoding_read_sample_exception_returns_none(monkeypatch, dummy_connection):
    """
    If _read_sample_bytes returns None (simulating a read failure),
    detect_encoding should return None.
    """
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: None)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result is None


def test_detect_encoding_with_unicode_characters(monkeypatch, dummy_connection):
    """
    Test detect_encoding with various unicode characters (Chinese, Arabic, etc.).
    """
    # Unicode text with various scripts
    unicode_text = "Hello 世界 مرحبا Привет 🌍"
    sample_bytes = unicode_text.encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result == "utf-8"


def test_detect_encoding_with_emojis(monkeypatch, dummy_connection):
    """
    Test detect_encoding with emoji characters.
    """
    emoji_text = "🎉🚀💻🔥⚡️🌟✨🎨"
    sample_bytes = emoji_text.encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result == "utf-8"


def test_detect_encoding_with_accents(monkeypatch, dummy_connection):
    """
    Test detect_encoding with accented characters.
    """
    accented_text = "café résumé naïve façade"
    sample_bytes = accented_text.encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result == "utf-8"


def test_detect_encoding_with_bom_utf8_sig(monkeypatch, dummy_connection):
    """
    Test detect_encoding with UTF-8 BOM (Byte Order Mark).
    """
    text = "Hello, world!"
    sample_bytes = text.encode("utf-8-sig")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    # utf-8-sig should be detected before utf-8 in candidate list
    assert result in ["utf-8", "utf-8-sig"]


def test_detect_encoding_with_custom_sample_bytes_size(monkeypatch, dummy_connection):
    """
    Test detect_encoding with custom sample_bytes parameter.
    """
    sample_bytes = "Test data".encode("utf-8")
    # Mock to verify sample_bytes size is passed
    called_with = []

    def mock_read(conn, path, size):
        called_with.append(size)
        return sample_bytes

    monkeypatch.setattr(encoding, "_read_sample_bytes", mock_read)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path", sample_bytes=1024)
    assert result == "utf-8"
    assert called_with[0] == 1024


def test_detect_encoding_with_special_characters(monkeypatch, dummy_connection):
    """
    Test detect_encoding with special characters and symbols.
    """
    special_text = "¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿"
    sample_bytes = special_text.encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert result == "utf-8"


def test_detect_encoding_with_null_bytes(monkeypatch, dummy_connection):
    """
    Test detect_encoding with null bytes mixed in.
    """
    sample_bytes = b"Hello\x00World\x00"
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    # UTF-8 should handle null bytes fine
    assert result == "utf-8"


def test_detect_encoding_empty_candidates_uses_default(monkeypatch, dummy_connection):
    """
    Test detect_encoding with empty candidate list falls back to default candidates.
    In Python, empty list is falsy, so `candidates or CANDIDATE_ENCODINGS` uses defaults.
    """
    sample_bytes = b"\xff\xfe"
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    result = encoding.detect_encoding(dummy_connection, "irrelevant_path", candidates=[])
    # Should fall back to default encodings and detect latin1
    assert result == "latin1"


def test_detect_encoding_roundtrip_utf8(monkeypatch, dummy_connection):
    """
    Test encoding/decoding roundtrip for UTF-8.
    """
    original_text = "Hello, 世界! 🌍"
    sample_bytes = original_text.encode("utf-8")
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    detected_encoding = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert detected_encoding == "utf-8"
    # Verify roundtrip
    decoded_text = sample_bytes.decode(detected_encoding)
    assert decoded_text == original_text


def test_detect_encoding_roundtrip_latin1(monkeypatch, dummy_connection):
    """
    Test encoding/decoding roundtrip for Latin1.
    """
    # Latin1 specific characters (not valid UTF-8)
    sample_bytes = b"\xe9\xe8\xe0"  # é è à in latin1
    monkeypatch.setattr(encoding, "_read_sample_bytes", lambda conn, path, size: sample_bytes)
    detected_encoding = encoding.detect_encoding(dummy_connection, "irrelevant_path")
    assert detected_encoding == "latin1"
    # Verify roundtrip
    decoded_text = sample_bytes.decode(detected_encoding)
    assert isinstance(decoded_text, str)
    # Re-encode and verify
    re_encoded = decoded_text.encode(detected_encoding)
    assert re_encoded == sample_bytes


# =============================================================================
# Tests for _is_valid_encoding()
# =============================================================================


def test_is_valid_encoding_valid_utf8():
    """
    Test _is_valid_encoding with valid UTF-8 bytes.
    """
    sample_bytes = "Hello, world!".encode("utf-8")
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is True


def test_is_valid_encoding_invalid_utf8():
    """
    Test _is_valid_encoding with invalid UTF-8 bytes.
    """
    # Invalid UTF-8 sequence
    sample_bytes = b"\xff\xfe"
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is False


def test_is_valid_encoding_valid_latin1():
    """
    Test _is_valid_encoding with Latin1 encoding.
    Latin1 accepts all byte values 0x00-0xFF.
    """
    sample_bytes = b"\xff\xfe\xfd"
    result = encoding._is_valid_encoding(sample_bytes, "latin1")
    assert result is True


def test_is_valid_encoding_empty_bytes():
    """
    Test _is_valid_encoding with empty bytes.
    Empty bytes should be valid for any encoding.
    """
    sample_bytes = b""
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is True


def test_is_valid_encoding_strict_error_handling():
    """
    Test _is_valid_encoding uses strict error handling.
    """
    # Invalid UTF-8 sequence - should raise error with strict
    sample_bytes = b"\x80\x81\x82"
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is False


def test_is_valid_encoding_with_unicode():
    """
    Test _is_valid_encoding with unicode characters.
    """
    unicode_text = "Привет мир 你好世界"
    sample_bytes = unicode_text.encode("utf-8")
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is True


def test_is_valid_encoding_with_emojis():
    """
    Test _is_valid_encoding with emoji bytes.
    """
    emoji_text = "🎉🚀💻"
    sample_bytes = emoji_text.encode("utf-8")
    result = encoding._is_valid_encoding(sample_bytes, "utf-8")
    assert result is True


def test_is_valid_encoding_cp1252():
    """
    Test _is_valid_encoding with cp1252 encoding.
    """
    text = "Windows™ — special chars"
    sample_bytes = text.encode("cp1252")
    result = encoding._is_valid_encoding(sample_bytes, "cp1252")
    assert result is True


# =============================================================================
# Tests for _read_sample_bytes()
# =============================================================================


def test_read_sample_bytes_local_file():
    """
    Test _read_sample_bytes with a local file path.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
        test_data = b"Test content for reading"
        f.write(test_data)
        temp_path = f.name

    try:
        # Mock connection (not used for local file reading)
        conn = DummyConnection()
        result = encoding._read_sample_bytes(conn, temp_path, len(test_data))
        assert result == test_data
    finally:
        # Clean up
        os.unlink(temp_path)


def test_read_sample_bytes_local_file_with_file_prefix():
    """
    Test _read_sample_bytes with file:// prefix.
    """
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
        test_data = b"Test content with file:// prefix"
        f.write(test_data)
        temp_path = f.name

    try:
        conn = DummyConnection()
        # Add file:// prefix
        file_uri = f"file://{temp_path}"
        result = encoding._read_sample_bytes(conn, file_uri, len(test_data))
        assert result == test_data
    finally:
        os.unlink(temp_path)


def test_read_sample_bytes_partial_read():
    """
    Test _read_sample_bytes reads only requested number of bytes.
    """
    # Create a temporary file with more data than we'll request
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
        test_data = b"A" * 1000
        f.write(test_data)
        temp_path = f.name

    try:
        conn = DummyConnection()
        result = encoding._read_sample_bytes(conn, temp_path, 100)
        assert len(result) == 100
        assert result == b"A" * 100
    finally:
        os.unlink(temp_path)


def test_read_sample_bytes_nonexistent_file():
    """
    Test _read_sample_bytes with nonexistent file returns None.
    """
    conn = DummyConnection()
    result = encoding._read_sample_bytes(conn, "/nonexistent/path/file.txt", 1024)
    assert result is None


def test_read_sample_bytes_remote_path_no_fsspec(monkeypatch):
    """
    Test _read_sample_bytes with remote path when fsspec is not available.
    """

    # Mock ImportError for fsspec
    def mock_import(name, *args, **kwargs):
        if name == "fsspec":
            raise ImportError("No module named 'fsspec'")
        return __builtins__.__import__(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", mock_import)

    conn = DummyConnection()
    # Remote path (has :// but not file://)
    result = encoding._read_sample_bytes(conn, "s3://bucket/key.csv", 1024)
    assert result is None


def test_read_sample_bytes_empty_file():
    """
    Test _read_sample_bytes with empty file.
    """
    # Create an empty temporary file
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as f:
        temp_path = f.name
        # Write nothing

    try:
        conn = DummyConnection()
        result = encoding._read_sample_bytes(conn, temp_path, 1024)
        assert result == b""
    finally:
        os.unlink(temp_path)


# =============================================================================
# Integration Tests
# =============================================================================


def test_detect_encoding_integration_with_real_file():
    """
    Integration test: Create a real file and detect its encoding.
    """
    # Create a temporary file with UTF-8 content
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False, suffix=".txt") as f:
        test_content = "Hello, 世界! 🌍 Café résumé"
        f.write(test_content)
        temp_path = f.name

    try:
        # Create a connection that returns the actual path
        class RealPathConnection:
            def get_path(self, path):
                return path

        conn = RealPathConnection()
        detected_encoding = encoding.detect_encoding(conn, temp_path)
        assert detected_encoding == "utf-8"

        # Verify we can read the file with detected encoding
        with open(temp_path, "r", encoding=detected_encoding) as f:
            content = f.read()
            assert content == test_content
    finally:
        os.unlink(temp_path)


def test_detect_encoding_integration_with_latin1_file():
    """
    Integration test: Create a real file with Latin1 encoding.
    """
    # Create a temporary file with Latin1 content
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f:
        # Latin1 specific bytes
        test_content = b"Caf\xe9 r\xe9sum\xe9"  # Café résumé in latin1
        f.write(test_content)
        temp_path = f.name

    try:

        class RealPathConnection:
            def get_path(self, path):
                return path

        conn = RealPathConnection()
        detected_encoding = encoding.detect_encoding(conn, temp_path)
        # Should detect latin1 since it's not valid UTF-8
        assert detected_encoding == "latin1"

        # Verify we can read the file with detected encoding
        with open(temp_path, "rb") as f:
            content = f.read()
            assert content == test_content
    finally:
        os.unlink(temp_path)
