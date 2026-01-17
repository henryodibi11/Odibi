import pytest
from odibi.utils import encoding


# Dummy connection to simulate the connection object with a get_path method.
class DummyConnection:
    def get_path(self, path):
        return "dummy_path"


@pytest.fixture
def dummy_connection():
    return DummyConnection()


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
