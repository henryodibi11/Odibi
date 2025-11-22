"""Encoding detection utilities."""

import logging
from typing import Optional, List, Any

logger = logging.getLogger(__name__)

# Common encodings to try
CANDIDATE_ENCODINGS = ["utf-8", "utf-8-sig", "latin1", "cp1252"]


def detect_encoding(
    connection: Any,
    path: str,
    sample_bytes: int = 65536,
    candidates: Optional[List[str]] = None,
) -> Optional[str]:
    """Detect text encoding of a file.

    Args:
        connection: Connection object
        path: File path (relative to connection base)
        sample_bytes: Number of bytes to read for detection
        candidates: List of encodings to try (default: common list)

    Returns:
        Detected encoding name or None if detection failed
    """
    full_path = connection.get_path(path)
    candidates = candidates or CANDIDATE_ENCODINGS

    # Read sample bytes
    sample = _read_sample_bytes(connection, full_path, sample_bytes)
    if not sample:
        return None

    # Try decoding
    for encoding in candidates:
        if _is_valid_encoding(sample, encoding):
            return encoding

    return None


def _read_sample_bytes(connection: Any, path: str, size: int) -> Optional[bytes]:
    """Read bytes from file using available methods."""
    # 1. Try fsspec (supports local and remote)
    try:
        import fsspec

        # Get storage options from connection if available
        storage_options = {}
        if hasattr(connection, "pandas_storage_options"):
            storage_options = connection.pandas_storage_options()

        with fsspec.open(path, "rb", **storage_options) as f:
            return f.read(size)
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"fsspec read failed for {path}: {e}")

    # 2. Try local open (if path is local)
    # Handle 'file://' prefix or plain path
    local_path = path
    if path.startswith("file://"):
        local_path = path[7:]
    elif "://" in path:
        # Remote path and no fsspec -> cannot read
        return None

    try:
        with open(local_path, "rb") as f:
            return f.read(size)
    except Exception as e:
        logger.debug(f"Local open failed for {local_path}: {e}")

    return None


def _is_valid_encoding(sample: bytes, encoding: str) -> bool:
    """Check if bytes can be decoded with encoding and look reasonable."""
    try:
        text = sample.decode(encoding)

        # Check for replacement characters (if decoder doesn't fail but inserts them)
        # Note: strict errors would raise exception, but some encodings might be loose.
        # We use strict check first.
        sample.decode(encoding, errors="strict")

        # Heuristics:
        # 1. Check for excessive non-printable characters?
        # For now, strict decoding is a strong signal.
        # Latin1 accepts everything, so it always succeeds.
        # So we prioritize UTF-8. If UTF-8 works, use it.
        # If not, Latin1 will work but might show garbage.
        # "Looks right" is hard.
        # Maybe check for common delimiters if it's CSV?

        return True
    except UnicodeError:
        return False
