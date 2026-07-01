"""Regression tests for cross-engine parity + connection-config strictness.

Covers three fixes:
1. REGEX_MATCH parity — Spark `rlike` / Polars `str.contains` are unanchored
   substring searches, while Pandas `str.match` anchors at the start. They used
   to disagree (a false-negative on Spark/Polars). `anchor_match` aligns them.
2. Connection-config strictness — a typo'd known-type connection block now hard
   errors with a did-you-mean hint instead of being silently dropped or falling
   through to the permissive CustomConnectionConfig.
3. Content-hash engine tagging — the stored hash records which engine computed
   it so a cross-engine comparison is diagnosable, not a silent reprocess.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from odibi.config import ConnectionConfig, ConnectionType
from odibi.validation.regex_compat import anchor_match


# --- 1. REGEX_MATCH parity -------------------------------------------------


def test_anchor_match_anchors_at_start_with_group():
    # Non-capturing group keeps alternation anchored on every branch.
    assert anchor_match("ABC") == "^(?:ABC)"
    assert anchor_match("a|b") == "^(?:a|b)"


def test_polars_regex_matches_pandas_after_anchoring():
    pl = pytest.importorskip("polars")
    import pandas as pd

    values = ["ABC123", "xABC", "ABC", "zzz"]
    pattern = "ABC"

    pandas_verdict = pd.Series(values).str.match(pattern, na=True).tolist()
    polars_unanchored = pl.Series(values).str.contains(pattern).to_list()
    polars_anchored = pl.Series(values).str.contains(anchor_match(pattern)).to_list()

    # The old behavior disagreed (substring matched "xABC"); the fix agrees.
    assert polars_unanchored != pandas_verdict
    assert polars_anchored == pandas_verdict


def test_quarantine_regex_uses_anchored_pattern_on_polars():
    pl = pytest.importorskip("polars")
    from pydantic import TypeAdapter

    from odibi.config import TestConfig
    from odibi.validation.quarantine import split_valid_invalid

    df = pl.DataFrame({"code": ["ABC1", "xABC", "ABC"]})

    class _Eng:  # minimal engine stub; only count_rows is consulted
        def count_rows(self, d):
            return d.height

    # TestConfig is a discriminated Union alias, so build it through a TypeAdapter.
    test = TypeAdapter(TestConfig).validate_python(
        {
            "type": "regex_match",
            "column": "code",
            "pattern": "ABC",
            "on_fail": "quarantine",
        }
    )
    result = split_valid_invalid(df, [test], _Eng())
    invalid_codes = set(result.invalid_df["code"].to_list())
    # "xABC" does NOT start with ABC -> must be quarantined (matches pandas).
    assert "xABC" in invalid_codes
    assert "ABC1" not in invalid_codes


# --- 2. Connection-config strictness --------------------------------------

TA = TypeAdapter(ConnectionConfig)


def test_valid_known_connection_constructs():
    conn = TA.validate_python({"type": "local", "base_path": "./data"})
    assert conn.__class__.__name__ == "LocalConnectionConfig"
    assert conn.base_path == "./data"


@pytest.mark.parametrize(
    "block,bad_key,suggestion",
    [
        ({"type": "local", "base_pth": "./d"}, "base_pth", "base_path"),
        (
            {"type": "azure_blob", "account_name": "a", "containr": "c"},
            "containr",
            "container",
        ),
    ],
)
def test_typo_in_known_connection_is_rejected_not_swallowed(block, bad_key, suggestion):
    with pytest.raises(ValidationError) as ei:
        TA.validate_python(block)
    msg = str(ei.value)
    assert bad_key in msg
    assert f"did you mean '{suggestion}'" in msg, msg


def test_unknown_plugin_type_routes_to_custom():
    conn = TA.validate_python({"type": "my_plugin", "whatever": 123})
    assert conn.__class__.__name__ == "CustomConnectionConfig"
    # extra="allow" preserves arbitrary plugin fields
    assert conn.whatever == 123


def test_auth_block_typo_is_rejected():
    with pytest.raises(ValidationError) as ei:
        TA.validate_python(
            {
                "type": "azure_blob",
                "account_name": "a",
                "container": "c",
                "auth": {"mode": "account_key", "accont_key": "x"},
            }
        )
    assert "accont_key" in str(ei.value)


def test_all_known_connection_types_have_discriminator_tags():
    # Guard against adding a ConnectionType without wiring a Tag for it.
    conn = TA.validate_python({"type": "http", "base_url": "https://x"})
    assert conn.__class__.__name__ == "HttpConnectionConfig"
    assert {e.value for e in ConnectionType} == {
        "local",
        "azure_blob",
        "delta",
        "sql_server",
        "http",
        "unity_catalog",
    }


# --- 3. Content-hash engine tagging ---------------------------------------


def test_content_hash_record_round_trips_engine():
    from odibi.utils.content_hash import (
        get_content_hash_from_state,
        get_content_hash_record,
        set_content_hash_in_state,
    )

    class _Backend:
        def __init__(self):
            self._store = {}

        def set_hwm(self, key, value):
            self._store[key] = value

        def get_hwm(self, key):
            return self._store.get(key)

    backend = _Backend()
    set_content_hash_in_state(backend, "node_a", "tbl", "deadbeef", engine="spark")

    assert get_content_hash_from_state(backend, "node_a", "tbl") == "deadbeef"
    record = get_content_hash_record(backend, "node_a", "tbl")
    assert record["hash"] == "deadbeef"
    assert record["engine"] == "spark"


def test_content_hash_engine_optional_for_back_compat():
    from odibi.utils.content_hash import get_content_hash_record, set_content_hash_in_state

    class _Backend:
        def __init__(self):
            self._store = {}

        def set_hwm(self, key, value):
            self._store[key] = value

        def get_hwm(self, key):
            return self._store.get(key)

    backend = _Backend()
    set_content_hash_in_state(backend, "n", "t", "abc123")  # no engine
    record = get_content_hash_record(backend, "n", "t")
    assert record["hash"] == "abc123"
    assert "engine" not in record
