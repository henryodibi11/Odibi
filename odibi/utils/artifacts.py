"""Nonmutating diagnostic output policy, deliberately separate from execution models.

Configuration snapshots are diagnostic projections, not replayable configurations.
Unknown request/transform option values and SQL literals are not safe diagnostics.
Operational models, hashes, watermarks and resource resolution must use originals.
"""

import copy
import json
import re
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from threading import RLock
from urllib.parse import parse_qsl, quote, unquote, urlsplit

from pydantic import BaseModel

REDACTED = "[REDACTED]"
_secrets = set()
_lock = RLock()
_CONFIG = {"config", "config_snapshot", "previous_config_snapshot", "resolved_config"}
_OPAQUE = {
    "options",
    "params",
    "request_body",
    "body",
    "headers",
    "auth",
    "credentials",
    "storage_options",
    "archive_options",
}
_SAFE_OPTIONS = {
    "timeout",
    "timeout_s",
    "max_retries",
    "page_size",
    "batch_size",
    "method",
    "format",
    "mode",
    "encoding",
    "delimiter",
    "header",
    "compression",
    "columns",
    "column",
    "partition_by",
    "order_by",
    "ascending",
    "limit",
}
_SQL = {
    "sql",
    "definition_sql",
    "query",
    "executed_sql",
    "sql_generated",
    "generated_sql",
    "first_run_query",
    "pre_sql",
    "post_sql",
    "expr",
    "expression",
}
_SECRET_NAME = re.compile(
    r"(?:password|passwd|pwd|secret|token|apikey|accesskey|accountkey|privatekey|"
    r"credential|authorization|cookie|signature|connectionstring|sastoken)$",
    re.I,
)
_ASSIGNMENT = re.compile(
    r"(?i)((?:password|passwd|pwd|client_secret|api[_-]?key|access[_-]?token|"
    r"refresh[_-]?token|token|accountkey|sig|signature|sas)(?:%3[dD]|[\"']?\s*[=:]\s*[\"']?))"
    r"([^\s&;\"'<>},)]+)"
)
_USERINFO = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/@]+@")
_BEARER = re.compile(r"(?i)\b(Bearer|Basic)\s+[^\s\"'<>},)]+")
_LITERAL = re.compile(r"'(?:''|[^'])*'")
_URL = re.compile(r"[a-zA-Z][a-zA-Z0-9+.-]*://[^\s\"'<>]+")
_SAFE_QUERY = {"part", "revision", "version", "limit", "offset", "page", "date"}
_CONTAINER_AUTHORITY = {"abfs", "abfss", "wasb", "wasbs"}


def _secret_name(key):
    return bool(_SECRET_NAME.search(re.sub(r"[^a-zA-Z0-9]", "", str(key))))


def _opacity(name, config):
    if name in _OPAQUE and (config or name in {"headers", "auth", "credentials"}):
        # Request bodies/params/headers have no universally safe field names.
        return 1 if name == "options" else 2
    return 0


def register_secret(value):
    """Share explicitly known credentials with every diagnostic output owner."""
    if isinstance(value, str) and value and value != REDACTED:
        with _lock:
            _secrets.add(value)


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _strings(item)


def _url_secrets(text):
    for match in _URL.finditer(text):
        try:
            url = urlsplit(match[0])
            if url.username and url.scheme.lower() not in _CONTAINER_AUTHORITY:
                yield unquote(url.username)
            if url.password:
                yield unquote(url.password)
            for key, value in parse_qsl(url.query, keep_blank_values=True):
                if key.lower() not in _SAFE_QUERY and value:
                    yield value
        except ValueError:
            continue


def _collect(value, found, config=False, opaque=False, request=False):
    if is_dataclass(value) and not isinstance(value, type):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key).lower()
            if _secret_name(key) or (
                request and (opaque == 2 or (opaque and name not in _SAFE_OPTIONS))
            ):
                found.update(s for s in _strings(item) if s and s != REDACTED)
            if name in {"config_json", "metrics_json"} and isinstance(item, str):
                try:
                    _collect(json.loads(item), found, name == "config_json")
                except (ValueError, TypeError):
                    pass
            _collect(
                item,
                found,
                config or name in _CONFIG,
                2 if opaque == 2 else _opacity(name, config),
                request or name in (_OPAQUE - {"params"}) or name in {"read", "write"},
            )
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            _collect(item, found, config, opaque, request)
    elif isinstance(value, str):
        found.update(_url_secrets(value))


def register_config_secrets(config):
    """Learn credentials before execution can echo them into an exception or log."""
    found = set()
    _collect(config, found, config=True)
    for value in found:
        register_secret(value)


def _text(value, secrets):
    text = str(value)
    for secret in sorted(secrets, key=len, reverse=True):
        for form in {secret, quote(secret, safe=""), json.dumps(secret)[1:-1]}:
            if form:
                text = text.replace(form, REDACTED)
    text = _USERINFO.sub(
        lambda m: (
            m[0] if m[1].split(":")[0].lower() in _CONTAINER_AUTHORITY else m[1] + REDACTED + "@"
        ),
        text,
    )
    text = _BEARER.sub(r"\1 " + REDACTED, text)
    return _ASSIGNMENT.sub(lambda m: m[1] + REDACTED, text)


def safe_artifact(value, *, config=False, secrets=()):
    """Copy a diagnostic payload, including historical inputs, without credentials.

    Dataclass types are retained for renderers. JSON-bearing catalog fields are
    decoded and sanitized before re-encoding. Unknown objects are stringified
    *before* redaction, never left for an unsafe serializer fallback.
    """
    with _lock:
        known = _secrets.copy()
    known.update(s for s in secrets if isinstance(s, str) and s)
    _collect(value, known, config)

    def visit(item, in_config=False, opaque=False, sql=False):
        if is_dataclass(item) and not isinstance(item, type):
            result = copy.copy(item)
            values = visit({f.name: getattr(item, f.name) for f in fields(item)}, in_config)
            for key, val in values.items():
                object.__setattr__(result, key, val)
            return result
        if isinstance(item, BaseModel):
            return visit(item.model_dump(mode="python"), in_config, opaque, sql)
        if isinstance(item, Enum):
            return visit(item.value, in_config, opaque, sql)
        if isinstance(item, Mapping):
            result = {}
            for key, val in item.items():
                name = str(key).lower()
                # Preserve structural field names even when a short credential
                # happens to equal one; redaction must not rename `rows`, etc.
                safe_key = key
                if opaque == 2 or (opaque and name not in _SAFE_OPTIONS):
                    result[safe_key] = (
                        visit(val, in_config, 2)
                        if isinstance(val, (Mapping, list, tuple))
                        else (None if val is None else REDACTED)
                    )
                elif name in {"version_hash", "sql_hash"}:
                    result[safe_key] = val
                elif _secret_name(key):
                    result[safe_key] = REDACTED if val is not None else None
                elif name in _CONFIG and isinstance(val, str):
                    # An opaque historical snapshot cannot be safely projected.
                    result[safe_key] = REDACTED
                elif name in {"config_json", "metrics_json"} and isinstance(val, str):
                    try:
                        result[safe_key] = json.dumps(visit(json.loads(val), name == "config_json"))
                    except (ValueError, TypeError):
                        result[safe_key] = REDACTED
                else:
                    result[safe_key] = visit(
                        val,
                        in_config or name in _CONFIG,
                        _opacity(name, in_config),
                        name in _SQL,
                    )
            return result
        if isinstance(item, (list, tuple, set)):
            values = [visit(v, in_config, opaque, sql) for v in item]
            return tuple(values) if isinstance(item, tuple) else values
        if item is None or isinstance(item, (bool, int, float)):
            return REDACTED if opaque and item is not None else item
        # Datetimes remain datetimes for story calculations and backend schemas.
        from datetime import date, datetime

        if isinstance(item, (date, datetime)):
            return item
        text = _text(item, known)
        if opaque:
            return REDACTED
        return _LITERAL.sub("'" + REDACTED + "'", text) if sql else text

    return visit(value, config)


def safe_identity(value):
    """Diagnostic resource identity, not an execution path.

    Preserve local filename punctuation and non-auth query fields; mask only
    known credentials, URL userinfo and credential assignments. Never use the
    result to replace an operational resource locator.
    """
    return safe_artifact(value)
