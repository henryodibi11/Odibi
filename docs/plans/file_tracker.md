# File Tracker — Implementation Plan

> **Status:** Planned (not yet started)
> **Created:** 2026-03-31
> **Estimated effort:** ~9 days
> **Priority:** Deferred — build pipelines first, implement when pain point is real

## Overview

An engine-agnostic file-level tracker that detects new/changed files in a directory and passes only unprocessed files to the engine. Similar in concept to Databricks Auto Loader's "directory listing" mode, but not coupled to Databricks or any specific format.

## Motivation

- HWM-based incremental loading works for timestamped data but requires reading all files each run
- File-level tracking avoids re-reading already-processed files (performance at scale)
- Works with any format (CSV, Parquet, JSON, Excel, Delta)
- Works across all engines (Pandas, Polars, Spark)

## Design Decisions

- **Name:** `file_tracker` (not "auto loader" — avoid Databricks branding)
- **Checkpoint storage:** Per-node directory with JSON file (easy to inspect, reset, and version control)
- **Reset mechanism:** Delete checkpoint directory or use CLI command
- **State backend sync:** Optional — teams can sync to catalog for centralized tracking
- **No new dependencies:** Uses `fsspec` (already installed) for file listing across protocols

## Proposed YAML Config

```yaml
read:
  path: "data/incoming/*.csv"
  format: csv
  file_tracker:
    enabled: true
    checkpoint_path: "data/.checkpoints/${node.name}"  # auto-derived default
    tracking_mode: mtime        # mtime | content_hash | mtime+size
    batch_size: 100             # max files per run (optional, backpressure)
```

## Checkpoint Format

```
data/.checkpoints/
  └── raw_sensor_data/
        └── tracker.json
```

```json
{
  "version": 1,
  "last_run": "2026-03-31T14:30:00Z",
  "files": {
    "data/incoming/sensor_20260301.csv": {
      "mtime": "2026-03-01T08:00:00Z",
      "size": 1048576,
      "hash": null,
      "status": "processed",
      "processed_at": "2026-03-31T14:30:00Z"
    }
  }
}
```

---

## Phase 1: Foundation (~2 days)

| Component | Description | Location |
|---|---|---|
| `FileTrackerConfig` | Pydantic model: `enabled`, `checkpoint_path`, `tracking_mode`, `file_pattern`, `batch_size` | `config.py` |
| `FileTracker` class | Core logic: list files via `fsspec`, diff against checkpoint, return new/changed files, persist state | `odibi/state/file_tracker.py` |
| Checkpoint format | JSON file per node with file metadata and processing status | — |
| Unit tests | Tracker init, first run (all new), subsequent run (only new), empty dir, reset | `tests/unit/test_file_tracker.py` |

## Phase 2: Node Integration (~1.5 days)

| Component | Description | Location |
|---|---|---|
| Read phase hook | Before engine read, call tracker to filter file list. Pass only new files to engine | `node.py` `_execute_read_phase` |
| Post-write commit | After successful write, update checkpoint with processed files (crash safety) | `node.py` `_execute_write_phase` |
| Fallback behavior | No checkpoint = full load (first run). Missing checkpoint dir = auto-create | `node.py` |
| Integration tests | End-to-end: write files → run tracker → verify only new files read | `tests/integration/test_file_tracker.py` |

## Phase 3: Engine Support (~1.5 days)

| Component | Description | Location |
|---|---|---|
| Pandas engine | `read()` accepts file list filter, reads only specified files from glob | `pandas_engine.py` |
| Polars engine | Same interface | `polars_engine.py` |
| Spark engine | Same interface (bridges to native Auto Loader when on Databricks) | `spark_engine.py` |
| Engine parity tests | Verify all 3 engines handle filtered file lists identically | `tests/unit/test_engine_file_tracker.py` |

## Phase 4: Advanced Features (~2 days)

| Component | Description | Location |
|---|---|---|
| Tracking modes | `mtime` (fast, default), `content_hash` (reliable, slower), `mtime+size` (balanced) | `file_tracker.py` |
| File deletion detection | Optionally detect removed files and flag them | `file_tracker.py` |
| Concurrent access | File lock on checkpoint via `portalocker` (already a dependency) | `file_tracker.py` |
| Cloud protocols | Test with `abfss://`, `s3://`, `gs://` via fsspec | `tests/integration/` |
| Max files per batch | `batch_size` config to limit files per run (backpressure) | `config.py` + `file_tracker.py` |

## Phase 5: CLI & Observability (~1 day)

| Component | Description | Location |
|---|---|---|
| `odibi tracker status <node>` | Show tracked files, pending count, last run time | `cli/` |
| `odibi tracker reset <node>` | Delete checkpoint, next run = full load | `cli/` |
| `odibi tracker list <node>` | List pending (unprocessed) files | `cli/` |
| Telemetry | Log `files_tracked`, `files_new`, `files_skipped` per run | `node.py` |

## Phase 6: Docs & Recipes (~1 day)

| Component | Description | Location |
|---|---|---|
| Reference docs | YAML schema, all config options | `docs/reference/` |
| Guide | "Incremental file loading" walkthrough | `docs/guides/` |
| Recipe | `file_tracker_bronze` reusable recipe template | `odibi/recipes/` |
| Examples | CSV, Parquet, mixed-format examples | `examples/` |

## Existing Infrastructure to Leverage

- **`fsspec`** — file listing across local/cloud (already a dependency)
- **`StateManager`** — HWM get/set (optional sync target)
- **`portalocker`** — file locking for concurrent access (already a dependency)
- **`content_hash`** — SHA256 hashing utilities (already built in `odibi/utils/content_hash.py`)
- **`LazyDataset`** — Pandas engine file wrapper (already exists)

## Current Alternative

Until this is built, use **HWM-based incremental loading** with glob paths:

```yaml
read:
  path: "data/incoming/*.csv"
  format: csv
  incremental:
    mode: stateful
    key_column: timestamp
```

This reads all files each run but filters rows by the high-water mark. Works for moderate file counts.
