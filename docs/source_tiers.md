# Source Tiers - Offline Data Preparation

**Phase**: 7.E  
**Status**: Active  
**Last Updated**: 2025-12-14

---

## Overview

Source Tiers provide disk-backed, deterministic datasets at various scales for Odibi testing and development. All data is:

- **Deterministic**: Fixed seed-based transformations, fully reproducible
- **Hash-locked**: SHA256 integrity manifests for every file
- **Replayable**: No network access needed after initial download
- **Scalable**: From 1GB (tier0) to 2TB (tier2tb)

---

## Tier Summary

| Tier | Size | Datasets | Use Case |
|------|------|----------|----------|
| `tier0` | ~1GB | 1 | Quick testing, CI |
| `tier20gb` | ~20GB | 2 | Local development |
| `tier100gb` | ~100GB | 2 | Integration testing |
| `tier600gb` | ~600GB | 3 | Large-scale validation |
| `tier2tb` | ~2TB | 6 | Full production simulation |

---

## Tier Details

### tier0 (Minimal - ~1GB)

Quick testing tier for CI and rapid iteration.

| Dataset | Format | Size | Source |
|---------|--------|------|--------|
| `sample_enwiki_pages` | xml.bz2 | ~300MB | Wikimedia Foundation (Simple English Wikipedia) |

**Use Cases:**
- CI pipeline testing
- Quick smoke tests
- Schema validation

---

### tier20gb (Small - ~20GB)

Local development tier with real-world datasets.

| Dataset | Format | Size | Source | Messy Variant |
|---------|--------|------|--------|---------------|
| `enwiki_all_titles` | gz | ~400MB | Wikimedia Foundation | No |
| `osm_liechtenstein` | osm.pbf | ~3MB | OpenStreetMap / Geofabrik | Yes (seed=42) |

**Use Cases:**
- Local development testing
- Feature development
- Performance baselines

---

### tier100gb (Medium - ~100GB)

Integration testing tier with full Wikipedia and OSM extracts.

| Dataset | Format | Size | Source | Messy Variant |
|---------|--------|------|--------|---------------|
| `enwiki_pages_articles` | xml.bz2 | ~22GB | Wikimedia Foundation | Yes (seed=12345) |
| `osm_iceland` | osm.pbf | ~60MB | OpenStreetMap / Geofabrik | No |

**Messy Variant Recipe (enwiki_pages_articles):**
```yaml
operations:
  - inject_nulls
  - type_drift
  - invalid_timestamps
null_rate: 0.01
type_drift_rate: 0.005
invalid_timestamp_rate: 0.002
seed: 12345
```

---

### tier600gb (Large - ~600GB)

Large-scale validation tier with massive real-world datasets.

| Dataset | Format | Size | Source | Messy Variant |
|---------|--------|------|--------|---------------|
| `fineweb_cc_2024_10_sample` | parquet | ~500GB | Hugging Face / FineWeb | Yes (seed=77777) |
| `osm_planet` | osm.pbf | ~75GB | OpenStreetMap Foundation | No |
| `enwiki_pages_articles_full` | xml.bz2 | ~22GB | Wikimedia Foundation | No |

**FineWeb Dataset:**
- Common Crawl web text dataset
- License: ODC-BY 1.0
- Columns: `text`, `id`, `dump`, `url`, `date`, `file_path`, `language`, `language_score`

**Messy Variant Recipe (fineweb_cc_2024_10_sample):**
```yaml
operations:
  - inject_nulls
  - duplicate_rows
  - type_drift
  - truncate_lines
null_rate: 0.02
duplicate_rate: 0.01
type_drift_rate: 0.005
truncate_rate: 0.003
seed: 77777
```

---

### tier2tb (Massive - ~2TB)

Full production simulation tier with multiple FineWeb snapshots.

Includes all `tier600gb` datasets plus:

| Dataset | Format | Size | Source |
|---------|--------|------|--------|
| `fineweb_cc_2024_06_sample` | parquet | ~500GB | Hugging Face / FineWeb |
| `fineweb_cc_2024_02_sample` | parquet | ~500GB | Hugging Face / FineWeb |
| `fineweb_cc_2023_10_sample` | parquet | ~500GB | Hugging Face / FineWeb |

---

## Storage Budget

| Tier | Raw Data | Messy Variants | Metadata | Total |
|------|----------|----------------|----------|-------|
| tier0 | 1 GB | 0 GB | <1 MB | ~1 GB |
| tier20gb | 20 GB | 0.5 GB | <1 MB | ~21 GB |
| tier100gb | 100 GB | 25 GB | <1 MB | ~125 GB |
| tier600gb | 600 GB | 100 GB | <1 MB | ~700 GB |
| tier2tb | 2,000 GB | 200 GB | <1 MB | ~2.2 TB |

**Note**: Messy variants are optional and require `--build-messy-variants` flag.

---

## Directory Layout

```
.odibi/
├── source_cache/
│   ├── README.md
│   ├── integrity_manifests.json
│   │
│   └── tiers/
│       ├── tier0/
│       │   └── sample_enwiki_pages/
│       │       └── raw/
│       │           └── simplewiki-latest-pages-articles.xml.bz2
│       │
│       ├── tier20gb/
│       │   ├── enwiki_abstract/
│       │   │   └── raw/
│       │   └── osm_liechtenstein/
│       │       ├── raw/
│       │       └── messy/
│       │           ├── liechtenstein-latest_messy.osm.pbf
│       │           └── transform_log.json
│       │
│       ├── tier100gb/
│       │   ├── enwiki_pages_articles/
│       │   └── osm_iceland/
│       │
│       ├── tier600gb/
│       │   ├── fineweb_cc_2024_10_sample/
│       │   ├── osm_planet/
│       │   └── enwiki_pages_articles_full/
│       │
│       └── tier2tb/
│           └── [all tier600gb + additional fineweb snapshots]
│
└── source_metadata/
    ├── pool_index.yaml
    └── pools/
        ├── tier0_sample_enwiki_pages.yaml
        ├── tier20gb_enwiki_abstract.yaml
        ├── tier20gb_osm_liechtenstein.yaml
        ├── tier20gb_osm_liechtenstein_messy.yaml
        └── [...]
```

---

## Usage

### List Available Tiers

```bash
python scripts/prepare_source_tiers.py --list-tiers
```

### Dry Run (Show What Would Happen)

```bash
python scripts/prepare_source_tiers.py --tier tier600gb --download --verify --dry-run
```

### Download and Verify

```bash
python scripts/prepare_source_tiers.py --tier tier0 --download --verify
```

### Build Messy Variants

```bash
python scripts/prepare_source_tiers.py --tier tier100gb --build-messy-variants
```

### Full Preparation

```bash
python scripts/prepare_source_tiers.py --tier tier20gb --download --verify --build-messy-variants --emit-metadata
```

### Resume Interrupted Download

Simply rerun the same command - downloads are resume-capable:

```bash
python scripts/prepare_source_tiers.py --tier tier600gb --download
```

---

## Invariants

1. **No Network During Cycles**: Once prepared, tiers require no network access
2. **Hash Verification**: All files verified against SHA256 manifests
3. **Atomic Writes**: Temp files + atomic rename prevents corruption
4. **Deterministic Messy Variants**: Fixed seed ensures reproducible transforms
5. **Frozen Pools**: `lifecycle: FROZEN` in pool metadata
6. **No Agent Modifications**: Agents MAY NOT modify tier data

---

## Integrity Verification

All tier datasets are registered in `integrity_manifests.json`:

```json
{
  "tier600gb_fineweb_cc_2024_10_sample": {
    "algorithm": "sha256",
    "file_hashes": {
      "000_00000.parquet": "abc123..."
    },
    "manifest_hash": "def456...",
    "frozen_at": "2025-12-14T10:00:00Z",
    "frozen_by": "prepare_source_tiers.py"
  }
}
```

To verify manually:

```python
import hashlib
from pathlib import Path

def verify_file(path: Path, expected_hash: str) -> bool:
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    return actual == expected_hash
```

---

## Messy Variant Transforms

Available transform operations:

| Operation | Description |
|-----------|-------------|
| `inject_nulls` | Replace random values with empty strings/null |
| `duplicate_rows` | Insert duplicate rows at random positions |
| `type_drift` | Append units to numeric values (e.g., "100 units") |
| `truncate_lines` | Randomly truncate CSV rows |
| `invalid_timestamps` | Replace dates with "9999-99-99 00:00:00" |

All transforms are logged in `messy/transform_log.json`:

```json
[
  {
    "input": ".../raw/data.csv",
    "output": ".../messy/data_messy.csv",
    "seed": 12345,
    "recipe": {...},
    "stats": {
      "nulls_injected": 1234,
      "duplicates_added": 567,
      "total_rows_input": 100000,
      "total_rows_output": 100567
    },
    "timestamp": "2025-12-14T10:00:00Z"
  }
]
```

---

## Dataset Sources

| Source | URL | License |
|--------|-----|---------|
| Wikimedia Foundation | https://dumps.wikimedia.org/ | CC-BY-SA 4.0 |
| OpenStreetMap | https://planet.openstreetmap.org/ | ODbL 1.0 |
| OpenStreetMap (Geofabrik) | https://download.geofabrik.de/ | ODbL 1.0 |
| Hugging Face FineWeb | https://huggingface.co/datasets/HuggingFaceFW/fineweb | ODC-BY 1.0 |

---

## Troubleshooting

### Download Interrupted

Downloads are resume-capable. Simply rerun the command.

### Hash Mismatch

If a file fails verification:

1. Delete the corrupted file
2. Rerun with `--download --verify`

### Disk Space

Check available space before large tier downloads:

```bash
# Windows
wmic logicaldisk get size,freespace,caption

# Linux/Mac
df -h
```

### Permission Errors

Ensure write permissions to `.odibi/source_cache/tiers/`.

---

## Related Documentation

- [Source Pools Design](source_pools_design.md)
- [Source Cache README](.odibi/source_cache/README.md)
- [Pool Index](.odibi/source_metadata/pool_index.yaml)
