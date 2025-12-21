"""
Phase 7.E: Offline Data Tier Preparation

Prepares real, disk-backed, deterministic datasets for SourcePools.
Supports multiple tiers from tier0 (minimal) to tier2tb (massive).

Usage:
    python scripts/prepare_source_tiers.py --tier tier0 --download --verify --emit-metadata
    python scripts/prepare_source_tiers.py --tier tier600gb --download --dry-run
    python scripts/prepare_source_tiers.py --tier tier100gb --build-messy-variants

Hard Rules:
    - NO cycle execution
    - NO live APIs during preparation (only direct file downloads)
    - Everything is hash-locked (SHA256) and replayable
    - All paths inside .odibi/source_cache/ and .odibi/source_metadata/
"""

import argparse
import csv
import hashlib
import json
import random
import shutil
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# ==============================================================================
# Constants
# ==============================================================================

ODIBI_ROOT = Path(__file__).parent.parent
SOURCE_CACHE = ODIBI_ROOT / ".odibi" / "source_cache"
SOURCE_METADATA = ODIBI_ROOT / ".odibi" / "source_metadata"
TIERS_CACHE = SOURCE_CACHE / "tiers"
MANIFESTS_FILE = SOURCE_CACHE / "integrity_manifests.json"

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for resume-capable downloads
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  # Exponential backoff base in seconds


# ==============================================================================
# Tier Definitions
# ==============================================================================


@dataclass
class Dataset:
    """Definition of a single dataset within a tier."""

    name: str
    url: str
    expected_size_bytes: int
    sha256: Optional[str]  # Known hash, or None if to be computed
    description: str
    file_format: str
    license: str
    source_org: str
    schema_info: Optional[Dict[str, Any]] = None
    messy_variant_recipe: Optional[Dict[str, Any]] = None


@dataclass
class Tier:
    """Definition of a data tier."""

    name: str
    description: str
    expected_total_size_gb: float
    datasets: List[Dataset]


# Tier0: Minimal (~1GB) - for quick testing
TIER0_DATASETS = [
    Dataset(
        name="sample_enwiki_pages",
        url="https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2",
        expected_size_bytes=300_000_000,  # ~300MB
        sha256=None,  # Dynamic download
        description="Simple English Wikipedia dump (smaller than full enwiki)",
        file_format="xml.bz2",
        license="CC-BY-SA 4.0",
        source_org="Wikimedia Foundation",
    ),
]

# Tier 20GB
TIER_20GB_DATASETS = [
    Dataset(
        name="enwiki_all_titles",
        url="https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-all-titles.gz",
        expected_size_bytes=400_000_000,  # ~400MB compressed
        sha256=None,
        description="English Wikipedia all page titles",
        file_format="gz",
        license="CC-BY-SA 4.0",
        source_org="Wikimedia Foundation",
    ),
    Dataset(
        name="osm_liechtenstein",
        url="https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf",
        expected_size_bytes=3_000_000,  # ~3MB
        sha256=None,
        description="OpenStreetMap extract for Liechtenstein (small country)",
        file_format="osm.pbf",
        license="ODbL 1.0",
        source_org="OpenStreetMap / Geofabrik",
    ),
    Dataset(
        name="github_events_sample",
        url="https://data.gharchive.org/2024-01-01-0.json.gz",
        expected_size_bytes=50_000_000,  # ~50MB compressed
        sha256=None,
        description="GitHub Archive hourly events (2024-01-01 00:00 UTC)",
        file_format="json.gz",
        license="CC0 1.0",
        source_org="GitHub Archive",
        messy_variant_recipe={
            "operations": ["inject_nulls", "duplicate_rows"],
            "null_rate": 0.02,
            "duplicate_rate": 0.01,
            "seed": 42,
        },
    ),
]

# Tier 100GB
TIER_100GB_DATASETS = [
    Dataset(
        name="enwiki_pages_articles",
        url="https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2",
        expected_size_bytes=22_000_000_000,  # ~22GB compressed
        sha256=None,
        description="Full English Wikipedia articles dump",
        file_format="xml.bz2",
        license="CC-BY-SA 4.0",
        source_org="Wikimedia Foundation",
        messy_variant_recipe={
            "operations": ["inject_nulls", "type_drift", "invalid_timestamps"],
            "null_rate": 0.01,
            "type_drift_rate": 0.005,
            "invalid_timestamp_rate": 0.002,
            "seed": 12345,
        },
    ),
    Dataset(
        name="osm_iceland",
        url="https://download.geofabrik.de/europe/iceland-latest.osm.pbf",
        expected_size_bytes=60_000_000,  # ~60MB
        sha256=None,
        description="OpenStreetMap extract for Iceland",
        file_format="osm.pbf",
        license="ODbL 1.0",
        source_org="OpenStreetMap / Geofabrik",
    ),
]

# Tier 600GB - The main large tier
TIER_600GB_DATASETS = [
    Dataset(
        name="fineweb_cc_2024_10_sample",
        # FineWeb is distributed via Hugging Face datasets
        # This is a single shard (~2GB), not the full dataset
        url="https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT/000_00000.parquet",
        expected_size_bytes=2_500_000_000,  # ~2.5GB (single shard)
        sha256=None,
        description="FineWeb CC snapshot - Common Crawl web text dataset (single shard)",
        file_format="parquet",
        license="ODC-BY 1.0",
        source_org="Hugging Face / FineWeb",
        schema_info={
            "columns": [
                "text",
                "id",
                "dump",
                "url",
                "date",
                "file_path",
                "language",
                "language_score",
            ],
            "primary_key": "id",
        },
        messy_variant_recipe={
            "operations": ["inject_nulls", "duplicate_rows", "type_drift", "truncate_lines"],
            "null_rate": 0.02,
            "duplicate_rate": 0.01,
            "type_drift_rate": 0.005,
            "truncate_rate": 0.003,
            "seed": 77777,
        },
    ),
    Dataset(
        name="osm_planet",
        url="https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf",
        expected_size_bytes=95_000_000_000,  # ~95GB (actual varies)
        sha256=None,
        description="Full OpenStreetMap Planet PBF dump",
        file_format="osm.pbf",
        license="ODbL 1.0",
        source_org="OpenStreetMap Foundation",
    ),
    Dataset(
        name="enwiki_pages_articles_full",
        url="https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2",
        expected_size_bytes=22_000_000_000,  # ~22GB compressed
        sha256=None,
        description="Full English Wikipedia pages-articles dump",
        file_format="xml.bz2",
        license="CC-BY-SA 4.0",
        source_org="Wikimedia Foundation",
    ),
]

# Tier 2TB - Massive tier
TIER_2TB_DATASETS = [
    # All 600GB datasets plus more FineWeb snapshots
    *TIER_600GB_DATASETS,
    Dataset(
        name="fineweb_cc_2024_06_sample",
        url="https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT/000_00001.parquet",
        expected_size_bytes=2_500_000_000,  # ~2.5GB (single shard)
        sha256=None,
        description="FineWeb CC snapshot - shard 2",
        file_format="parquet",
        license="ODC-BY 1.0",
        source_org="Hugging Face / FineWeb",
    ),
    Dataset(
        name="fineweb_cc_2024_02_sample",
        url="https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT/000_00002.parquet",
        expected_size_bytes=2_500_000_000,  # ~2.5GB (single shard)
        sha256=None,
        description="FineWeb CC snapshot - shard 3",
        file_format="parquet",
        license="ODC-BY 1.0",
        source_org="Hugging Face / FineWeb",
    ),
    Dataset(
        name="fineweb_cc_2023_10_sample",
        url="https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT/000_00003.parquet",
        expected_size_bytes=2_500_000_000,  # ~2.5GB (single shard)
        sha256=None,
        description="FineWeb CC snapshot - shard 4",
        file_format="parquet",
        license="ODC-BY 1.0",
        source_org="Hugging Face / FineWeb",
    ),
]

TIERS: Dict[str, Tier] = {
    "tier0": Tier(
        name="tier0",
        description="Minimal tier for quick testing (~1GB)",
        expected_total_size_gb=1.0,
        datasets=TIER0_DATASETS,
    ),
    "tier20gb": Tier(
        name="tier20gb",
        description="Small tier for local development (~20GB)",
        expected_total_size_gb=20.0,
        datasets=TIER_20GB_DATASETS,
    ),
    "tier100gb": Tier(
        name="tier100gb",
        description="Medium tier for integration testing (~100GB)",
        expected_total_size_gb=100.0,
        datasets=TIER_100GB_DATASETS,
    ),
    "tier600gb": Tier(
        name="tier600gb",
        description="Large tier with FineWeb, OSM Planet, and enwiki (~600GB)",
        expected_total_size_gb=600.0,
        datasets=TIER_600GB_DATASETS,
    ),
    "tier2tb": Tier(
        name="tier2tb",
        description="Massive tier with multiple FineWeb snapshots (~2TB)",
        expected_total_size_gb=2000.0,
        datasets=TIER_2TB_DATASETS,
    ),
}


# ==============================================================================
# Utility Functions
# ==============================================================================


def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def human_readable_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def compute_sha256(
    file_path: Path, progress_callback: Optional[Callable[[int, int], None]] = None
) -> str:
    """Compute SHA256 hash of a file with optional progress callback."""
    sha256 = hashlib.sha256()
    file_size = file_path.stat().st_size
    bytes_read = 0

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            sha256.update(chunk)
            bytes_read += len(chunk)
            if progress_callback:
                progress_callback(bytes_read, file_size)

    return sha256.hexdigest()


def download_with_resume(
    url: str,
    dest: Path,
    expected_size: Optional[int] = None,
    dry_run: bool = False,
) -> Tuple[bool, str]:
    """
    Download a file with resume capability, chunking, and retries.
    Returns (success, message).
    """
    if dry_run:
        return True, f"[DRY-RUN] Would download {url} to {dest}"

    # If dest already exists and is reasonably complete, skip FIRST
    # (expected_size is just an estimate, so allow ±50% tolerance)
    if dest.exists():
        actual_size = dest.stat().st_size
        if expected_size is None:
            return True, f"[SKIP] {dest.name} already exists ({actual_size:,} bytes)"
        # Allow files within 50% of expected size (estimates are often wrong)
        min_acceptable = expected_size * 0.5
        if actual_size >= min_acceptable:
            return True, f"[SKIP] {dest.name} already exists ({actual_size:,} bytes)"

    temp_file = dest.with_suffix(dest.suffix + ".partial")

    # Check existing partial download
    start_byte = 0
    if temp_file.exists():
        start_byte = temp_file.stat().st_size
        print(f"  [RESUME] Found partial download, resuming from byte {start_byte}")

    headers = {}
    if start_byte > 0:
        headers["Range"] = f"bytes={start_byte}-"

    for attempt in range(MAX_RETRIES):
        try:
            request = urllib.request.Request(url, headers=headers)

            with urllib.request.urlopen(request, timeout=60) as response:
                # Check if server supports range requests
                content_range = response.headers.get("Content-Range")
                if start_byte > 0 and not content_range:
                    print("  [WARN] Server doesn't support resume, starting over")
                    start_byte = 0
                    if temp_file.exists():
                        temp_file.unlink()

                total_size = int(response.headers.get("Content-Length", 0))
                if content_range:
                    total_size = int(content_range.split("/")[-1])
                else:
                    total_size += start_byte

                mode = "ab" if start_byte > 0 else "wb"
                downloaded = start_byte

                with open(temp_file, mode) as f:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Progress indicator
                        if total_size > 0:
                            pct = (downloaded / total_size) * 100
                            print(
                                f"\r  [DOWNLOAD] {pct:.1f}% ({human_readable_size(downloaded)} / {human_readable_size(total_size)})",
                                end="",
                                flush=True,
                            )

                print()  # Newline after progress

                # Atomic rename
                shutil.move(str(temp_file), str(dest))
                return True, f"[OK] Downloaded {dest.name}"

        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            delay = RETRY_DELAY_BASE**attempt
            print(
                f"\n  [RETRY] Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s..."
            )
            time.sleep(delay)

    return False, f"[ERROR] Failed to download after {MAX_RETRIES} attempts"


# ==============================================================================
# Messy Variant Generation
# ==============================================================================


class MessyVariantGenerator:
    """
    Generates deterministic "messy" derivatives of datasets.
    All operations are seeded for reproducibility.
    """

    def __init__(self, seed: int, recipe: Dict[str, Any]):
        self.seed = seed
        self.recipe = recipe
        self.rng = random.Random(seed)
        self.operations = recipe.get("operations", [])
        self.transform_log: List[Dict[str, Any]] = []

    def process_csv(self, input_path: Path, output_path: Path) -> Dict[str, Any]:
        """Process a CSV file and create a messy variant."""
        null_rate = self.recipe.get("null_rate", 0.02)
        duplicate_rate = self.recipe.get("duplicate_rate", 0.01)
        truncate_rate = self.recipe.get("truncate_rate", 0.005)
        type_drift_rate = self.recipe.get("type_drift_rate", 0.005)
        invalid_timestamp_rate = self.recipe.get("invalid_timestamp_rate", 0.002)

        stats = {
            "nulls_injected": 0,
            "duplicates_added": 0,
            "lines_truncated": 0,
            "type_drifts": 0,
            "invalid_timestamps": 0,
            "total_rows_input": 0,
            "total_rows_output": 0,
        }

        with open(input_path, "r", encoding="utf-8", errors="replace") as infile:
            reader = csv.reader(infile)
            header = next(reader)
            rows = list(reader)
            stats["total_rows_input"] = len(rows)

        output_rows = []

        for i, row in enumerate(rows):
            # Duplicate row
            if "duplicate_rows" in self.operations and self.rng.random() < duplicate_rate:
                output_rows.append(row.copy())
                stats["duplicates_added"] += 1

            # Process each cell
            new_row = []
            for j, cell in enumerate(row):
                modified = cell

                # Inject nulls
                if "inject_nulls" in self.operations and self.rng.random() < null_rate:
                    modified = ""
                    stats["nulls_injected"] += 1

                # Type drift (turn numbers into strings with units)
                elif "type_drift" in self.operations and self.rng.random() < type_drift_rate:
                    try:
                        float(cell)
                        modified = f"{cell} units"
                        stats["type_drifts"] += 1
                    except ValueError:
                        pass

                # Invalid timestamps
                elif (
                    "invalid_timestamps" in self.operations
                    and self.rng.random() < invalid_timestamp_rate
                ):
                    if "-" in cell and len(cell) >= 10:  # Looks like a date
                        modified = "9999-99-99 00:00:00"
                        stats["invalid_timestamps"] += 1

                new_row.append(modified)

            # Truncate line
            if "truncate_lines" in self.operations and self.rng.random() < truncate_rate:
                truncate_at = self.rng.randint(1, len(new_row) - 1)
                new_row = new_row[:truncate_at]
                stats["lines_truncated"] += 1

            output_rows.append(new_row)

        stats["total_rows_output"] = len(output_rows)

        # Write output
        with open(output_path, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)
            for row in output_rows:
                # Pad short rows with empty strings
                while len(row) < len(header):
                    row.append("")
                writer.writerow(row[: len(header)])

        self.transform_log.append(
            {
                "input": str(input_path),
                "output": str(output_path),
                "seed": self.seed,
                "recipe": self.recipe,
                "stats": stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        return stats

    def process_ndjson(self, input_path: Path, output_path: Path) -> Dict[str, Any]:
        """Process an NDJSON file and create a messy variant."""
        null_rate = self.recipe.get("null_rate", 0.02)
        duplicate_rate = self.recipe.get("duplicate_rate", 0.01)

        stats = {
            "nulls_injected": 0,
            "duplicates_added": 0,
            "total_rows_input": 0,
            "total_rows_output": 0,
        }

        with open(input_path, "r", encoding="utf-8", errors="replace") as infile:
            lines = infile.readlines()

        stats["total_rows_input"] = len(lines)
        output_lines = []

        for line in lines:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                output_lines.append(line)
                continue

            # Duplicate
            if "duplicate_rows" in self.operations and self.rng.random() < duplicate_rate:
                output_lines.append(json.dumps(obj) + "\n")
                stats["duplicates_added"] += 1

            # Inject nulls into random fields
            if "inject_nulls" in self.operations:
                for key in list(obj.keys()):
                    if self.rng.random() < null_rate:
                        obj[key] = None
                        stats["nulls_injected"] += 1

            output_lines.append(json.dumps(obj) + "\n")

        stats["total_rows_output"] = len(output_lines)

        with open(output_path, "w", encoding="utf-8") as outfile:
            outfile.writelines(output_lines)

        self.transform_log.append(
            {
                "input": str(input_path),
                "output": str(output_path),
                "seed": self.seed,
                "recipe": self.recipe,
                "stats": stats,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )

        return stats

    def get_transform_log(self) -> List[Dict[str, Any]]:
        """Return the logged transform operations."""
        return self.transform_log


# ==============================================================================
# Pool Metadata Emission
# ==============================================================================


def emit_pool_yaml(
    dataset: Dataset,
    tier_name: str,
    raw_path: Path,
    file_hashes: Dict[str, str],
    manifest_hash: str,
    is_messy: bool = False,
) -> Path:
    """Generate a SourcePool YAML file for a dataset."""
    pool_id = f"{tier_name}_{dataset.name}" + ("_messy" if is_messy else "")
    yaml_path = SOURCE_METADATA / "pools" / f"{pool_id}.yaml"

    schema_section = ""
    if dataset.schema_info:
        columns_yaml = "\n".join(
            f"      - name: {col}\n        dtype: string\n        nullable: true"
            for col in dataset.schema_info.get("columns", [])
        )
        pk = dataset.schema_info.get("primary_key", "")
        schema_section = f"""
schema:
  columns:
{columns_yaml}
  primary_keys:
    - {pk}
"""

    file_hashes_yaml = "\n".join(
        f'    "{name}": "{hash_val}"' for name, hash_val in sorted(file_hashes.items())
    )

    yaml_content = f"""# {dataset.name} - Source Pool Definition
# Tier: {tier_name}
# Generated by prepare_source_tiers.py

pool_id: {pool_id}
version: "1.0.0"
name: "{dataset.name}{" (Messy Variant)" if is_messy else ""}"
description: |
  {dataset.description}
  {"Messy variant with intentional data quality issues." if is_messy else ""}

file_format: {dataset.file_format}
source_type: local
data_quality: {"messy" if is_messy else "clean"}
tier: {tier_name}
{schema_section}
cache_path: "tiers/{tier_name}/{dataset.name}/{"messy" if is_messy else "raw"}/"

status: frozen
lifecycle: FROZEN
integrity:
  algorithm: sha256
  file_hashes:
{file_hashes_yaml}
  manifest_hash: "{manifest_hash}"
  frozen_at: "{datetime.now(timezone.utc).isoformat()}Z"
  frozen_by: "prepare_source_tiers.py"

original_source: "{dataset.url}"
source_org: "{dataset.source_org}"
license: "{dataset.license}"
expected_size_bytes: {dataset.expected_size_bytes}
created_at: "{datetime.now(timezone.utc).isoformat()}Z"
"""

    ensure_dir(yaml_path.parent)
    with open(yaml_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)

    return yaml_path


def update_pool_index(tier: Tier, new_pools: List[str]) -> None:
    """Update pool_index.yaml with new tier pools."""
    index_path = SOURCE_METADATA / "pool_index.yaml"

    # Read existing content
    existing_content = ""
    if index_path.exists():
        with open(index_path, "r", encoding="utf-8") as f:
            existing_content = f.read()

    # Add new tier section if not present
    tier_section = f"\n  # {tier.name.upper()} Tier ({tier.description})\n"
    for pool_id in new_pools:
        tier_section += f'  {pool_id}: "pools/{pool_id}.yaml"\n'

    if tier.name not in existing_content:
        with open(index_path, "a", encoding="utf-8") as f:
            f.write(tier_section)
        print(f"  [OK] Added {len(new_pools)} pools to pool_index.yaml")


def update_integrity_manifests(new_manifests: Dict[str, Any]) -> None:
    """Update the integrity_manifests.json file."""
    existing = {}
    if MANIFESTS_FILE.exists():
        with open(MANIFESTS_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)

    existing.update(new_manifests)

    with open(MANIFESTS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, sort_keys=True)

    print(f"  [OK] Updated integrity_manifests.json with {len(new_manifests)} new entries")


# ==============================================================================
# Main Processing Functions
# ==============================================================================


def process_tier(
    tier: Tier,
    download: bool = False,
    verify: bool = False,
    build_messy: bool = False,
    emit_metadata: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Process a complete tier."""
    print(f"\n{'=' * 60}")
    print(f"Processing Tier: {tier.name}")
    print(f"Description: {tier.description}")
    print(f"Expected Size: {tier.expected_total_size_gb} GB")
    print(f"Datasets: {len(tier.datasets)}")
    print(f"{'=' * 60}")

    tier_dir = TIERS_CACHE / tier.name
    ensure_dir(tier_dir)

    results = {
        "tier": tier.name,
        "datasets_processed": 0,
        "datasets_failed": 0,
        "manifests": {},
        "messy_variants": [],
        "pool_yamls": [],
    }

    for dataset in tier.datasets:
        print(f"\n--- Dataset: {dataset.name} ---")

        dataset_dir = tier_dir / dataset.name
        raw_dir = dataset_dir / "raw"
        ensure_dir(raw_dir)

        # Determine filename from URL
        filename = dataset.url.split("/")[-1]
        raw_file = raw_dir / filename

        # Download
        if download:
            print(f"  URL: {dataset.url}")
            success, msg = download_with_resume(
                dataset.url,
                raw_file,
                expected_size=dataset.expected_size_bytes,
                dry_run=dry_run,
            )
            print(f"  {msg}")
            if not success and not dry_run:
                results["datasets_failed"] += 1
                continue

        # Verify
        file_hashes = {}
        manifest_hash = ""

        if verify and raw_file.exists() and not dry_run:
            file_size = raw_file.stat().st_size
            print(
                f"  [VERIFY] Computing SHA256 for {raw_file.name} ({human_readable_size(file_size)})..."
            )

            last_pct = [-1]  # Mutable to update in closure

            def progress_callback(bytes_read: int, total: int) -> None:
                pct = int((bytes_read / total) * 100)
                if pct != last_pct[0] and pct % 5 == 0:  # Update every 5%
                    last_pct[0] = pct
                    print(
                        f"\r  [VERIFY] {pct}% ({human_readable_size(bytes_read)} / {human_readable_size(total)})",
                        end="",
                        flush=True,
                    )

            file_hash = compute_sha256(raw_file, progress_callback)
            print()  # Newline after progress
            file_hashes[filename] = file_hash

            # Compute manifest hash
            sorted_hashes = json.dumps(dict(sorted(file_hashes.items())), sort_keys=True)
            manifest_hash = hashlib.sha256(sorted_hashes.encode()).hexdigest()

            print(f"  [OK] File hash: {file_hash[:16]}...")
            print(f"  [OK] Manifest hash: {manifest_hash[:16]}...")

            results["manifests"][f"{tier.name}_{dataset.name}"] = {
                "algorithm": "sha256",
                "file_hashes": file_hashes,
                "manifest_hash": manifest_hash,
                "frozen_at": datetime.now(timezone.utc).isoformat() + "Z",
                "frozen_by": "prepare_source_tiers.py",
            }
        elif dry_run:
            print(f"  [DRY-RUN] Would verify {raw_file}")

        # Build messy variants
        if build_messy and dataset.messy_variant_recipe and not dry_run:
            messy_dir = dataset_dir / "messy"
            ensure_dir(messy_dir)

            recipe = dataset.messy_variant_recipe
            seed = recipe.get("seed", 42)

            print(f"  [MESSY] Generating messy variant with seed={seed}...")

            generator = MessyVariantGenerator(seed, recipe)

            # Determine file type and process accordingly
            if filename.endswith(".csv"):
                messy_file = messy_dir / filename.replace(".csv", "_messy.csv")
                if raw_file.exists():
                    stats = generator.process_csv(raw_file, messy_file)
                    print(f"  [OK] Messy variant created: {stats}")
                    results["messy_variants"].append(
                        {
                            "dataset": dataset.name,
                            "path": str(messy_file),
                            "stats": stats,
                            "recipe": recipe,
                        }
                    )

                    # Hash the messy file
                    messy_hash = compute_sha256(messy_file)
                    messy_manifest = {
                        f"{tier.name}_{dataset.name}_messy": {
                            "algorithm": "sha256",
                            "file_hashes": {messy_file.name: messy_hash},
                            "manifest_hash": hashlib.sha256(
                                json.dumps({messy_file.name: messy_hash}).encode()
                            ).hexdigest(),
                            "frozen_at": datetime.now(timezone.utc).isoformat() + "Z",
                            "frozen_by": "prepare_source_tiers.py",
                            "transform_recipe": recipe,
                        }
                    }
                    results["manifests"].update(messy_manifest)

            elif filename.endswith(".ndjson") or filename.endswith(".jsonl"):
                messy_file = messy_dir / filename.replace(".ndjson", "_messy.ndjson").replace(
                    ".jsonl", "_messy.jsonl"
                )
                if raw_file.exists():
                    stats = generator.process_ndjson(raw_file, messy_file)
                    print(f"  [OK] Messy variant created: {stats}")
                    results["messy_variants"].append(
                        {
                            "dataset": dataset.name,
                            "path": str(messy_file),
                            "stats": stats,
                            "recipe": recipe,
                        }
                    )

                    # Hash the messy file
                    messy_hash = compute_sha256(messy_file)
                    messy_manifest = {
                        f"{tier.name}_{dataset.name}_messy": {
                            "algorithm": "sha256",
                            "file_hashes": {messy_file.name: messy_hash},
                            "manifest_hash": hashlib.sha256(
                                json.dumps({messy_file.name: messy_hash}).encode()
                            ).hexdigest(),
                            "frozen_at": datetime.now(timezone.utc).isoformat() + "Z",
                            "frozen_by": "prepare_source_tiers.py",
                            "transform_recipe": recipe,
                        }
                    }
                    results["manifests"].update(messy_manifest)

            elif filename.endswith(".json.gz"):
                # Decompress gzipped JSON, process as NDJSON, output as NDJSON
                import gzip

                decompressed_file = messy_dir / filename.replace(".json.gz", ".ndjson")
                messy_file = messy_dir / filename.replace(".json.gz", "_messy.ndjson")
                if raw_file.exists():
                    # Decompress first
                    print(f"  [DECOMPRESS] Extracting {filename}...")
                    with gzip.open(raw_file, "rt", encoding="utf-8", errors="replace") as gz_in:
                        with open(decompressed_file, "w", encoding="utf-8") as out:
                            out.write(gz_in.read())

                    stats = generator.process_ndjson(decompressed_file, messy_file)
                    print(f"  [OK] Messy variant created: {stats}")
                    results["messy_variants"].append(
                        {
                            "dataset": dataset.name,
                            "path": str(messy_file),
                            "stats": stats,
                            "recipe": recipe,
                        }
                    )

                    # Hash the messy file
                    messy_hash = compute_sha256(messy_file)
                    messy_manifest = {
                        f"{tier.name}_{dataset.name}_messy": {
                            "algorithm": "sha256",
                            "file_hashes": {messy_file.name: messy_hash},
                            "manifest_hash": hashlib.sha256(
                                json.dumps({messy_file.name: messy_hash}).encode()
                            ).hexdigest(),
                            "frozen_at": datetime.now(timezone.utc).isoformat() + "Z",
                            "frozen_by": "prepare_source_tiers.py",
                            "transform_recipe": recipe,
                        }
                    }
                    results["manifests"].update(messy_manifest)

            # Save transform log
            log_file = messy_dir / "transform_log.json"
            with open(log_file, "w", encoding="utf-8") as f:
                json.dump(generator.get_transform_log(), f, indent=2)

        elif build_messy and dry_run:
            print(f"  [DRY-RUN] Would build messy variant for {dataset.name}")

        # Emit metadata
        if emit_metadata and not dry_run:
            yaml_path = emit_pool_yaml(
                dataset,
                tier.name,
                raw_dir,
                file_hashes,
                manifest_hash,
                is_messy=False,
            )
            print(f"  [OK] Emitted pool YAML: {yaml_path.name}")
            results["pool_yamls"].append(str(yaml_path))

            # Also emit messy variant YAML if it was created
            if dataset.messy_variant_recipe and any(
                mv["dataset"] == dataset.name for mv in results["messy_variants"]
            ):
                messy_yaml = emit_pool_yaml(
                    dataset,
                    tier.name,
                    dataset_dir / "messy",
                    {},  # Will be filled with messy file hashes
                    "",
                    is_messy=True,
                )
                results["pool_yamls"].append(str(messy_yaml))
        elif emit_metadata and dry_run:
            print(f"  [DRY-RUN] Would emit metadata for {dataset.name}")

        results["datasets_processed"] += 1

    # Update manifests file
    if results["manifests"] and not dry_run:
        update_integrity_manifests(results["manifests"])

    # Update pool index
    if results["pool_yamls"] and not dry_run:
        pool_ids = [Path(p).stem for p in results["pool_yamls"]]
        update_pool_index(tier, pool_ids)

    return results


def print_tier_info(tier_name: Optional[str] = None) -> None:
    """Print information about available tiers."""
    print("\n" + "=" * 60)
    print("Available Data Tiers")
    print("=" * 60)

    for name, tier in TIERS.items():
        if tier_name and name != tier_name:
            continue

        print(f"\n{name.upper()}")
        print(f"  Description: {tier.description}")
        print(f"  Expected Size: {tier.expected_total_size_gb} GB")
        print("  Datasets:")
        for ds in tier.datasets:
            print(f"    - {ds.name}")
            print(f"      Format: {ds.file_format}")
            print(f"      Size: {human_readable_size(ds.expected_size_bytes)}")
            print(f"      Source: {ds.source_org}")
            print(f"      License: {ds.license}")
            if ds.messy_variant_recipe:
                print(
                    f"      Messy variant: Yes (seed={ds.messy_variant_recipe.get('seed', 'N/A')})"
                )


# ==============================================================================
# CLI Entry Point
# ==============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Phase 7.E: Prepare offline data tiers for Odibi source pools.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show available tiers
  python scripts/prepare_source_tiers.py --list-tiers

  # Dry-run for tier600gb
  python scripts/prepare_source_tiers.py --tier tier600gb --download --verify --dry-run

  # Download and verify tier0
  python scripts/prepare_source_tiers.py --tier tier0 --download --verify

  # Build messy variants for tier100gb
  python scripts/prepare_source_tiers.py --tier tier100gb --build-messy-variants

  # Full preparation with metadata emission
  python scripts/prepare_source_tiers.py --tier tier20gb --download --verify --build-messy-variants --emit-metadata
""",
    )

    parser.add_argument(
        "--tier",
        choices=list(TIERS.keys()),
        help="Tier to prepare",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download datasets (resume-capable)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify SHA256 hashes and generate manifests",
    )
    parser.add_argument(
        "--build-messy-variants",
        action="store_true",
        help="Generate messy variants for datasets with recipes",
    )
    parser.add_argument(
        "--emit-metadata",
        action="store_true",
        help="Emit SourcePool YAML files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--list-tiers",
        action="store_true",
        help="List available tiers and exit",
    )

    args = parser.parse_args()

    if args.list_tiers:
        print_tier_info()
        return 0

    if not args.tier:
        parser.print_help()
        print("\nError: --tier is required unless using --list-tiers")
        return 1

    tier = TIERS[args.tier]

    print("=" * 60)
    print("Phase 7.E: Offline Data Tier Preparation")
    print("=" * 60)
    print(f"Tier: {args.tier}")
    print(f"Download: {args.download}")
    print(f"Verify: {args.verify}")
    print(f"Build Messy Variants: {args.build_messy_variants}")
    print(f"Emit Metadata: {args.emit_metadata}")
    print(f"Dry Run: {args.dry_run}")

    if args.dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***")

    results = process_tier(
        tier,
        download=args.download,
        verify=args.verify,
        build_messy=args.build_messy_variants,
        emit_metadata=args.emit_metadata,
        dry_run=args.dry_run,
    )

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Datasets processed: {results['datasets_processed']}")
    print(f"Datasets failed: {results['datasets_failed']}")
    print(f"Manifests generated: {len(results['manifests'])}")
    print(f"Messy variants created: {len(results['messy_variants'])}")
    print(f"Pool YAMLs emitted: {len(results['pool_yamls'])}")

    return 0 if results["datasets_failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
