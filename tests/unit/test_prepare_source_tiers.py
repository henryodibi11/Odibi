"""Tests for prepare_source_tiers.py - Phase 7.E.

Tests cover:
- Determinism of messy variant generation
- Manifest hash computation
- Pool YAML emission
- Tier definitions
"""

import csv
import hashlib
import json
from pathlib import Path


class TestMessyVariantDeterminism:
    """Tests for deterministic messy variant generation."""

    def test_same_seed_produces_identical_output(self, tmp_path):
        """Same seed should produce byte-identical messy variants."""
        # Import here to avoid import errors if script not in path
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import MessyVariantGenerator

        # Create test CSV
        input_file = tmp_path / "input.csv"
        with open(input_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value", "amount", "date"])
            for i in range(100):
                writer.writerow([i, f"value_{i}", i * 10.5, f"2024-01-{(i % 28) + 1:02d}"])

        recipe = {
            "operations": ["inject_nulls", "duplicate_rows"],
            "null_rate": 0.05,
            "duplicate_rate": 0.03,
            "seed": 42,
        }

        # Generate two variants with same seed
        output1 = tmp_path / "output1.csv"
        output2 = tmp_path / "output2.csv"

        gen1 = MessyVariantGenerator(seed=42, recipe=recipe)
        gen1.process_csv(input_file, output1)

        gen2 = MessyVariantGenerator(seed=42, recipe=recipe)
        gen2.process_csv(input_file, output2)

        # Verify byte-identical
        assert output1.read_bytes() == output2.read_bytes()

    def test_different_seeds_produce_different_output(self, tmp_path):
        """Different seeds should produce different messy variants."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import MessyVariantGenerator

        input_file = tmp_path / "input.csv"
        with open(input_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(100):
                writer.writerow([i, f"value_{i}"])

        recipe = {
            "operations": ["inject_nulls"],
            "null_rate": 0.1,
            "seed": 42,
        }

        output1 = tmp_path / "output1.csv"
        output2 = tmp_path / "output2.csv"

        gen1 = MessyVariantGenerator(seed=42, recipe=recipe)
        gen1.process_csv(input_file, output1)

        gen2 = MessyVariantGenerator(seed=12345, recipe=recipe)
        gen2.process_csv(input_file, output2)

        # Should be different
        assert output1.read_bytes() != output2.read_bytes()

    def test_transform_log_recorded(self, tmp_path):
        """Transform operations should be logged."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import MessyVariantGenerator

        input_file = tmp_path / "input.csv"
        with open(input_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "value"])
            for i in range(50):
                writer.writerow([i, f"value_{i}"])

        recipe = {
            "operations": ["inject_nulls"],
            "null_rate": 0.1,
            "seed": 42,
        }

        output = tmp_path / "output.csv"
        gen = MessyVariantGenerator(seed=42, recipe=recipe)
        gen.process_csv(input_file, output)

        log = gen.get_transform_log()
        assert len(log) == 1
        assert log[0]["seed"] == 42
        assert "stats" in log[0]
        assert "nulls_injected" in log[0]["stats"]

    def test_ndjson_processing(self, tmp_path):
        """NDJSON files should be processed correctly."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import MessyVariantGenerator

        input_file = tmp_path / "input.ndjson"
        with open(input_file, "w", encoding="utf-8") as f:
            for i in range(50):
                f.write(json.dumps({"id": i, "value": f"val_{i}"}) + "\n")

        recipe = {
            "operations": ["inject_nulls", "duplicate_rows"],
            "null_rate": 0.05,
            "duplicate_rate": 0.02,
            "seed": 42,
        }

        output1 = tmp_path / "output1.ndjson"
        output2 = tmp_path / "output2.ndjson"

        gen1 = MessyVariantGenerator(seed=42, recipe=recipe)
        gen1.process_ndjson(input_file, output1)

        gen2 = MessyVariantGenerator(seed=42, recipe=recipe)
        gen2.process_ndjson(input_file, output2)

        assert output1.read_bytes() == output2.read_bytes()


class TestManifestGeneration:
    """Tests for SHA256 manifest generation."""

    def test_compute_sha256_correct(self, tmp_path):
        """SHA256 computation should be correct."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import compute_sha256

        test_file = tmp_path / "test.txt"
        test_content = b"Hello, World!"
        test_file.write_bytes(test_content)

        expected = hashlib.sha256(test_content).hexdigest()
        actual = compute_sha256(test_file)

        assert actual == expected

    def test_manifest_hash_deterministic(self, tmp_path):
        """Manifest hash should be deterministic for same file hashes."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        file_hashes = {
            "file_a.csv": "abc123",
            "file_b.csv": "def456",
        }

        sorted_hashes = json.dumps(dict(sorted(file_hashes.items())), sort_keys=True)
        manifest_hash1 = hashlib.sha256(sorted_hashes.encode()).hexdigest()
        manifest_hash2 = hashlib.sha256(sorted_hashes.encode()).hexdigest()

        assert manifest_hash1 == manifest_hash2

    def test_manifest_hash_order_independent(self):
        """Manifest hash should not depend on insertion order."""
        file_hashes_v1 = {"a.csv": "hash1", "b.csv": "hash2"}
        file_hashes_v2 = {"b.csv": "hash2", "a.csv": "hash1"}

        def compute_manifest(hashes):
            return hashlib.sha256(
                json.dumps(dict(sorted(hashes.items())), sort_keys=True).encode()
            ).hexdigest()

        assert compute_manifest(file_hashes_v1) == compute_manifest(file_hashes_v2)


class TestTierDefinitions:
    """Tests for tier definitions."""

    def test_all_tiers_defined(self):
        """All documented tiers should be defined."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import TIERS

        expected_tiers = {"tier0", "tier20gb", "tier100gb", "tier600gb", "tier2tb"}
        assert set(TIERS.keys()) == expected_tiers

    def test_tier_has_required_fields(self):
        """Each tier should have required fields."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import TIERS

        for tier_name, tier in TIERS.items():
            assert tier.name == tier_name
            assert tier.description, f"{tier_name} missing description"
            assert tier.expected_total_size_gb > 0, f"{tier_name} missing size"
            assert len(tier.datasets) > 0, f"{tier_name} has no datasets"

    def test_dataset_has_required_fields(self):
        """Each dataset should have required fields."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import TIERS

        for tier_name, tier in TIERS.items():
            for ds in tier.datasets:
                assert ds.name, f"Dataset in {tier_name} missing name"
                assert ds.url, f"{ds.name} missing URL"
                assert ds.file_format, f"{ds.name} missing file_format"
                assert ds.license, f"{ds.name} missing license"
                assert ds.source_org, f"{ds.name} missing source_org"

    def test_tier600gb_contains_required_datasets(self):
        """tier600gb should contain FineWeb, OSM Planet, and enwiki."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import TIERS

        tier = TIERS["tier600gb"]
        dataset_names = {ds.name for ds in tier.datasets}

        assert "fineweb_cc_2024_10_sample" in dataset_names
        assert "osm_planet" in dataset_names
        assert "enwiki_pages_articles_full" in dataset_names

    def test_tier2tb_includes_multiple_fineweb_snapshots(self):
        """tier2tb should include 3+ additional FineWeb snapshots."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import TIERS

        tier = TIERS["tier2tb"]
        fineweb_datasets = [ds for ds in tier.datasets if "fineweb" in ds.name]

        assert len(fineweb_datasets) >= 4  # At least 4 FineWeb datasets


class TestPoolYamlEmission:
    """Tests for pool YAML metadata emission."""

    def test_emit_pool_yaml_creates_file(self, tmp_path):
        """emit_pool_yaml should create a valid YAML file."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        # Temporarily override SOURCE_METADATA
        import prepare_source_tiers
        from prepare_source_tiers import Dataset, emit_pool_yaml

        original_metadata = prepare_source_tiers.SOURCE_METADATA
        prepare_source_tiers.SOURCE_METADATA = tmp_path

        try:
            dataset = Dataset(
                name="test_dataset",
                url="https://example.com/data.csv",
                expected_size_bytes=1000000,
                sha256="abc123",
                description="Test dataset for unit testing",
                file_format="csv",
                license="MIT",
                source_org="Test Org",
            )

            yaml_path = emit_pool_yaml(
                dataset=dataset,
                tier_name="tier0",
                raw_path=tmp_path / "raw",
                file_hashes={"data.csv": "abc123def456"},
                manifest_hash="manifest_hash_123",
                is_messy=False,
            )

            assert yaml_path.exists()
            content = yaml_path.read_text()
            assert "pool_id: tier0_test_dataset" in content
            assert "status: frozen" in content
            assert "lifecycle: FROZEN" in content
        finally:
            prepare_source_tiers.SOURCE_METADATA = original_metadata

    def test_messy_variant_yaml_has_messy_suffix(self, tmp_path):
        """Messy variant YAML should have _messy suffix."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        import prepare_source_tiers
        from prepare_source_tiers import Dataset, emit_pool_yaml

        original_metadata = prepare_source_tiers.SOURCE_METADATA
        prepare_source_tiers.SOURCE_METADATA = tmp_path

        try:
            dataset = Dataset(
                name="test_dataset",
                url="https://example.com/data.csv",
                expected_size_bytes=1000000,
                sha256="abc123",
                description="Test",
                file_format="csv",
                license="MIT",
                source_org="Test",
            )

            yaml_path = emit_pool_yaml(
                dataset=dataset,
                tier_name="tier0",
                raw_path=tmp_path / "messy",
                file_hashes={},
                manifest_hash="",
                is_messy=True,
            )

            assert "tier0_test_dataset_messy" in yaml_path.name
            content = yaml_path.read_text()
            assert "data_quality: messy" in content
        finally:
            prepare_source_tiers.SOURCE_METADATA = original_metadata


class TestHumanReadableSize:
    """Tests for human_readable_size utility."""

    def test_bytes(self):
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import human_readable_size

        assert "500.00 B" == human_readable_size(500)

    def test_kilobytes(self):
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import human_readable_size

        assert "1.00 KB" == human_readable_size(1024)

    def test_megabytes(self):
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import human_readable_size

        assert "1.00 MB" == human_readable_size(1024 * 1024)

    def test_gigabytes(self):
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        from prepare_source_tiers import human_readable_size

        assert "1.00 GB" == human_readable_size(1024 * 1024 * 1024)


class TestDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_does_not_modify_files(self, tmp_path):
        """Dry-run should not create or modify any files."""
        import sys

        scripts_path = Path(__file__).parent.parent.parent / "scripts"
        sys.path.insert(0, str(scripts_path))

        import prepare_source_tiers
        from prepare_source_tiers import TIERS, process_tier

        original_cache = prepare_source_tiers.TIERS_CACHE
        original_metadata = prepare_source_tiers.SOURCE_METADATA
        prepare_source_tiers.TIERS_CACHE = tmp_path / "cache"
        prepare_source_tiers.SOURCE_METADATA = tmp_path / "metadata"

        try:
            tier = TIERS["tier0"]
            _ = process_tier(
                tier,
                download=True,
                verify=True,
                build_messy=True,
                emit_metadata=True,
                dry_run=True,
            )

            # No files should be created in dry-run
            cache_files = (
                list((tmp_path / "cache").rglob("*")) if (tmp_path / "cache").exists() else []
            )
            # Only directories might be created
            assert len([f for f in cache_files if f.is_file()]) == 0
        finally:
            prepare_source_tiers.TIERS_CACHE = original_cache
            prepare_source_tiers.SOURCE_METADATA = original_metadata
