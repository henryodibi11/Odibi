"""End-to-End Integration Tests for Explorer Trust Boundary Verification.

Phase E6: End-to-End Integration & Trust Boundary Verification

These tests prove that the Explorer can execute a full cycle end-to-end
without modifying or corrupting the trusted Odibi system.

The full cycle includes:
1. Clone a sandbox
2. Run an experiment
3. Capture a diff
4. Append to ExplorerMemory
5. Submit a PromotionEntry

Trust boundary verifications:
- Trusted Odibi codebase is unchanged
- agents/core/* files are byte-identical before and after
- learning_harness is untouched
- PromotionBucket contains exactly one PENDING entry
- ExplorerMemory contains exactly one immutable entry
"""

import hashlib
import os
from pathlib import Path
from typing import Optional

import pytest

from odibi.agents.explorer.clone_manager import RepoCloneManager
from odibi.agents.explorer.experiment_runner import ExperimentRunner, ExperimentSpec
from odibi.agents.explorer.memory import (
    ExplorerMemory,
    MemoryEntry,
    ExplorationOutcome,
    create_memory_entry_from_experiment,
)
from odibi.agents.explorer.promotion_bucket import (
    CandidateStatus,
    PromotionBucket,
    PromotionEntry,
    create_promotion_entry_from_memory,
)


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def compute_directory_hash(
    dir_path: Path,
    skip_patterns: Optional[list[str]] = None,
) -> dict[str, str]:
    """Compute hashes of all files in a directory recursively."""
    if skip_patterns is None:
        skip_patterns = ["__pycache__", ".pyc", ".pyo", ".lock", ".git"]

    hashes = {}
    dir_path = Path(dir_path).resolve()

    if not dir_path.exists():
        return hashes

    for root, dirs, files in os.walk(dir_path):
        dirs[:] = [d for d in dirs if not any(p in d for p in skip_patterns)]

        for file_name in files:
            if any(file_name.endswith(p) for p in skip_patterns):
                continue

            file_path = Path(root) / file_name
            rel_path = file_path.relative_to(dir_path)
            rel_path_str = str(rel_path).replace("\\", "/")

            try:
                hashes[rel_path_str] = compute_file_hash(file_path)
            except (OSError, PermissionError):
                continue

    return hashes


def verify_directory_unchanged(
    before_hashes: dict[str, str],
    after_hashes: dict[str, str],
) -> tuple[bool, list[str]]:
    """Verify that a directory is unchanged by comparing hashes."""
    changes = []
    all_files = set(before_hashes.keys()) | set(after_hashes.keys())

    for file_path in sorted(all_files):
        before_hash = before_hashes.get(file_path)
        after_hash = after_hashes.get(file_path)

        if before_hash is None:
            changes.append(f"ADDED: {file_path}")
        elif after_hash is None:
            changes.append(f"DELETED: {file_path}")
        elif before_hash != after_hash:
            changes.append(f"MODIFIED: {file_path}")

    return len(changes) == 0, changes


def get_odibi_root() -> Path:
    """Get the root of the Odibi repository."""
    current = Path(__file__).resolve()
    while current.parent != current:
        if (current / "agents" / "core").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find Odibi repository root")


def get_agents_core_path() -> Path:
    """Get the path to agents/core directory."""
    return get_odibi_root() / "agents" / "core"


def get_learning_harness_path() -> Path:
    """Get the path to the learning harness directory."""
    return get_odibi_root() / ".odibi" / "learning_harness"


class TestDirectoryHashing:
    """Tests for directory hashing utilities."""

    def test_compute_file_hash_consistent(self, tmp_path: Path):
        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        hash1 = compute_file_hash(test_file)
        hash2 = compute_file_hash(test_file)

        assert hash1 == hash2
        assert len(hash1) == 64

    def test_compute_file_hash_different_content(self, tmp_path: Path):
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("Content A")
        file2.write_text("Content B")

        assert compute_file_hash(file1) != compute_file_hash(file2)

    def test_compute_directory_hash(self, tmp_path: Path):
        (tmp_path / "file1.txt").write_text("Content 1")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "file2.txt").write_text("Content 2")

        hashes = compute_directory_hash(tmp_path)

        assert "file1.txt" in hashes
        assert "subdir/file2.txt" in hashes
        assert len(hashes) == 2

    def test_compute_directory_hash_skips_pycache(self, tmp_path: Path):
        (tmp_path / "file.py").write_text("print('hello')")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "cached.pyc").write_bytes(b"bytecode")

        hashes = compute_directory_hash(tmp_path)

        assert "file.py" in hashes
        assert "__pycache__/cached.pyc" not in hashes

    def test_verify_directory_unchanged_identical(self, tmp_path: Path):
        (tmp_path / "file.txt").write_text("Hello")
        hashes = compute_directory_hash(tmp_path)

        is_unchanged, changes = verify_directory_unchanged(hashes, hashes)

        assert is_unchanged is True
        assert changes == []

    def test_verify_directory_unchanged_detects_modification(self, tmp_path: Path):
        test_file = tmp_path / "file.txt"
        test_file.write_text("Original")
        before = compute_directory_hash(tmp_path)

        test_file.write_text("Modified")
        after = compute_directory_hash(tmp_path)

        is_unchanged, changes = verify_directory_unchanged(before, after)

        assert is_unchanged is False
        assert any("MODIFIED" in c for c in changes)

    def test_verify_directory_unchanged_detects_addition(self, tmp_path: Path):
        (tmp_path / "original.txt").write_text("Original")
        before = compute_directory_hash(tmp_path)

        (tmp_path / "new.txt").write_text("New file")
        after = compute_directory_hash(tmp_path)

        is_unchanged, changes = verify_directory_unchanged(before, after)

        assert is_unchanged is False
        assert any("ADDED" in c for c in changes)

    def test_verify_directory_unchanged_detects_deletion(self, tmp_path: Path):
        test_file = tmp_path / "file.txt"
        test_file.write_text("Will be deleted")
        before = compute_directory_hash(tmp_path)

        test_file.unlink()
        after = compute_directory_hash(tmp_path)

        is_unchanged, changes = verify_directory_unchanged(before, after)

        assert is_unchanged is False
        assert any("DELETED" in c for c in changes)


@pytest.fixture
def test_environment(tmp_path: Path) -> dict:
    """Create a complete test environment with proper isolation.

    Creates:
    - trusted_area/test_project: Source project (acts as "trusted" for test)
    - sandbox_root: Where sandboxes are created (sibling to trusted_area)
    - storage: For memory and bucket files
    """
    trusted_area = tmp_path / "trusted_area"
    trusted_area.mkdir()

    project_dir = trusted_area / "test_project"
    project_dir.mkdir()
    (project_dir / "main.py").write_text("def hello():\n    return 'Hello, World!'\n")
    (project_dir / "config.json").write_text('{"version": "1.0"}\n')
    (project_dir / "README.md").write_text("# Test Project\n")

    sandbox_root = tmp_path / "sandbox_root"
    sandbox_root.mkdir()

    storage = tmp_path / "storage"
    storage.mkdir()

    return {
        "source_project": project_dir,
        "trusted_repo_root": trusted_area,
        "sandbox_root": sandbox_root,
        "memory_path": storage / "explorer_memory.jsonl",
        "bucket_path": storage / "promotion_bucket.jsonl",
    }


class TestFullCycleIntegration:
    """End-to-end integration tests for Explorer full cycle."""

    def test_full_cycle_sandbox_to_promotion(self, test_environment: dict):
        """Test complete cycle: clone -> experiment -> memory -> promotion."""
        env = test_environment
        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        assert sandbox_info.is_active
        assert sandbox_info.sandbox_path.exists()

        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="add_feature",
            description="Add a new greeting function",
            commands=[
                'echo "def greet(name):" >> main.py',
                "echo \"    return f'Hello, {name}!'\" >> main.py",
            ],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.diff_path is not None
        assert result.diff_hash != ""
        assert not result.diff_is_empty

        memory_entry = create_memory_entry_from_experiment(
            experiment_result=result,
            hypothesis="Adding a greet function improves usability",
            observation="Function was added successfully",
        )

        assert memory_entry.promoted is False
        entry_id = memory.append(memory_entry)
        assert entry_id == memory_entry.entry_id

        promotion_entry = create_promotion_entry_from_memory(
            memory_entry=memory_entry,
            diff_summary="Added greet() function to main.py",
        )

        assert promotion_entry.status == CandidateStatus.PENDING
        candidate_id = bucket.submit(promotion_entry)
        assert candidate_id == promotion_entry.candidate_id

        assert len(memory) == 1
        assert len(bucket) == 1

        stored_memory = memory.get(entry_id)
        assert stored_memory is not None
        assert stored_memory.promoted is False
        assert stored_memory.diff_hash == result.diff_hash

        stored_candidate = bucket.get(candidate_id)
        assert stored_candidate is not None
        assert stored_candidate.status == CandidateStatus.PENDING
        assert stored_candidate.diff_hash == result.diff_hash
        assert stored_candidate.memory_entry_id == entry_id

        clone_manager.cleanup_all()

    def test_full_cycle_preserves_source_project(self, test_environment: dict):
        """Test that full cycle does not modify the source project."""
        env = test_environment
        before_hashes = compute_directory_hash(env["source_project"])

        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="modify_all_files",
            description="Make changes to all files",
            commands=[
                'echo "# Modified" >> README.md',
                'echo "new_config = True" >> main.py',
            ],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)

        if not result.diff_is_empty:
            memory_entry = create_memory_entry_from_experiment(
                experiment_result=result,
                hypothesis="Test hypothesis",
                observation="Test observation",
            )
            memory.append(memory_entry)

            promotion_entry = create_promotion_entry_from_memory(
                memory_entry=memory_entry,
                diff_summary="Test changes",
            )
            bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        after_hashes = compute_directory_hash(env["source_project"])
        is_unchanged, changes = verify_directory_unchanged(before_hashes, after_hashes)

        assert is_unchanged, f"Source project was modified! Changes: {changes}"


class TestTrustBoundaryVerification:
    """Tests that verify trust boundaries are maintained."""

    @pytest.fixture
    def trusted_dirs_hashes(self) -> dict[str, dict[str, str]]:
        """Capture hashes of trusted directories before tests."""
        return {
            "agents_core": compute_directory_hash(get_agents_core_path()),
            "learning_harness": compute_directory_hash(get_learning_harness_path()),
        }

    def test_agents_core_unchanged_after_full_cycle(
        self,
        trusted_dirs_hashes: dict[str, dict[str, str]],
        test_environment: dict,
    ):
        """Verify agents/core/* is byte-identical before and after full cycle."""
        env = test_environment
        before_hashes = trusted_dirs_hashes["agents_core"]

        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="test_experiment",
            description="Experiment that should not affect agents/core",
            commands=['echo "new content" >> main.py'],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)

        if not result.diff_is_empty:
            memory_entry = create_memory_entry_from_experiment(
                result, "Test hypothesis", "Test observation"
            )
            memory.append(memory_entry)
            promotion_entry = create_promotion_entry_from_memory(memory_entry, "Test diff")
            bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        after_hashes = compute_directory_hash(get_agents_core_path())
        is_unchanged, changes = verify_directory_unchanged(before_hashes, after_hashes)

        assert is_unchanged, (
            "TRUST BOUNDARY VIOLATION: agents/core/* was modified!\n"
            "Changes detected:\n" + "\n".join(f"  - {c}" for c in changes)
        )

    def test_learning_harness_unchanged_after_full_cycle(
        self,
        trusted_dirs_hashes: dict[str, dict[str, str]],
        test_environment: dict,
    ):
        """Verify learning_harness is untouched after full cycle."""
        env = test_environment
        harness_path = get_learning_harness_path()
        if not harness_path.exists():
            pytest.skip("learning_harness directory not found")

        before_hashes = trusted_dirs_hashes["learning_harness"]

        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="test_experiment",
            description="Experiment that should not affect learning_harness",
            commands=['echo "modified" >> main.py'],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)

        if not result.diff_is_empty:
            memory_entry = create_memory_entry_from_experiment(
                result, "Test hypothesis", "Test observation"
            )
            memory.append(memory_entry)
            promotion_entry = create_promotion_entry_from_memory(memory_entry, "Test diff")
            bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        after_hashes = compute_directory_hash(harness_path)
        is_unchanged, changes = verify_directory_unchanged(before_hashes, after_hashes)

        assert is_unchanged, (
            "TRUST BOUNDARY VIOLATION: learning_harness was modified!\n"
            "Changes detected:\n" + "\n".join(f"  - {c}" for c in changes)
        )

    def test_promotion_bucket_contains_exactly_one_pending_entry(
        self,
        test_environment: dict,
    ):
        """Verify PromotionBucket contains exactly one PENDING entry after cycle."""
        env = test_environment
        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="single_experiment",
            description="Single experiment for testing",
            commands=['echo "change" >> main.py'],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)
        assert not result.diff_is_empty, "Expected experiment to produce changes"

        memory_entry = create_memory_entry_from_experiment(
            result, "Test hypothesis", "Test observation"
        )
        memory.append(memory_entry)

        promotion_entry = create_promotion_entry_from_memory(
            memory_entry, "Added change to main.py"
        )
        bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        assert len(bucket) == 1, f"Expected exactly 1 entry, got {len(bucket)}"
        assert bucket.count_pending() == 1, "Expected exactly 1 PENDING entry"

        all_entries = bucket.list_all()
        assert all_entries[0].status == CandidateStatus.PENDING
        assert all_entries[0].diff_hash == result.diff_hash

    def test_explorer_memory_contains_exactly_one_immutable_entry(
        self,
        test_environment: dict,
    ):
        """Verify ExplorerMemory contains exactly one immutable entry after cycle."""
        env = test_environment
        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory = ExplorerMemory(env["memory_path"])
        bucket = PromotionBucket(env["bucket_path"])

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="single_experiment",
            description="Single experiment for testing",
            commands=['echo "modification" >> main.py'],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)
        assert not result.diff_is_empty

        memory_entry = create_memory_entry_from_experiment(
            result, "Test hypothesis", "Test observation"
        )
        entry_id = memory.append(memory_entry)

        promotion_entry = create_promotion_entry_from_memory(memory_entry, "Test diff")
        bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        assert len(memory) == 1, f"Expected exactly 1 entry, got {len(memory)}"

        stored_entry = memory.get(entry_id)
        assert stored_entry is not None
        assert stored_entry.promoted is False
        assert stored_entry.diff_hash == result.diff_hash

        with pytest.raises(AttributeError):
            stored_entry.promoted = True

    def test_full_cycle_with_trust_boundary_verification(
        self,
        trusted_dirs_hashes: dict[str, dict[str, str]],
        test_environment: dict,
    ):
        """Complete integration test with all trust boundary verifications."""
        env = test_environment
        agents_core_before = trusted_dirs_hashes["agents_core"]
        harness_before = trusted_dirs_hashes["learning_harness"]

        clone_manager = RepoCloneManager(
            sandbox_root=env["sandbox_root"],
            trusted_repo_root=env["trusted_repo_root"],
        )
        experiment_runner = ExperimentRunner(clone_manager)
        memory_path = env["memory_path"]
        bucket_path = env["bucket_path"]
        memory = ExplorerMemory(memory_path)
        bucket = PromotionBucket(bucket_path)

        sandbox_info = clone_manager.create_sandbox(env["source_project"])
        experiment_runner.create_baseline(sandbox_info.sandbox_id)

        spec = ExperimentSpec(
            name="complete_test",
            description="Full cycle test experiment",
            commands=[
                'echo "# New feature" >> main.py',
                'echo "def new_func(): pass" >> main.py',
            ],
            timeout_seconds=30,
            capture_diff=True,
        )

        result = experiment_runner.run_experiment(spec, sandbox_info.sandbox_id)

        assert result.diff_path is not None
        assert not result.diff_is_empty
        assert result.diff_hash != ""

        memory_entry = create_memory_entry_from_experiment(
            result,
            hypothesis="Adding new function improves modularity",
            observation="Function was added successfully to main.py",
        )
        memory.append(memory_entry)

        promotion_entry = create_promotion_entry_from_memory(
            memory_entry,
            diff_summary="Added new_func() to main.py",
        )
        bucket.submit(promotion_entry)

        clone_manager.cleanup_all()

        agents_core_after = compute_directory_hash(get_agents_core_path())
        agents_unchanged, agents_changes = verify_directory_unchanged(
            agents_core_before, agents_core_after
        )
        assert agents_unchanged, f"agents/core/* modified: {agents_changes}"

        harness_path = get_learning_harness_path()
        if harness_path.exists():
            harness_after = compute_directory_hash(harness_path)
            harness_unchanged, harness_changes = verify_directory_unchanged(
                harness_before, harness_after
            )
            assert harness_unchanged, f"learning_harness modified: {harness_changes}"

        assert len(bucket) == 1
        assert bucket.count_pending() == 1
        pending_entry = bucket.list_pending()[0]
        assert pending_entry.status == CandidateStatus.PENDING

        assert len(memory) == 1
        stored_entry = memory.list_all()[0]
        assert stored_entry.promoted is False

        assert pending_entry.diff_hash == stored_entry.diff_hash
        assert pending_entry.memory_entry_id == stored_entry.entry_id

        memory2 = ExplorerMemory(memory_path)
        bucket2 = PromotionBucket(bucket_path)

        assert len(memory2) == 1
        assert len(bucket2) == 1
        assert memory2.get(stored_entry.entry_id) == stored_entry
        assert bucket2.get(pending_entry.candidate_id).status == CandidateStatus.PENDING


class TestNoAutoPromotionOrApproval:
    """Tests verifying that no auto-promotion or approval occurs."""

    def test_memory_entry_never_promoted_by_explorer(self, tmp_path: Path):
        """Verify Explorer cannot create promoted=True entries."""
        memory = ExplorerMemory(tmp_path / "memory.jsonl")

        entry = MemoryEntry(
            entry_id="test_entry",
            experiment_id="exp_1",
            sandbox_id="sb_1",
            timestamp="2024-01-01T00:00:00",
            outcome=ExplorationOutcome.SUCCESS,
            hypothesis="Test",
            observation="Test",
            diff_hash="hash123",
            promoted=False,
        )

        memory.append(entry)
        stored = memory.get("test_entry")

        assert stored.promoted is False

    def test_promotion_entry_always_pending(self, tmp_path: Path):
        """Verify all submitted promotions start as PENDING."""
        bucket = PromotionBucket(tmp_path / "bucket.jsonl")

        entry = PromotionEntry(
            candidate_id="promo_1",
            diff_hash="hash123",
            diff_path=Path("/test/path.diff"),
            memory_entry_id="mem_1",
            experiment_id="exp_1",
            submitted_at="2024-01-01T00:00:00",
            diff_summary="Test",
            hypothesis="Test",
            observation="Test",
            status=CandidateStatus.PENDING,
        )

        bucket.submit(entry)
        stored = bucket.get("promo_1")

        assert stored.status == CandidateStatus.PENDING

    def test_cannot_submit_approved_entry(self, tmp_path: Path):
        """Verify Explorer cannot submit APPROVED entries."""
        from odibi.agents.explorer.promotion_bucket import SubmitStatusViolation

        bucket = PromotionBucket(tmp_path / "bucket.jsonl")

        entry = PromotionEntry(
            candidate_id="promo_1",
            diff_hash="hash123",
            diff_path=Path("/test/path.diff"),
            memory_entry_id="mem_1",
            experiment_id="exp_1",
            submitted_at="2024-01-01T00:00:00",
            diff_summary="Test",
            hypothesis="Test",
            observation="Test",
            status=CandidateStatus.APPROVED,
        )

        with pytest.raises(SubmitStatusViolation):
            bucket.submit(entry)

    def test_cannot_submit_applied_entry(self, tmp_path: Path):
        """Verify Explorer cannot submit APPLIED entries."""
        from odibi.agents.explorer.promotion_bucket import SubmitStatusViolation

        bucket = PromotionBucket(tmp_path / "bucket.jsonl")

        entry = PromotionEntry(
            candidate_id="promo_1",
            diff_hash="hash123",
            diff_path=Path("/test/path.diff"),
            memory_entry_id="mem_1",
            experiment_id="exp_1",
            submitted_at="2024-01-01T00:00:00",
            diff_summary="Test",
            hypothesis="Test",
            observation="Test",
            status=CandidateStatus.APPLIED,
        )

        with pytest.raises(SubmitStatusViolation):
            bucket.submit(entry)
