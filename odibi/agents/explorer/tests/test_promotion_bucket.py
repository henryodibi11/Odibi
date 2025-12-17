"""Tests for PromotionBucket submit-only store.

Phase E5: Promotion Bucket

These tests verify that:
1. Submit-only behavior (Explorer can only add PENDING candidates)
2. Rejection of non-PENDING status submissions
3. Duplicate candidate rejection (by ID and diff_hash)
4. Read/list APIs work correctly
5. Immutability of existing entries (no update/delete/status change)
6. Persistence and reload from disk
"""

import json
import threading
from pathlib import Path

import pytest

from odibi.agents.explorer.promotion_bucket import (
    BucketCorruptionError,
    CandidateStatus,
    DuplicateCandidateError,
    PromotionBucket,
    PromotionCandidate,
    PromotionEntry,
    SubmitStatusViolation,
    generate_candidate_id,
)


def make_entry(
    candidate_id: str = "promo_test123",
    diff_hash: str = "hash_abc123",
    diff_path: Path = None,
    memory_entry_id: str = "mem_xyz789",
    experiment_id: str = "exp_abc",
    status: CandidateStatus = CandidateStatus.PENDING,
) -> PromotionEntry:
    """Helper to create test entries."""
    if diff_path is None:
        diff_path = Path("/sandbox/artifacts/test.diff")
    return PromotionEntry(
        candidate_id=candidate_id,
        diff_hash=diff_hash,
        diff_path=diff_path,
        memory_entry_id=memory_entry_id,
        experiment_id=experiment_id,
        submitted_at="2024-01-01T00:00:00",
        diff_summary="Test diff summary",
        hypothesis="Test hypothesis",
        observation="Test observation",
        status=status,
        metadata=(("key", "value"),),
    )


class TestPromotionEntry:
    """Tests for PromotionEntry dataclass."""

    def test_to_dict(self):
        entry = make_entry()
        d = entry.to_dict()

        assert d["candidate_id"] == "promo_test123"
        assert d["diff_hash"] == "hash_abc123"
        assert d["memory_entry_id"] == "mem_xyz789"
        assert d["experiment_id"] == "exp_abc"
        assert d["status"] == "pending"

    def test_to_json(self):
        entry = make_entry()
        json_str = entry.to_json()
        parsed = json.loads(json_str)

        assert parsed["candidate_id"] == "promo_test123"
        assert parsed["diff_hash"] == "hash_abc123"

    def test_from_dict(self):
        original = make_entry()
        d = original.to_dict()
        restored = PromotionEntry.from_dict(d)

        assert restored.candidate_id == original.candidate_id
        assert restored.diff_hash == original.diff_hash
        assert restored.memory_entry_id == original.memory_entry_id
        assert restored.status == original.status

    def test_from_json(self):
        original = make_entry()
        json_str = original.to_json()
        restored = PromotionEntry.from_json(json_str)

        assert restored.candidate_id == original.candidate_id
        assert restored.diff_hash == original.diff_hash

    def test_immutable(self):
        entry = make_entry()

        with pytest.raises(AttributeError):
            entry.candidate_id = "new_id"

        with pytest.raises(AttributeError):
            entry.status = CandidateStatus.APPROVED

    def test_roundtrip_preserves_all_fields(self):
        original = make_entry()
        json_str = original.to_json()
        restored = PromotionEntry.from_json(json_str)

        assert restored.candidate_id == original.candidate_id
        assert restored.diff_hash == original.diff_hash
        assert restored.memory_entry_id == original.memory_entry_id
        assert restored.experiment_id == original.experiment_id
        assert restored.submitted_at == original.submitted_at
        assert restored.diff_summary == original.diff_summary
        assert restored.hypothesis == original.hypothesis
        assert restored.observation == original.observation
        assert restored.status == original.status

    def test_alias_promotion_candidate(self):
        """PromotionCandidate is alias for PromotionEntry."""
        assert PromotionCandidate is PromotionEntry


class TestSubmitOnlyBehavior:
    """Tests for submit-only behavior."""

    def test_submit_adds_entry(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        returned_id = bucket.submit(entry)

        assert returned_id == entry.candidate_id
        assert bucket.get(entry.candidate_id) == entry
        assert len(bucket) == 1

    def test_submit_multiple_entries(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entries = [make_entry(candidate_id=f"promo_{i}", diff_hash=f"hash_{i}") for i in range(5)]
        for entry in entries:
            bucket.submit(entry)

        assert len(bucket) == 5
        for entry in entries:
            assert bucket.get(entry.candidate_id) == entry

    def test_submit_persists_to_disk(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        assert storage_path.exists()
        content = storage_path.read_text()
        assert entry.candidate_id in content
        assert entry.diff_hash in content

    def test_submit_uses_jsonl_format(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry1 = make_entry(candidate_id="promo_1", diff_hash="hash_1")
        entry2 = make_entry(candidate_id="promo_2", diff_hash="hash_2")
        bucket.submit(entry1)
        bucket.submit(entry2)

        lines = storage_path.read_text().strip().split("\n")
        assert len(lines) == 2

        parsed1 = json.loads(lines[0])
        parsed2 = json.loads(lines[1])
        assert parsed1["candidate_id"] == "promo_1"
        assert parsed2["candidate_id"] == "promo_2"


class TestRejectionOfNonPendingStatus:
    """Tests for rejection of non-PENDING status submissions."""

    def test_submit_rejects_approved_status(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.APPROVED)

        with pytest.raises(SubmitStatusViolation) as exc_info:
            bucket.submit(entry)

        assert "SUBMIT STATUS VIOLATION" in str(exc_info.value)
        assert "PENDING" in str(exc_info.value)
        assert "approved" in str(exc_info.value)

    def test_submit_rejects_rejected_status(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.REJECTED)

        with pytest.raises(SubmitStatusViolation) as exc_info:
            bucket.submit(entry)

        assert "SUBMIT STATUS VIOLATION" in str(exc_info.value)
        assert "rejected" in str(exc_info.value)

    def test_submit_rejects_applied_status(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.APPLIED)

        with pytest.raises(SubmitStatusViolation) as exc_info:
            bucket.submit(entry)

        assert "SUBMIT STATUS VIOLATION" in str(exc_info.value)
        assert "applied" in str(exc_info.value)

    def test_submit_accepts_pending_status(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.PENDING)
        bucket.submit(entry)

        assert bucket.get(entry.candidate_id) is not None

    def test_rejected_submission_does_not_persist(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.APPROVED)

        try:
            bucket.submit(entry)
        except SubmitStatusViolation:
            pass

        assert not storage_path.exists() or storage_path.read_text().strip() == ""


class TestDuplicateCandidateRejection:
    """Tests for duplicate candidate rejection."""

    def test_submit_rejects_duplicate_candidate_id(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry1 = make_entry(candidate_id="promo_dup", diff_hash="hash_1")
        entry2 = make_entry(candidate_id="promo_dup", diff_hash="hash_2")

        bucket.submit(entry1)

        with pytest.raises(DuplicateCandidateError) as exc_info:
            bucket.submit(entry2)

        assert "promo_dup" in str(exc_info.value)
        assert "candidate_id already exists" in str(exc_info.value)

    def test_submit_rejects_duplicate_diff_hash(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry1 = make_entry(candidate_id="promo_1", diff_hash="same_hash")
        entry2 = make_entry(candidate_id="promo_2", diff_hash="same_hash")

        bucket.submit(entry1)

        with pytest.raises(DuplicateCandidateError) as exc_info:
            bucket.submit(entry2)

        assert "same_hash" in str(exc_info.value)
        assert "already submitted" in str(exc_info.value)

    def test_duplicate_rejection_preserves_original(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        original = make_entry(candidate_id="promo_orig", diff_hash="hash_orig")
        bucket.submit(original)

        duplicate = make_entry(candidate_id="promo_orig", diff_hash="hash_new")
        try:
            bucket.submit(duplicate)
        except DuplicateCandidateError:
            pass

        assert len(bucket) == 1
        retrieved = bucket.get("promo_orig")
        assert retrieved.diff_hash == "hash_orig"


class TestReadListAPIs:
    """Tests for read/list APIs."""

    def test_get_returns_entry(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        retrieved = bucket.get(entry.candidate_id)
        assert retrieved == entry

    def test_get_returns_none_for_missing(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert bucket.get("nonexistent") is None

    def test_get_by_diff_hash(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(diff_hash="unique_hash")
        bucket.submit(entry)

        results = bucket.get_by_diff_hash("unique_hash")
        assert len(results) == 1
        assert results[0].candidate_id == entry.candidate_id

    def test_get_by_diff_hash_not_found(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        results = bucket.get_by_diff_hash("nonexistent")
        assert results == []

    def test_has_diff_hash(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(diff_hash="exists_hash")
        bucket.submit(entry)

        assert bucket.has_diff_hash("exists_hash") is True
        assert bucket.has_diff_hash("nonexistent") is False

    def test_list_all(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entries = [make_entry(candidate_id=f"promo_{i}", diff_hash=f"hash_{i}") for i in range(3)]
        for entry in entries:
            bucket.submit(entry)

        all_entries = bucket.list_all()
        assert len(all_entries) == 3

    def test_list_pending(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(status=CandidateStatus.PENDING)
        bucket.submit(entry)

        pending = bucket.list_pending()
        assert len(pending) == 1
        assert pending[0].status == CandidateStatus.PENDING

    def test_list_by_status(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        by_status = bucket.list_by_status(CandidateStatus.PENDING)
        assert len(by_status) == 1

    def test_count(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert bucket.count() == 0

        for i in range(3):
            bucket.submit(make_entry(candidate_id=f"promo_{i}", diff_hash=f"hash_{i}"))

        assert bucket.count() == 3

    def test_count_pending(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        assert bucket.count_pending() == 1

    def test_contains(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry(candidate_id="promo_exists")
        bucket.submit(entry)

        assert "promo_exists" in bucket
        assert "promo_nonexistent" not in bucket

    def test_iteration(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entries = [make_entry(candidate_id=f"promo_{i}", diff_hash=f"hash_{i}") for i in range(3)]
        for entry in entries:
            bucket.submit(entry)

        iterated = list(bucket)
        assert len(iterated) == 3

    def test_len(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert len(bucket) == 0

        bucket.submit(make_entry())
        assert len(bucket) == 1


class TestImmutabilityOfExistingEntries:
    """Tests for immutability of existing entries (no update/delete/status change)."""

    def test_no_delete_method(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert not hasattr(bucket, "delete")
        assert not hasattr(bucket, "remove")

    def test_no_update_method(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert not hasattr(bucket, "update")
        assert not hasattr(bucket, "modify")
        assert not hasattr(bucket, "set")

    def test_no_set_status_method(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        assert not hasattr(bucket, "set_status")
        assert not hasattr(bucket, "approve")
        assert not hasattr(bucket, "reject")
        assert not hasattr(bucket, "apply")

    def test_entries_list_is_copy(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        entries = bucket.list_all()
        entries.clear()

        assert len(bucket) == 1

    def test_entry_itself_is_frozen(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        retrieved = bucket.get(entry.candidate_id)
        with pytest.raises(AttributeError):
            retrieved.status = CandidateStatus.APPROVED

    def test_cannot_modify_status_after_submit(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        entry = make_entry()
        bucket.submit(entry)

        retrieved = bucket.get(entry.candidate_id)
        assert retrieved.status == CandidateStatus.PENDING

        with pytest.raises(AttributeError):
            retrieved.status = CandidateStatus.APPROVED


class TestReloadFromDisk:
    """Tests for bucket reload from disk."""

    def test_reload_restores_entries(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"

        bucket1 = PromotionBucket(storage_path)
        entry = make_entry()
        bucket1.submit(entry)

        bucket2 = PromotionBucket(storage_path)

        assert len(bucket2) == 1
        assert bucket2.get(entry.candidate_id) == entry

    def test_reload_preserves_order(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"

        bucket1 = PromotionBucket(storage_path)
        entries = [make_entry(candidate_id=f"promo_{i}", diff_hash=f"hash_{i}") for i in range(5)]
        for entry in entries:
            bucket1.submit(entry)

        bucket2 = PromotionBucket(storage_path)
        reloaded = bucket2.list_all()

        assert len(reloaded) == 5
        for i, entry in enumerate(reloaded):
            assert entry.candidate_id == f"promo_{i}"

    def test_reload_restores_indices(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"

        bucket1 = PromotionBucket(storage_path)
        entry = make_entry(diff_hash="unique_hash_123")
        bucket1.submit(entry)

        bucket2 = PromotionBucket(storage_path)

        assert entry.candidate_id in bucket2
        assert bucket2.has_diff_hash("unique_hash_123")
        by_hash = bucket2.get_by_diff_hash("unique_hash_123")
        assert len(by_hash) == 1
        assert by_hash[0].candidate_id == entry.candidate_id

    def test_reload_handles_empty_file(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        storage_path.write_text("")

        bucket = PromotionBucket(storage_path)

        assert len(bucket) == 0

    def test_reload_handles_nonexistent_file(self, tmp_path: Path):
        storage_path = tmp_path / "nonexistent.jsonl"

        bucket = PromotionBucket(storage_path)

        assert len(bucket) == 0

    def test_reload_detects_corruption(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        storage_path.write_text("not valid json\n")

        with pytest.raises(BucketCorruptionError) as exc_info:
            PromotionBucket(storage_path)

        assert exc_info.value.line_number == 1


class TestConcurrentSubmissions:
    """Tests for concurrent submission safety."""

    def test_concurrent_submits_no_data_loss(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        num_threads = 10
        entries_per_thread = 5
        results = []
        errors = []

        def submit_entries(thread_id: int):
            for i in range(entries_per_thread):
                entry = make_entry(
                    candidate_id=f"promo_t{thread_id}_e{i}",
                    diff_hash=f"hash_t{thread_id}_e{i}",
                )
                try:
                    bucket.submit(entry)
                    results.append(entry.candidate_id)
                except Exception as e:
                    errors.append(str(e))

        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=submit_entries, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Errors: {errors}"
        expected_count = num_threads * entries_per_thread
        assert len(bucket) == expected_count
        assert len(results) == expected_count

    def test_concurrent_submits_persist_correctly(self, tmp_path: Path):
        storage_path = tmp_path / "bucket.jsonl"
        bucket = PromotionBucket(storage_path)

        num_threads = 5
        entries_per_thread = 3

        def submit_entries(thread_id: int):
            for i in range(entries_per_thread):
                entry = make_entry(
                    candidate_id=f"promo_t{thread_id}_e{i}",
                    diff_hash=f"hash_t{thread_id}_e{i}",
                )
                bucket.submit(entry)

        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=submit_entries, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        bucket2 = PromotionBucket(storage_path)
        expected_count = num_threads * entries_per_thread
        assert len(bucket2) == expected_count


class TestGenerateCandidateId:
    """Tests for candidate ID generation."""

    def test_generates_unique_ids(self):
        id1 = generate_candidate_id("hash1", "exp_1", "2024-01-01T00:00:00")
        id2 = generate_candidate_id("hash2", "exp_1", "2024-01-01T00:00:00")

        assert id1 != id2

    def test_deterministic(self):
        id1 = generate_candidate_id("hash1", "exp_1", "2024-01-01T00:00:00")
        id2 = generate_candidate_id("hash1", "exp_1", "2024-01-01T00:00:00")

        assert id1 == id2

    def test_format(self):
        candidate_id = generate_candidate_id("hash1", "exp_1", "2024-01-01T00:00:00")

        assert candidate_id.startswith("promo_")
        assert len(candidate_id) == 18  # "promo_" + 12 chars


class TestStorageSeparation:
    """Tests to verify bucket storage is separate from ExplorerMemory."""

    def test_bucket_uses_separate_file(self, tmp_path: Path):
        from odibi.agents.explorer.memory import ExplorerMemory, MemoryEntry, ExplorationOutcome

        memory_path = tmp_path / "explorer_memory.jsonl"
        bucket_path = tmp_path / "promotion_bucket.jsonl"

        memory = ExplorerMemory(memory_path)
        bucket = PromotionBucket(bucket_path)

        mem_entry = MemoryEntry(
            entry_id="mem_123",
            experiment_id="exp_1",
            sandbox_id="sb_1",
            timestamp="2024-01-01T00:00:00",
            outcome=ExplorationOutcome.SUCCESS,
            hypothesis="test",
            observation="test",
            diff_hash="mem_hash",
            promoted=False,
        )
        memory.append(mem_entry)

        bucket_entry = make_entry(candidate_id="promo_123", diff_hash="bucket_hash")
        bucket.submit(bucket_entry)

        assert memory_path.exists()
        assert bucket_path.exists()
        assert memory_path != bucket_path

        memory_content = memory_path.read_text()
        bucket_content = bucket_path.read_text()

        assert "mem_123" in memory_content
        assert "promo_123" not in memory_content
        assert "promo_123" in bucket_content
        assert "mem_123" not in bucket_content
