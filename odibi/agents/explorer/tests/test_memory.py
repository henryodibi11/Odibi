"""Tests for ExplorerMemory append-only store.

Phase E4: Append-Only Explorer Memory

These tests verify that:
1. Append behavior works correctly
2. Memory is immutable (no overwrite/delete)
3. Concurrent appends are safe (basic locking)
4. promoted=True is rejected
5. Memory reloads correctly from disk
"""

import json
import threading
from pathlib import Path

import pytest

from odibi.agents.explorer.memory import (
    ExplorationOutcome,
    ExplorerMemory,
    MemoryAppendError,
    MemoryCorruptionError,
    MemoryEntry,
    PromotedViolation,
    generate_entry_id,
)


def make_entry(
    entry_id: str = "mem_test123",
    experiment_id: str = "exp_abc",
    sandbox_id: str = "sandbox_xyz",
    outcome: ExplorationOutcome = ExplorationOutcome.SUCCESS,
    diff_hash: str = "hash123",
    promoted: bool = False,
) -> MemoryEntry:
    """Helper to create test entries."""
    return MemoryEntry(
        entry_id=entry_id,
        experiment_id=experiment_id,
        sandbox_id=sandbox_id,
        timestamp="2024-01-01T00:00:00",
        outcome=outcome,
        hypothesis="Test hypothesis",
        observation="Test observation",
        diff_hash=diff_hash,
        diff_path=Path("/test/path.diff"),
        diff_is_empty=False,
        exit_code=0,
        stdout_summary="stdout",
        stderr_summary="stderr",
        promoted=promoted,
        metadata=(("key", "value"),),
    )


class TestMemoryEntry:
    """Tests for MemoryEntry dataclass."""

    def test_to_dict(self):
        entry = make_entry()
        d = entry.to_dict()

        assert d["entry_id"] == "mem_test123"
        assert d["experiment_id"] == "exp_abc"
        assert d["outcome"] == "success"
        assert d["promoted"] is False

    def test_to_json(self):
        entry = make_entry()
        json_str = entry.to_json()
        parsed = json.loads(json_str)

        assert parsed["entry_id"] == "mem_test123"
        assert parsed["diff_hash"] == "hash123"

    def test_from_dict(self):
        original = make_entry()
        d = original.to_dict()
        restored = MemoryEntry.from_dict(d)

        assert restored.entry_id == original.entry_id
        assert restored.experiment_id == original.experiment_id
        assert restored.outcome == original.outcome
        assert restored.diff_hash == original.diff_hash

    def test_from_json(self):
        original = make_entry()
        json_str = original.to_json()
        restored = MemoryEntry.from_json(json_str)

        assert restored.entry_id == original.entry_id
        assert restored.diff_hash == original.diff_hash

    def test_immutable(self):
        entry = make_entry()

        with pytest.raises(AttributeError):
            entry.entry_id = "new_id"

    def test_roundtrip_preserves_all_fields(self):
        original = make_entry()
        json_str = original.to_json()
        restored = MemoryEntry.from_json(json_str)

        assert restored.entry_id == original.entry_id
        assert restored.experiment_id == original.experiment_id
        assert restored.sandbox_id == original.sandbox_id
        assert restored.timestamp == original.timestamp
        assert restored.outcome == original.outcome
        assert restored.hypothesis == original.hypothesis
        assert restored.observation == original.observation
        assert restored.diff_hash == original.diff_hash
        assert restored.diff_is_empty == original.diff_is_empty
        assert restored.exit_code == original.exit_code
        assert restored.promoted == original.promoted


class TestAppendBehavior:
    """Tests for append behavior."""

    def test_append_adds_entry(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry()
        returned_id = memory.append(entry)

        assert returned_id == entry.entry_id
        assert memory.get(entry.entry_id) == entry
        assert len(memory) == 1

    def test_append_multiple_entries(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entries = [make_entry(entry_id=f"mem_{i}") for i in range(5)]
        for entry in entries:
            memory.append(entry)

        assert len(memory) == 5
        for entry in entries:
            assert memory.get(entry.entry_id) == entry

    def test_append_persists_to_disk(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry()
        memory.append(entry)

        assert storage_path.exists()
        content = storage_path.read_text()
        assert entry.entry_id in content
        assert entry.diff_hash in content

    def test_append_uses_jsonl_format(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry1 = make_entry(entry_id="mem_1")
        entry2 = make_entry(entry_id="mem_2")
        memory.append(entry1)
        memory.append(entry2)

        lines = storage_path.read_text().strip().split("\n")
        assert len(lines) == 2

        parsed1 = json.loads(lines[0])
        parsed2 = json.loads(lines[1])
        assert parsed1["entry_id"] == "mem_1"
        assert parsed2["entry_id"] == "mem_2"

    def test_append_rejects_duplicate_id(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(entry_id="mem_duplicate")
        memory.append(entry)

        with pytest.raises(MemoryAppendError) as exc_info:
            memory.append(entry)

        assert "duplicate" in str(exc_info.value).lower()


class TestImmutability:
    """Tests for memory immutability (no overwrite/delete)."""

    def test_no_delete_method(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        assert not hasattr(memory, "delete")
        assert not hasattr(memory, "remove")

    def test_no_update_method(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        assert not hasattr(memory, "update")
        assert not hasattr(memory, "modify")
        assert not hasattr(memory, "set")

    def test_entries_list_is_copy(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry()
        memory.append(entry)

        entries = memory.list_all()
        entries.clear()

        assert len(memory) == 1

    def test_entry_itself_is_frozen(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry()
        memory.append(entry)

        retrieved = memory.get(entry.entry_id)
        with pytest.raises(AttributeError):
            retrieved.outcome = ExplorationOutcome.FAILURE


class TestPromotedRejection:
    """Tests for rejection of promoted=True."""

    def test_append_rejects_promoted_true(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(promoted=True)

        with pytest.raises(PromotedViolation) as exc_info:
            memory.append(entry)

        assert "PROMOTION VIOLATION" in str(exc_info.value)
        assert entry.entry_id in str(exc_info.value)

    def test_append_accepts_promoted_false(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(promoted=False)
        memory.append(entry)

        assert memory.get(entry.entry_id) is not None

    def test_promoted_violation_does_not_persist(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(promoted=True)

        try:
            memory.append(entry)
        except PromotedViolation:
            pass

        assert not storage_path.exists() or storage_path.read_text().strip() == ""


class TestReloadFromDisk:
    """Tests for memory reload from disk."""

    def test_reload_restores_entries(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"

        memory1 = ExplorerMemory(storage_path)
        entry = make_entry()
        memory1.append(entry)

        memory2 = ExplorerMemory(storage_path)

        assert len(memory2) == 1
        assert memory2.get(entry.entry_id) == entry

    def test_reload_preserves_order(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"

        memory1 = ExplorerMemory(storage_path)
        entries = [make_entry(entry_id=f"mem_{i}") for i in range(5)]
        for entry in entries:
            memory1.append(entry)

        memory2 = ExplorerMemory(storage_path)
        reloaded = memory2.list_all()

        assert len(reloaded) == 5
        for i, entry in enumerate(reloaded):
            assert entry.entry_id == f"mem_{i}"

    def test_reload_restores_indices(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"

        memory1 = ExplorerMemory(storage_path)
        entry = make_entry(diff_hash="unique_hash_123")
        memory1.append(entry)

        memory2 = ExplorerMemory(storage_path)

        assert entry.entry_id in memory2
        assert memory2.has_diff_hash("unique_hash_123")
        by_hash = memory2.get_by_diff_hash("unique_hash_123")
        assert len(by_hash) == 1
        assert by_hash[0].entry_id == entry.entry_id

    def test_reload_handles_empty_file(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        storage_path.write_text("")

        memory = ExplorerMemory(storage_path)

        assert len(memory) == 0

    def test_reload_handles_nonexistent_file(self, tmp_path: Path):
        storage_path = tmp_path / "nonexistent.jsonl"

        memory = ExplorerMemory(storage_path)

        assert len(memory) == 0

    def test_reload_detects_corruption(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        storage_path.write_text("not valid json\n")

        with pytest.raises(MemoryCorruptionError) as exc_info:
            ExplorerMemory(storage_path)

        assert exc_info.value.line_number == 1


class TestConcurrentAppends:
    """Tests for concurrent append safety."""

    def test_concurrent_appends_no_data_loss(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        num_threads = 10
        entries_per_thread = 5
        results = []
        errors = []

        def append_entries(thread_id: int):
            for i in range(entries_per_thread):
                entry = make_entry(
                    entry_id=f"mem_t{thread_id}_e{i}",
                    diff_hash=f"hash_t{thread_id}_e{i}",
                )
                try:
                    memory.append(entry)
                    results.append(entry.entry_id)
                except Exception as e:
                    errors.append(str(e))

        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=append_entries, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0, f"Errors: {errors}"
        expected_count = num_threads * entries_per_thread
        assert len(memory) == expected_count
        assert len(results) == expected_count

    def test_concurrent_appends_persist_correctly(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        num_threads = 5
        entries_per_thread = 3

        def append_entries(thread_id: int):
            for i in range(entries_per_thread):
                entry = make_entry(
                    entry_id=f"mem_t{thread_id}_e{i}",
                    diff_hash=f"hash_t{thread_id}_e{i}",
                )
                memory.append(entry)

        threads = []
        for t in range(num_threads):
            thread = threading.Thread(target=append_entries, args=(t,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        memory2 = ExplorerMemory(storage_path)
        expected_count = num_threads * entries_per_thread
        assert len(memory2) == expected_count


class TestContentAddressable:
    """Tests for content-addressable lookup by diff_hash."""

    def test_get_by_diff_hash(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(diff_hash="unique_hash")
        memory.append(entry)

        results = memory.get_by_diff_hash("unique_hash")

        assert len(results) == 1
        assert results[0].entry_id == entry.entry_id

    def test_get_by_diff_hash_multiple(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry1 = make_entry(entry_id="mem_1", diff_hash="same_hash")
        entry2 = make_entry(entry_id="mem_2", diff_hash="same_hash")
        memory.append(entry1)
        memory.append(entry2)

        results = memory.get_by_diff_hash("same_hash")

        assert len(results) == 2
        entry_ids = [r.entry_id for r in results]
        assert "mem_1" in entry_ids
        assert "mem_2" in entry_ids

    def test_get_by_diff_hash_not_found(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        results = memory.get_by_diff_hash("nonexistent")

        assert results == []

    def test_has_diff_hash(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(diff_hash="exists_hash")
        memory.append(entry)

        assert memory.has_diff_hash("exists_hash") is True
        assert memory.has_diff_hash("nonexistent") is False


class TestQueryMethods:
    """Tests for query methods."""

    def test_list_by_outcome(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry1 = make_entry(entry_id="mem_1", outcome=ExplorationOutcome.SUCCESS)
        entry2 = make_entry(entry_id="mem_2", outcome=ExplorationOutcome.FAILURE)
        entry3 = make_entry(entry_id="mem_3", outcome=ExplorationOutcome.SUCCESS)
        memory.append(entry1)
        memory.append(entry2)
        memory.append(entry3)

        successes = memory.list_by_outcome(ExplorationOutcome.SUCCESS)

        assert len(successes) == 2
        assert all(e.outcome == ExplorationOutcome.SUCCESS for e in successes)

    def test_list_unpromoted(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry1 = make_entry(entry_id="mem_1", promoted=False)
        entry2 = make_entry(entry_id="mem_2", promoted=False)
        memory.append(entry1)
        memory.append(entry2)

        unpromoted = memory.list_unpromoted()

        assert len(unpromoted) == 2

    def test_list_with_changes(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry_with_changes = MemoryEntry(
            entry_id="mem_1",
            experiment_id="exp_1",
            sandbox_id="sb_1",
            timestamp="2024-01-01T00:00:00",
            outcome=ExplorationOutcome.SUCCESS,
            hypothesis="test",
            observation="test",
            diff_hash="hash",
            diff_is_empty=False,
            promoted=False,
        )
        entry_no_changes = MemoryEntry(
            entry_id="mem_2",
            experiment_id="exp_2",
            sandbox_id="sb_2",
            timestamp="2024-01-01T00:00:00",
            outcome=ExplorationOutcome.INCONCLUSIVE,
            hypothesis="test",
            observation="test",
            diff_hash="",
            diff_is_empty=True,
            promoted=False,
        )
        memory.append(entry_with_changes)
        memory.append(entry_no_changes)

        with_changes = memory.list_with_changes()

        assert len(with_changes) == 1
        assert with_changes[0].entry_id == "mem_1"

    def test_contains(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entry = make_entry(entry_id="mem_exists")
        memory.append(entry)

        assert "mem_exists" in memory
        assert "mem_nonexistent" not in memory

    def test_iteration(self, tmp_path: Path):
        storage_path = tmp_path / "memory.jsonl"
        memory = ExplorerMemory(storage_path)

        entries = [make_entry(entry_id=f"mem_{i}") for i in range(3)]
        for entry in entries:
            memory.append(entry)

        iterated = list(memory)

        assert len(iterated) == 3


class TestGenerateEntryId:
    """Tests for entry ID generation."""

    def test_generates_unique_ids(self):
        id1 = generate_entry_id("exp_1", "2024-01-01T00:00:00")
        id2 = generate_entry_id("exp_1", "2024-01-01T00:00:01")

        assert id1 != id2

    def test_deterministic(self):
        id1 = generate_entry_id("exp_1", "2024-01-01T00:00:00")
        id2 = generate_entry_id("exp_1", "2024-01-01T00:00:00")

        assert id1 == id2

    def test_format(self):
        entry_id = generate_entry_id("exp_1", "2024-01-01T00:00:00")

        assert entry_id.startswith("mem_")
        assert len(entry_id) == 16
