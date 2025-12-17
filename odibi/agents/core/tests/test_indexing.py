"""Tests for CycleIndexManager.

Phase 5.C: Manual Re-Indexing of Cycle Learnings tests.
Phase 5.D: File-change summary indexing tests.
"""

import json
import os
import tempfile

import pytest

from odibi.agents.core.cycle import CycleConfig, CycleState, CycleStepLog, GoldenProjectConfig
from odibi.agents.core.indexing import (
    CycleIndexManager,
    FileChangeType,
    IndexedCycleDocument,
    IndexedFileChange,
    IndexingResult,
)


class MockVectorStore:
    """Mock vector store for testing."""

    def __init__(self):
        self.chunks = []
        self._deleted = False

    def add_chunks(self, chunks):
        self.chunks.extend(chunks)
        return {"succeeded": len(chunks), "failed": 0}

    def count(self):
        return len(self.chunks)

    def delete_all(self):
        self.chunks = []
        self._deleted = True
        return True

    def exists(self):
        return len(self.chunks) > 0


class MockEmbedder:
    """Mock embedder for testing."""

    @property
    def dimension(self):
        return 384

    def embed_texts(self, texts, batch_size=16):
        return [[0.1] * 384 for _ in texts]

    def embed_query(self, query):
        return [0.1] * 384


class TestIndexedCycleDocument:
    """Tests for IndexedCycleDocument."""

    def test_to_indexable_text_basic(self):
        """Test basic text generation."""
        doc = IndexedCycleDocument(
            cycle_id="test-123",
            timestamp="2024-01-15T10:00:00",
            mode="guided_execution",
            workspace_root="/projects/test",
            golden_projects=["project_a", "project_b"],
            approved_improvements=[],
            regression_results=[],
            final_status="SUCCESS",
            conclusions="Test completed.",
        )

        text = doc.to_indexable_text()

        assert "test-123" in text
        assert "guided_execution" in text
        assert "SUCCESS" in text
        assert "project_a" in text
        assert "Test completed." in text

    def test_to_indexable_text_with_improvements(self):
        """Test text generation with approved improvements."""
        doc = IndexedCycleDocument(
            cycle_id="test-456",
            timestamp="2024-01-15T10:00:00",
            mode="scheduled",
            workspace_root="/projects/test",
            golden_projects=[],
            approved_improvements=[{"title": "Fix null handling", "rationale": "Prevents crashes"}],
            regression_results=[],
            final_status="SUCCESS",
            conclusions="",
        )

        text = doc.to_indexable_text()

        assert "Approved Improvements" in text
        assert "Fix null handling" in text
        assert "Prevents crashes" in text

    def test_to_indexable_text_with_regressions(self):
        """Test text generation with regression results."""
        doc = IndexedCycleDocument(
            cycle_id="test-789",
            timestamp="2024-01-15T10:00:00",
            mode="guided_execution",
            workspace_root="/projects/test",
            golden_projects=[],
            approved_improvements=[],
            regression_results=[
                {"name": "sales_etl", "status": "PASSED"},
                {"name": "inventory", "status": "FAILED"},
            ],
            final_status="PARTIAL",
            conclusions="",
        )

        text = doc.to_indexable_text()

        assert "Regression Results" in text
        assert "sales_etl: PASSED" in text
        assert "inventory: FAILED" in text


class TestIndexingResult:
    """Tests for IndexingResult."""

    def test_total_processed(self):
        """Test total_processed calculation."""
        result = IndexingResult(
            indexed=["a", "b"],
            skipped_already_indexed=["c"],
            skipped_incomplete=["d", "e"],
            skipped_failed=["f"],
        )

        assert result.total_processed == 6

    def test_to_dict(self):
        """Test dictionary conversion."""
        result = IndexingResult(indexed=["test-1"])
        data = result.to_dict()

        assert data["indexed"] == ["test-1"]
        assert data["total_processed"] == 1


class TestCycleIndexManager:
    """Tests for CycleIndexManager."""

    @pytest.fixture
    def temp_odibi_dir(self):
        """Create a temporary .odibi directory with cycles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)
            os.makedirs(os.path.join(odibi_dir, "memories", "cycles"))
            os.makedirs(os.path.join(odibi_dir, "reports"))
            yield odibi_dir

    @pytest.fixture
    def sample_completed_state(self):
        """Create a completed cycle state."""
        config = CycleConfig(
            project_root="/projects/test",
            golden_projects=[GoldenProjectConfig(name="test_proj", path="test.yaml")],
        )
        state = CycleState(
            cycle_id="completed-cycle-1",
            config=config,
            mode="guided_execution",
            completed=True,
            completed_at="2024-01-15T10:00:00",
            exit_status="COMPLETED",
            improvements_approved=1,
            review_approved=True,
            summary="Cycle completed successfully.",
            golden_project_results=[{"name": "test_proj", "status": "PASSED"}],
        )
        state.logs = [
            CycleStepLog(
                step="improvement",
                agent_role="improvement",
                started_at="2024-01-15T09:30:00",
                completed_at="2024-01-15T09:35:00",
                input_summary="",
                output_summary="",
                success=True,
                metadata={"proposal": {"title": "Test improvement", "rationale": "For testing"}},
            )
        ]
        return state

    @pytest.fixture
    def sample_incomplete_state(self):
        """Create an incomplete cycle state."""
        config = CycleConfig(project_root="/projects/test")
        return CycleState(
            cycle_id="incomplete-cycle-1",
            config=config,
            completed=False,
        )

    @pytest.fixture
    def sample_failed_state(self):
        """Create a failed cycle state."""
        config = CycleConfig(project_root="/projects/test")
        return CycleState(
            cycle_id="failed-cycle-1",
            config=config,
            completed=True,
            exit_status="FAILURE",
        )

    def _save_cycle_state(self, odibi_dir: str, state: CycleState):
        """Helper to save cycle state to disk."""
        cycles_dir = os.path.join(odibi_dir, "memories", "cycles")
        path = os.path.join(cycles_dir, f"{state.cycle_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state.to_dict(), f)

    def test_eligibility_completed_cycle(self, temp_odibi_dir, sample_completed_state):
        """Test that completed cycles are eligible."""
        manager = CycleIndexManager(temp_odibi_dir)
        eligible, reason = manager._is_eligible_for_indexing(sample_completed_state)

        assert eligible is True
        assert reason == ""

    def test_eligibility_incomplete_cycle(self, temp_odibi_dir, sample_incomplete_state):
        """Test that incomplete cycles are not eligible."""
        manager = CycleIndexManager(temp_odibi_dir)
        eligible, reason = manager._is_eligible_for_indexing(sample_incomplete_state)

        assert eligible is False
        assert reason == "incomplete"

    def test_eligibility_failed_cycle(self, temp_odibi_dir, sample_failed_state):
        """Test that failed cycles are not eligible."""
        manager = CycleIndexManager(temp_odibi_dir)
        eligible, reason = manager._is_eligible_for_indexing(sample_failed_state)

        assert eligible is False
        assert reason == "failed"

    def test_eligibility_interrupted_cycle(self, temp_odibi_dir):
        """Test that interrupted cycles are not eligible."""
        config = CycleConfig(project_root="/test")
        state = CycleState(
            cycle_id="interrupted-1",
            config=config,
            completed=True,
            interrupted=True,
            exit_status="INTERRUPTED",
        )
        manager = CycleIndexManager(temp_odibi_dir)
        eligible, reason = manager._is_eligible_for_indexing(state)

        assert eligible is False
        assert reason == "interrupted"

    def test_build_document(self, temp_odibi_dir, sample_completed_state):
        """Test document building from cycle state."""
        manager = CycleIndexManager(temp_odibi_dir)
        doc = manager._build_document(sample_completed_state)

        assert doc.cycle_id == "completed-cycle-1"
        assert doc.mode == "guided_execution"
        assert doc.final_status == "SUCCESS"
        assert doc.conclusions == "Cycle completed successfully."
        assert "test_proj" in doc.golden_projects

    def test_extract_approved_improvements(self, temp_odibi_dir, sample_completed_state):
        """Test extraction of approved improvements."""
        manager = CycleIndexManager(temp_odibi_dir)
        improvements = manager._extract_approved_improvements(sample_completed_state)

        assert len(improvements) == 1
        assert improvements[0]["title"] == "Test improvement"

    def test_extract_no_improvements_when_rejected(self, temp_odibi_dir, sample_completed_state):
        """Test that rejected improvements are not extracted."""
        sample_completed_state.review_approved = False
        sample_completed_state.improvements_approved = 0

        manager = CycleIndexManager(temp_odibi_dir)
        improvements = manager._extract_approved_improvements(sample_completed_state)

        assert len(improvements) == 0

    def test_idempotency_skips_already_indexed(self, temp_odibi_dir, sample_completed_state):
        """Test that already indexed cycles are skipped."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        result1 = manager.index_completed_cycles()
        assert len(result1.indexed) == 1
        assert sample_completed_state.cycle_id in result1.indexed

        result2 = manager.index_completed_cycles()
        assert len(result2.indexed) == 0
        assert sample_completed_state.cycle_id in result2.skipped_already_indexed

    def test_force_reindex_overrides_idempotency(self, temp_odibi_dir, sample_completed_state):
        """Test that force=True re-indexes already indexed cycles."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        manager.index_completed_cycles()
        result = manager.index_completed_cycles(force=True)

        assert len(result.indexed) == 1
        assert sample_completed_state.cycle_id in result.indexed

    def test_payload_structure(self, temp_odibi_dir, sample_completed_state):
        """Test that indexed payload has correct structure."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        manager.index_completed_cycles()

        assert len(mock_store.chunks) == 1
        chunk = mock_store.chunks[0]

        assert chunk["source"] == "cycle_report"
        assert chunk["cycle_id"] == "completed-cycle-1"
        assert chunk["mode"] == "guided_execution"
        assert chunk["final_status"] == "SUCCESS"
        assert "content" in chunk
        assert "content_vector" in chunk
        assert len(chunk["content_vector"]) == 384

    def test_filters_incomplete_cycles(
        self, temp_odibi_dir, sample_completed_state, sample_incomplete_state
    ):
        """Test that incomplete cycles are filtered out."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)
        self._save_cycle_state(temp_odibi_dir, sample_incomplete_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        result = manager.index_completed_cycles()

        assert len(result.indexed) == 1
        assert sample_completed_state.cycle_id in result.indexed
        assert sample_incomplete_state.cycle_id in result.skipped_incomplete

    def test_filters_failed_cycles(
        self, temp_odibi_dir, sample_completed_state, sample_failed_state
    ):
        """Test that failed cycles are filtered out."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)
        self._save_cycle_state(temp_odibi_dir, sample_failed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        result = manager.index_completed_cycles()

        assert len(result.indexed) == 1
        assert sample_completed_state.cycle_id in result.indexed
        assert sample_failed_state.cycle_id in result.skipped_failed

    def test_get_index_stats(self, temp_odibi_dir, sample_completed_state):
        """Test index statistics retrieval."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        manager.index_completed_cycles()
        stats = manager.get_index_stats()

        assert stats["indexed_cycle_count"] == 1
        assert stats["vector_document_count"] == 1
        assert sample_completed_state.cycle_id in stats["indexed_cycle_ids"]

    def test_clear_index(self, temp_odibi_dir, sample_completed_state):
        """Test clearing the index."""
        self._save_cycle_state(temp_odibi_dir, sample_completed_state)

        mock_store = MockVectorStore()
        mock_embedder = MockEmbedder()
        manager = CycleIndexManager(
            temp_odibi_dir,
            vector_store=mock_store,
            embedder=mock_embedder,
        )

        manager.index_completed_cycles()
        assert manager.get_index_stats()["indexed_cycle_count"] == 1

        manager.clear_index()

        assert manager.get_index_stats()["indexed_cycle_count"] == 0
        assert mock_store._deleted is True


class TestGenerateChunkId:
    """Tests for chunk ID generation."""

    def test_deterministic_ids(self):
        """Test that chunk IDs are deterministic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)

            manager = CycleIndexManager(odibi_dir)
            id1 = manager._generate_chunk_id("cycle-123")
            id2 = manager._generate_chunk_id("cycle-123")
            id3 = manager._generate_chunk_id("cycle-456")

            assert id1 == id2
            assert id1 != id3
            assert "cycle-123" in id1


class TestIndexedFileChange:
    """Tests for IndexedFileChange (Phase 5.D)."""

    def test_from_proposal_change_modify(self):
        """Test creating file change from modify proposal."""
        change = {
            "file": "odibi/transformers/cast.py",
            "before": "old_code()",
            "after": "new_code()",
        }

        fc = IndexedFileChange.from_proposal_change(
            change=change,
            issue_types=["CONFIGURATION_ERROR"],
            cycle_id="test-cycle-1",
            validated=True,
        )

        assert fc.file_path == "odibi/transformers/cast.py"
        assert fc.change_type == FileChangeType.MODIFY
        assert fc.issue_types == ["CONFIGURATION_ERROR"]
        assert fc.cycle_id == "test-cycle-1"
        assert fc.validated_by_golden_projects is True

    def test_from_proposal_change_add(self):
        """Test creating file change from add proposal."""
        change = {
            "file": "new_file.py",
            "before": "",
            "after": "new content",
        }

        fc = IndexedFileChange.from_proposal_change(
            change=change,
            issue_types=["DX"],
            cycle_id="test-cycle-2",
            validated=False,
        )

        assert fc.change_type == FileChangeType.ADD
        assert "Added" in fc.summary

    def test_from_proposal_change_remove(self):
        """Test creating file change from remove proposal."""
        change = {
            "file": "deprecated.py",
            "before": "old content",
            "after": "",
        }

        fc = IndexedFileChange.from_proposal_change(
            change=change,
            issue_types=["BUG"],
            cycle_id="test-cycle-3",
            validated=True,
        )

        assert fc.change_type == FileChangeType.REMOVE
        assert "Removed" in fc.summary

    def test_to_dict_contains_no_code(self):
        """Test that to_dict contains no executable content."""
        fc = IndexedFileChange(
            file_path="test.py",
            change_type=FileChangeType.MODIFY,
            summary="Changed function signature",
            issue_types=["BUG"],
            cycle_id="test-1",
            validated_by_golden_projects=True,
        )

        data = fc.to_dict()

        assert "file_path" in data
        assert "change_type" in data
        assert "summary" in data
        assert "before" not in data
        assert "after" not in data
        assert "diff" not in data
        assert "code" not in data

    def test_to_indexable_text_is_safe(self):
        """Test that indexable text contains no code."""
        fc = IndexedFileChange(
            file_path="test.py",
            change_type=FileChangeType.MODIFY,
            summary="Updated error handling",
            issue_types=["EXECUTION_ERROR"],
            cycle_id="test-1",
            validated_by_golden_projects=True,
        )

        text = fc.to_indexable_text()

        assert "test.py" in text
        assert "MODIFY" in text
        assert "Updated error handling" in text
        assert "Validated: Yes" in text


class TestFileChangeExtraction:
    """Tests for file change extraction from cycles (Phase 5.D)."""

    @pytest.fixture
    def temp_odibi_dir(self):
        """Create a temporary .odibi directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)
            os.makedirs(os.path.join(odibi_dir, "memories", "cycles"))
            yield odibi_dir

    @pytest.fixture
    def state_with_changes(self):
        """Create a state with file changes in the proposal."""
        config = CycleConfig(
            project_root="/projects/test",
            golden_projects=[GoldenProjectConfig(name="test_proj", path="test.yaml")],
        )
        state = CycleState(
            cycle_id="change-cycle-1",
            config=config,
            completed=True,
            exit_status="COMPLETED",
            improvements_approved=1,
            review_approved=True,
            golden_project_results=[{"name": "test_proj", "status": "PASSED"}],
        )
        state.logs = [
            CycleStepLog(
                step="improvement",
                agent_role="improvement",
                started_at="2024-01-15T09:30:00",
                completed_at="2024-01-15T09:35:00",
                input_summary="",
                output_summary="",
                success=True,
                metadata={
                    "proposal": {
                        "title": "Fix null handling",
                        "rationale": "Prevents crashes",
                        "based_on_issues": ["DATA_QUALITY", "BUG"],
                        "changes": [
                            {
                                "file": "odibi/core/parser.py",
                                "before": "value = data['key']",
                                "after": "value = data.get('key')",
                            },
                            {
                                "file": "odibi/core/utils.py",
                                "before": "",
                                "after": "def safe_get(): pass",
                            },
                        ],
                    }
                },
            )
        ]
        return state

    def test_extracts_file_changes_from_approved(self, temp_odibi_dir, state_with_changes):
        """Test that file changes are extracted from approved improvements."""
        manager = CycleIndexManager(temp_odibi_dir)
        file_changes = manager._extract_file_changes(state_with_changes)

        assert len(file_changes) == 2
        assert file_changes[0].file_path == "odibi/core/parser.py"
        assert file_changes[0].change_type == FileChangeType.MODIFY
        assert file_changes[1].file_path == "odibi/core/utils.py"
        assert file_changes[1].change_type == FileChangeType.ADD

    def test_no_file_changes_when_rejected(self, temp_odibi_dir, state_with_changes):
        """Test that no file changes are extracted when improvement rejected."""
        state_with_changes.review_approved = False
        state_with_changes.improvements_approved = 0

        manager = CycleIndexManager(temp_odibi_dir)
        file_changes = manager._extract_file_changes(state_with_changes)

        assert len(file_changes) == 0

    def test_file_changes_capture_issue_types(self, temp_odibi_dir, state_with_changes):
        """Test that issue types are captured in file changes."""
        manager = CycleIndexManager(temp_odibi_dir)
        file_changes = manager._extract_file_changes(state_with_changes)

        assert file_changes[0].issue_types == ["DATA_QUALITY", "BUG"]

    def test_file_changes_capture_validation_status(self, temp_odibi_dir, state_with_changes):
        """Test that validation status is captured correctly."""
        manager = CycleIndexManager(temp_odibi_dir)
        file_changes = manager._extract_file_changes(state_with_changes)

        assert file_changes[0].validated_by_golden_projects is True

        state_with_changes.golden_project_results = [{"name": "test_proj", "status": "FAILED"}]
        file_changes = manager._extract_file_changes(state_with_changes)

        assert file_changes[0].validated_by_golden_projects is False

    def test_document_includes_file_changes(self, temp_odibi_dir, state_with_changes):
        """Test that IndexedCycleDocument includes file changes."""
        manager = CycleIndexManager(temp_odibi_dir)
        doc = manager._build_document(state_with_changes)

        assert len(doc.file_changes) == 2
        assert doc.file_changes[0].file_path == "odibi/core/parser.py"

    def test_indexable_text_includes_file_changes(self, temp_odibi_dir, state_with_changes):
        """Test that indexable text includes file change summaries."""
        manager = CycleIndexManager(temp_odibi_dir)
        doc = manager._build_document(state_with_changes)
        text = doc.to_indexable_text()

        assert "File Changes" in text
        assert "odibi/core/parser.py" in text
        assert "[MODIFY]" in text
        assert "[ADD]" in text
