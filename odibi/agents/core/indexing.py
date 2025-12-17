from __future__ import annotations

"""Manual Re-Indexing of Cycle Learnings.

Phase 5.C: Implements a manual, explicit re-indexing step for cycle learnings.
Phase 5.D: Adds file-change summary indexing for approved improvements.

This module indexes high-signal, human-reviewable artifacts from completed
cycles so future agents can reason over validated learnings.

CRITICAL CONSTRAINTS:
- This is NOT automatic, NOT autonomous, NOT triggered by cycles
- Must be explicitly invoked by the user
- Only indexes validated, human-reviewable content
- No raw agent messages, no rejected proposals, no partial cycles
- No full diffs, no raw code, no executable content
"""

import json
import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from odibi.agents.core.cycle import CycleState, CycleStore
from odibi.agents.core.embeddings import BaseEmbedder, get_default_embedder
from odibi.agents.core.vector_store import BaseVectorStore

logger = logging.getLogger(__name__)


class FileChangeType(Enum):
    """Types of file changes in approved improvements."""

    ADD = "ADD"
    MODIFY = "MODIFY"
    REMOVE = "REMOVE"


@dataclass
class IndexedFileChange:
    """Safe summary of a file change from an approved improvement.

    This structure contains ONLY safe, human-readable information.
    It explicitly does NOT contain:
    - Full diffs
    - Raw code content
    - Executable content
    - Before/after snapshots
    """

    file_path: str
    change_type: FileChangeType
    summary: str
    issue_types: list[str]
    cycle_id: str
    validated_by_golden_projects: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "change_type": self.change_type.value,
            "summary": self.summary,
            "issue_types": self.issue_types,
            "cycle_id": self.cycle_id,
            "validated_by_golden_projects": self.validated_by_golden_projects,
        }

    def to_indexable_text(self) -> str:
        """Generate safe text for embedding."""
        parts = [
            f"File Change: {self.file_path}",
            f"Type: {self.change_type.value}",
            f"Summary: {self.summary}",
        ]
        if self.issue_types:
            parts.append(f"Issues Addressed: {', '.join(self.issue_types)}")
        parts.append(f"Validated: {'Yes' if self.validated_by_golden_projects else 'No'}")
        return "\n".join(parts)

    @classmethod
    def from_proposal_change(
        cls,
        change: dict[str, Any],
        issue_types: list[str],
        cycle_id: str,
        validated: bool,
    ) -> "IndexedFileChange":
        """Create from a proposal change dict.

        Extracts ONLY safe summary information, discarding raw code.

        Args:
            change: The change dict from ImprovementProposal.
            issue_types: Issue types addressed by this change.
            cycle_id: The cycle ID.
            validated: Whether golden projects passed.

        Returns:
            Safe IndexedFileChange with no executable content.
        """
        file_path = change.get("file", "unknown")
        before = change.get("before", "")
        after = change.get("after", "")

        if not before and after:
            change_type = FileChangeType.ADD
            summary = f"Added new content to {file_path}"
        elif before and not after:
            change_type = FileChangeType.REMOVE
            summary = f"Removed content from {file_path}"
        else:
            change_type = FileChangeType.MODIFY
            summary = f"Modified {file_path}"

        return cls(
            file_path=file_path,
            change_type=change_type,
            summary=summary,
            issue_types=issue_types,
            cycle_id=cycle_id,
            validated_by_golden_projects=validated,
        )


@dataclass
class IndexedCycleDocument:
    """Document structure for indexed cycle learnings."""

    cycle_id: str
    timestamp: str
    mode: str
    workspace_root: str
    golden_projects: list[str]
    approved_improvements: list[dict[str, Any]]
    regression_results: list[dict[str, Any]]
    final_status: str
    conclusions: str
    file_changes: list[IndexedFileChange] = field(default_factory=list)
    is_learning_mode: bool = False
    observations: list[dict[str, Any]] = field(default_factory=list)
    patterns_discovered: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "cycle_id": self.cycle_id,
            "timestamp": self.timestamp,
            "mode": self.mode,
            "workspace_root": self.workspace_root,
            "golden_projects": self.golden_projects,
            "approved_improvements": self.approved_improvements,
            "regression_results": self.regression_results,
            "final_status": self.final_status,
            "conclusions": self.conclusions,
            "file_changes": [fc.to_dict() for fc in self.file_changes],
            "is_learning_mode": self.is_learning_mode,
            "observations": self.observations,
            "patterns_discovered": self.patterns_discovered,
        }

    def to_indexable_text(self) -> str:
        """Generate text content for embedding.

        Returns only high-signal, validated content suitable for
        semantic search and agent reasoning.
        """
        parts = [
            f"# Cycle Learning: {self.cycle_id}",
            f"Mode: {self.mode}",
            f"Status: {self.final_status}",
            f"Workspace: {self.workspace_root}",
        ]

        if self.is_learning_mode:
            parts.append("Type: LEARNING (observation-only, no changes)")

        if self.golden_projects:
            parts.append(f"Golden Projects: {', '.join(self.golden_projects)}")

        if self.observations:
            parts.append("\n## Observations")
            for obs in self.observations:
                severity = obs.get("severity", "INFO")
                obs_type = obs.get("type", "UNKNOWN")
                description = obs.get("description", "")
                parts.append(f"- [{severity}] {obs_type}: {description}")

        if self.patterns_discovered:
            parts.append("\n## Patterns Discovered")
            for pattern in self.patterns_discovered:
                parts.append(f"- {pattern}")

        if self.approved_improvements:
            parts.append("\n## Approved Improvements")
            for imp in self.approved_improvements:
                title = imp.get("title", "Untitled")
                rationale = imp.get("rationale", "")
                parts.append(f"- {title}")
                if rationale:
                    parts.append(f"  Rationale: {rationale}")

        if self.file_changes:
            parts.append("\n## File Changes")
            for fc in self.file_changes:
                parts.append(f"- [{fc.change_type.value}] {fc.file_path}: {fc.summary}")

        if self.regression_results:
            parts.append("\n## Regression Results")
            for result in self.regression_results:
                name = result.get("name", "unknown")
                status = result.get("status", "UNKNOWN")
                parts.append(f"- {name}: {status}")

        if self.conclusions:
            parts.append(f"\n## Conclusions\n{self.conclusions}")

        return "\n".join(parts)


@dataclass
class IndexingResult:
    """Result of an indexing operation."""

    indexed: list[str] = field(default_factory=list)
    skipped_already_indexed: list[str] = field(default_factory=list)
    skipped_incomplete: list[str] = field(default_factory=list)
    skipped_failed: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total_processed(self) -> int:
        return (
            len(self.indexed)
            + len(self.skipped_already_indexed)
            + len(self.skipped_incomplete)
            + len(self.skipped_failed)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "indexed": self.indexed,
            "skipped_already_indexed": self.skipped_already_indexed,
            "skipped_incomplete": self.skipped_incomplete,
            "skipped_failed": self.skipped_failed,
            "errors": self.errors,
            "total_processed": self.total_processed,
        }


class CycleIndexManager:
    """Manages manual re-indexing of cycle learnings.

    This class provides the ability to index validated cycle learnings
    into a vector store for semantic retrieval by future agents.

    USAGE:
        manager = CycleIndexManager(odibi_root)
        result = manager.index_completed_cycles()

    IMPORTANT: This must be explicitly invoked by the user.
    """

    COLLECTION_NAME = "odibi-cycle-learnings"
    INDEX_METADATA_FILE = "indexed_cycles.json"

    def __init__(
        self,
        odibi_root: str,
        vector_store: BaseVectorStore | None = None,
        embedder: BaseEmbedder | None = None,
    ):
        """Initialize the cycle index manager.

        Args:
            odibi_root: Path to the .odibi directory.
            vector_store: Optional vector store (defaults to ChromaDB).
            embedder: Optional embedder (defaults to local embedder).
        """
        self.odibi_root = odibi_root
        self.memories_dir = os.path.join(odibi_root, "memories")
        self.reports_dir = os.path.join(odibi_root, "reports")

        self._vector_store = vector_store
        self._embedder = embedder
        self._indexed_cycles: set[str] | None = None

    @property
    def vector_store(self) -> BaseVectorStore:
        """Lazy-load vector store."""
        if self._vector_store is None:
            from odibi.agents.core.chroma_store import ChromaVectorStore

            persist_dir = os.path.join(self.odibi_root, "cycle_index")
            self._vector_store = ChromaVectorStore(
                persist_dir=persist_dir,
                collection_name=self.COLLECTION_NAME,
            )
        return self._vector_store

    @property
    def embedder(self) -> BaseEmbedder:
        """Lazy-load embedder."""
        if self._embedder is None:
            self._embedder = get_default_embedder()
        return self._embedder

    def _load_indexed_cycles(self) -> set[str]:
        """Load set of already indexed cycle IDs."""
        if self._indexed_cycles is not None:
            return self._indexed_cycles

        metadata_path = os.path.join(self.odibi_root, self.INDEX_METADATA_FILE)
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._indexed_cycles = set(data.get("indexed_cycle_ids", []))
            except Exception as e:
                logger.warning(f"Failed to load index metadata: {e}")
                self._indexed_cycles = set()
        else:
            self._indexed_cycles = set()

        return self._indexed_cycles

    def _save_indexed_cycles(self) -> None:
        """Save set of indexed cycle IDs."""
        if self._indexed_cycles is None:
            return

        metadata_path = os.path.join(self.odibi_root, self.INDEX_METADATA_FILE)
        try:
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(
                    {"indexed_cycle_ids": list(self._indexed_cycles)},
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.error(f"Failed to save index metadata: {e}")

    def _is_already_indexed(self, cycle_id: str) -> bool:
        """Check if a cycle is already indexed."""
        return cycle_id in self._load_indexed_cycles()

    def _mark_as_indexed(self, cycle_id: str) -> None:
        """Mark a cycle as indexed."""
        self._load_indexed_cycles().add(cycle_id)
        self._save_indexed_cycles()

    def _is_eligible_for_indexing(self, state: CycleState) -> tuple[bool, str]:
        """Check if a cycle state is eligible for indexing.

        Returns:
            Tuple of (eligible, reason).
        """
        if not state.completed:
            return False, "incomplete"

        if state.exit_status == "FAILURE":
            return False, "failed"

        if state.interrupted:
            return False, "interrupted"

        return True, ""

    def _extract_approved_improvements(self, state: CycleState) -> list[dict[str, Any]]:
        """Extract only approved improvements from cycle state."""
        if state.improvements_approved == 0:
            return []

        if not state.review_approved:
            return []

        for log in state.logs:
            if log.step == "improvement" and log.metadata:
                proposal = log.metadata.get("proposal")
                if proposal and isinstance(proposal, dict):
                    return [proposal]

        return []

    def _extract_file_changes(self, state: CycleState) -> list[IndexedFileChange]:
        """Extract safe file change summaries from approved improvements.

        ONLY extracts changes from APPROVED AND APPLIED improvements.
        Does NOT store:
        - Full diffs
        - Raw code
        - Rejected or failed changes
        - Executable content

        Args:
            state: The cycle state.

        Returns:
            List of safe file change summaries.
        """
        if state.improvements_approved == 0 or not state.review_approved:
            return []

        validated = not any(
            r.get("status") in ("FAILED", "ERROR") for r in state.golden_project_results
        )

        file_changes = []

        for log in state.logs:
            if log.step == "improvement" and log.metadata:
                proposal = log.metadata.get("proposal")
                if proposal and isinstance(proposal, dict):
                    changes = proposal.get("changes", [])
                    issue_types = proposal.get("based_on_issues", [])

                    for change in changes:
                        if isinstance(change, dict) and change.get("file"):
                            fc = IndexedFileChange.from_proposal_change(
                                change=change,
                                issue_types=issue_types,
                                cycle_id=state.cycle_id,
                                validated=validated,
                            )
                            file_changes.append(fc)

        return file_changes

    def _build_document(self, state: CycleState) -> IndexedCycleDocument:
        """Build an IndexedCycleDocument from CycleState."""
        return IndexedCycleDocument(
            cycle_id=state.cycle_id,
            timestamp=state.completed_at or state.started_at,
            mode=state.mode,
            workspace_root=state.config.project_root,
            golden_projects=[gp.name for gp in state.config.golden_projects],
            approved_improvements=self._extract_approved_improvements(state),
            regression_results=state.golden_project_results,
            final_status=state.get_final_status(),
            conclusions=state.summary,
            file_changes=self._extract_file_changes(state),
            is_learning_mode=state.config.is_learning_mode,
            observations=self._extract_observations(state),
            patterns_discovered=self._extract_patterns(state),
        )

    def _extract_observations(self, state: CycleState) -> list[dict[str, Any]]:
        """Extract observations from the cycle state.

        Only extracts structured observations from the ObserverAgent.
        Excludes raw agent reasoning and rejected proposals.
        """
        observations = []

        for log in state.logs:
            if log.step == "observation" and log.metadata:
                observer_output = log.metadata.get("observer_output")
                if observer_output and isinstance(observer_output, dict):
                    issues = observer_output.get("issues", [])
                    for issue in issues:
                        if isinstance(issue, dict):
                            observations.append(
                                {
                                    "severity": issue.get("severity", "INFO"),
                                    "type": issue.get("type", "UNKNOWN"),
                                    "description": issue.get("description", ""),
                                    "location": issue.get("location", ""),
                                }
                            )

        return observations

    def _extract_patterns(self, state: CycleState) -> list[str]:
        """Extract discovered patterns from the cycle state.

        Only extracts validated patterns, not raw agent chatter.
        """
        patterns = []

        for log in state.logs:
            if log.step == "observation" and log.metadata:
                observer_output = log.metadata.get("observer_output")
                if observer_output and isinstance(observer_output, dict):
                    summary = observer_output.get("observation_summary", "")
                    if summary:
                        patterns.append(summary)

        return patterns

    def _generate_chunk_id(self, cycle_id: str) -> str:
        """Generate a deterministic chunk ID for a cycle."""
        return f"cycle-learning-{cycle_id}"

    def index_completed_cycles(self, force: bool = False) -> IndexingResult:
        """Index all eligible completed cycles.

        This is the main entry point for manual re-indexing.
        Must be explicitly invoked by the user.

        Args:
            force: If True, re-index even if already indexed.

        Returns:
            IndexingResult with details of what was processed.
        """
        logger.info("Starting manual cycle indexing...")
        result = IndexingResult()

        cycle_store = CycleStore(self.memories_dir)

        try:
            cycles_list = cycle_store.list_cycles(limit=1000)
        except FileNotFoundError:
            logger.info("No cycles directory found")
            return result
        except Exception as e:
            result.errors.append(f"Failed to list cycles: {e}")
            return result

        chunks_to_index = []

        for cycle_info in cycles_list:
            cycle_id = cycle_info.get("cycle_id")
            if not cycle_id:
                continue

            if not force and self._is_already_indexed(cycle_id):
                logger.info(f"Skipped (already indexed): {cycle_id}")
                result.skipped_already_indexed.append(cycle_id)
                continue

            try:
                state = cycle_store.load(cycle_id)
            except Exception as e:
                logger.warning(f"Failed to load cycle {cycle_id}: {e}")
                result.errors.append(f"Failed to load {cycle_id}: {e}")
                continue

            eligible, reason = self._is_eligible_for_indexing(state)
            if not eligible:
                if reason == "incomplete":
                    logger.info(f"Skipped (not completed): {cycle_id}")
                    result.skipped_incomplete.append(cycle_id)
                else:
                    logger.info(f"Skipped ({reason}): {cycle_id}")
                    result.skipped_failed.append(cycle_id)
                continue

            document = self._build_document(state)
            text_content = document.to_indexable_text()

            chunks_to_index.append(
                {
                    "cycle_id": cycle_id,
                    "document": document,
                    "content": text_content,
                }
            )

        if not chunks_to_index:
            logger.info("No new cycles to index")
            return result

        texts = [c["content"] for c in chunks_to_index]
        logger.info(f"Embedding {len(texts)} cycle documents...")

        try:
            embeddings = self.embedder.embed_texts(texts)
        except Exception as e:
            result.errors.append(f"Embedding failed: {e}")
            return result

        vector_chunks = []
        for i, chunk_data in enumerate(chunks_to_index):
            cycle_id = chunk_data["cycle_id"]
            document = chunk_data["document"]

            vector_chunks.append(
                {
                    "id": self._generate_chunk_id(cycle_id),
                    "content": chunk_data["content"],
                    "content_vector": embeddings[i],
                    "source": "cycle_report",
                    "cycle_id": cycle_id,
                    "timestamp": document.timestamp,
                    "mode": document.mode,
                    "final_status": document.final_status,
                    "workspace_root": document.workspace_root,
                    "has_improvements": len(document.approved_improvements) > 0,
                    "has_regressions": any(
                        r.get("status") in ("FAILED", "ERROR") for r in document.regression_results
                    ),
                }
            )

        try:
            add_result = self.vector_store.add_chunks(vector_chunks)
            succeeded = add_result.get("succeeded", 0)
            logger.info(f"Indexed {succeeded} cycle documents")

            for chunk_data in chunks_to_index:
                cycle_id = chunk_data["cycle_id"]
                self._mark_as_indexed(cycle_id)
                result.indexed.append(cycle_id)
                logger.info(f"Indexed cycle: {cycle_id}")

        except Exception as e:
            result.errors.append(f"Vector store insert failed: {e}")

        return result

    def get_index_stats(self) -> dict[str, Any]:
        """Get statistics about the current index.

        Returns:
            Dictionary with index statistics.
        """
        indexed_cycles = self._load_indexed_cycles()
        try:
            doc_count = self.vector_store.count()
        except Exception:
            doc_count = 0

        return {
            "indexed_cycle_count": len(indexed_cycles),
            "vector_document_count": doc_count,
            "indexed_cycle_ids": list(indexed_cycles),
        }

    def clear_index(self) -> bool:
        """Clear all indexed cycle data.

        Returns:
            True if successful.
        """
        try:
            self.vector_store.delete_all()
            self._indexed_cycles = set()
            self._save_indexed_cycles()
            logger.info("Cleared cycle index")
            return True
        except Exception as e:
            logger.error(f"Failed to clear index: {e}")
            return False
