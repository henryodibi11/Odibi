"""StatusTracker: Track and report campaign status.

Maintains status.json with atomic updates after each cycle.
Provides status summaries and markdown report generation.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from odibi.agents.improve.results import CycleResult

logger = logging.getLogger(__name__)


@dataclass
class CampaignSummary:
    """Summary of campaign status."""

    campaign_id: str
    status: str
    started_at: Optional[str]
    current_cycle: int
    cycles_completed: int
    improvements_promoted: int
    improvements_rejected: int
    master_changes: int
    last_promotion: Optional[str]
    convergence_counter: int
    issues_avoided: list[str]
    elapsed_hours: float
    stop_reason: Optional[str] = None


class StatusTracker:
    """Track and report campaign status.

    Maintains status.json with atomic updates after each cycle.
    Status is always queryable, even during campaign execution.
    """

    def __init__(
        self,
        status_file: Path,
        reports_dir: Optional[Path] = None,
    ) -> None:
        """Initialize status tracker.

        Args:
            status_file: Path to status.json file.
            reports_dir: Optional directory for markdown reports.
        """
        self._status_file = Path(status_file)
        self._reports_dir = Path(reports_dir) if reports_dir else status_file.parent / "reports"
        self._campaign_id: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._cycles: list[CycleResult] = []

    @property
    def status_file(self) -> Path:
        """Path to status.json."""
        return self._status_file

    def _load_status(self) -> dict:
        """Load current status from file."""
        if not self._status_file.exists():
            return {}
        try:
            with open(self._status_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load status file: {e}")
            return {}

    def _save_status(self, status: dict) -> None:
        """Save status to file atomically.

        Uses write-to-temp-then-rename pattern for atomicity.
        """
        self._status_file.parent.mkdir(parents=True, exist_ok=True)

        temp_file = self._status_file.with_suffix(".tmp")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2)
            temp_file.replace(self._status_file)
        except OSError as e:
            logger.error(f"Failed to save status: {e}")
            if temp_file.exists():
                temp_file.unlink()
            raise

    def start_campaign(self, campaign_id: str) -> None:
        """Mark campaign as started.

        Args:
            campaign_id: Unique identifier for this campaign.
        """
        self._campaign_id = campaign_id
        self._started_at = datetime.now()
        self._cycles = []

        status = {
            "campaign_id": campaign_id,
            "status": "RUNNING",
            "started_at": self._started_at.isoformat(),
            "current_cycle": 0,
            "cycles_completed": 0,
            "improvements_promoted": 0,
            "improvements_rejected": 0,
            "master_changes": 0,
            "last_promotion": None,
            "convergence_counter": 0,
            "issues_avoided": [],
            "elapsed_hours": 0.0,
        }
        self._save_status(status)
        logger.info(f"Started campaign: {campaign_id}")

    def update(self, cycle: CycleResult) -> None:
        """Update status after a cycle completes.

        Args:
            cycle: The completed cycle result.
        """
        self._cycles.append(cycle)
        status = self._load_status()

        status["current_cycle"] = len(self._cycles)
        status["cycles_completed"] = len(self._cycles)

        if cycle.promoted:
            status["improvements_promoted"] = status.get("improvements_promoted", 0) + 1
            status["master_changes"] = status.get("master_changes", 0) + len(cycle.files_modified)
            status["last_promotion"] = datetime.now().isoformat()
            status["convergence_counter"] = 0
        else:
            status["improvements_rejected"] = status.get("improvements_rejected", 0) + 1
            if not cycle.learning:
                status["convergence_counter"] = status.get("convergence_counter", 0) + 1
            else:
                status["convergence_counter"] = 0

        if cycle.rejection_reason and cycle.lesson:
            avoided = status.get("issues_avoided", [])
            if cycle.lesson not in avoided:
                avoided.append(cycle.lesson)
            status["issues_avoided"] = avoided[-20:]  # Keep last 20

        if self._started_at:
            elapsed = (datetime.now() - self._started_at).total_seconds() / 3600
            status["elapsed_hours"] = round(elapsed, 2)

        self._save_status(status)
        logger.debug(f"Updated status for cycle {cycle.cycle_id}")

    def finish_campaign(self, stop_reason: str) -> None:
        """Mark campaign as finished.

        Args:
            stop_reason: Why the campaign stopped.
        """
        status = self._load_status()
        status["status"] = "COMPLETED"
        status["stop_reason"] = stop_reason
        status["completed_at"] = datetime.now().isoformat()

        if self._started_at:
            elapsed = (datetime.now() - self._started_at).total_seconds() / 3600
            status["elapsed_hours"] = round(elapsed, 2)

        self._save_status(status)
        logger.info(f"Campaign finished: {stop_reason}")

    def get_summary(self) -> CampaignSummary:
        """Get current campaign summary.

        Returns:
            CampaignSummary with current state.
        """
        status = self._load_status()

        return CampaignSummary(
            campaign_id=status.get("campaign_id", "unknown"),
            status=status.get("status", "UNKNOWN"),
            started_at=status.get("started_at"),
            current_cycle=status.get("current_cycle", 0),
            cycles_completed=status.get("cycles_completed", 0),
            improvements_promoted=status.get("improvements_promoted", 0),
            improvements_rejected=status.get("improvements_rejected", 0),
            master_changes=status.get("master_changes", 0),
            last_promotion=status.get("last_promotion"),
            convergence_counter=status.get("convergence_counter", 0),
            issues_avoided=status.get("issues_avoided", []),
            elapsed_hours=status.get("elapsed_hours", 0.0),
            stop_reason=status.get("stop_reason"),
        )

    def get_elapsed_hours(self) -> float:
        """Get elapsed time in hours.

        Returns:
            Hours since campaign started.
        """
        if self._started_at:
            return (datetime.now() - self._started_at).total_seconds() / 3600
        status = self._load_status()
        return status.get("elapsed_hours", 0.0)

    def get_convergence_counter(self) -> int:
        """Get current convergence counter.

        Returns:
            Number of consecutive cycles without learning.
        """
        status = self._load_status()
        return status.get("convergence_counter", 0)

    def generate_report(self) -> str:
        """Generate markdown report of campaign progress.

        Returns:
            Markdown-formatted report string.
        """
        summary = self.get_summary()

        lines = [
            "# Campaign Report",
            "",
            f"**Campaign ID:** {summary.campaign_id}",
            f"**Status:** {summary.status}",
            f"**Started:** {summary.started_at or 'N/A'}",
            f"**Elapsed:** {summary.elapsed_hours:.2f} hours",
            "",
            "## Summary",
            "",
            f"- Cycles completed: {summary.cycles_completed}",
            f"- Improvements promoted: {summary.improvements_promoted}",
            f"- Improvements rejected: {summary.improvements_rejected}",
            f"- Master changes: {summary.master_changes}",
            f"- Convergence counter: {summary.convergence_counter}",
            "",
        ]

        if summary.stop_reason:
            lines.extend(
                [
                    "## Stop Reason",
                    "",
                    f"{summary.stop_reason}",
                    "",
                ]
            )

        if summary.last_promotion:
            lines.extend(
                [
                    "## Last Promotion",
                    "",
                    f"{summary.last_promotion}",
                    "",
                ]
            )

        if summary.issues_avoided:
            lines.extend(
                [
                    "## Issues Avoided",
                    "",
                ]
            )
            for issue in summary.issues_avoided:
                lines.append(f"- {issue}")
            lines.append("")

        if self._cycles:
            lines.extend(
                [
                    "## Cycle Details",
                    "",
                    "| Cycle | Promoted | Learning | Files Modified | Reason |",
                    "|-------|----------|----------|----------------|--------|",
                ]
            )
            for cycle in self._cycles:
                promoted = "✓" if cycle.promoted else "✗"
                learning = "✓" if cycle.learning else "✗"
                files = len(cycle.files_modified)
                reason = cycle.lesson or cycle.rejection_reason or "-"
                reason = reason[:50] + "..." if len(reason) > 50 else reason
                lines.append(f"| {cycle.cycle_id} | {promoted} | {learning} | {files} | {reason} |")
            lines.append("")

        return "\n".join(lines)

    def save_report(self, filename: Optional[str] = None) -> Path:
        """Save markdown report to file.

        Args:
            filename: Optional filename (defaults to campaign_id.md).

        Returns:
            Path to saved report.
        """
        summary = self.get_summary()
        self._reports_dir.mkdir(parents=True, exist_ok=True)

        if filename is None:
            filename = f"{summary.campaign_id}_report.md"

        report_path = self._reports_dir / filename
        report_content = self.generate_report()

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        logger.info(f"Saved report to {report_path}")
        return report_path
