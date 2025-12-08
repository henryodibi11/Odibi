"""Pipeline progress tracking with Rich visualization.

This module provides progress visualization for pipeline execution with
auto-detection of environment (CLI vs notebook) and graceful fallback
when Rich is not available.
"""

from typing import Any, Dict, List, Optional

from odibi.utils.console import is_rich_available, get_console, _is_notebook_environment


class NodeStatus:
    """Status constants for node execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineProgress:
    """Progress tracker for pipeline execution.

    Provides visual feedback during pipeline runs with auto-detection
    of environment (CLI/notebook) and Rich availability.

    Example:
        >>> progress = PipelineProgress("my_pipeline", ["node1", "node2"])
        >>> progress.start()
        >>> progress.update_node("node1", NodeStatus.SUCCESS, duration=1.5, rows=1000)
        >>> progress.update_node("node2", NodeStatus.FAILED, duration=0.5)
        >>> progress.finish()
    """

    def __init__(
        self,
        pipeline_name: str,
        node_names: List[str],
        engine: str = "pandas",
    ) -> None:
        """Initialize progress tracker.

        Args:
            pipeline_name: Name of the pipeline being executed.
            node_names: List of node names in execution order.
            engine: Engine type (pandas/spark).
        """
        self.pipeline_name = pipeline_name
        self.node_names = node_names
        self.engine = engine
        self.is_notebook = _is_notebook_environment()
        self.use_rich = is_rich_available()

        self._node_statuses: Dict[str, Dict[str, Any]] = {
            name: {"status": NodeStatus.PENDING, "duration": None, "rows": None}
            for name in node_names
        }
        self._live: Optional[Any] = None
        self._table: Optional[Any] = None
        self._start_time: Optional[float] = None

    def start(self) -> None:
        """Start progress display."""
        import time

        self._start_time = time.time()

        if self.use_rich:
            self._start_rich()
        else:
            self._start_plain()

    def _start_rich(self) -> None:
        """Start Rich live display."""
        from rich.live import Live

        console = get_console()

        header = self._create_header_panel()
        console.print(header)

        if not self.is_notebook:
            self._table = self._create_progress_table()
            self._live = Live(
                self._table,
                console=console,
                refresh_per_second=4,
                transient=True,
            )
            self._live.start()
        else:
            console.print(f"[dim]Executing {len(self.node_names)} nodes...[/dim]\n")

    def _start_plain(self) -> None:
        """Start plain text display."""
        print(f"\n{'=' * 60}")
        print(f"  Pipeline: {self.pipeline_name}")
        print(f"  Engine: {self.engine}")
        print(f"  Nodes: {len(self.node_names)}")
        print(f"{'=' * 60}\n")

    def _create_header_panel(self) -> Any:
        """Create the header panel."""
        from rich.panel import Panel
        from rich.text import Text

        header_text = Text()
        header_text.append("Pipeline: ", style="dim")
        header_text.append(f"{self.pipeline_name}\n", style="bold cyan")
        header_text.append("Engine: ", style="dim")
        header_text.append(f"{self.engine}  ", style="green")
        header_text.append("Nodes: ", style="dim")
        header_text.append(f"{len(self.node_names)}", style="yellow")

        return Panel(
            header_text,
            title="[bold]Odibi Pipeline[/bold]",
            border_style="blue",
            padding=(0, 2),
        )

    def _create_progress_table(self) -> Any:
        """Create the progress table."""
        from rich.table import Table

        table = Table(
            show_header=True,
            header_style="bold",
            box=None,
            padding=(0, 1),
        )
        table.add_column("Node", style="cyan", min_width=30)
        table.add_column("Status", justify="center", min_width=10)
        table.add_column("Duration", justify="right", min_width=10)
        table.add_column("Rows", justify="right", min_width=12)

        for name in self.node_names:
            info = self._node_statuses[name]
            status_str = self._format_status(info["status"])
            duration_str = self._format_duration(info["duration"])
            rows_str = self._format_rows(info["rows"])
            table.add_row(name, status_str, duration_str, rows_str)

        return table

    def _format_status(self, status: str) -> str:
        """Format status with Rich markup."""
        status_map = {
            NodeStatus.PENDING: "[dim]○ pending[/dim]",
            NodeStatus.RUNNING: "[yellow]◉ running[/yellow]",
            NodeStatus.SUCCESS: "[green]✓ success[/green]",
            NodeStatus.FAILED: "[red]✗ failed[/red]",
            NodeStatus.SKIPPED: "[dim]⏭ skipped[/dim]",
        }
        return status_map.get(status, status)

    def _format_status_plain(self, status: str) -> str:
        """Format status for plain text."""
        status_map = {
            NodeStatus.PENDING: "○ pending",
            NodeStatus.RUNNING: "◉ running",
            NodeStatus.SUCCESS: "✓ success",
            NodeStatus.FAILED: "✗ failed",
            NodeStatus.SKIPPED: "⏭ skipped",
        }
        return status_map.get(status, status)

    def _format_duration(self, duration: Optional[float]) -> str:
        """Format duration value."""
        if duration is None:
            return "-"
        if duration < 1:
            return f"{duration * 1000:.0f}ms"
        return f"{duration:.2f}s"

    def _format_rows(self, rows: Optional[int]) -> str:
        """Format row count."""
        if rows is None:
            return "-"
        if rows >= 1_000_000:
            return f"{rows / 1_000_000:.1f}M"
        if rows >= 1_000:
            return f"{rows / 1_000:.1f}K"
        return str(rows)

    def update_node(
        self,
        name: str,
        status: str,
        duration: Optional[float] = None,
        rows: Optional[int] = None,
        phase_timings: Optional[Dict[str, float]] = None,
    ) -> None:
        """Update node status.

        Args:
            name: Node name.
            status: Status from NodeStatus constants.
            duration: Execution duration in seconds.
            rows: Number of rows processed.
            phase_timings: Optional dict of phase name -> duration in ms.
        """
        if name not in self._node_statuses:
            return

        self._node_statuses[name] = {
            "status": status,
            "duration": duration,
            "rows": rows,
            "phase_timings": phase_timings,
        }

        if self.use_rich:
            self._update_rich(name, status, duration, rows)
        else:
            self._update_plain(name, status, duration, rows)

    def _update_rich(
        self,
        name: str,
        status: str,
        duration: Optional[float],
        rows: Optional[int],
    ) -> None:
        """Update Rich display."""
        if self._live and not self.is_notebook:
            self._table = self._create_progress_table()
            self._live.update(self._table)
        elif self.is_notebook:
            console = get_console()
            status_str = self._format_status(status)
            duration_str = self._format_duration(duration)
            rows_str = self._format_rows(rows)
            console.print(f"  {name}: {status_str} ({duration_str}, {rows_str} rows)")

    def _update_plain(
        self,
        name: str,
        status: str,
        duration: Optional[float],
        rows: Optional[int],
    ) -> None:
        """Update plain text display."""
        status_str = self._format_status_plain(status)
        duration_str = self._format_duration(duration)
        rows_str = self._format_rows(rows)
        print(f"  {name}: {status_str} ({duration_str}, {rows_str} rows)")

    def finish(
        self,
        completed: int = 0,
        failed: int = 0,
        skipped: int = 0,
        duration: Optional[float] = None,
    ) -> None:
        """Finish progress display and show summary.

        Args:
            completed: Number of completed nodes.
            failed: Number of failed nodes.
            skipped: Number of skipped nodes.
            duration: Total pipeline duration in seconds.
        """
        if self._live:
            self._live.stop()
            self._live = None

        import time

        total_duration = duration or ((time.time() - self._start_time) if self._start_time else 0)

        if self.use_rich:
            self._finish_rich(completed, failed, skipped, total_duration)
        else:
            self._finish_plain(completed, failed, skipped, total_duration)

    def _finish_rich(
        self,
        completed: int,
        failed: int,
        skipped: int,
        duration: float,
    ) -> None:
        """Finish with Rich summary."""
        from rich.panel import Panel
        from rich.text import Text

        console = get_console()

        final_table = self._create_progress_table()
        console.print(final_table)
        console.print()

        status = "[green]SUCCESS[/green]" if failed == 0 else "[red]FAILED[/red]"
        summary = Text()
        summary.append("Status: ")
        summary.append_text(Text.from_markup(status))
        summary.append("\n")
        summary.append("Duration: ", style="dim")
        summary.append(f"{duration:.2f}s\n")
        summary.append("Completed: ", style="dim")
        summary.append(f"{completed}", style="green")
        if failed > 0:
            summary.append("  Failed: ", style="dim")
            summary.append(f"{failed}", style="red")
        if skipped > 0:
            summary.append("  Skipped: ", style="dim")
            summary.append(f"{skipped}", style="yellow")

        panel_style = "green" if failed == 0 else "red"
        panel = Panel(
            summary,
            title="[bold]Pipeline Complete[/bold]",
            border_style=panel_style,
            padding=(0, 2),
        )
        console.print(panel)

    def _finish_plain(
        self,
        completed: int,
        failed: int,
        skipped: int,
        duration: float,
    ) -> None:
        """Finish with plain text summary."""
        status = "SUCCESS" if failed == 0 else "FAILED"
        print(f"\n{'=' * 60}")
        print(f"  Pipeline: {status}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Completed: {completed}, Failed: {failed}, Skipped: {skipped}")
        print(f"{'=' * 60}\n")

    def get_phase_timing_summary(self) -> Dict[str, Dict[str, float]]:
        """Get phase timing breakdown for all nodes.

        Returns:
            Dict mapping node names to their phase timings (in ms).
        """
        return {
            name: info.get("phase_timings", {})
            for name, info in self._node_statuses.items()
            if info.get("phase_timings")
        }

    def get_aggregate_phase_timings(self) -> Dict[str, float]:
        """Get max phase timings across all nodes (bottleneck per phase).

        Returns:
            Dict mapping phase names to max time spent by any node (in ms).
        """
        max_timings: Dict[str, float] = {}
        for info in self._node_statuses.values():
            phase_timings = info.get("phase_timings") or {}
            for phase, duration_ms in phase_timings.items():
                max_timings[phase] = max(max_timings.get(phase, 0), duration_ms)
        return {k: round(v, 2) for k, v in max_timings.items()}

    def print_phase_timing_report(self, pipeline_duration_s: Optional[float] = None) -> None:
        """Print a detailed phase timing report.

        Args:
            pipeline_duration_s: Actual pipeline wall-clock duration in seconds.
                Used for percentage calculations. Falls back to sum of max phases.
        """
        aggregate = self.get_aggregate_phase_timings()
        if not aggregate:
            return

        # Use actual pipeline duration for percentage, or fall back to sum of max phases
        if pipeline_duration_s is not None:
            total_ms = pipeline_duration_s * 1000
        else:
            total_ms = sum(aggregate.values())

        if self.use_rich:
            self._print_phase_timing_rich(aggregate, total_ms)
        else:
            self._print_phase_timing_plain(aggregate, total_ms)

    def _print_phase_timing_rich(self, aggregate: Dict[str, float], total_ms: float) -> None:
        """Print phase timing report with Rich."""
        from rich.panel import Panel
        from rich.table import Table

        console = get_console()

        table = Table(
            show_header=True,
            header_style="bold",
            box=None,
            padding=(0, 1),
        )
        table.add_column("Phase", style="cyan")
        table.add_column("Slowest", justify="right")
        table.add_column("% of Pipeline", justify="right")

        # Sort by time descending
        sorted_phases = sorted(aggregate.items(), key=lambda x: x[1], reverse=True)

        for phase, duration_ms in sorted_phases:
            pct = (duration_ms / total_ms * 100) if total_ms > 0 else 0
            duration_str = (
                f"{duration_ms:.0f}ms" if duration_ms < 1000 else f"{duration_ms / 1000:.2f}s"
            )
            table.add_row(phase, duration_str, f"{pct:.1f}%")

        panel = Panel(
            table,
            title="[bold]Phase Bottlenecks (slowest node per phase)[/bold]",
            border_style="dim",
            padding=(0, 1),
        )
        console.print(panel)

    def _print_phase_timing_plain(self, aggregate: Dict[str, float], total_ms: float) -> None:
        """Print phase timing report in plain text."""
        print("\n--- Phase Bottlenecks (slowest node per phase) ---")
        sorted_phases = sorted(aggregate.items(), key=lambda x: x[1], reverse=True)

        for phase, duration_ms in sorted_phases:
            pct = (duration_ms / total_ms * 100) if total_ms > 0 else 0
            duration_str = (
                f"{duration_ms:.0f}ms" if duration_ms < 1000 else f"{duration_ms / 1000:.2f}s"
            )
            print(f"  {phase}: {duration_str} ({pct:.1f}% of pipeline)")
        print("-" * 48 + "\n")
