# tests/fixtures/mcp_catalog.py
"""Mock catalog fixtures for MCP integration tests."""

from dataclasses import dataclass
from typing import List, Optional, Set
from datetime import datetime, timedelta
import pandas as pd


@dataclass
class MockRunRecord:
    """Mock run record."""

    run_id: str
    pipeline: str
    node: str
    project: str
    status: str
    timestamp: datetime
    duration_ms: float
    rows_processed: int


@dataclass
class MockFailureRecord:
    """Mock failure record."""

    failure_id: str
    run_id: str
    pipeline: str
    node: str
    project: str
    error_type: str
    error_message: str
    timestamp: datetime


class MockCatalogManager:
    """Mock catalog manager for testing."""

    def __init__(self):
        self.runs: List[MockRunRecord] = []
        self.failures: List[MockFailureRecord] = []
        self._access = None
        self._project: Optional[str] = None

    def set_access_context(self, ctx) -> None:
        """Set access context for project scoping."""
        self._access = ctx

    @property
    def project(self) -> Optional[str]:
        return self._project

    @project.setter
    def project(self, value: Optional[str]) -> None:
        self._project = value

    def add_run(self, run: MockRunRecord) -> None:
        """Add a run record."""
        self.runs.append(run)

    def add_failure(self, failure: MockFailureRecord) -> None:
        """Add a failure record."""
        self.failures.append(failure)

    def get_runs(self, pipeline: str, node: Optional[str] = None) -> List[MockRunRecord]:
        """Get runs, filtered by access context."""
        authorized = self._get_authorized_projects()
        runs = [r for r in self.runs if r.pipeline == pipeline and r.project in authorized]
        if node:
            runs = [r for r in runs if r.node == node]
        return runs

    def get_failures(self, pipeline: Optional[str] = None) -> List[MockFailureRecord]:
        """Get failures, filtered by access context."""
        authorized = self._get_authorized_projects()
        failures = [f for f in self.failures if f.project in authorized]
        if pipeline:
            failures = [f for f in failures if f.pipeline == pipeline]
        return failures

    def _get_authorized_projects(self) -> Set[str]:
        """Get authorized projects from access context."""
        if self._access is None:
            return {r.project for r in self.runs}
        return self._access.authorized_projects

    def _read_table(self, path: str, apply_project_scope: bool = True) -> pd.DataFrame:
        """Mock read table."""
        return pd.DataFrame()

    def _apply_project_scope(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply project scoping to dataframe."""
        if self._access is None or df.empty:
            return df
        if "project" not in df.columns:
            return df
        projects = list(self._access.authorized_projects)
        return df[df["project"].isin(projects)]


def create_mock_catalog_with_data() -> MockCatalogManager:
    """Create a mock catalog with sample data."""
    catalog = MockCatalogManager()
    now = datetime.now()

    # Add successful runs
    for i in range(5):
        catalog.add_run(
            MockRunRecord(
                run_id=f"run_{i:03d}",
                pipeline="sales_pipeline",
                node="transform_sales",
                project="project_a",
                status="SUCCESS",
                timestamp=now - timedelta(hours=i),
                duration_ms=1000 + i * 100,
                rows_processed=1000 * (i + 1),
            )
        )

    # Add some failures
    catalog.add_failure(
        MockFailureRecord(
            failure_id="fail_001",
            run_id="run_010",
            pipeline="sales_pipeline",
            node="validate_sales",
            project="project_a",
            error_type="ValidationError",
            error_message="Null values found in required column",
            timestamp=now - timedelta(hours=2),
        )
    )

    return catalog
