"""Tests for CycleReportGenerator.

Phase 5.B: Human-Readable Cycle Reports tests.
"""

import os
import tempfile

import pytest

from odibi.agents.core.cycle import CycleConfig, CycleState, CycleStepLog, GoldenProjectConfig
from odibi.agents.core.reports import CycleReportGenerator


class TestCycleReportGenerator:
    """Tests for CycleReportGenerator."""

    @pytest.fixture
    def temp_odibi_dir(self):
        """Create a temporary .odibi directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)
            yield odibi_dir

    @pytest.fixture
    def sample_config(self):
        """Create a sample CycleConfig."""
        return CycleConfig(
            project_root="D:/projects/test",
            task_description="Test cycle",
            max_improvements=3,
            golden_projects=[
                GoldenProjectConfig(name="test_project", path="test.yaml"),
            ],
        )

    @pytest.fixture
    def sample_state(self, sample_config):
        """Create a sample completed CycleState."""
        state = CycleState(
            cycle_id="test-cycle-123",
            config=sample_config,
            mode="guided_execution",
            current_step_index=10,
            completed=True,
            completed_at="2024-01-15T10:00:00",
            improvements_approved=1,
            improvements_rejected=0,
            regressions_detected=0,
            summary="Test cycle completed successfully.",
            exit_status="COMPLETED",
            golden_project_results=[
                {"name": "test_project", "status": "PASSED"},
            ],
        )
        state.logs = [
            CycleStepLog(
                step="observation",
                agent_role="observer",
                started_at="2024-01-15T09:30:00",
                completed_at="2024-01-15T09:35:00",
                input_summary="Analyze execution",
                output_summary="No issues found",
                success=True,
                metadata={
                    "observer_output": {
                        "issues": [
                            {
                                "type": "UX_FRICTION",
                                "location": "cli/main.py",
                                "description": "Unclear error message",
                                "severity": "LOW",
                                "evidence": "Error: unknown",
                            }
                        ],
                        "observation_summary": "Minor UX issues observed",
                    }
                },
            ),
            CycleStepLog(
                step="improvement",
                agent_role="improvement",
                started_at="2024-01-15T09:35:00",
                completed_at="2024-01-15T09:40:00",
                input_summary="Generate improvement",
                output_summary="Proposal generated",
                success=True,
                metadata={
                    "proposal": {
                        "title": "Improve error messages",
                        "rationale": "Better UX for users",
                    }
                },
            ),
        ]
        return state

    def test_report_generator_creates_reports_dir(self, temp_odibi_dir):
        """Test that reports directory is created."""
        generator = CycleReportGenerator(temp_odibi_dir)
        generator._ensure_reports_dir()

        reports_dir = os.path.join(temp_odibi_dir, "reports")
        assert os.path.exists(reports_dir)
        assert os.path.isdir(reports_dir)

    def test_report_file_is_written(self, temp_odibi_dir, sample_state):
        """Test that report file is written to disk."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_path = generator.write_report(sample_state)

        assert os.path.exists(report_path)
        assert report_path.endswith(f"cycle_{sample_state.cycle_id}.md")

    def test_report_contains_required_sections(self, temp_odibi_dir, sample_state):
        """Test that report contains all required sections."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        required_sections = [
            "# Cycle Report:",
            "## Cycle Metadata",
            "## Projects Exercised",
            "## Observations",
            "## Improvements",
            "## Regression Results",
            "## Final Status",
            "## Conclusions",
        ]

        for section in required_sections:
            assert section in report_content, f"Missing section: {section}"

    def test_report_includes_cycle_id(self, temp_odibi_dir, sample_state):
        """Test that report includes cycle ID."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert sample_state.cycle_id in report_content

    def test_report_includes_golden_projects(self, temp_odibi_dir, sample_state):
        """Test that report includes golden project information."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert "test_project" in report_content
        assert "test.yaml" in report_content

    def test_report_includes_observations(self, temp_odibi_dir, sample_state):
        """Test that report includes observation data."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert "UX_FRICTION" in report_content
        assert "cli/main.py" in report_content

    def test_report_includes_improvements_count(self, temp_odibi_dir, sample_state):
        """Test that report includes improvement counts."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert "**Approved:** 1" in report_content
        assert "**Rejected:** 0" in report_content

    def test_report_includes_final_status(self, temp_odibi_dir, sample_state):
        """Test that report includes final status."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert "SUCCESS" in report_content
        assert "**Completed:** Yes" in report_content

    def test_report_includes_conclusions(self, temp_odibi_dir, sample_state):
        """Test that report includes conclusions from summary."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert sample_state.summary in report_content

    def test_report_shows_no_improvements_message(self, temp_odibi_dir, sample_config):
        """Test that report shows explicit message when no improvements."""
        state = CycleState(
            cycle_id="no-improvements-cycle",
            config=sample_config,
            mode="guided_execution",
            completed=True,
            improvements_approved=0,
            improvements_rejected=0,
        )

        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(state)

        assert "No improvements were proposed" in report_content

    def test_report_highlights_regressions(self, temp_odibi_dir, sample_config):
        """Test that report highlights regressions clearly."""
        state = CycleState(
            cycle_id="regression-cycle",
            config=sample_config,
            mode="guided_execution",
            completed=True,
            regressions_detected=1,
            golden_project_results=[
                {"name": "test_project", "status": "FAILED", "error_message": "Test failed"},
            ],
        )

        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(state)

        assert "REGRESSIONS DETECTED" in report_content
        assert "❌" in report_content
        assert "FAILED" in report_content

    def test_report_handles_no_golden_projects(self, temp_odibi_dir):
        """Test that report handles no golden projects gracefully."""
        config = CycleConfig(
            project_root="D:/projects/test",
            golden_projects=[],
        )
        state = CycleState(
            cycle_id="no-golden-cycle",
            config=config,
            completed=True,
        )

        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(state)

        assert "No golden projects configured" in report_content


class TestReportEvidenceSection:
    """Tests for Execution Evidence section (Phase 6)."""

    @pytest.fixture
    def temp_odibi_dir(self):
        """Create a temporary .odibi directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)
            yield odibi_dir

    @pytest.fixture
    def sample_state(self):
        """Create a sample CycleState."""
        config = CycleConfig(project_root="/test")
        return CycleState(
            cycle_id="evidence-test",
            config=config,
            completed=True,
        )

    def test_report_includes_evidence_section(self, temp_odibi_dir, sample_state):
        """Test that report includes Execution Evidence section when evidence is set."""
        from odibi.agents.core.evidence import ArtifactRef, EvidenceStatus, ExecutionEvidence

        generator = CycleReportGenerator(temp_odibi_dir)

        evidence = ExecutionEvidence(
            raw_command="wsl env-check",
            exit_code=0,
            stdout="Environment OK",
            stderr="",
            started_at="2024-01-15T10:00:00",
            finished_at="2024-01-15T10:00:05",
            duration_seconds=5.0,
            status=EvidenceStatus.SUCCESS,
            artifacts=[
                ArtifactRef(path=".odibi/artifacts/test/stdout.log", artifact_type="stdout"),
            ],
        )
        generator.set_execution_evidence("env_validation", evidence)

        report_content = generator.generate_report(sample_state)

        assert "## Execution Evidence" in report_content
        assert "env_validation" in report_content
        assert "wsl env-check" in report_content
        assert "SUCCESS" in report_content
        assert evidence.evidence_hash in report_content

    def test_report_evidence_includes_artifact_links(self, temp_odibi_dir, sample_state):
        """Test that evidence section includes links to artifacts."""
        from odibi.agents.core.evidence import ArtifactRef, ExecutionEvidence

        generator = CycleReportGenerator(temp_odibi_dir)

        evidence = ExecutionEvidence(
            raw_command="wsl pipeline run",
            exit_code=1,
            stdout="",
            stderr="Error",
            started_at="2024-01-15T10:00:00",
            finished_at="2024-01-15T10:01:00",
            duration_seconds=60.0,
            artifacts=[
                ArtifactRef(path=".odibi/artifacts/test/stdout.log", artifact_type="stdout"),
                ArtifactRef(path=".odibi/artifacts/test/stderr.log", artifact_type="stderr"),
            ],
        )
        generator.set_execution_evidence("user_execution", evidence)

        report_content = generator.generate_report(sample_state)

        assert "[stdout]" in report_content
        assert "[stderr]" in report_content
        assert ".odibi/artifacts/test/stdout.log" in report_content

    def test_report_without_evidence_has_no_section(self, temp_odibi_dir, sample_state):
        """Test that report without evidence does not include empty section."""
        generator = CycleReportGenerator(temp_odibi_dir)
        report_content = generator.generate_report(sample_state)

        assert "## Execution Evidence" not in report_content

    def test_report_evidence_shows_total_duration(self, temp_odibi_dir, sample_state):
        """Test that evidence section shows total duration."""
        from odibi.agents.core.evidence import ExecutionEvidence

        generator = CycleReportGenerator(temp_odibi_dir)

        ev1 = ExecutionEvidence(
            raw_command="cmd1",
            exit_code=0,
            stdout="",
            stderr="",
            started_at="2024-01-15T10:00:00",
            finished_at="2024-01-15T10:00:10",
            duration_seconds=10.0,
        )
        ev2 = ExecutionEvidence(
            raw_command="cmd2",
            exit_code=0,
            stdout="",
            stderr="",
            started_at="2024-01-15T10:00:10",
            finished_at="2024-01-15T10:00:35",
            duration_seconds=25.0,
        )

        generator.set_execution_evidence("step1", ev1)
        generator.set_execution_evidence("step2", ev2)

        report_content = generator.generate_report(sample_state)

        assert "35.00s" in report_content


class TestReportWriteIntegration:
    """Integration tests for report writing."""

    def test_report_can_be_read_back(self):
        """Test that written report can be read and parsed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_dir = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_dir)

            config = CycleConfig(project_root="/test")
            state = CycleState(
                cycle_id="readback-test",
                config=config,
                completed=True,
                summary="Integration test completed.",
            )

            generator = CycleReportGenerator(odibi_dir)
            report_path = generator.write_report(state)

            with open(report_path, "r", encoding="utf-8") as f:
                content = f.read()

            assert "# Cycle Report: readback-test" in content
            assert "Integration test completed." in content
