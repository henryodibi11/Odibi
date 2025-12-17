"""Tests for evidence-based execution (Phase 6)."""

import json
import os
import tempfile

from odibi.agents.core.evidence import (
    TRUNCATION_MARKER,
    ArtifactRef,
    ArtifactWriter,
    EvidenceStatus,
    ExecutionEvidence,
    truncate_output,
)


class TestTruncation:
    """Tests for deterministic truncation."""

    def test_truncation_no_change_if_under_limit(self):
        """Short text should not be modified."""
        text = "Hello, world!"
        result = truncate_output(text, limit=100)
        assert result == text
        assert TRUNCATION_MARKER not in result

    def test_truncation_adds_marker(self):
        """Long text should be truncated with marker."""
        text = "x" * 200
        result = truncate_output(text, limit=100)
        assert len(result) == 100
        assert result.endswith(TRUNCATION_MARKER)

    def test_truncation_deterministic(self):
        """Same input should always produce same output."""
        text = "a" * 500
        result1 = truncate_output(text, limit=100)
        result2 = truncate_output(text, limit=100)
        result3 = truncate_output(text, limit=100)
        assert result1 == result2 == result3

    def test_truncation_empty_string(self):
        """Empty string should return empty."""
        result = truncate_output("", limit=100)
        assert result == ""

    def test_truncation_exactly_at_limit(self):
        """Text exactly at limit should not be truncated."""
        text = "x" * 100
        result = truncate_output(text, limit=100)
        assert result == text

    def test_truncation_one_over_limit(self):
        """Text one char over should be truncated."""
        text = "x" * 101
        result = truncate_output(text, limit=100)
        assert len(result) == 100
        assert TRUNCATION_MARKER in result


class TestEvidenceHash:
    """Tests for evidence hashing."""

    def test_evidence_hash_deterministic(self):
        """Same evidence should produce same hash."""
        ev1 = ExecutionEvidence(
            raw_command="echo hello",
            exit_code=0,
            stdout="hello",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        ev2 = ExecutionEvidence(
            raw_command="echo hello",
            exit_code=0,
            stdout="hello",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        assert ev1.evidence_hash == ev2.evidence_hash

    def test_evidence_hash_changes_with_command(self):
        """Different command should produce different hash."""
        ev1 = ExecutionEvidence(
            raw_command="echo hello",
            exit_code=0,
            stdout="hello",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        ev2 = ExecutionEvidence(
            raw_command="echo world",
            exit_code=0,
            stdout="hello",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        assert ev1.evidence_hash != ev2.evidence_hash

    def test_evidence_hash_changes_with_stdout(self):
        """Different stdout should produce different hash."""
        ev1 = ExecutionEvidence(
            raw_command="echo hello",
            exit_code=0,
            stdout="output1",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        ev2 = ExecutionEvidence(
            raw_command="echo hello",
            exit_code=0,
            stdout="output2",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        assert ev1.evidence_hash != ev2.evidence_hash

    def test_evidence_hash_changes_with_exit_code(self):
        """Different exit code should produce different hash."""
        ev1 = ExecutionEvidence(
            raw_command="test",
            exit_code=0,
            stdout="",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        ev2 = ExecutionEvidence(
            raw_command="test",
            exit_code=1,
            stdout="",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        assert ev1.evidence_hash != ev2.evidence_hash

    def test_evidence_hash_is_16_chars(self):
        """Hash should be 16 characters (truncated SHA256)."""
        ev = ExecutionEvidence(
            raw_command="test",
            exit_code=0,
            stdout="",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
        )
        assert len(ev.evidence_hash) == 16


class TestExecutionEvidence:
    """Tests for ExecutionEvidence schema."""

    def test_evidence_serialization_roundtrip(self):
        """Evidence should serialize and deserialize correctly."""
        original = ExecutionEvidence(
            raw_command="wsl pipeline run test.yaml",
            exit_code=0,
            stdout="Pipeline completed successfully",
            stderr="",
            started_at="2024-01-01T10:00:00",
            finished_at="2024-01-01T10:01:30",
            duration_seconds=90.0,
            artifacts=[ArtifactRef(path="artifacts/cycle1/stdout.log", artifact_type="stdout")],
            status=EvidenceStatus.SUCCESS,
        )

        json_str = original.to_json()
        restored = ExecutionEvidence.from_json(json_str)

        assert restored.raw_command == original.raw_command
        assert restored.exit_code == original.exit_code
        assert restored.stdout == original.stdout
        assert restored.stderr == original.stderr
        assert restored.started_at == original.started_at
        assert restored.finished_at == original.finished_at
        assert restored.duration_seconds == original.duration_seconds
        assert restored.status == original.status
        assert restored.evidence_hash == original.evidence_hash
        assert len(restored.artifacts) == 1
        assert restored.artifacts[0].path == "artifacts/cycle1/stdout.log"

    def test_evidence_from_dict(self):
        """Evidence should be creatable from dict."""
        data = {
            "raw_command": "echo test",
            "exit_code": 0,
            "stdout": "test",
            "stderr": "",
            "started_at": "2024-01-01T00:00:00",
            "finished_at": "2024-01-01T00:00:01",
            "duration_seconds": 1.0,
            "status": "SUCCESS",
            "artifacts": [],
        }
        ev = ExecutionEvidence.from_dict(data)
        assert ev.raw_command == "echo test"
        assert ev.status == EvidenceStatus.SUCCESS
        assert ev.evidence_hash  # Should be computed

    def test_no_evidence_status(self):
        """NO_EVIDENCE should be created correctly."""
        ev = ExecutionEvidence.no_evidence("Execution gateway unavailable")
        assert ev.is_no_evidence()
        assert ev.status == EvidenceStatus.NO_EVIDENCE
        assert ev.exit_code == -1
        assert "unavailable" in ev.stderr
        assert ev.raw_command == ""

    def test_no_evidence_summary(self):
        """NO_EVIDENCE summary should include reason."""
        ev = ExecutionEvidence.no_evidence("Permission denied")
        summary = ev.get_summary()
        assert "NO_EVIDENCE" in summary
        assert "Permission denied" in summary

    def test_success_summary(self):
        """Success summary should include key details."""
        ev = ExecutionEvidence(
            raw_command="wsl env-check",
            exit_code=0,
            stdout="OK",
            stderr="",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:05",
            duration_seconds=5.0,
            status=EvidenceStatus.SUCCESS,
        )
        summary = ev.get_summary()
        assert "env-check" in summary
        assert "Exit: 0" in summary
        assert "SUCCESS" in summary
        assert "5.00s" in summary

    def test_failure_status(self):
        """Failure status should be set correctly."""
        ev = ExecutionEvidence(
            raw_command="test",
            exit_code=1,
            stdout="",
            stderr="Error occurred",
            started_at="2024-01-01T00:00:00",
            finished_at="2024-01-01T00:00:01",
            duration_seconds=1.0,
            status=EvidenceStatus.FAILURE,
        )
        assert ev.status == EvidenceStatus.FAILURE
        assert not ev.is_no_evidence()


class TestArtifactRef:
    """Tests for ArtifactRef."""

    def test_artifact_ref_roundtrip(self):
        """ArtifactRef should serialize and deserialize."""
        ref = ArtifactRef(
            path=".odibi/artifacts/cycle1/execution/env/stdout.log",
            artifact_type="stdout",
        )
        data = ref.to_dict()
        restored = ArtifactRef.from_dict(data)
        assert restored.path == ref.path
        assert restored.artifact_type == ref.artifact_type


class TestArtifactWriter:
    """Tests for ArtifactWriter."""

    def test_artifact_writer_creates_directory(self):
        """Writer should create artifact directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_root = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_root)

            writer = ArtifactWriter(odibi_root)
            artifact_dir = writer.get_artifact_dir("cycle-123", "env_validation")

            writer._ensure_dir(artifact_dir)
            assert os.path.isdir(artifact_dir)

    def test_artifact_writer_writes_all_files(self):
        """Writer should create command.txt, stdout.log, stderr.log, evidence.json."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_root = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_root)

            writer = ArtifactWriter(odibi_root)

            evidence = ExecutionEvidence(
                raw_command="wsl env-check",
                exit_code=0,
                stdout="Environment OK\nSpark OK",
                stderr="",
                started_at="2024-01-01T10:00:00",
                finished_at="2024-01-01T10:00:05",
                duration_seconds=5.0,
                status=EvidenceStatus.SUCCESS,
            )

            artifacts = writer.write_execution_artifacts(
                cycle_id="cycle-abc",
                step_name="env_validation",
                evidence=evidence,
            )

            assert len(artifacts) == 4

            artifact_types = {a.artifact_type for a in artifacts}
            assert artifact_types == {"command", "stdout", "stderr", "evidence"}

            artifact_dir = writer.get_artifact_dir("cycle-abc", "env_validation")
            assert os.path.exists(os.path.join(artifact_dir, "command.txt"))
            assert os.path.exists(os.path.join(artifact_dir, "stdout.log"))
            assert os.path.exists(os.path.join(artifact_dir, "stderr.log"))
            assert os.path.exists(os.path.join(artifact_dir, "evidence.json"))

            with open(os.path.join(artifact_dir, "command.txt")) as f:
                assert f.read() == "wsl env-check"

            with open(os.path.join(artifact_dir, "stdout.log")) as f:
                assert "Environment OK" in f.read()

            with open(os.path.join(artifact_dir, "evidence.json")) as f:
                data = json.load(f)
                assert data["exit_code"] == 0
                assert data["status"] == "SUCCESS"

    def test_artifact_writer_multiple_steps(self):
        """Writer should handle multiple steps in same cycle."""
        with tempfile.TemporaryDirectory() as tmpdir:
            odibi_root = os.path.join(tmpdir, ".odibi")
            os.makedirs(odibi_root)

            writer = ArtifactWriter(odibi_root)

            ev1 = ExecutionEvidence(
                raw_command="env-check",
                exit_code=0,
                stdout="OK",
                stderr="",
                started_at="2024-01-01T10:00:00",
                finished_at="2024-01-01T10:00:01",
                duration_seconds=1.0,
            )
            ev2 = ExecutionEvidence(
                raw_command="pipeline run",
                exit_code=1,
                stdout="",
                stderr="Failed",
                started_at="2024-01-01T10:01:00",
                finished_at="2024-01-01T10:02:00",
                duration_seconds=60.0,
                status=EvidenceStatus.FAILURE,
            )

            writer.write_execution_artifacts("cycle-x", "env_validation", ev1)
            writer.write_execution_artifacts("cycle-x", "user_execution", ev2)

            env_dir = writer.get_artifact_dir("cycle-x", "env_validation")
            user_dir = writer.get_artifact_dir("cycle-x", "user_execution")

            assert os.path.isdir(env_dir)
            assert os.path.isdir(user_dir)
            assert os.path.exists(os.path.join(env_dir, "evidence.json"))
            assert os.path.exists(os.path.join(user_dir, "evidence.json"))


class TestEvidenceFromExecutionResult:
    """Tests for creating evidence from ExecutionResult."""

    def test_evidence_from_execution_result_success(self):
        """Evidence should capture success correctly."""
        from odibi.agents.core.execution import ExecutionResult

        result = ExecutionResult(
            stdout="Pipeline completed",
            stderr="",
            exit_code=0,
            command="wsl pipeline run test.yaml",
            success=True,
        )

        evidence = ExecutionEvidence.from_execution_result(
            result,
            started_at="2024-01-01T10:00:00",
            finished_at="2024-01-01T10:00:30",
        )

        assert evidence.raw_command == result.command
        assert evidence.exit_code == 0
        assert evidence.stdout == "Pipeline completed"
        assert evidence.status == EvidenceStatus.SUCCESS
        assert evidence.duration_seconds == 30.0

    def test_evidence_from_execution_result_failure(self):
        """Evidence should capture failure correctly."""
        from odibi.agents.core.execution import ExecutionResult

        result = ExecutionResult(
            stdout="",
            stderr="Error: Pipeline failed",
            exit_code=1,
            command="wsl pipeline run bad.yaml",
            success=False,
        )

        evidence = ExecutionEvidence.from_execution_result(
            result,
            started_at="2024-01-01T10:00:00",
            finished_at="2024-01-01T10:00:10",
        )

        assert evidence.exit_code == 1
        assert "Pipeline failed" in evidence.stderr
        assert evidence.status == EvidenceStatus.FAILURE

    def test_evidence_truncates_long_output(self):
        """Long output should be truncated."""
        from odibi.agents.core.execution import ExecutionResult

        long_output = "x" * 20000
        result = ExecutionResult(
            stdout=long_output,
            stderr="",
            exit_code=0,
            command="test",
            success=True,
        )

        evidence = ExecutionEvidence.from_execution_result(
            result,
            started_at="2024-01-01T10:00:00",
            finished_at="2024-01-01T10:00:01",
            truncation_limit=1000,
        )

        assert len(evidence.stdout) == 1000
        assert TRUNCATION_MARKER in evidence.stdout
