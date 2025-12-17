"""Tests for Phase 9.F: Observation Integrity.

Test coverage:
1. Validation failure with exit code 0
2. Runtime failure detection
3. Mixed success/failure runs
4. Observer-convergence consistency invariant
5. Pipeline status detection patterns
"""

import pytest

from odibi.agents.core.observation_integrity import (
    ObservationIntegrityError,
    check_observer_convergence_consistency,
    count_pipeline_failures_from_evidence,
    validate_observation_summary,
    validate_observer_output_integrity,
)
from odibi.agents.core.pipeline_status import (
    PipelineExecutionResult,
    PipelineExecutionStatus,
    PipelineValidationError,
    create_pipeline_result_from_execution,
    detect_pipeline_status,
    parse_validation_errors,
)


class TestPipelineStatusDetection:
    """Tests for pipeline status detection from output."""

    def test_validation_failure_with_exit_code_0(self):
        """Pipeline validation failure should be detected even with exit code 0."""
        stdout = ""
        stderr = """ERROR Pipeline failed: 1 validation error for ProjectConfig
pipelines.0.nodes.2.params.1
  Input should be a valid string"""

        status, error_msg, validation_errors = detect_pipeline_status(
            exit_code=0, stdout=stdout, stderr=stderr
        )

        assert status == PipelineExecutionStatus.VALIDATION_FAILED
        assert "validation error" in error_msg.lower() or len(validation_errors) > 0

    def test_pydantic_validation_error_parsing(self):
        """Should parse Pydantic v2 validation error format."""
        output = """1 validation error for ProjectConfig
pipelines.0.nodes.2.params.1
  Input should be a valid string"""

        errors = parse_validation_errors(output)

        assert len(errors) >= 1
        # Should extract the field path
        assert any("pipelines" in e.field_path for e in errors) or errors[0].message

    def test_runtime_failure_detection(self):
        """Should detect runtime failures from output patterns."""
        stdout = """Pipeline: scale_join
Status: FAILURE
ERROR: SparkException occurred during execution"""
        stderr = ""

        status, error_msg, _ = detect_pipeline_status(exit_code=0, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.RUNTIME_FAILED

    def test_success_detection(self):
        """Should detect success when no failure patterns and exit code 0."""
        stdout = """Pipeline: scale_join
Status: SUCCESS
All nodes completed successfully."""
        stderr = ""

        status, error_msg, _ = detect_pipeline_status(exit_code=0, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.SUCCESS
        assert error_msg == ""

    def test_abort_detection(self):
        """Should detect aborted execution."""
        stdout = ""
        stderr = "Process timed out after 300 seconds"

        status, error_msg, _ = detect_pipeline_status(exit_code=-2, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.ABORTED

    def test_non_zero_exit_code_fallback(self):
        """Should detect failure on non-zero exit code without patterns."""
        stdout = "Some output"
        stderr = "Unknown error occurred"

        status, error_msg, _ = detect_pipeline_status(exit_code=1, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.RUNTIME_FAILED

    def test_false_positive_resume_from_failure(self):
        """Should NOT detect failure when 'resume_from_failure=False' appears in success log."""
        stdout = """INFO Pipeline: skew_null_test
INFO resume_from_failure=False
INFO All nodes completed
Pipeline SUCCESS: skew_null_test"""
        stderr = ""

        status, error_msg, _ = detect_pipeline_status(exit_code=0, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.SUCCESS
        assert error_msg == ""

    def test_explicit_success_overrides_failure_substrings(self):
        """Explicit SUCCESS pattern should override false positive failure matches."""
        stdout = """INFO Pipeline: schema_drift_test
INFO config: resume_from_failure=False, retry_on_failure=True
INFO FAILURE_THRESHOLD=0.1
Pipeline SUCCESS: schema_drift_test
status=SUCCESS"""
        stderr = ""

        status, error_msg, _ = detect_pipeline_status(exit_code=0, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.SUCCESS

    def test_real_failure_still_detected(self):
        """Real failures should still be detected with word boundary pattern."""
        stdout = """INFO Pipeline: test_pipeline
Status: FAILURE
ERROR: Job failed"""
        stderr = ""

        status, error_msg, _ = detect_pipeline_status(exit_code=0, stdout=stdout, stderr=stderr)

        assert status == PipelineExecutionStatus.RUNTIME_FAILED


class TestPipelineExecutionResult:
    """Tests for PipelineExecutionResult creation."""

    def test_create_from_validation_failure(self):
        """Should create result with validation failure status."""
        result = create_pipeline_result_from_execution(
            pipeline_name="scale_join.odibi.yaml",
            exit_code=0,
            stdout="",
            stderr="""ERROR Pipeline failed: 1 validation error for ProjectConfig
pipelines.0.nodes.2.params.1
  Input should be a valid string""",
        )

        assert result.status == PipelineExecutionStatus.VALIDATION_FAILED
        assert result.pipeline_name == "scale_join.odibi.yaml"
        assert result.error_stage == "config_validation"
        assert result.is_failure()
        assert not result.is_success()

    def test_create_from_runtime_failure(self):
        """Should create result with runtime failure status."""
        result = create_pipeline_result_from_execution(
            pipeline_name="etl_pipeline.yaml",
            exit_code=0,
            stdout="Pipeline failed: Out of memory error",
            stderr="",
        )

        assert result.status == PipelineExecutionStatus.RUNTIME_FAILED
        assert result.error_stage == "execution"
        assert result.is_failure()

    def test_create_from_success(self):
        """Should create result with success status."""
        result = create_pipeline_result_from_execution(
            pipeline_name="simple_pipeline.yaml",
            exit_code=0,
            stdout="All nodes completed successfully",
            stderr="",
        )

        assert result.status == PipelineExecutionStatus.SUCCESS
        assert result.is_success()
        assert not result.is_failure()

    def test_to_dict_and_from_dict(self):
        """Should serialize and deserialize correctly."""
        original = PipelineExecutionResult(
            pipeline_name="test.yaml",
            status=PipelineExecutionStatus.VALIDATION_FAILED,
            exit_code=0,
            validation_errors=[
                PipelineValidationError(
                    field_path="pipelines.0.nodes.1",
                    message="Input should be a valid string",
                )
            ],
            runtime_error=None,
            error_stage="config_validation",
            output_excerpt="validation error excerpt",
        )

        dict_form = original.to_dict()
        restored = PipelineExecutionResult.from_dict(dict_form)

        assert restored.pipeline_name == original.pipeline_name
        assert restored.status == original.status
        assert restored.exit_code == original.exit_code
        assert len(restored.validation_errors) == 1


class TestObserverConvergenceConsistency:
    """Tests for observer-convergence consistency invariant."""

    def test_consistent_when_no_failures(self):
        """Should be consistent when no pipelines failed."""
        observer_output = {
            "issues": [],
            "observation_summary": "No issues observed",
        }
        pipeline_results = [
            {"pipeline_name": "test.yaml", "status": "SUCCESS"},
        ]

        check = check_observer_convergence_consistency(
            observer_output=observer_output,
            pipeline_results=pipeline_results,
            fail_fast=False,
        )

        assert check.is_valid
        assert check.pipeline_failure_count == 0
        assert check.observer_issue_count == 0

    def test_consistent_when_failures_detected(self):
        """Should be consistent when observer reports issues for failures."""
        observer_output = {
            "issues": [
                {
                    "type": "EXECUTION_FAILURE",
                    "location": "test.yaml",
                    "description": "Pipeline failed",
                    "severity": "HIGH",
                    "evidence": "Status: FAILURE",
                }
            ],
            "observation_summary": "1 issue found",
        }
        pipeline_results = [
            {"pipeline_name": "test.yaml", "status": "RUNTIME_FAILED"},
        ]

        check = check_observer_convergence_consistency(
            observer_output=observer_output,
            pipeline_results=pipeline_results,
            fail_fast=False,
        )

        assert check.is_valid
        assert check.pipeline_failure_count == 1
        assert check.observer_issue_count == 1

    def test_inconsistent_when_failures_missed(self):
        """Should detect inconsistency when observer misses failures."""
        observer_output = {
            "issues": [],
            "observation_summary": "No issues observed",
        }
        pipeline_results = [
            {"pipeline_name": "test.yaml", "status": "VALIDATION_FAILED"},
        ]

        check = check_observer_convergence_consistency(
            observer_output=observer_output,
            pipeline_results=pipeline_results,
            fail_fast=False,
        )

        assert not check.is_valid
        assert check.pipeline_failure_count == 1
        assert check.observer_issue_count == 0
        assert check.error_message is not None
        assert "INCONSISTENCY" in check.error_message

    def test_fail_fast_raises_exception(self):
        """Should raise exception when fail_fast=True and inconsistent."""
        observer_output = {
            "issues": [],
            "observation_summary": "No issues observed",
        }
        pipeline_results = [
            {"pipeline_name": "test.yaml", "status": "RUNTIME_FAILED"},
        ]

        with pytest.raises(ObservationIntegrityError) as exc_info:
            check_observer_convergence_consistency(
                observer_output=observer_output,
                pipeline_results=pipeline_results,
                fail_fast=True,
            )

        assert exc_info.value.observer_issue_count == 0
        assert exc_info.value.pipeline_failure_count == 1

    def test_mixed_success_and_failure(self):
        """Should correctly count mixed results."""
        observer_output = {
            "issues": [
                {
                    "type": "EXECUTION_FAILURE",
                    "location": "fail.yaml",
                    "description": "failed",
                    "severity": "HIGH",
                    "evidence": "err",
                }
            ],
            "observation_summary": "1 issue found",
        }
        pipeline_results = [
            {"pipeline_name": "success.yaml", "status": "SUCCESS"},
            {"pipeline_name": "fail.yaml", "status": "RUNTIME_FAILED"},
        ]

        check = check_observer_convergence_consistency(
            observer_output=observer_output,
            pipeline_results=pipeline_results,
            fail_fast=False,
        )

        assert check.is_valid
        assert check.pipeline_failure_count == 1
        assert check.observer_issue_count == 1


class TestValidateObservationSummary:
    """Tests for observation summary validation."""

    def test_valid_when_no_issues_and_no_failures(self):
        """Should be valid when summary says no issues and no failures exist."""
        is_valid, error = validate_observation_summary(
            observation_summary="No issues observed",
            pipeline_results=[{"status": "SUCCESS"}],
        )

        assert is_valid
        assert error == ""

    def test_invalid_when_no_issues_but_failures_exist(self):
        """Should be invalid when summary says no issues but failures exist."""
        is_valid, error = validate_observation_summary(
            observation_summary="No issues observed",
            pipeline_results=[{"pipeline_name": "test.yaml", "status": "VALIDATION_FAILED"}],
        )

        assert not is_valid
        assert "No issues observed" in error
        assert "failed" in error.lower()


class TestValidateObserverOutputIntegrity:
    """Tests for full observer output integrity validation."""

    def test_validates_with_execution_evidence(self):
        """Should validate against execution evidence."""
        observer_metadata = {
            "observer_output": {
                "issues": [
                    {
                        "type": "EXECUTION_FAILURE",
                        "location": "test.yaml",
                        "description": "failed",
                        "severity": "HIGH",
                        "evidence": "err",
                    }
                ],
                "observation_summary": "1 issue found",
            }
        }
        execution_evidence = {
            "pipeline_results": [{"pipeline_name": "test.yaml", "status": "RUNTIME_FAILED"}]
        }

        check = validate_observer_output_integrity(
            observer_metadata=observer_metadata,
            execution_evidence=execution_evidence,
            fail_fast=False,
        )

        assert check.is_valid


class TestCountPipelineFailures:
    """Tests for pipeline failure counting."""

    def test_count_failures(self):
        """Should count non-SUCCESS pipelines."""
        evidence = {
            "pipeline_results": [
                {"status": "SUCCESS"},
                {"status": "VALIDATION_FAILED"},
                {"status": "RUNTIME_FAILED"},
                {"status": "ABORTED"},
            ]
        }

        count = count_pipeline_failures_from_evidence(evidence)

        assert count == 3

    def test_count_empty(self):
        """Should return 0 for empty pipeline results."""
        evidence = {"pipeline_results": []}

        count = count_pipeline_failures_from_evidence(evidence)

        assert count == 0

    def test_count_all_success(self):
        """Should return 0 when all succeed."""
        evidence = {
            "pipeline_results": [
                {"status": "SUCCESS"},
                {"status": "SUCCESS"},
            ]
        }

        count = count_pipeline_failures_from_evidence(evidence)

        assert count == 0
