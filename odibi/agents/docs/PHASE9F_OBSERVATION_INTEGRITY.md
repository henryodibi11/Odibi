# Phase 9.F: Observation Integrity

## Overview

Phase 9.F ensures the observer reliably detects pipeline failures and emits structured issues regardless of shell exit codes. This phase addresses a critical bug where:

- Some pipelines fail validation or execution
- The CLI exits with code 0
- The observer reports "No issues observed"
- Convergence later correctly flags the failure

This creates **observer–convergence inconsistency**, which invalidates learning.

## Design Principle

**Learning correctness > convenience.**

The observer must treat pipeline failures as real issues, regardless of shell exit code.

## Key Components

### 1. Pipeline Execution Status (`pipeline_status.py`)

Introduces canonical status values for pipeline execution:

| Status | Description |
|--------|-------------|
| `SUCCESS` | Pipeline completed all stages without errors |
| `VALIDATION_FAILED` | Pipeline failed during config/schema validation (e.g., Pydantic errors) |
| `RUNTIME_FAILED` | Pipeline failed during execution (e.g., Spark errors, data errors) |
| `SKIPPED` | Pipeline was not executed (e.g., precondition not met) |
| `ABORTED` | Pipeline execution was aborted before completion (timeout, interrupt) |

**Critical Rule:** Do NOT infer success from shell exit code alone. Parse pipeline output for explicit failure markers.

### 2. Pipeline Execution Result Schema

```python
@dataclass
class PipelineExecutionResult:
    pipeline_name: str                              # Name or path of pipeline config
    status: PipelineExecutionStatus                 # Canonical execution status
    exit_code: int                                  # Shell exit code (may be 0 even on failure!)
    validation_errors: list[PipelineValidationError]  # Pydantic errors if validation failed
    runtime_error: Optional[str]                    # Error message if runtime failed
    error_stage: Optional[str]                      # 'config_validation' | 'execution'
    output_excerpt: str                             # Relevant excerpt from output
```

### 3. Failure Pattern Detection

The system detects failures by pattern matching against output, not exit codes:

**Validation Failure Patterns:**
- `validation error for \w+`
- `Input should be a valid`
- `ValidationError`
- `Invalid config`

**Runtime Failure Patterns:**
- `Pipeline failed`
- `SparkException`
- `Status: FAILURE`
- `Job aborted`

**Abort Patterns:**
- `Timeout`
- `Interrupted`
- `SIGTERM`

### 4. ExecutionEvidence Enhancement

`ExecutionEvidence` now includes a `pipeline_results` field:

```python
@dataclass
class ExecutionEvidence:
    # ... existing fields ...

    # Phase 9.F: Structured pipeline execution results
    pipeline_results: list[dict[str, Any]] = field(default_factory=list)

    def has_pipeline_failures(self) -> bool:
        """Check if any pipeline failed (authoritative check)."""
        ...

    def get_failed_pipelines(self) -> list[dict[str, Any]]:
        """Get all failed pipeline results."""
        ...
```

### 5. Observer Agent Updates

The `ObserverAgent` now:

1. **Extracts pipeline issues from structured results** (not just LLM parsing)
2. **Emits PIPELINE_EXECUTION_ERROR** for each failed pipeline
3. **Merges structured issues with LLM-detected issues**
4. **Never reports "No issues observed"** when failures exist

```python
def _extract_pipeline_issues(self, execution_evidence: dict) -> list[ObservedIssue]:
    """Extract issues from pipeline execution results.

    Phase 9.F: This method ensures the observer detects pipeline failures
    even when exit code is 0.
    """
    issues = []
    for result in execution_evidence.get("pipeline_results", []):
        if result.get("status") != "SUCCESS":
            issues.append(self._create_issue_from_pipeline_result(result))
    return issues
```

### 6. Observer-Convergence Consistency Invariant

**Invariant:**
```
If convergence reports failed pipelines,
observer must have emitted ≥1 issue.
```

This is enforced in `observation_integrity.py`:

```python
def check_observer_convergence_consistency(
    observer_output: dict,
    pipeline_results: list[dict],
    fail_fast: bool = True,
) -> ObservationIntegrityCheck:
    """
    Raises ObservationIntegrityError if:
    - pipeline_results contains failures AND
    - observer_output contains 0 issues
    """
```

### 7. CycleRunner Integration

The `CycleRunner._post_process_step` method now validates observation integrity after the OBSERVATION step:

```python
if step == CycleStep.OBSERVATION:
    integrity_check = validate_observer_output_integrity(
        observer_metadata=response.metadata,
        execution_evidence=evidence_dict,
        fail_fast=False,  # Log warning, don't crash cycle
    )

    if not integrity_check.is_valid:
        self._emit_event(CycleEvent(
            event_type="integrity_violation",
            message=f"⚠️ Observer-Convergence Inconsistency: ..."
        ))
```

## Issue Schema

When the observer detects a pipeline failure, it emits:

```json
{
  "type": "PIPELINE_EXECUTION_ERROR",
  "pipeline": "scale_join.odibi.yaml",
  "stage": "config_validation | execution",
  "message": "Pipeline validation failed: pipelines.0.nodes.2.params.1 - Input should be a valid string",
  "severity": "MEDIUM | HIGH",
  "evidence": "Status: VALIDATION_FAILED | Exit code: 0 | Output: ..."
}
```

**Note:** `"No issues observed"` is only valid if ALL pipelines succeeded.

## File Changes

| File | Changes |
|------|---------|
| `agents/core/pipeline_status.py` | **NEW** - Pipeline execution status enum and detection |
| `agents/core/observation_integrity.py` | **NEW** - Consistency invariant and validation |
| `agents/core/evidence.py` | Added `pipeline_results` field and methods |
| `agents/prompts/cycle_agents.py` | Updated `ObserverAgent._extract_pipeline_issues()` |
| `agents/core/cycle.py` | Added integrity check in `_post_process_step` |
| `agents/core/tests/test_observation_integrity.py` | **NEW** - Unit tests |

## Test Coverage

1. **Validation failure with exit code 0** - Most critical case
2. **Runtime failure detection** - SparkException, job failures
3. **Mixed success/failure runs** - Some pipelines succeed, some fail
4. **Observer-convergence consistency** - Invariant enforcement
5. **Pipeline status detection patterns** - All failure patterns

## Constraints Preserved

- ✅ Does not modify Odibi pipeline execution semantics
- ✅ Does not enable ImprovementAgent
- ✅ Does not change learning harness configs
- ✅ Preserves determinism and observation-only guarantees

## Definition of Done

- [x] Observer reports issues for failed pipelines
- [x] Convergence and observer agree on failure counts
- [x] No false "No issues observed" when failures occur
- [x] All tests pass

## Example: Before vs After

### Before Phase 9.F

```
Pipeline execution:
  scale_join.odibi.yaml: exit_code=0
  stderr: "1 validation error for ProjectConfig..."

Observer output:
  {"issues": [], "observation_summary": "No issues observed"}

Result: BUG - Observer missed the validation failure!
```

### After Phase 9.F

```
Pipeline execution:
  scale_join.odibi.yaml: exit_code=0
  stderr: "1 validation error for ProjectConfig..."

Pipeline status detection:
  status: VALIDATION_FAILED
  validation_errors: [{field: "pipelines.0.nodes.2.params.1", message: "..."}]

Observer output:
  {
    "issues": [{
      "type": "CONFIGURATION_ERROR",
      "location": "scale_join.odibi.yaml",
      "description": "Pipeline validation failed: pipelines.0.nodes.2.params.1 - Input should be a valid string",
      "severity": "HIGH",
      "evidence": "Status: VALIDATION_FAILED | Exit code: 0"
    }],
    "observation_summary": "1 pipeline execution error(s) detected"
  }

Result: Failure correctly detected and reported!
```
