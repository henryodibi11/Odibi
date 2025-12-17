# Evidence-Based Execution (Phase 6)

## Overview

Phase 6 introduces evidence-based execution to ensure that all execution outputs in the Odibi AI Agent Suite come from **real commands** via ExecutionGateway (WSL), not fabricated or imagined logs.

**Core Principle:** Agents MUST NOT fabricate pipeline runs, configs, errors, or durations.

## What Counts as Evidence

Evidence is defined as structured output captured from actual ExecutionGateway calls:

| Evidence Field | Description |
|----------------|-------------|
| `raw_command` | The exact command that was executed |
| `exit_code` | Integer exit code from the process |
| `stdout` | Standard output (truncated if > 10KB) |
| `stderr` | Standard error (truncated if > 10KB) |
| `started_at` | ISO timestamp when execution began |
| `finished_at` | ISO timestamp when execution ended |
| `duration_seconds` | Wall-clock duration |
| `status` | SUCCESS, FAILURE, or NO_EVIDENCE |
| `evidence_hash` | SHA256 fingerprint for verification |
| `artifacts` | Paths to persisted artifact files |

## What is Forbidden

The following are **strictly forbidden**:

1. **Fabricated execution logs** - Agents must not invent pipeline output
2. **Imagined error messages** - Error text must come from real stderr
3. **Assumed exit codes** - Exit codes must come from actual process execution
4. **Made-up durations** - Timing must be measured from real start/end times
5. **Hallucinated configs** - Config validation must run real `odibi validate`

## Evidence Status Types

| Status | Meaning |
|--------|---------|
| `SUCCESS` | Command executed and returned exit code 0 |
| `FAILURE` | Command executed and returned non-zero exit code |
| `NO_EVIDENCE` | Command could not be executed (see reasons below) |

### NO_EVIDENCE Reasons

`NO_EVIDENCE` is returned when:
- ExecutionGateway is unavailable
- Agent lacks `can_execute_tasks` permission
- WSL is not installed or not running
- System is in simulation/test mode without real execution

**Important:** When evidence is `NO_EVIDENCE`, agents MUST:
1. Report the lack of evidence explicitly
2. NOT invent alternative execution results
3. Include the reason in the stderr field

## How Artifacts are Stored

Artifacts are persisted to disk with deterministic paths:

```
.odibi/
├── artifacts/
│   └── <cycle_id>/
│       └── execution/
│           ├── env_validation/
│           │   ├── command.txt      # The raw command
│           │   ├── stdout.log       # Full stdout output
│           │   ├── stderr.log       # Full stderr output
│           │   └── evidence.json    # Complete evidence JSON
│           └── user_execution/
│               └── ... (same structure)
├── cycles/
├── reports/
└── memories/
```

### Artifact Contents

| File | Contents |
|------|----------|
| `command.txt` | The exact command string |
| `stdout.log` | Full standard output (not truncated) |
| `stderr.log` | Full standard error (not truncated) |
| `evidence.json` | Complete ExecutionEvidence as JSON |

## How Observer Should Behave with Evidence

The Observer agent receives execution evidence in its context metadata:

```python
context.metadata = {
    "execution_evidence": {
        "raw_command": "wsl pipeline run test.yaml",
        "exit_code": 1,
        "stdout": "...",
        "stderr": "ERROR: API authentication failed",
        "status": "FAILURE",
        "evidence_hash": "abc123...",
        ...
    }
}
```

### Observer Rules

1. **Derive issues FROM evidence** - Look at exit_code, stderr, stdout
2. **Match patterns to issue types:**
   - `exit_code != 0` → `EXECUTION_FAILURE`
   - `"auth" in stderr.lower()` → `AUTH_ERROR`
   - `"timeout" in stderr.lower()` → `PERFORMANCE`
   - `"schema" in stderr.lower()` → `DATA_ERROR`
3. **Handle NO_EVIDENCE explicitly:**
   - Emit a single `EXECUTION_ERROR` issue
   - Set description to "Execution evidence unavailable"
   - Include the reason from stderr

### Example Observer Behavior

```python
# If evidence shows authentication failure:
evidence = context.metadata.get("execution_evidence", {})
if "auth" in evidence.get("stderr", "").lower():
    issue = ObservedIssue(
        type=IssueType.AUTH_ERROR.value,
        location="user_execution",
        description="Authentication failed during pipeline execution",
        severity=IssueSeverity.HIGH.value,
        evidence=evidence.get("stderr", "")[:500],
    )
```

## Report Evidence Section

Cycle reports include an Evidence section linking to artifacts:

```markdown
## Execution Evidence

_Evidence from real ExecutionGateway calls. Full outputs in linked artifacts._

| Step | Command | Exit Code | Status | Hash | Artifacts |
|------|---------|-----------|--------|------|-----------|
| env_validation | `wsl env-check` | 0 | SUCCESS | `abc123...` | [stdout](...), [stderr](...) |
| user_execution | `wsl pipeline run...` | 1 | FAILURE | `def456...` | [stdout](...), [stderr](...) |

_Total duration: 35.50s_
```

## Determinism Guarantees

1. **Hash is deterministic:** SHA256 over `command|stdout|stderr|exit_code`
2. **Truncation is deterministic:** Same input → same truncated output
3. **Artifacts written before reports:** Links are always valid
4. **No randomness:** All evidence fields are derived from actual execution

## Memory Access

Evidence does **not** grant memory access to Observer or Improvement agents:
- Memory access contract (Phase 5.D) remains unchanged
- Evidence is contextual execution data, not learned knowledge
- Only Curriculum and Convergence agents can write to memory

## Integration Points

| Component | How Evidence is Used |
|-----------|---------------------|
| `ExecutionGateway` | Returns `ExecutionResult` converted to evidence |
| `CycleRunner` | Captures evidence, calls ArtifactWriter, wires to Observer |
| `ArtifactWriter` | Persists evidence files to disk |
| `Observer` | Receives evidence in context.metadata |
| `CycleReportGenerator` | Generates Evidence section in reports |

## Truncation Behavior

Long outputs are truncated to prevent memory issues:

- Default limit: 10,000 characters
- Truncation marker: `\n\n[TRUNCATED - output exceeded limit]`
- Full output is preserved in artifact files

Example:
```
...first 9950 characters of output...

[TRUNCATED - output exceeded limit]
```
