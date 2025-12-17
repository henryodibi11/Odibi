# Phase 6: Evidence-Based Cycles - Design Document

## Design Summary

### Goals
- Make `user_execution` and `env_validation` produce evidence-based outputs by running real commands via ExecutionGateway (WSL)
- Capture outputs into deterministic artifacts
- Agents must never fabricate pipeline runs, configs, errors, or durations

### Key Deliverables

1. **ExecutionEvidence Schema** - Structured evidence from real execution
2. **Artifact Persistence** - Write artifacts to `.odibi/artifacts/<cycle_id>/`
3. **UserAgent Update** - Report ONLY based on real ExecutionEvidence
4. **Observer Wiring** - Observer receives ExecutionEvidence in context
5. **Report Upgrades** - Evidence section with artifact links
6. **EVIDENCE.md Documentation**

---

## File-by-File Change List

### New Files

| File | Purpose |
|------|---------|
| `agents/core/evidence.py` | ExecutionEvidence schema, hashing, truncation, ArtifactWriter |
| `agents/core/tests/test_evidence.py` | Tests for evidence hashing, truncation, artifact writer, serialization |
| `agents/docs/EVIDENCE.md` | Documentation explaining evidence system |

### Modified Files

| File | Changes |
|------|---------|
| `agents/core/execution.py` | Add `to_evidence()` method on ExecutionResult |
| `agents/core/cycle.py` | Wire ExecutionEvidence to Observer context; call ArtifactWriter |
| `agents/core/reports.py` | Add "Evidence" section with artifact paths |
| `agents/core/schemas.py` | No changes (Observer derives from evidence, no schema change needed) |

---

## Detailed Design

### 1. ExecutionEvidence Schema (`agents/core/evidence.py`)

```python
@dataclass
class ArtifactRef:
    path: str          # Relative path: .odibi/artifacts/<cycle_id>/execution/<step>/stdout.log
    artifact_type: str # "stdout" | "stderr" | "command" | "evidence"

class EvidenceStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    NO_EVIDENCE = "NO_EVIDENCE"

@dataclass
class ExecutionEvidence:
    raw_command: str
    exit_code: int
    stdout: str           # Truncated with marker
    stderr: str           # Truncated with marker
    started_at: str       # ISO timestamp
    finished_at: str      # ISO timestamp
    duration_seconds: float
    artifacts: list[ArtifactRef]
    status: EvidenceStatus
    evidence_hash: str    # sha256(command + stdout + stderr + exit_code)
```

**Key Methods:**
- `compute_hash()` - Deterministic SHA256 over (command, stdout, stderr, exit_code)
- `truncate_output(s, max_len=10000)` - Deterministic truncation with `[TRUNCATED]` marker
- `to_dict()` / `from_dict()` - Serialization

### 2. ArtifactWriter (`agents/core/evidence.py`)

```python
class ArtifactWriter:
    def __init__(self, odibi_root: str):
        self.artifacts_dir = os.path.join(odibi_root, "artifacts")

    def write_execution_artifacts(
        self,
        cycle_id: str,
        step_name: str,
        evidence: ExecutionEvidence
    ) -> list[ArtifactRef]:
        """Write artifacts to .odibi/artifacts/<cycle_id>/execution/<step_name>/"""
        # Creates: command.txt, stdout.log, stderr.log, evidence.json
```

**Directory Structure:**
```
.odibi/
├── artifacts/
│   └── <cycle_id>/
│       └── execution/
│           ├── env_validation/
│           │   ├── command.txt
│           │   ├── stdout.log
│           │   ├── stderr.log
│           │   └── evidence.json
│           └── user_execution/
│               └── ...
├── cycles/
├── reports/
└── memories/
```

### 3. ExecutionResult.to_evidence() (`agents/core/execution.py`)

Add method to convert ExecutionResult to ExecutionEvidence:

```python
def to_evidence(self, started_at: str, finished_at: str) -> "ExecutionEvidence":
    from .evidence import ExecutionEvidence, EvidenceStatus
    return ExecutionEvidence.from_execution_result(
        self, started_at, finished_at
    )
```

### 4. CycleRunner Wiring (`agents/core/cycle.py`)

**Changes to `_build_context()`:**
- For `CycleStep.OBSERVATION`, include `execution_evidence` in metadata
- Evidence comes from the `user_execution` step log

**Changes to `run_next_step()`:**
- After `env_validation` and `user_execution` steps, call `ArtifactWriter`
- Store evidence in step log metadata

### 5. UserAgent Behavior Change

UserAgent MUST:
- Return real ExecutionEvidence from ExecutionGateway calls
- If unable to run (no gateway, permission denied), return `NO_EVIDENCE` status
- NEVER fabricate execution logs

This is enforced by:
- UserAgent using ExecutionGateway.run_pipeline() / run_task()
- Converting ExecutionResult to ExecutionEvidence
- Storing evidence in response metadata

### 6. Observer Wiring

**Observer receives in context.metadata:**
```python
{
    "execution_evidence": {
        "raw_command": "...",
        "exit_code": 0,
        "status": "SUCCESS",
        "evidence_hash": "abc123...",
        "artifacts": [...]
    }
}
```

Observer MUST derive issues from evidence:
- If `exit_code != 0` → EXECUTION_FAILURE
- If stderr contains "auth" → AUTH_ERROR
- If evidence is NO_EVIDENCE → emit appropriate issue

### 7. Report Evidence Section (`agents/core/reports.py`)

New section in report:

```markdown
## Execution Evidence

| Step | Command | Exit Code | Status | Evidence Hash | Artifacts |
|------|---------|-----------|--------|---------------|-----------|
| env_validation | `wsl env-check` | 0 | SUCCESS | `abc123...` | [stdout](artifacts/...) |
| user_execution | `wsl pipeline run ...` | 1 | FAILURE | `def456...` | [stdout](artifacts/...) |
```

---

## New Tests List

| Test File | Tests |
|-----------|-------|
| `test_evidence.py` | `test_evidence_hash_deterministic` |
| | `test_evidence_hash_changes_with_input` |
| | `test_truncation_deterministic` |
| | `test_truncation_adds_marker` |
| | `test_truncation_no_change_if_under_limit` |
| | `test_artifact_writer_creates_directory` |
| | `test_artifact_writer_writes_all_files` |
| | `test_evidence_serialization_roundtrip` |
| | `test_evidence_from_execution_result` |
| | `test_no_evidence_status` |
| `test_reports.py` | `test_report_includes_evidence_section` |
| `test_cycle.py` (new) | `test_cycle_runner_wires_evidence_to_observer` |

---

## What Remains UNCHANGED

| Component | Status |
|-----------|--------|
| Memory access contract | Unchanged - Observer/Improvement have NO memory access |
| Review gating logic | Unchanged - Reviewer decisions ignore advisory_context |
| Step order | Unchanged - 10 steps in fixed order |
| Permissions model | Unchanged - existing AgentPermissions enforced |
| Advisory-only disclaimers | Unchanged - scorecard remains advisory |
| Regression ordering | Unchanged - golden projects run after review |
| Golden project WSL path fix | Preserved - uses existing `_to_wsl_path()` |

---

## Safety and Determinism

1. **Truncation is deterministic**: Same input → same truncated output (no randomness)
2. **Hash is deterministic**: SHA256 over concatenated strings
3. **Artifacts written before report**: Report generator can link to artifacts
4. **NO_EVIDENCE is explicit**: UserAgent returns structured status, not fabricated logs

---

## Implementation Order

1. Create `evidence.py` with ExecutionEvidence + ArtifactWriter
2. Add `test_evidence.py` with all tests
3. Add `to_evidence()` to ExecutionResult
4. Update CycleRunner to wire evidence
5. Update reports.py for Evidence section
6. Create EVIDENCE.md documentation
7. Run all tests
