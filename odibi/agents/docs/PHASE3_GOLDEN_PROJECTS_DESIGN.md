# Phase 3: Golden Projects and Regression Baseline

## Design Document for Review

**Status:** ✅ IMPLEMENTED  
**Phase:** 3  
**Author:** AI Agent Suite  
**Date:** 2024

---

## Section 1: Golden Project Mental Model (Plain English)

### What is a Golden Project?

A **Golden Project** is an Odibi pipeline configuration that **must always work**. It is a regression contract: if a golden project fails after any change, that change has broken something critical.

### What Golden Projects Are

- **Verification targets** — They exist to detect regressions, not to be worked on
- **Immutable during cycles** — Agents cannot modify golden projects
- **Independent of workspace root** — Golden projects may live anywhere on disk
- **Absolute or relative paths** — Paths can be absolute (`D:/projects/etl/config.yaml`) or relative to workspace root
- **Run after improvements** — They execute during the `regression_checks` step
- **Pass/fail contracts** — A golden project either passes (pipeline runs successfully) or fails

### What Golden Projects Are NOT

- ❌ **NOT work targets** — The system does NOT select golden projects to "work on"
- ❌ **NOT the focus of cycles** — Cycles work on the workspace root; golden projects only verify
- ❌ **NOT UI-selectable** — There is NO "select a golden project to work on" workflow
- ❌ **NOT modifiable by agents** — Golden projects are read-only verification targets
- ❌ **NOT optional in Improvement Mode** — If `max_improvements > 0` and `golden_projects` is empty, the system should warn loudly

### How Golden Projects Differ from Workspace Root

| Aspect | Workspace Root | Golden Projects |
|--------|----------------|-----------------|
| Purpose | Scope for improvement work | Regression verification targets |
| Selection | User-provided or ProjectAgent-selected | Always user-declared, never agent-selected |
| Mutability | May be modified by ImprovementAgent | NEVER modified during cycles |
| Cardinality | One (single directory) | Zero or more (list of paths) |
| Execution | UserAgent runs pipelines here | RegressionGuardAgent runs these |

### The Trust Model

Golden projects represent the "floor" of what must work. Every cycle that makes improvements must prove those improvements do not break the floor. This is how we build trust in autonomous improvement cycles.

---

## Section 2: Schema / Configuration Definition

### GoldenProjectConfig Dataclass

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class GoldenProjectConfig:
    """A single golden project declaration.

    Golden projects are immutable verification targets.
    They are never modified by agents during cycles.
    """

    name: str
    """Human-readable name for logging and display."""

    path: str
    """Path to the pipeline config file.

    May be:
    - Absolute: D:/projects/sales_etl/pipeline.yaml
    - Relative: sales_etl/pipeline.yaml (resolved against workspace_root)
    """

    description: Optional[str] = None
    """Optional description of what this golden project tests."""

    expected_outcome: str = "success"
    """Expected outcome: 'success' (default) or 'specific_error:<pattern>'"""
```

### CycleConfig Updates

```python
@dataclass
class CycleConfig:
    """Configuration for a cycle run."""

    project_root: str
    task_description: str = "Scheduled maintenance cycle"
    max_improvements: int = 3
    max_runtime_hours: float = 8.0
    gated_improvements: bool = True
    stop_on_convergence: bool = True

    # CHANGED: From list[str] to list[GoldenProjectConfig]
    golden_projects: list[GoldenProjectConfig] = field(default_factory=list)

    @property
    def is_learning_mode(self) -> bool:
        return self.max_improvements == 0

    @property
    def is_improvement_mode(self) -> bool:
        return self.max_improvements > 0

    def validate_golden_projects_for_mode(self) -> list[str]:
        """Return validation warnings.

        Returns:
            List of warning messages (empty if valid).
        """
        warnings = []

        if self.is_improvement_mode and not self.golden_projects:
            warnings.append(
                "⚠️ IMPROVEMENT MODE with no golden projects configured. "
                "Regressions cannot be detected. Consider adding golden projects."
            )

        return warnings
```

### YAML Configuration Format (for reference)

```yaml
# Example: .odibi/config.yaml or passed via UI
golden_projects:
  - name: sales_daily_etl
    path: D:/odibi_projects/sales_daily_etl/pipeline.yaml
    description: Daily ETL for sales data - must never break

  - name: customer_sync
    path: customer_sync/config.yaml  # Relative to workspace_root
    description: Customer data synchronization pipeline

  - name: inventory_check
    path: D:/production/inventory/check.yaml
    expected_outcome: success
```

### UI Input Format (Backwards Compatible)

Current UI accepts comma-separated strings. For backwards compatibility:

**Simple format (existing):**
```
sales_etl.yaml, customer_sync.yaml, inventory.yaml
```

**Named format (new):**
```
sales_daily_etl:D:/projects/sales_etl/pipeline.yaml, customer_sync:sync/config.yaml
```

**Parsing Logic:**
```python
def parse_golden_projects_input(input_str: str, workspace_root: str) -> list[GoldenProjectConfig]:
    """Parse UI input into GoldenProjectConfig list.

    Supports:
    - Simple: "path1.yaml, path2.yaml"
    - Named: "name:path, name:path"
    """
    if not input_str.strip():
        return []

    projects = []
    for item in input_str.split(","):
        item = item.strip()
        if not item:
            continue

        if ":" in item and not item[1] == ":":  # Avoid matching D:\...
            name, path = item.split(":", 1)
            name = name.strip()
            path = path.strip()
        else:
            path = item
            name = Path(item).stem  # Use filename as name

        projects.append(GoldenProjectConfig(name=name, path=path))

    return projects
```

---

## Section 3: Execution Flow (Step-by-Step)

### Cycle Execution Order (Unchanged)

```
1. ENV_VALIDATION      - EnvironmentAgent
2. PROJECT_SELECTION   - ProjectAgent (skipped in learning mode)
3. USER_EXECUTION      - UserAgent
4. OBSERVATION         - ObserverAgent
5. IMPROVEMENT         - ImprovementAgent (skipped if max_improvements=0)
6. REVIEW              - ReviewerAgent (skipped if no proposal)
7. REGRESSION_CHECKS   - RegressionGuardAgent ← GOLDEN PROJECTS RUN HERE
8. MEMORY_PERSISTENCE  - CurriculumAgent
9. SUMMARY             - ConvergenceAgent
10. EXIT               - ConvergenceAgent
```

### Regression Checks Step Execution Flow

```
REGRESSION_CHECKS step begins
│
├── Check: Are there golden projects?
│   ├── NO → Skip step with reason "No golden projects configured"
│   └── YES → Continue
│
├── Check: Was there an approved improvement?
│   ├── NO → Skip step with reason "No improvement to verify"
│   └── YES → Continue
│
├── For EACH golden project:
│   │
│   ├── Resolve path (absolute or relative to workspace_root)
│   │
│   ├── Validate path exists
│   │   └── If NOT exists → Record as FAILURE (path not found)
│   │
│   ├── Execute via ExecutionGateway.run_pipeline()
│   │   └── agent_permissions = REGRESSION_GUARD permissions
│   │   └── agent_role = AgentRole.REGRESSION_GUARD
│   │
│   ├── Capture result:
│   │   ├── SUCCESS → Pipeline exit_code == 0
│   │   ├── FAILURE → Pipeline exit_code != 0
│   │   └── ERROR → Execution exception
│   │
│   └── Record in GoldenProjectResult
│
├── Aggregate results:
│   ├── Count: passed, failed, total
│   ├── Determine overall status
│   └── Update state.regressions_detected
│
└── Determine cycle impact:
    ├── ALL PASSED → Improvement is verified
    ├── ANY FAILED → Improvement is flagged/rejected
    └── Update state accordingly
```

### GoldenProjectResult Structure

```python
@dataclass
class GoldenProjectResult:
    """Result of running a single golden project."""

    name: str
    path: str
    status: str  # "PASSED" | "FAILED" | "ERROR" | "SKIPPED"
    exit_code: Optional[int] = None
    output_summary: str = ""
    error_message: str = ""
    execution_time_seconds: float = 0.0

    @property
    def is_regression(self) -> bool:
        return self.status in ("FAILED", "ERROR")


@dataclass
class RegressionCheckResult:
    """Aggregate result of all golden project checks."""

    results: list[GoldenProjectResult]
    total_count: int = 0
    passed_count: int = 0
    failed_count: int = 0
    regression_detected: bool = False

    def __post_init__(self):
        self.total_count = len(self.results)
        self.passed_count = sum(1 for r in self.results if r.status == "PASSED")
        self.failed_count = sum(1 for r in self.results if r.is_regression)
        self.regression_detected = self.failed_count > 0
```

---

## Section 4: Status Semantics Table

### Cycle Final Status Definitions

| Status | Condition | Meaning |
|--------|-----------|---------|
| **SUCCESS** | `completed=True` AND `regressions_detected=0` AND `(improvements_approved > 0 OR max_improvements=0)` | Cycle completed normally. All golden projects passed. |
| **PARTIAL** | `completed=True` AND `regressions_detected > 0` | Cycle completed but golden projects failed. Improvements may be unsafe. |
| **PARTIAL** | `completed=True` AND `improvements_approved=0` AND `improvements_rejected > 0` | Cycle completed but all improvements were rejected. |
| **FAILURE** | `interrupted=True` | Cycle was interrupted (timeout, user, error). |
| **FAILURE** | `exit_status="error"` | Critical execution error occurred. |

### Status Determination Logic

```python
def get_final_status(state: CycleState) -> str:
    """Return explicit final execution status."""

    # FAILURE cases (interruption or critical errors)
    if state.interrupted:
        return "FAILURE"
    if not state.completed:
        return "FAILURE"
    if state.exit_status == "error":
        return "FAILURE"

    # PARTIAL cases (completed but with issues)
    if state.regressions_detected > 0:
        return f"PARTIAL ({state.regressions_detected} golden project(s) failed)"
    if state.improvements_rejected > 0 and state.improvements_approved == 0:
        return "PARTIAL (all improvements rejected)"

    # SUCCESS
    return "SUCCESS"
```

### Individual Golden Project Status

| Status | Meaning |
|--------|---------|
| **PASSED** | Pipeline executed successfully (exit_code = 0) |
| **FAILED** | Pipeline execution failed (exit_code ≠ 0) |
| **ERROR** | Execution threw an exception (path not found, permission denied, etc.) |
| **SKIPPED** | Project was not run (e.g., path could not be resolved) |

### Regression Impact on Improvements

| Improvement Status | Golden Projects | Result |
|-------------------|-----------------|--------|
| Approved by ReviewerAgent | All PASSED | Improvement is verified and stands |
| Approved by ReviewerAgent | Any FAILED | Improvement is **flagged** (regression warning) |
| Rejected by ReviewerAgent | N/A | Regression checks still run (validates no accidental changes) |
| No proposal | N/A | Regression checks skipped (nothing to verify) |

---

## Section 5: Safety Guarantees

### Invariant 1: Golden Projects Are Never Modified

**Guarantee:** No agent in the cycle can modify golden project files.

**Enforcement:**
- Golden project paths are resolved at the START of regression_checks step
- RegressionGuardAgent has `can_edit_source=False`
- ExecutionGateway.run_pipeline() is read-only when called by RegressionGuardAgent
- Paths are frozen in `GoldenProjectConfig(frozen=True)`

### Invariant 2: Golden Projects Run AFTER Improvements

**Guarantee:** Improvements are applied before golden projects are verified.

**Enforcement:**
- Cycle step order is fixed: `REVIEW → REGRESSION_CHECKS`
- RegressionGuardAgent runs in step 7, ImprovementAgent runs in step 5
- This order is defined in `CYCLE_ORDER` and is immutable

### Invariant 3: Empty Golden Projects in Improvement Mode Triggers Warning

**Guarantee:** If `max_improvements > 0` and `golden_projects` is empty, the system warns loudly.

**Enforcement:**
```python
# In CycleRunner.start_cycle():
warnings = config.validate_golden_projects_for_mode()
for warning in warnings:
    self._emit_event(CycleEvent(
        event_type="warning",
        step="pre_cycle",
        agent_role="system",
        message=warning,
        is_error=False,
    ))
```

**Option (configurable):** Block scheduled cycles if no golden projects.
```python
# In ScheduledConfig:
require_golden_projects: bool = True  # Blocks scheduled runs if empty
```

### Invariant 4: Deterministic Execution

**Guarantee:** Golden projects run in declaration order, with deterministic results.

**Enforcement:**
- `golden_projects` list is iterated in order
- No parallel execution (sequential to avoid race conditions)
- Each project gets a clean execution context
- Results are recorded in same order as declaration

### Invariant 5: Regression Detection Updates Cycle State

**Guarantee:** If any golden project fails, `state.regressions_detected` is incremented.

**Enforcement:**
```python
# In CycleRunner._post_process_step():
if step == CycleStep.REGRESSION_CHECKS and response:
    metadata = response.metadata or {}
    state.regressions_detected = metadata.get("regressions_detected", 0)
```

---

## Section 6: Non-Goals (What Phase 3 Does NOT Do)

### ❌ No New Agents

Phase 3 does NOT introduce any new agents. `RegressionGuardAgent` already exists and will be updated.

### ❌ No Changes to ProjectAgent Autonomy

ProjectAgent behavior is unchanged:
- Still proposes projects based on coverage gaps
- Still skipped in learning mode
- No new constraints on what it can propose

### ❌ No Changes to Workspace Root Semantics

Workspace root remains:
- The directory scope for cycle work
- Used by ProjectAgent for proposal scope
- Used by UserAgent for execution context
- Unrelated to golden project paths

### ❌ No Changes to Cycle Execution Order

The 10-step cycle order is unchanged. `REGRESSION_CHECKS` remains step 7.

### ❌ No Project Selection UI

There is NO "select a project to work on" UI element. Golden projects are declared in config, not selected interactively.

### ❌ No Automatic Golden Project Discovery

Golden projects are NOT auto-discovered from the filesystem. They must be explicitly declared.

### ❌ No Golden Project Editing

Users cannot edit golden project content through the agent UI. They are config paths only.

### ❌ No Baseline Management

Phase 3 does NOT implement baseline storage/comparison for golden projects. A project either runs successfully or fails. Future phases may add snapshot baselines.

### ❌ No Automatic Rollback

If a regression is detected, Phase 3 does NOT automatically rollback the improvement. It only flags/records the regression. Future phases may add rollback.

---

## Summary: What Phase 3 Delivers

| Deliverable | Description |
|-------------|-------------|
| `GoldenProjectConfig` dataclass | Typed, frozen, validated golden project declarations |
| Updated `CycleConfig` | `golden_projects: list[GoldenProjectConfig]` |
| `GoldenProjectResult` dataclass | Per-project execution results |
| `RegressionCheckResult` dataclass | Aggregate check results |
| Updated `RegressionGuardAgent` | Executes golden projects, records results |
| Status semantics | Clear SUCCESS / PARTIAL / FAILURE rules |
| Improvement mode warning | Loud warning if no golden projects |
| Documentation | This document (PHASE3_GOLDEN_PROJECTS_DESIGN.md) |

---

## Implementation Checklist (Post-Review)

After this design is approved, implementation will:

- [ ] Add `GoldenProjectConfig` to `core/cycle.py`
- [ ] Update `CycleConfig.golden_projects` type
- [ ] Add `GoldenProjectResult` and `RegressionCheckResult`
- [ ] Update `RegressionGuardAgent.process()` with new execution logic
- [ ] Add `validate_golden_projects_for_mode()` method
- [ ] Update `CycleRunner._post_process_step()` for proper state updates
- [ ] Add UI input parsing for named format
- [ ] Add warning emission for empty golden projects in improvement mode
- [ ] Update `ENFORCEMENT.md` with golden project invariants
- [ ] Add tests for golden project execution

---

## Open Questions for Review

1. **Blocking vs Warning:** Should empty `golden_projects` in Improvement Mode be a **warning** (log + continue) or a **block** (prevent scheduled cycles)?

2. **Relative Path Resolution:** If a golden project path is relative, should it resolve against:
   - `workspace_root` (current design)
   - `odibi_root`
   - A new `golden_projects_base` config

3. **Expected Outcome:** Should Phase 3 support `expected_outcome: "specific_error:<pattern>"` for negative tests, or defer to a future phase?

4. **Execution Mode:** Should golden projects run with `dry_run=True` (current) or `dry_run=False` (full execution)?

---

*This document is ready for review. No implementation will proceed until design is approved.*
