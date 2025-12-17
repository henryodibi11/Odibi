# Golden Projects

Golden projects are **immutable verification targets** that must always pass. They serve as regression contracts: if a golden project fails after any improvement, that improvement has broken something critical.

## What Golden Projects Are

- **Verification targets** — They exist to detect regressions, not to be worked on
- **Immutable during cycles** — Agents cannot modify golden project files
- **Run after improvements** — Executed during the `regression_checks` step
- **Pass/fail contracts** — A golden project either passes or fails

## What Golden Projects Are NOT

- ❌ **NOT work targets** — The system does NOT select golden projects to "work on"
- ❌ **NOT the focus of cycles** — Cycles work on the workspace root; golden projects only verify
- ❌ **NOT modifiable by agents** — Golden projects are read-only verification targets

## Configuring Golden Projects

### Via UI (Comma-Separated)

In the Scheduled Assistant Config panel:

```
sales_etl.yaml, customer_sync.yaml, inventory.yaml
```

### Via UI (Named Format)

```
sales_daily_etl:D:/projects/sales_etl/pipeline.yaml, customer_sync:sync/config.yaml
```

### Via Code

```python
from agents.core.cycle import GoldenProjectConfig

golden_projects = [
    GoldenProjectConfig(
        name="sales_daily_etl",
        path="D:/projects/sales_etl/pipeline.yaml",
        description="Daily ETL for sales data"
    ),
    GoldenProjectConfig(
        name="customer_sync",
        path="sync/config.yaml",  # Relative to workspace_root
    ),
]

runner.start_guided_cycle(
    project_root="D:/workspace",
    max_improvements=3,
    golden_projects=golden_projects,
)
```

## Path Resolution

Golden project paths can be:

- **Absolute**: `D:/projects/sales_etl/pipeline.yaml`
- **Relative**: `sync/config.yaml` (resolved against `workspace_root`)

## Execution Flow

1. Cycle runs steps 1-6 (including improvement and review)
2. Step 7 runs **REGRESSION_CHECKS** whenever golden projects are configured
3. RegressionGuardAgent executes each golden project sequentially
4. Each project result is recorded: PASSED, FAILED, or ERROR
5. If any project fails, `state.regressions_detected` is incremented
6. Cycle status becomes PARTIAL if regressions are detected

**Note:** Regression checks run even if no improvement was proposed or approved. This ensures golden projects are verified every cycle as a baseline health check.

## Status Semantics

| Status | Meaning |
|--------|---------|
| **SUCCESS** | All golden projects passed |
| **PARTIAL** | One or more golden projects failed |
| **FAILURE** | Cycle interrupted or critical error |

### Individual Golden Project Status

| Status | Meaning |
|--------|---------|
| **PASSED** | Pipeline executed successfully |
| **FAILED** | Pipeline execution failed |
| **ERROR** | Path not found or execution exception |
| **SKIPPED** | Project was not run |

## Warnings and Blocking

### Guided Execution Mode

If you run in **Improvement Mode** (`max_improvements > 0`) without any golden projects configured, the system will emit a warning:

> ⚠️ IMPROVEMENT MODE with no golden projects configured. Regressions cannot be detected. Consider adding golden projects.

This warning appears in the cycle events but does not block execution.

### Scheduled Assistant Mode

Scheduled cycles in **Improvement Mode** **require** golden projects. If you attempt to run a scheduled cycle with `max_improvements > 0` and no golden projects, the system will raise an error:

> Scheduled cycles in IMPROVEMENT MODE require golden projects. Configure golden_projects or set max_improvements_per_cycle=0 for learning mode.

This blocking requirement ensures that autonomous overnight runs always have regression verification.

## Safety Guarantees

1. **Immutability**: Golden projects cannot be modified by any agent
2. **Post-improvement execution**: Golden projects run AFTER improvements are applied
3. **Sequential execution**: Projects run in declaration order, deterministically
4. **State tracking**: All results are stored in `state.golden_project_results`

## Difference from Workspace Root

| Aspect | Workspace Root | Golden Projects |
|--------|----------------|-----------------|
| Purpose | Scope for improvement work | Regression verification |
| Selection | User-provided or ProjectAgent | Always user-declared |
| Mutability | May be modified | NEVER modified |
| Cardinality | One directory | Zero or more paths |
| Execution | UserAgent works here | RegressionGuardAgent runs these |

## Example Output

When regression checks run, you'll see output like:

```
REGRESSION CHECK REPORT:

Projects Tested: 3
Passed: 2
Failed: 1

Results:
1. sales_daily_etl
   Path: D:/projects/sales_etl/pipeline.yaml
   Status: PASSED
2. customer_sync
   Path: D:/workspace/sync/config.yaml
   Status: PASSED
3. inventory_check
   Path: D:/production/inventory/check.yaml
   Status: FAILED
   Error: Pipeline exited with code 1

Overall Status: REGRESSIONS_DETECTED
Regressions: 1

Recommendation: VETO - regressions detected, improvements may be unsafe
```

## Best Practices

1. **Always configure golden projects in Improvement Mode** — Without them, regressions cannot be detected
2. **Use absolute paths for production pipelines** — Avoids path resolution ambiguity
3. **Keep golden projects minimal but representative** — Cover critical paths without excessive runtime
4. **Review failed golden projects before accepting improvements** — The PARTIAL status is a warning, not a block
