# Explorer Layer Design Document

## Status: DRAFT — For Human Review

---

## 1. System Boundary Analysis

### 1.1 Existing Trusted System (Phases 1–12)

The current Odibi system has clear trust boundaries:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         TRUSTED KERNEL                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │   Execution     │    │   Enforcement   │    │   Escalation    │     │
│  │   Gateway       │    │   Module        │    │   (Phase 12)    │     │
│  │   (Phase 0)     │    │   (Phase 0)     │    │                 │     │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘     │
│           │                      │                      │               │
│           ▼                      ▼                      ▼               │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    INVARIANT LAYER                              │   │
│  │  - Permission enforcement (can_execute, can_edit_source)        │   │
│  │  - Wall-clock timeout (mechanical, not advisory)                │   │
│  │  - Review gating (APPROVED keyword required)                    │   │
│  │  - Cycle termination guarantee                                  │   │
│  │  - Learning harness protection (never modifiable)               │   │
│  │  - Rollback guarantees                                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │  Autonomous     │    │   Controlled    │    │   Issue         │     │
│  │  Learning       │    │   Improvement   │    │   Discovery     │     │
│  │  (Phase 9.A)    │    │   (Phase 9.G)   │    │   (Phase 10-11) │     │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    PROTECTED ASSETS                             │   │
│  │  - Golden projects (.odibi/learning_harness/)                   │   │
│  │  - Regression tests                                             │   │
│  │  - Trust kernel code (enforcement.py, escalation.py, etc.)      │   │
│  │  - Memory guardrails                                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Key Invariants (From Code Analysis)

| Invariant | Source | Enforcement |
|-----------|--------|-------------|
| Execution requires explicit permissions | `enforcement.py:30` | `PermissionDeniedError` |
| Wall-clock timeout is mechanical | `enforcement.py:36` | `WallClockTimeoutError` |
| Review gating requires APPROVED keyword | `enforcement.py:42` | `ReviewGatingError` |
| Learning harness files are never modifiable | `controlled_improvement.py:65-83` | `LearningHarnessViolation` |
| Scope violations fail hard | `controlled_improvement.py:52-62` | `ImprovementScopeViolation` |
| Discovery is forever passive | `escalation.py:8-13` | Design constraint |
| Human intent is the authorization boundary | `escalation.py:15-21` | Manual gating |
| Improvements must be fully reversible | `escalation.py:39-44` | Snapshot + rollback |
| Memory queries require explicit intent | `memory_guardrails.py:23-43` | `MissingIntentError` |

### 1.3 What the Explorer CANNOT Touch

| Protected Area | Reason |
|----------------|--------|
| `agents/core/enforcement.py` | Trust kernel |
| `agents/core/escalation.py` | Human gating logic |
| `agents/core/controlled_improvement.py` | Rollback guarantees |
| `agents/core/memory_guardrails.py` | Action-directing query blocks |
| `.odibi/learning_harness/**` | Golden projects (regression) |
| `agents/core/tests/test_phase13_trust_validation.py` | Trust tests |

---

## 2. Existing Capabilities Explorer Can Reuse

### 2.1 Execution Layer (Read-Only Access)

| Component | File | Reuse Strategy |
|-----------|------|----------------|
| `ExecutionGateway` | `execution.py` | Run experiments via `run_task()` in sandbox |
| `ExecutionResult` | `execution.py:39-79` | Capture stdout/stderr/exit_code |
| `TaskDefinition` | `execution.py:86-100` | Define experiment tasks |

**Constraint**: Explorer uses execution in sandbox clones only.

### 2.2 Evidence Collection (Read-Only)

| Component | File | Reuse Strategy |
|-----------|------|----------------|
| `ExecutionEvidence` | `evidence.py` | Structured evidence from experiments |
| `ArtifactWriter` | `evidence.py` | Write experiment artifacts to sandbox |

### 2.3 Schema Contracts (Import As-Is)

| Component | File | Reuse Strategy |
|-----------|------|----------------|
| `ObservedIssue` | `schemas.py:63-93` | Structure experiment observations |
| `ImprovementProposal` | `schemas.py:240-373` | Format candidate diffs |
| `ProposalChange` | `schemas.py:198-219` | Represent code changes |

### 2.4 UI Components (If Applicable)

| Component | File | Reuse Strategy |
|-----------|------|----------------|
| `llm_client.py` | `ui/llm_client.py` | LLM calls for experiment generation |
| `streaming.py` | `ui/streaming.py` | Stream experiment output to UI |

---

## 3. Explorer Architecture

### 3.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXPLORER LAYER (UNTRUSTED)                      │
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │ RepoCloneManager│───▶│ ExperimentRunner│───▶│ ExplorerMemory  │     │
│  │                 │    │                 │    │ (append-only)   │     │
│  │ - Clone repos   │    │ - Run in sandbox│    │                 │     │
│  │ - Manage sandbox│    │ - Capture diffs │    │ - Record outcome│     │
│  │ - Cleanup       │    │ - Record results│    │ - No deletions  │     │
│  └────────┬────────┘    └────────┬────────┘    └────────┬────────┘     │
│           │                      │                      │               │
│           │                      │                      ▼               │
│           │                      │             ┌─────────────────┐     │
│           │                      │             │ PromotionBucket │     │
│           │                      │             │ (submit only)   │     │
│           │                      │             │                 │     │
│           │                      │             │ - Add candidates│     │
│           │                      │             │ - Read status   │     │
│           │                      │             │ - NO promote    │     │
│           │                      │             └────────┬────────┘     │
│           │                      │                      │               │
└───────────┼──────────────────────┼──────────────────────┼───────────────┘
            │                      │                      │
════════════╪══════════════════════╪══════════════════════╪═══════════════
            │                      │                      │  TRUST BOUNDARY
            │                      │                      │
            │                      │                      ▼
┌───────────┼──────────────────────┼──────────────────────────────────────┐
│           │                      │         HUMAN REVIEW                 │
│           ▼                      ▼         (Outside Explorer)           │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │ Trusted Odibi   │    │ ExecutionGateway│    │ Review UI       │     │
│  │ Codebase        │    │ (read-only use) │    │                 │     │
│  │ (clone source)  │    │                 │    │ - Approve/Reject│     │
│  └─────────────────┘    └─────────────────┘    │ - Apply to trust│     │
│                                                 └─────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Data Flow

```
1. CLONE
   Trusted Odibi Repo ──(copy)──▶ Sandbox Clone

2. EXPERIMENT
   Sandbox Clone ──(modify Python)──▶ Run via ExecutionGateway
                 ──(capture)──▶ ExperimentResult (stdout, stderr, diff)

3. RECORD
   ExperimentResult ──(append)──▶ ExplorerMemory (immutable entry)

4. SUBMIT
   ExplorerMemory ──(if successful)──▶ PromotionBucket (candidate diff)

5. REVIEW (outside Explorer)
   PromotionBucket ──(human reads)──▶ Review UI
   Review UI ──(human approves)──▶ Trusted Codebase
```

---

## 4. Phased Implementation Plan

### Phase E0: Scaffolding (DONE)

**Goal**: Minimal directory layout and stub interfaces.

- [x] Create `agents/explorer/` directory
- [x] Stub `RepoCloneManager`
- [x] Stub `ExperimentRunner`
- [x] Stub `ExplorerMemory`
- [x] Stub `PromotionBucket`
- [x] Define data classes (`SandboxInfo`, `ExperimentResult`, `MemoryEntry`, etc.)

**Output**: Compiles, imports, no functionality.

---

### Phase E1: Sandbox Isolation

**Goal**: Prove we can clone and destroy sandboxes safely.

**Tasks**:
1. Implement `RepoCloneManager.create_sandbox()` using `shutil.copytree()`
2. Implement `RepoCloneManager.destroy_sandbox()` using `shutil.rmtree()`
3. Add path validation to reject sandbox roots inside trusted paths
4. Add cleanup-on-exit hook

**Tests**:
- Clone creates isolated directory
- Destroy removes all files
- Sandbox root cannot be inside `agents/core/`
- Cleanup removes all sandboxes on shutdown

**Deliverables**:
- Working clone/destroy cycle
- Path validation enforced
- No interaction with trusted code

---

### Phase E2: Basic Experiment Execution

**Goal**: Run a shell command in sandbox and capture output.

**Tasks**:
1. Implement `ExperimentRunner.run_experiment()` using subprocess
2. Wire up to existing `ExecutionGateway` (pass sandbox path as working_dir)
3. Capture stdout, stderr, exit_code
4. Add timeout enforcement (reuse `WallClockTimeoutError`)

**Tests**:
- Simple command runs in sandbox
- Output captured correctly
- Timeout terminates experiment
- Exit code propagated

**Deliverables**:
- Experiments run in isolation
- Output captured as `ExperimentResult`
- Timeout enforced

---

### Phase E3: Diff Capture

**Goal**: Capture git diff after experiment modifies sandbox files.

**Tasks**:
1. Initialize sandbox as git repo (or clone with .git)
2. Implement `ExperimentRunner.extract_diff()` using `git diff`
3. Write diff to file in sandbox artifacts directory
4. Link diff path in `ExperimentResult`

**Tests**:
- Modify file in sandbox → diff captured
- No changes → empty diff
- Diff path points to valid file

**Deliverables**:
- Diffs captured for all experiments
- Diffs stored in sandbox (not trusted repo)

---

### Phase E4: Append-Only Memory

**Goal**: Persist experiment outcomes immutably.

**Tasks**:
1. Implement `ExplorerMemory.append()` with JSON-lines persistence
2. Implement `ExplorerMemory` read methods (list, filter, count)
3. Ensure `promoted=True` raises `ValueError` (Explorer cannot set)
4. Add file locking for concurrent access

**Tests**:
- Append adds entry to file
- Entries cannot be deleted or modified
- `promoted=True` rejected
- Concurrent appends don't corrupt

**Deliverables**:
- Append-only memory store
- JSON-lines format for easy inspection
- No mutation paths

---

### Phase E5: Promotion Bucket

**Goal**: Submit candidates for human review.

**Tasks**:
1. Implement `PromotionBucket.submit()` with status=PENDING enforcement
2. Implement read methods (list_pending, get, list_all)
3. Persist to separate file from memory
4. Status changes happen outside Explorer (separate review tool)

**Tests**:
- Submit with status≠PENDING raises error
- Duplicate candidate_id raises error
- List methods return correct subsets

**Deliverables**:
- Candidates submitted to bucket
- Explorer has no path to change status
- Bucket readable by review UI

---

### Phase E6: Integration Test

**Goal**: End-to-end: clone → experiment → memory → bucket.

**Tasks**:
1. Write integration test exercising full flow
2. Verify no writes to trusted codebase
3. Verify memory is append-only across test
4. Verify bucket receives candidate

**Tests**:
- Full flow completes without error
- Trusted repo unchanged (hash comparison)
- Memory file grows monotonically
- Bucket has candidate entry

**Deliverables**:
- Integration test passes
- Trust boundary verified

---

### Phase E7: Explorer Session Orchestrator (Optional)

**Goal**: Coordinate multi-experiment sessions with policy.

**Tasks**:
1. Create `ExplorerSession` class
2. Add experiment budget (max experiments per session)
3. Add disk budget (max sandbox size)
4. Add session timeout

**Tests**:
- Budget enforced
- Session terminates on timeout
- Cleanup runs on session end

**Deliverables**:
- Session-level resource management
- Policy enforcement

---

## 5. Assumptions

| # | Assumption | Risk if Wrong |
|---|------------|---------------|
| A1 | Trusted Odibi repo can be cloned via filesystem copy | May need git clone instead if .git is required |
| A2 | `ExecutionGateway` can run with arbitrary `working_dir` | May need to extend gateway to accept sandbox path |
| A3 | Sandbox root on Windows (D:\) has sufficient disk space | May need disk guard ported from `disk_guard.py` |
| A4 | Explorer memory and bucket are single-process | May need file locking for concurrent access |
| A5 | Human review UI exists or will be built separately | Bucket is useless without review UI |
| A6 | `shutil.copytree` is sufficient for repo cloning | May be slow for large repos; consider git worktrees |

---

## 6. Open Questions

| # | Question | Owner | Status | Answer |
|---|----------|-------|--------|--------|
| Q1 | Where should sandbox root be located? | Human | **RESOLVED** | `D:\odibi_explorer_sandboxes\` — outside trusted repo, disposable, supports large disk |
| Q2 | Should sandboxes be git repos or plain directories? | Human | Open | Start plain |
| Q3 | What is the maximum sandbox lifetime before auto-cleanup? | Human | Open | TBD |
| Q4 | Does the review UI already exist? | Human | **RESOLVED** | Yes. Explorer only writes to PromotionBucket. Status transitions happen in review UI. |
| Q5 | Should Explorer memory be queryable by the trusted system? | Human | Open | TBD |
| Q6 | How are LLM experiments generated? | Human | Open | TBD |

---

## 7. Explicit Out of Scope

The following are **NOT** part of this design:

| Item | Reason |
|------|--------|
| Changes to `enforcement.py`, `escalation.py`, `controlled_improvement.py` | Trust kernel is frozen |
| Modifications to golden projects or learning harness | Protected by design |
| Auto-promotion of candidates | Human review required |
| Explorer access to trusted memory | Separate memory systems |
| Multi-agent coordination in Explorer | Start simple |
| LLM prompt engineering for experiments | Separate concern |
| Experiment generation logic | Scaffolding only |
| Review UI implementation | Outside Explorer boundary |

---

## 8. Risk Checklist

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Sandbox path escapes to trusted repo | Medium | Critical | Path validation in `RepoCloneManager`, reject paths containing `agents/core` |
| Disk exhaustion from uncleaned sandboxes | High | High | Disk guard, session timeout, cleanup-on-exit |
| Explorer memory corruption | Low | Medium | Append-only JSON-lines, file locking |
| Promotion bucket used as backdoor | Low | Critical | Status changes only in separate review tool; Explorer cannot import review tool |
| Experiments modify trusted repo directly | Low | Critical | All `working_dir` must be validated as sandbox path |
| Explorer imports enforcement and bypasses | Low | Critical | Explorer module should not import `enforcement.py` modification logic |
| Memory grows unbounded | Medium | Medium | Implement rotation/archival in later phase |
| LLM generates malicious experiments | Medium | High | Sandbox isolation; no network access from sandbox |

---

## 9. Verification Criteria

Before any phase is considered complete:

1. **Trusted repo unchanged**: Hash of `agents/core/` before and after must match
2. **No new imports in trust kernel**: `enforcement.py`, `escalation.py` must not import from `explorer/`
3. **Memory is append-only**: No code paths allow deletion or modification
4. **Promotion bucket is submit-only**: No code paths allow status change from Explorer
5. **Tests pass**: All existing tests in `agents/core/tests/` still pass
6. **Sandbox cleanup works**: No orphan sandboxes after test runs

---

## 10. Next Steps

1. **Human Decision Required**: Answer Q1 (sandbox root location) and Q4 (review UI status)
2. **Proceed to Phase E1**: Implement sandbox isolation once Q1 answered
3. **Parallel Work**: Design review UI if not already planned

---

*Document generated by Amp. Subject to human review and approval.*
