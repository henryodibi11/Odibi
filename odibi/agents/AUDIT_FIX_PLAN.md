# Odibi Agents Audit Fix Plan

**Audit Date:** 2025-12-16  
**Status:** Phases 1-4 COMPLETE, Phase 5 is future work

---

## Phase 1: Critical Security & Stability (Do First) ✅ COMPLETE

### 1.1 Shell Injection Fix
- **File:** `agents/ui/tools/shell_tools.py`
- **Lines:** 26-60
- **Issue:** `run_command` uses `shell=True` with arbitrary user input
- **Fix:** Use `shlex.split()` and `shell=False` for non-shell commands, or sanitize input

### 1.2 Threading Race Conditions
- **File:** `agents/ui/components/cycle_panel.py`
- **Lines:** 499, 1872
- **Issue:** Mutable list `[False]` used for cross-thread stop signals
- **Fix:** Replace with `threading.Event()` for thread-safe signaling
- **Affected variables:**
  - `_stop_requested` (line 478)
  - `_learning_session_stop_requested` (line 1634)
  - `_improvement_stop_requested` (line 1872 - must move to module level)

---

## Phase 2: Bug Fixes (Medium Priority) ✅ COMPLETE

### 2.1 Sound Toggle Bug
- **File:** `agents/ui/components/enhanced_chat.py`
- **Lines:** 1335-1336
- **Issue:** `if True` always returns same emoji
- **Fix:** Track state in handler or use Gradio State component

### 2.2 Windows Path Handling
- **File:** `agents/ui/components/escalation_panel.py`
- **Lines:** 502-503
- **Issue:** `rsplit("/", 1)[0]` breaks on Windows backslashes
- **Fix:** Use `Path(target_file).parent`

### 2.3 Type Hints
- **Files:**
  - `agents/ui/components/memories.py` (line 221)
  - `agents/ui/components/conversation.py` (line 321)
- **Issue:** `callable` should be `Callable` (from typing)
- **Fix:** Add import and fix type hints

### 2.4 HTML String Replacement
- **File:** `agents/ui/components/enhanced_chat.py`
- **Line:** 854
- **Issue:** Brittle string replacement in HTML
- **Fix:** Use template or proper HTML manipulation

---

## Phase 3: Hardcoded Paths (Medium Priority) ✅ COMPLETE

Replace all hardcoded `d:/odibi` paths with config-based resolution:

| File | Line | Current | Replace With |
|------|------|---------|--------------|
| `agents/core/memory.py` | 156 | `"d:/odibi/.odibi/memories"` | `config.odibi_root` or env var |
| `agents/ui/components/explorer_panel.py` | 37-38 | `"D:/explorer_sandboxes"`, `"D:/odibi"` | Config defaults |
| `agents/ui/components/cycle_panel.py` | 1801, 1936 | `"d:/odibi"` | `runner.config.odibi_root` |
| `agents/ui/components/escalation_panel.py` | 213, 359, 500 | `"d:/odibi"` | Config |
| `agents/ui/components/issue_discovery_panel.py` | 149 | `"d:/odibi"` | Config |

**Recommended approach:** Create a `get_odibi_root()` utility function that checks:
1. Config value
2. Environment variable `ODIBI_ROOT`
3. Fallback to relative path detection

---

## Phase 4: Incomplete Features (Lower Priority) ✅ COMPLETE

### 4.1 Branch Feature
- **File:** `agents/ui/components/enhanced_chat.py`
- **Lines:** 1166-1176
- **Decision needed:** Complete implementation or remove entirely
- **Recommendation:** Remove for now (marked as Phase 9.A cleanup)

### 4.2 Validation Stubs
- **File:** `agents/core/controlled_improvement.py`
- **Issues:**
  - `validate_improvement()` (lines 1115-1130) always returns `True`
  - `check_regressions()` (lines 1132-1154) returns empty failures
- **Fix:** Implement actual pipeline execution and golden project checks

### 4.3 Discovery-Escalation Link
- **File:** `agents/ui/components/escalation_panel.py`
- **Lines:** 627-651
- **Issue:** `link_discovery_to_escalation` incomplete
- **Fix:** Add "Use for Improvement" button to discovery panel that populates escalation fields

### 4.4 Explorer State Isolation
- **File:** `agents/ui/components/explorer_panel.py`
- **Lines:** 24-30
- **Issue:** Module-level `_explorer_state` shared across sessions
- **Fix:** Use Gradio `gr.State()` for session isolation

---

## Phase 5: Enhancements (Future)

1. Add progress streaming to Learning Session UI
2. Implement `verify_changes` tool (referenced but missing)
3. Add semantic search fallback when ChromaDB unavailable
4. Improve file undo history tracking

---

## Testing Checklist

After each phase, verify:
- [ ] `pytest agents/core/tests/ -v` passes (687 tests)
- [ ] `ruff check agents/` has no errors
- [ ] UI launches: `from agents.ui import launch; launch()`
- [ ] Chat tool execution works
- [ ] Cycle execution works in Guided mode

---

## Files Changed Summary

| Phase | Files Modified |
|-------|----------------|
| 1 | `shell_tools.py`, `cycle_panel.py` |
| 2 | `enhanced_chat.py`, `escalation_panel.py`, `memories.py`, `conversation.py` |
| 3 | `memory.py`, `explorer_panel.py`, `cycle_panel.py`, `escalation_panel.py`, `issue_discovery_panel.py` |
| 4 | `enhanced_chat.py`, `controlled_improvement.py`, `escalation_panel.py`, `explorer_panel.py` |
