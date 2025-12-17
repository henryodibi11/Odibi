# E4 Implementation Prompt

Copy this prompt into a new Amp thread to implement Phase E4.

---

## Prompt

Implement Phase E4 (Stability Sprint) from the Odibi Agent Improvement Roadmap.

### Context

Read `d:/odibi/agents/ROADMAP.md` for full context. Phases E0-E3 are complete.

The improvement infrastructure exists in `d:/odibi/agents/improve/`:
- `environment.py` - ImprovementEnvironment (sandbox/snapshot management)
- `runner.py` - OdibiPipelineRunner (runs pipelines/tests/lint)
- `results.py` - CycleResult, GateCheckResult, etc.
- `campaign.py` - CampaignRunner (orchestration, but NO LLM integration yet)
- `memory.py` - CampaignMemory (JSONL persistence)
- `status.py` - StatusTracker (status.json updates)
- `config.py` - CampaignConfig

The agent UI exists in `d:/odibi/agents/ui/`:
- `app.py` - Main Gradio application
- `llm_client.py` - Provider-agnostic LLMClient (OpenAI, Azure, Ollama)
- `components/` - chat.py, settings.py, memories.py

The existing `ExecutionGateway` is in `d:/odibi/agents/core/execution.py`.

### Your Task: Complete E4

#### E4.1: Create ImprovementBrain (`agents/improve/brain.py`)

```python
class ImprovementBrain:
    """LLM-powered code improvement within sandboxes."""

    def __init__(
        self,
        llm_client: LLMClient,
        max_attempts_per_sandbox: int = 3,
    ):
        self._llm = llm_client
        self._max_attempts = max_attempts_per_sandbox

    def improve_sandbox(
        self,
        sandbox: SandboxInfo,
        gate_result: GateCheckResult,
        avoid_issues: list[str],
        campaign_goal: str,
        run_gates: Callable[[SandboxInfo], GateCheckResult],
    ) -> tuple[GateCheckResult, int]:
        """Try to fix gate failures using LLM.

        Inner loop:
        1. Build prompt from gate_result failures
        2. Call LLM with file tools (read, edit)
        3. LLM proposes and applies fixes
        4. Re-run gates
        5. Repeat until gates pass or max_attempts reached

        Returns: (final_gate_result, attempts_used)
        """
```

**Key requirements:**
- Use `agents/ui/llm_client.py` LLMClient for LLM calls
- Create tool definitions for file operations (read_file, edit_file, list_directory)
- Build prompts that include:
  - Gate failure details (test names, lint errors)
  - Known bad approaches to avoid (from memory)
  - Campaign goal
- Handle tool calls in a loop until LLM is done or gives up
- Low temperature (0.1-0.2) for determinism

#### E4.2: Integrate Brain into CampaignRunner

Update `agents/improve/campaign.py`:

```python
def run_cycle(self, cycle_num: int) -> CycleResult:
    sandbox = self._env.create_sandbox(cycle_id)
    try:
        gate_result = self.check_gates(sandbox)

        # NEW: If gates fail, try LLM improvement
        if not gate_result.all_passed and self._config.enable_llm_improvements:
            gate_result, attempts = self._brain.improve_sandbox(
                sandbox=sandbox,
                gate_result=gate_result,
                avoid_issues=self._memory.get_failed_approaches(),
                campaign_goal=self._config.goal,
                run_gates=lambda s: self.check_gates(s),
            )
            result.improvement_attempts = attempts

        if gate_result.all_passed:
            self._env.snapshot_master(cycle_id)
            self._env.promote_to_master(sandbox)
            result.mark_complete(promoted=True, learning=True, ...)
        else:
            result.mark_complete(promoted=False, ...)
    finally:
        self._env.destroy_sandbox(sandbox)
```

#### E4.3: Update CampaignConfig

Add to `agents/improve/config.py` CampaignConfig:

```python
enable_llm_improvements: bool = True
max_improvement_attempts_per_cycle: int = 3
llm_model: str = "gpt-4o-mini"
llm_endpoint: Optional[str] = None  # Uses env vars if None
llm_api_key: Optional[str] = None   # Uses env vars if None
```

#### E4.4: Update CLI

Add to `agents/improve/__main__.py`:

```bash
python -m agents.improve run --root D:/improve_odibi \
    --max-cycles 10 \
    --max-hours 4 \
    --llm-model gpt-4o-mini \
    --max-attempts 3 \
    --no-llm  # Optional flag to disable LLM improvements
```

#### E4.5: Create UI Panel (`agents/ui/components/campaigns.py`)

```python
class CampaignsPanel:
    """Campaign monitoring and control in Gradio UI."""

    def __init__(self, environment_root: Path):
        self._root = environment_root
        self._status_tracker = StatusTracker(environment_root / "status.json")

    def render(self) -> gr.Column:
        """Render:
        - Status card (campaign ID, status, elapsed, cycles)
        - Progress indicators
        - Recent cycles table
        - Start/stop buttons
        - Reports viewer (markdown)
        """
```

#### E4.6: Integrate into UI App

Update `agents/ui/app.py` to add Campaigns tab:

```python
with gr.Tab("Campaigns"):
    campaigns = CampaignsPanel(environment_root=Path("D:/improve_odibi"))
    campaigns.render()
```

#### E4.7: Write Tests

Create `agents/improve/tests/test_brain.py`:
- `test_brain_attempts_fix_on_failure`
- `test_brain_stops_after_max_attempts`
- `test_brain_uses_avoid_issues`
- `test_brain_returns_passing_result_on_success`

### Verification

After implementation:

```bash
# Lint
python -m ruff check agents/improve/ agents/ui/

# Tests
python -m pytest agents/improve/tests/ -v

# Test campaign with LLM
python -m agents.improve run --root D:/improve_odibi --max-cycles 1 --llm-model gpt-4o-mini

# Launch UI
python -c "from agents.ui import launch; launch()"
```

### Reference Files

Read these to understand existing patterns:
- `agents/improve/campaign.py` - Current CampaignRunner
- `agents/ui/llm_client.py` - LLMClient with tool support
- `agents/ui/components/chat.py` - Example Gradio component
- `agents/core/execution.py` - ExecutionGateway (if using)
- `agents/ui/tools/file_tools.py` - Existing file tool implementations

### Environment

The improvement environment is initialized at `D:/improve_odibi/` with:
- `master/` - Cloned from D:/odibi (full repo)
- `sandboxes/` - Temporary experiment clones
- `memory/` - JSONL lesson files
- `status.json` - Campaign status
- `reports/` - Markdown reports

### Key Decisions Already Made

1. **Use LLMClient from agents/ui/** - Provider-agnostic, already tested
2. **Inner improvement loop** - Try up to 3 fixes per sandbox before giving up
3. **CLI-first, UI-second** - Core logic in improve/, UI just displays status
4. **Bounded attempts** - Prevent infinite loops with max_attempts
5. **Low temperature** - 0.1-0.2 for deterministic overnight runs

Return a summary of what was implemented and any issues encountered.
