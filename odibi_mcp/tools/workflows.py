"""
Workflow execution engine for Odibi MCP (Phase 1: Core Engine).

Provides deterministic, step-by-step workflow execution with:
- State management (templating, path access)
- Basic step types: set, log, call
- Simple workflows for validation
"""

from typing import Any, Dict, List, Optional, Callable
from copy import deepcopy
import re
import time
import base64
import json

# ----------------------
# State helpers
# ----------------------


def _get_path(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Get nested value from dict using dot notation: 'params.pipeline_name'"""
    cur = d
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _set_path(d: Dict[str, Any], path: str, value: Any) -> None:
    """Set nested value in dict using dot notation."""
    parts = path.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


TEMPLATE_RE = re.compile(r"\{([^{}]+)\}")


def _apply_templates(value: Any, state: Dict[str, Any]) -> Any:
    """Replace {path.to.value} with state value."""
    if isinstance(value, str):

        def repl(match):
            key = match.group(1).strip()
            v = _get_path(state, key, None)
            return str(v) if v is not None else ""

        return TEMPLATE_RE.sub(repl, value)
    if isinstance(value, dict):
        return {k: _apply_templates(v, state) for k, v in value.items()}
    if isinstance(value, list):
        return [_apply_templates(x, state) for x in value]
    return value


# ----------------------
# Tool registry (Phase 2: + construction tools)
# ----------------------
# ruff: noqa: E402

from odibi_mcp.tools.execution import test_pipeline, validate_yaml_runnable
from odibi_mcp.tools.validation import validate_pipeline as validate_pipeline_enhanced
from odibi_mcp.tools.construction import list_patterns, apply_pattern_template, list_transformers
from odibi_mcp.tools.smart import map_environment, profile_source, profile_folder
from odibi_mcp.tools.diagnose import diagnose
from odibi_mcp.tools.story import story_read, node_sample, node_failed_rows, lineage_graph

TOOL_REGISTRY: Dict[str, Callable[..., Dict[str, Any]]] = {
    # Validation
    "test_pipeline": test_pipeline,
    "validate_yaml_runnable": validate_yaml_runnable,
    "validate_pipeline_enhanced": validate_pipeline_enhanced,
    # Construction
    "list_patterns": list_patterns,
    "apply_pattern_template": apply_pattern_template,
    "list_transformers": list_transformers,
    # Discovery
    "map_environment": map_environment,
    "profile_source": profile_source,
    "profile_folder": profile_folder,
    # Diagnostics
    "diagnose": diagnose,
    # Story/Inspection (Phase 3)
    "story_read": story_read,
    "node_sample": node_sample,
    "node_failed_rows": node_failed_rows,
    "lineage_graph": lineage_graph,
}


def _call_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Call a registered tool with args."""
    if name not in TOOL_REGISTRY:
        return {"error": {"code": "UNKNOWN_TOOL", "message": f"Tool '{name}' not registered"}}
    try:
        fn = TOOL_REGISTRY[name]
        result = fn(**(args or {}))
        return _to_json(result)
    except Exception as e:
        return {"error": {"code": "EXCEPTION", "message": str(e)}}


def _to_json(obj: Any) -> Any:
    """Convert to JSON-serializable format."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _to_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json(v) for v in obj]
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)


# ----------------------
# Conditions (for branch steps)
# ----------------------


def _check_condition(state: Dict[str, Any], cond: Dict[str, Any]) -> bool:
    """Check if condition is true. Supports: equals, not_equals, in, gt, gte, lt, lte, exists."""
    path = cond.get("path")
    val = _get_path(state, path, None) if path else None

    if "equals" in cond:
        return val == cond["equals"]
    if "not_equals" in cond:
        return val != cond["not_equals"]
    if "in" in cond:
        return val in cond["in"]
    if "gt" in cond:
        return val is not None and val > cond["gt"]
    if "gte" in cond:
        return val is not None and val >= cond["gte"]
    if "lt" in cond:
        return val is not None and val < cond["lt"]
    if "lte" in cond:
        return val is not None and val <= cond["lte"]
    if "exists" in cond:
        return (_get_path(state, path, None) is not None) is bool(cond["exists"])
    return False


# ----------------------
# Execution engine
# ----------------------


class Engine:
    """Execute workflow steps deterministically."""

    def __init__(
        self,
        workflow: Dict[str, Any],
        params: Dict[str, Any],
        resume: Optional[Dict[str, Any]] = None,
    ):
        self.workflow = deepcopy(workflow)
        self.state: Dict[str, Any] = {
            "params": deepcopy(params or {}),
            "results": {},
            "context": {"attempts": {}},  # Track loop attempts
        }
        self.events: List[Dict[str, Any]] = []
        self.ip = 0  # instruction pointer
        self.labels = self._index_labels(self.workflow.get("steps", []))

        # Resume from saved state
        if resume:
            self.state = resume.get("state", self.state)
            self.ip = resume.get("ip", 0)
            self.events = resume.get("events", [])

    def _event(self, type_: str, **kwargs):
        """Record an event."""
        e = {"ts": time.time(), "type": type_}
        e.update(kwargs)
        self.events.append(e)

    def _index_labels(self, steps: List[Dict[str, Any]]) -> Dict[str, int]:
        """Index all labels for goto jumps."""
        idx = {}
        for i, s in enumerate(steps):
            if s.get("label") and s.get("type") != "goto":
                idx[s["label"]] = i
        return idx

    def _goto(self, label: str):
        """Jump to a label."""
        if label not in self.labels:
            self._event("error", message=f"GOTO label not found: {label}")
            return
        self.ip = self.labels[label]

    def _progress(self) -> float:
        """Calculate progress percentage."""
        total = len(self.workflow.get("steps", [])) or 1
        return round(min(100.0, (self.ip / total) * 100.0), 1)

    def step(self) -> Dict[str, Any]:
        """Execute one step."""
        steps = self.workflow.get("steps", [])
        if self.ip >= len(steps):
            return {"done": True}

        step = steps[self.ip]
        stype = step.get("type")
        self._event("step_start", index=self.ip, type=stype)

        # SET: Set state values
        if stype == "set":
            values = _apply_templates(step.get("values", {}), self.state)
            for path, val in values.items():
                _set_path(self.state, path, val)
            self._event("set", values=values)
            self.ip += 1
            return {"ok": True}

        # LOG: Log a message
        if stype == "log":
            msg = _apply_templates(step.get("message", ""), self.state)
            self._event("log", message=msg)
            self.ip += 1
            return {"ok": True}

        # CALL: Call a tool
        if stype == "call":
            tool = step.get("tool")
            args = _apply_templates(step.get("args", {}), self.state)

            # Filter out empty string values (from missing template vars)
            args = {k: v for k, v in args.items() if v != "" and v is not None}

            # Retry configuration
            retry_config = step.get("retry", {})
            max_attempts = retry_config.get("max_attempts", 1)
            backoff = retry_config.get("backoff", 1.0)
            retry_on_codes = retry_config.get("retry_on_codes", [])
            retry_on_exception = retry_config.get("retry_on_exception", False)
            on_error = step.get("on_error", {})

            result = None
            last_error = None

            for attempt in range(max_attempts):
                if attempt > 0:
                    wait_time = backoff**attempt
                    self._event("retry_wait", tool=tool, attempt=attempt + 1, wait_time=wait_time)
                    time.sleep(wait_time)

                self._event("tool_call", tool=tool, args=args, attempt=attempt + 1)

                try:
                    result = _call_tool(tool, args)

                    err = (isinstance(result, dict) and result.get("error")) or None
                    if err:
                        error_code = result.get("code") if isinstance(result, dict) else None
                        should_retry = (error_code and error_code in retry_on_codes) or (
                            retry_on_exception and attempt < max_attempts - 1
                        )

                        if should_retry:
                            self._event(
                                "tool_error_retry",
                                tool=tool,
                                error=err,
                                code=error_code,
                                attempt=attempt + 1,
                            )
                            last_error = err
                            continue
                        else:
                            self._event("tool_error", tool=tool, error=err, code=error_code)
                            last_error = err
                            break
                    else:
                        self._event("tool_result", tool=tool, attempt=attempt + 1)
                        break

                except Exception as e:
                    should_retry = retry_on_exception and attempt < max_attempts - 1
                    if should_retry:
                        self._event(
                            "tool_exception_retry", tool=tool, exception=str(e), attempt=attempt + 1
                        )
                        last_error = str(e)
                        continue
                    else:
                        self._event("tool_exception", tool=tool, exception=str(e))
                        last_error = str(e)
                        result = {"error": str(e)}
                        break

            # Handle final error state
            if last_error and on_error.get("goto"):
                self._goto(on_error["goto"])
                return {"ok": True}

            # Assign result to state
            if step.get("assign"):
                _set_path(self.state, step["assign"], result)

            self.ip += 1
            return {"ok": True}

        # GOTO: Jump to label
        if stype == "goto":
            label = step.get("label")
            if label:
                self._goto(label)
                self._event("goto", label=label)
                return {"ok": True}
            self.ip += 1
            return {"ok": True}

        # BRANCH: Conditional branching
        if stype == "branch":
            for case in step.get("cases", []):
                when = case.get("when", {})
                if _check_condition(self.state, when):
                    self._event("branch_taken", when=when, goto=case.get("goto"))
                    self._goto(case.get("goto"))
                    return {"ok": True}
            default = step.get("default")
            if default:
                self._event("branch_default", goto=default)
                self._goto(default)
                return {"ok": True}
            self.ip += 1
            return {"ok": True}

        # REQUEST_INPUT: Pause for user input
        if stype == "request_input":
            fields = step.get("fields", [])
            missing = False
            prompts = []

            for f in fields:
                path = f["path"]
                current = _get_path(self.state, path, None)
                if current is None:
                    missing = True
                    # Only add to prompts if actually missing
                    prompts.append(
                        {
                            "path": path,
                            "prompt": f.get("prompt", path),
                            "hint": f.get("hint"),
                            "default": f.get("default"),
                        }
                    )

            if missing:
                # Pause workflow - return resume token
                self._event("awaiting_input", prompts=prompts)
                return {"pause": {"prompts": prompts}}

            # All fields present, continue
            self.ip += 1
            return {"ok": True}

        # LOOP: Execute body until condition is true or max_attempts reached
        if stype == "loop":
            label = step.get("label", f"loop_{self.ip}")
            max_attempts = step.get("max_attempts", 10)
            until = step.get("until", {})
            body = step.get("body", [])
            on_exhausted = step.get("on_exhausted")

            # Track attempts and injected state
            attempts_key = f"loop_attempts.{label}"
            injected_key = f"loop_injected.{label}"
            attempts = _get_path(self.state, f"context.{attempts_key}", 0)
            injected = _get_path(self.state, f"context.{injected_key}", False)

            # Check if condition already met
            if _check_condition(self.state, until):
                self._event("loop_complete", label=label, attempts=attempts, reason="condition_met")
                # Clean up injected steps if any
                if injected:
                    steps = self.workflow.get("steps", [])
                    remove_until = self.ip + len(body) + 1  # Index of injected trailing goto
                    self.workflow["steps"] = steps[: self.ip + 1] + steps[remove_until + 1 :]
                    self.labels = self._index_labels(self.workflow.get("steps", []))
                    _set_path(self.state, f"context.{injected_key}", False)
                self.ip += 1
                return {"ok": True}

            # Check if exhausted
            if attempts >= max_attempts:
                self._event("loop_exhausted", label=label, attempts=attempts)
                # Clean up injected steps
                if injected:
                    steps = self.workflow.get("steps", [])
                    remove_until = self.ip + len(body) + 1
                    self.workflow["steps"] = steps[: self.ip + 1] + steps[remove_until + 1 :]
                    self.labels = self._index_labels(self.workflow.get("steps", []))
                    _set_path(self.state, f"context.{injected_key}", False)
                if on_exhausted:
                    self._goto(on_exhausted)
                else:
                    self.ip += 1
                return {"ok": True}

            # First time encountering this loop - inject body steps once
            if not injected:
                _set_path(self.state, f"context.{injected_key}", True)
                body_steps = deepcopy(body)
                body_steps.append({"type": "goto", "label": label})
                steps = self.workflow.get("steps", [])
                insert_pos = self.ip + 1
                self.workflow["steps"] = steps[:insert_pos] + body_steps + steps[insert_pos:]
                self.labels = self._index_labels(self.workflow.get("steps", []))

            # Increment attempt counter and continue to body
            _set_path(self.state, f"context.{attempts_key}", attempts + 1)
            self._event("loop_iteration", label=label, attempt=attempts + 1)
            self.ip += 1
            return {"ok": True}

        # Unknown step type
        self._event("error", message=f"Unknown step type: {stype}")
        self.ip += 1
        return {"ok": False}

    def run(self, max_steps: int = 1000) -> Dict[str, Any]:
        """Run workflow to completion or until paused for input."""
        started = time.time()

        while max_steps > 0:
            res = self.step()

            # Check if paused for input
            if "pause" in res:
                token = _encode_resume({"ip": self.ip, "state": self.state, "events": self.events})
                return {
                    "status": "AWAITING_INPUT",
                    "resume_token": token,
                    "progress": self._progress(),
                    "events": self.events,
                    "prompts": res["pause"]["prompts"],
                }

            if res.get("done"):
                break
            max_steps -= 1

        duration = round(time.time() - started, 3)

        # Extract common outputs - try multiple paths
        outputs = {}
        outputs["yaml"] = (
            _get_path(self.state, "results.yaml")
            or _get_path(self.state, "results.apply.yaml")
            or _get_path(self.state, "draft.yaml")
        )
        outputs["validation"] = _get_path(self.state, "results.validation") or _get_path(
            self.state, "results.apply"
        )
        outputs["apply_result"] = _get_path(self.state, "results.apply")

        return {
            "status": "COMPLETED",
            "progress": 100.0,
            "events": self.events,
            "outputs": outputs,
            "duration_s": duration,
            "state": self.state,
        }


def _encode_resume(obj: Dict[str, Any]) -> str:
    """Encode resume state as base64 token."""
    return base64.b64encode(json.dumps(obj).encode("utf-8")).decode("utf-8")


def _decode_resume(token: str) -> Dict[str, Any]:
    """Decode resume token."""
    return json.loads(base64.b64decode(token.encode("utf-8")).decode("utf-8"))


# ----------------------
# Workflow definitions (Phase 1: simple validation)
# ----------------------

WORKFLOWS: Dict[str, Dict[str, Any]] = {}


def _register(name: str, wf: Dict[str, Any]):
    """Register a workflow."""
    wf = deepcopy(wf)
    wf["name"] = name
    WORKFLOWS[name] = wf


# Simple validation workflow
_register(
    "validate_yaml_simple",
    {
        "description": "Validate a pipeline YAML (structure + dry-run)",
        "steps": [
            {"type": "log", "message": "Validating YAML..."},
            {
                "type": "call",
                "tool": "test_pipeline",
                "args": {"yaml_content": "{params.yaml}", "mode": "validate"},
                "assign": "results.validation",
            },
            {"type": "log", "message": "Validation complete. Valid={results.validation.valid}"},
            {"type": "set", "values": {"results.done": True}},
        ],
    },
)

# Build and validate workflow (Phase 2)
_register(
    "build_and_validate",
    {
        "description": "Build pipeline YAML from pattern and validate with dry-run",
        "steps": [
            {"type": "log", "message": "Starting build_and_validate"},
            {"type": "set", "values": {"params.__workflow_name": "build_and_validate"}},
            # Collect required params
            {
                "label": "collect_params",
                "type": "request_input",
                "fields": [
                    {
                        "path": "params.pattern",
                        "prompt": "Which pattern? (dimension/scd2/fact/merge/date_dimension)",
                    },
                    {"path": "params.pipeline_name", "prompt": "Pipeline name?"},
                    {"path": "params.source_connection", "prompt": "Source connection?"},
                    {"path": "params.target_connection", "prompt": "Target connection?"},
                    {"path": "params.target_path", "prompt": "Target path?"},
                    {"path": "params.source_table", "prompt": "Source table/file?"},
                ],
            },
            # Check pattern-specific params
            {
                "type": "branch",
                "cases": [
                    {
                        "when": {"path": "params.pattern", "equals": "dimension"},
                        "goto": "need_dimension_params",
                    },
                    {
                        "when": {"path": "params.pattern", "equals": "scd2"},
                        "goto": "need_scd2_params",
                    },
                ],
                "default": "generate_yaml",
            },
            {
                "label": "need_dimension_params",
                "type": "request_input",
                "fields": [
                    {"path": "params.natural_key", "prompt": "Natural key column?"},
                    {"path": "params.surrogate_key", "prompt": "Surrogate key column name?"},
                ],
            },
            {"type": "goto", "label": "generate_yaml"},
            {
                "label": "need_scd2_params",
                "type": "request_input",
                "fields": [
                    {"path": "params.keys", "prompt": "Business key columns (list)?"},
                    {"path": "params.tracked_columns", "prompt": "Tracked columns (list)?"},
                ],
            },
            # Generate YAML
            {
                "label": "generate_yaml",
                "type": "call",
                "tool": "apply_pattern_template",
                "args": {
                    "pattern": "{params.pattern}",
                    "pipeline_name": "{params.pipeline_name}",
                    "source_connection": "{params.source_connection}",
                    "target_connection": "{params.target_connection}",
                    "target_path": "{params.target_path}",
                    "source_table": "{params.source_table}",
                    "source_format": "{params.source_format}",
                    "target_format": "{params.target_format}",
                    "keys": "{params.keys}",
                    "tracked_columns": "{params.tracked_columns}",
                    "natural_key": "{params.natural_key}",
                    "surrogate_key": "{params.surrogate_key}",
                },
                "assign": "results.apply",
            },
            # Check if generation succeeded
            {
                "type": "branch",
                "cases": [
                    {"when": {"path": "results.apply.valid", "equals": True}, "goto": "test_yaml"},
                ],
                "default": "generation_failed",
            },
            {
                "label": "generation_failed",
                "type": "log",
                "message": "YAML generation failed. Errors: {results.apply.errors}",
            },
            {"type": "set", "values": {"results.done": True}},
            {"type": "goto", "label": "end"},
            # Test the generated YAML
            {
                "label": "test_yaml",
                "type": "set",
                "values": {"results.yaml": "{results.apply.yaml}"},
            },
            {
                "type": "call",
                "tool": "test_pipeline",
                "args": {"yaml_content": "{results.yaml}", "mode": "dry-run"},
                "assign": "results.validation",
            },
            {
                "type": "log",
                "message": "Validation result: {results.validation.valid}",
            },
            {"label": "end", "type": "set", "values": {"results.done": True}},
        ],
    },
)

# Debug pipeline workflow
_register(
    "debug_pipeline",
    {
        "description": "Systematic pipeline troubleshooting with diagnostics",
        "steps": [
            {"type": "log", "message": "Starting debug_pipeline"},
            {
                "type": "request_input",
                "fields": [
                    {"path": "params.yaml", "prompt": "Pipeline YAML to debug?"},
                ],
            },
            {
                "type": "call",
                "tool": "validate_yaml_runnable",
                "args": {"yaml_content": "{params.yaml}"},
                "assign": "results.quick",
            },
            {
                "type": "branch",
                "cases": [
                    {"when": {"path": "results.quick.valid", "equals": True}, "goto": "looks_ok"},
                ],
                "default": "deep_validate",
            },
            {
                "label": "deep_validate",
                "type": "call",
                "tool": "validate_pipeline_enhanced",
                "args": {"yaml_content": "{params.yaml}", "check_connections": True},
                "assign": "results.enhanced",
            },
            {
                "type": "log",
                "message": "Enhanced validation complete. See results.enhanced for details.",
            },
            {"type": "goto", "label": "end"},
            {
                "label": "looks_ok",
                "type": "log",
                "message": "YAML structure looks OK. Use test_pipeline for execution plan.",
            },
            {"label": "end", "type": "set", "values": {"results.done": True}},
        ],
    },
)

# Iterate until valid workflow (demonstrates loop + retry)
_register(
    "iterate_until_valid",
    {
        "description": "Build and validate pipeline, iterating until valid or max attempts",
        "steps": [
            {"type": "log", "message": "Starting iterate_until_valid workflow"},
            {"type": "set", "values": {"results.valid": False}},
            # Collect base parameters
            {
                "type": "request_input",
                "fields": [
                    {"path": "params.pattern", "prompt": "Pattern (dimension/scd2/fact)?"},
                    {"path": "params.name", "prompt": "Pipeline name?"},
                    {"path": "params.source", "prompt": "Source path/table?"},
                ],
            },
            # Validation loop with retry
            {
                "type": "loop",
                "label": "validation_loop",
                "max_attempts": 3,
                "until": {"path": "results.valid", "equals": True},
                "body": [
                    {
                        "type": "log",
                        "message": "Validation attempt {context.loop_attempts.validation_loop}",
                    },
                    {
                        "type": "call",
                        "tool": "apply_pattern_template",
                        "args": {
                            "pattern": "{params.pattern}",
                            "name": "{params.name}",
                            "source_path": "{params.source}",
                        },
                        "retry": {"max_attempts": 2, "backoff": 1.0, "retry_on_exception": True},
                        "assign": "results.yaml",
                        "on_error": {"goto": "build_failed"},
                    },
                    {
                        "type": "call",
                        "tool": "test_pipeline",
                        "args": {"yaml_content": "{results.yaml}", "mode": "validate"},
                        "assign": "results.test",
                    },
                    {
                        "type": "branch",
                        "cases": [
                            {
                                "when": {"path": "results.test.valid", "equals": True},
                                "goto": "success",
                            },
                        ],
                    },
                    {"type": "log", "message": "Validation failed, requesting adjustments"},
                    {
                        "type": "request_input",
                        "fields": [
                            {
                                "path": "params.source",
                                "prompt": "Adjust source path?",
                                "hint": "Current: {params.source}",
                            },
                        ],
                    },
                    {"type": "goto", "label": "continue_iter"},
                    {"type": "set", "label": "success", "values": {"results.valid": True}},
                    {"type": "log", "message": "Pipeline validated!"},
                    {"type": "goto", "label": "continue_iter"},
                    {
                        "type": "set",
                        "label": "build_failed",
                        "values": {"results.error": "Build failed"},
                    },
                    {"type": "log", "label": "continue_iter", "message": "Iteration complete"},
                ],
                "on_exhausted": "max_attempts",
            },
            {"type": "log", "message": "Success! Pipeline is valid."},
            {"type": "goto", "label": "finish"},
            {
                "type": "log",
                "label": "max_attempts",
                "message": "Max attempts reached - validation failed",
            },
            {"type": "set", "label": "finish", "values": {"results.done": True}},
        ],
    },
)

# Phase 3: Story workflows
_register(
    "inspect_pipeline_run",
    {
        "description": "Inspect pipeline run results, samples, and failures",
        "steps": [
            {"type": "log", "message": "Starting inspect_pipeline_run"},
            {
                "type": "request_input",
                "fields": [
                    {"path": "params.pipeline", "prompt": "Pipeline name?"},
                ],
            },
            {
                "type": "call",
                "tool": "story_read",
                "args": {"pipeline": "{params.pipeline}"},
                "retry": {"max_attempts": 2, "backoff": 0.5, "retry_on_exception": True},
                "assign": "results.story",
            },
            {
                "type": "log",
                "message": "Pipeline: {params.pipeline}, Status: {results.story.status}, Nodes: {results.story.node_count}",
            },
            {
                "type": "branch",
                "cases": [
                    {
                        "when": {"path": "results.story.status", "equals": "not_found"},
                        "goto": "story_not_found",
                    },
                    {
                        "when": {"path": "results.story.failure_count", "gt": 0},
                        "goto": "check_failures",
                    },
                ],
                "default": "sample_outputs",
            },
            {
                "type": "set",
                "label": "story_not_found",
                "values": {"results.error": "Story not found"},
            },
            {"type": "log", "message": "No story found for {params.pipeline}"},
            {"type": "goto", "label": "end"},
            {
                "label": "check_failures",
                "type": "log",
                "message": "Found {results.story.failure_count} failed nodes",
            },
            {
                "type": "request_input",
                "fields": [
                    {
                        "path": "params.inspect_failed_node",
                        "prompt": "Which failed node to inspect?",
                        "hint": "Available: {results.story.nodes}",
                    },
                ],
            },
            {
                "type": "call",
                "tool": "node_failed_rows",
                "args": {
                    "pipeline": "{params.pipeline}",
                    "node": "{params.inspect_failed_node}",
                    "limit": 10,
                },
                "assign": "results.failed_rows",
            },
            {
                "type": "log",
                "message": "Failed rows: {results.failed_rows.row_count}, Validations: {results.failed_rows.validations}",
            },
            {"type": "goto", "label": "sample_outputs"},
            {
                "label": "sample_outputs",
                "type": "request_input",
                "fields": [
                    {
                        "path": "params.sample_node",
                        "prompt": "Which node to sample?",
                        "hint": "Available: {results.story.nodes}",
                    },
                ],
            },
            {
                "type": "call",
                "tool": "node_sample",
                "args": {
                    "pipeline": "{params.pipeline}",
                    "node": "{params.sample_node}",
                    "limit": 10,
                },
                "assign": "results.sample",
            },
            {
                "type": "log",
                "message": "Sample rows: {results.sample.row_count}, Columns: {results.sample.columns}",
            },
            {"type": "set", "label": "end", "values": {"results.done": True}},
        ],
    },
)

_register(
    "debug_failed_run",
    {
        "description": "Debug failed pipeline run with detailed failure analysis",
        "steps": [
            {"type": "log", "message": "Starting debug_failed_run"},
            {
                "type": "request_input",
                "fields": [
                    {"path": "params.pipeline", "prompt": "Pipeline name?"},
                ],
            },
            {
                "type": "call",
                "tool": "story_read",
                "args": {"pipeline": "{params.pipeline}"},
                "assign": "results.story",
                "on_error": {"goto": "story_error"},
            },
            {
                "type": "branch",
                "cases": [
                    {
                        "when": {"path": "results.story.status", "equals": "not_found"},
                        "goto": "story_not_found",
                    },
                    {
                        "when": {"path": "results.story.failure_count", "equals": 0},
                        "goto": "no_failures",
                    },
                ],
            },
            {"type": "log", "message": "Found {results.story.failure_count} failed nodes"},
            {
                "type": "loop",
                "label": "analyze_failures",
                "max_attempts": 5,
                "until": {"path": "results.analysis_complete", "equals": True},
                "body": [
                    {
                        "type": "request_input",
                        "fields": [
                            {
                                "path": "params.failed_node",
                                "prompt": "Which failed node to analyze?",
                                "hint": "{results.story.nodes}",
                            },
                        ],
                    },
                    {
                        "type": "call",
                        "tool": "node_failed_rows",
                        "args": {
                            "pipeline": "{params.pipeline}",
                            "node": "{params.failed_node}",
                            "limit": 50,
                        },
                        "assign": "results.failures",
                    },
                    {
                        "type": "log",
                        "message": "Failed {results.failures.row_count} rows - Validations: {results.failures.validations}",
                    },
                    {
                        "type": "request_input",
                        "fields": [
                            {
                                "path": "params.continue_analysis",
                                "prompt": "Analyze another node? (yes/no)",
                            },
                        ],
                    },
                    {
                        "type": "branch",
                        "cases": [
                            {
                                "when": {
                                    "path": "params.continue_analysis",
                                    "in": ["no", "n", "false"],
                                },
                                "goto": "mark_complete",
                            },
                        ],
                    },
                    {"type": "goto", "label": "continue_loop"},
                    {
                        "type": "set",
                        "label": "mark_complete",
                        "values": {"results.analysis_complete": True},
                    },
                    {"type": "log", "label": "continue_loop", "message": "Continuing analysis"},
                ],
            },
            {"type": "log", "message": "Analysis complete"},
            {"type": "goto", "label": "end"},
            {
                "type": "set",
                "label": "story_not_found",
                "values": {"results.error": "Story not found"},
            },
            {"type": "goto", "label": "end"},
            {
                "type": "set",
                "label": "no_failures",
                "values": {"results.message": "No failures found"},
            },
            {"type": "goto", "label": "end"},
            {
                "type": "set",
                "label": "story_error",
                "values": {"results.error": "Failed to read story"},
            },
            {"type": "set", "label": "end", "values": {"results.done": True}},
        ],
    },
)


# ----------------------
# Public API
# ----------------------


def list_workflows() -> Dict[str, Any]:
    """List available workflows."""
    return {
        "workflows": [
            {"name": n, "description": wf.get("description")} for n, wf in WORKFLOWS.items()
        ]
    }


def get_workflow(name: str) -> Dict[str, Any]:
    """Get workflow definition."""
    if name not in WORKFLOWS:
        return {
            "error": {"code": "UNKNOWN_WORKFLOW", "message": f"{name} not found"},
            "available": list(WORKFLOWS.keys()),
        }
    return {"workflow": deepcopy(WORKFLOWS[name])}


def run_workflow(workflow_name: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Execute a workflow."""
    if workflow_name not in WORKFLOWS:
        return {
            "error": {"code": "UNKNOWN_WORKFLOW", "message": f"{workflow_name} not found"},
            "available": list(WORKFLOWS.keys()),
        }

    engine = Engine(WORKFLOWS[workflow_name], params or {})
    return engine.run()


def resume_workflow(resume_token: str, inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Resume a paused workflow with user inputs."""
    try:
        data = _decode_resume(resume_token)
    except Exception as e:
        return {"error": {"code": "INVALID_TOKEN", "message": str(e)}}

    # Get workflow name from state if stored
    workflow_name = _get_path(data, "state.params.__workflow_name")
    if not workflow_name or workflow_name not in WORKFLOWS:
        # Fallback: use first workflow (should store name in future)
        workflow_name = list(WORKFLOWS.keys())[0] if WORKFLOWS else None
        if not workflow_name:
            return {
                "error": {
                    "code": "NO_WORKFLOW",
                    "message": "Cannot resume - no workflows available",
                }
            }

    wf = WORKFLOWS[workflow_name]

    # Inject user inputs into restored state
    state = data.get("state", {})
    for k, v in (inputs or {}).items():
        _set_path(state, k, v)

    # Don't advance IP - let the request_input step re-check and advance itself
    # It will see all values are present and continue
    ip = data.get("ip", 0)

    # Create engine with resumed state
    resume_data = {
        "ip": ip,  # Stay at the request_input step to re-evaluate
        "state": state,
        "events": data.get("events", []),
    }
    engine = Engine(wf, {}, resume=resume_data)
    return engine.run()
