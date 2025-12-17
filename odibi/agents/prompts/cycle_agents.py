"""Cycle-based agents for the Odibi AI Agent Suite.

These agents are used in the Scheduled Assistant mode to perform
bounded, deterministic cycles of work. Each agent has specific
permissions and responsibilities.

Agents:
- EnvironmentAgent: Validates WSL, Java, Spark, Python env
- UserAgent: Uses Odibi like a human engineer
- ObserverAgent: Reads logs and classifies pain points
- ImprovementAgent: Proposes source code modifications
- ReviewerAgent: Approves or rejects improvements
- RegressionGuardAgent: Re-runs golden projects
- ProjectAgent: Proposes projects based on coverage
- CurriculumAgent: Orders projects by difficulty
- ConvergenceAgent: Detects learning plateau
- ProductObserverAgent: Captures product signals
- UXFrictionAgent: Identifies unnecessary friction
"""

from typing import Optional

from odibi.agents.core.agent_base import (
    AgentContext,
    AgentResponse,
    AgentRole,
    OdibiAgent,
)
from odibi.agents.core.azure_client import AzureConfig


def _safe_truncate(value: Optional[str], limit: int = 2000) -> str:
    """Safely truncate a string value that may be None.

    Args:
        value: String value or None
        limit: Maximum characters to return

    Returns:
        Truncated string, or empty string if value is None
    """
    return (value or "")[:limit]


def _parse_env_check_output(stdout: str) -> dict:
    """Parse the output of env-check to determine component status.

    Args:
        stdout: Raw stdout from env-check command

    Returns:
        Dict with component status and issues list
    """
    result = {
        "wsl_ok": False,
        "java_ok": False,
        "spark_ok": False,
        "python_ok": False,
        "odibi_ok": False,
        "issues": [],
        "warnings": [],
    }

    if not stdout:
        result["issues"].append("No output from environment check")
        return result

    lines = stdout.lower()

    def _get_section(text: str, header: str) -> str:
        """Extract section content after a header until next section or end."""
        if header not in text:
            return ""
        after_header = text.split(header, 1)[1]
        next_sections = [
            "wsl:",
            "java:",
            "spark:",
            "python venv:",
            "python:",
            "odibi:",
            "resources:",
            "===",
        ]
        end_pos = len(after_header)
        for ns in next_sections:
            if ns != header and ns in after_header:
                pos = after_header.find(ns)
                if pos > 0 and pos < end_pos:
                    end_pos = pos
        return after_header[:end_pos]

    wsl_section = _get_section(lines, "wsl:")
    if (
        "status: ok" in wsl_section
        or "wsl detected" in wsl_section
        or "native linux" in wsl_section
    ):
        result["wsl_ok"] = True
    elif "issue" in wsl_section:
        result["issues"].append("WSL not available")

    java_section = _get_section(lines, "java:")
    if "status: ok" in java_section:
        result["java_ok"] = True
    elif "issue" in java_section:
        result["issues"].append("Java not installed - run: sudo apt install openjdk-11-jdk")

    spark_section = _get_section(lines, "spark:")
    if "status: ok" in spark_section:
        result["spark_ok"] = True
    elif "issue" in spark_section or "not found" in spark_section:
        result["warnings"].append("Spark not installed - optional for observation mode")

    python_section = _get_section(lines, "python venv:")
    if not python_section:
        python_section = _get_section(lines, "python:")
    if "status: ok" in python_section:
        result["python_ok"] = True
    elif "issue" in python_section:
        result["issues"].append("Python venv not active")

    odibi_section = _get_section(lines, "odibi:")
    if "status: ok" in odibi_section:
        result["odibi_ok"] = True
    elif "issue" in odibi_section or "not importable" in odibi_section:
        result["issues"].append("Odibi not importable - run: pip install -e .")

    if "=== check complete ===" in lines and not result["issues"]:
        if not result["wsl_ok"]:
            result["wsl_ok"] = True

    return result


ENVIRONMENT_AGENT_PROMPT = """
# Environment Agent

## Purpose
Validate the execution environment to ensure all prerequisites are met.
You perform idempotent setup and validation - you do NOT edit framework code.

## Responsibilities
1. Check WSL (Ubuntu) is available and functioning
2. Verify Java installation (required for Spark)
3. Confirm Spark is installed and configured for local mode
4. Validate Python virtual environment is active
5. Ensure Odibi is importable
6. Check disk space and memory availability

## Execution Boundary
All checks run via run_task.sh in WSL. You MUST NOT:
- Control mouse or keyboard
- Click UI elements
- Run Windows-native commands
- Install global Windows software

## Response Format
Report environment status as structured output:
```
ENVIRONMENT STATUS:
- WSL: OK | ISSUE: <details>
- Java: OK | ISSUE: <details>
- Spark: OK | ISSUE: <details>
- Python venv: OK | ISSUE: <details>
- Odibi: OK | ISSUE: <details>
- Resources: OK | ISSUE: <details>

OVERALL: READY | NOT_READY
ISSUES: <count>
```

## Constraints
- Read-only operations only
- No framework modifications
- Be idempotent - running twice should give same results
"""


class EnvironmentAgent(OdibiAgent):
    """Validates the execution environment.

    Phase 6 Enforcement:
    - Returns ExecutionEvidence from real env-check command
    - Returns NO_EVIDENCE if gateway unavailable
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=ENVIRONMENT_AGENT_PROMPT,
            role=AgentRole.ENVIRONMENT,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        from datetime import datetime

        from odibi.agents.core.evidence import ExecutionEvidence

        gateway = context.metadata.get("execution_gateway")

        if not gateway:
            evidence = ExecutionEvidence.no_evidence(
                "No execution gateway available - cannot validate environment"
            )
            response_text = """ENVIRONMENT STATUS:
- WSL: UNKNOWN (no gateway)
- Java: UNKNOWN
- Spark: UNKNOWN
- Python venv: UNKNOWN
- Odibi: UNKNOWN
- Resources: UNKNOWN

OVERALL: NOT_READY
ISSUES: 1 (no execution gateway)
"""
            return AgentResponse(
                content=response_text,
                agent_role=self.role,
                confidence=0.3,
                metadata={
                    "environment_ready": False,
                    "execution_evidence": evidence,
                },
            )

        import sys

        from odibi.agents.core.execution import TaskDefinition

        context.emit_progress(
            "🔍 Checking environment...",
            "Validating WSL, Java, Spark, Python, Odibi",
        )

        started_at = datetime.now().isoformat()

        task = TaskDefinition(
            task_type="env-check",
            args=[],
            timeout_seconds=120,  # Increased from 60s for slower systems
            description="Validate execution environment",
        )

        def on_output(line: str, is_stderr: bool):
            prefix = "⚠️" if is_stderr else "📋"
            line_preview = line.strip()[:100]
            if line_preview:
                context.emit_progress(
                    f"{prefix} {line_preview}",
                    line.strip(),
                )
                print(f"  {prefix} {line_preview}")
                sys.stdout.flush()

        result = gateway.run_task_streaming(
            task,
            agent_permissions=self.permissions,
            agent_role=self.role,
            on_output=on_output,
        )
        finished_at = datetime.now().isoformat()

        evidence = result.to_evidence(started_at, finished_at)

        parsed_status = _parse_env_check_output(result.stdout)

        critical_ready = (
            parsed_status.get("wsl_ok", False)
            and parsed_status.get("python_ok", False)
            and parsed_status.get("odibi_ok", False)
        )

        all_ready = critical_ready and parsed_status.get("spark_ok", False)

        issues = parsed_status.get("issues", [])
        warnings = parsed_status.get("warnings", [])

        if result.exit_code == -2:
            issues.append("Environment check timed out - WSL may be slow to start")
            critical_ready = False

        status_icon = "✅" if all_ready else ("⚠️" if critical_ready else "❌")
        status_text = (
            "READY" if all_ready else ("READY (with warnings)" if critical_ready else "NOT_READY")
        )

        context.emit_progress(
            f"{status_icon} Environment check: {status_text}",
            f"Exit code: {result.exit_code}",
        )

        wsl_status = "OK" if parsed_status.get("wsl_ok") else "ISSUE"
        java_status = "OK" if parsed_status.get("java_ok") else "ISSUE"
        spark_status = "OK" if parsed_status.get("spark_ok") else "NOT INSTALLED (optional)"
        python_status = "OK" if parsed_status.get("python_ok") else "ISSUE"
        odibi_status = "OK" if parsed_status.get("odibi_ok") else "ISSUE"

        issues_text = "\n".join(f"  - {issue}" for issue in issues) if issues else "  None"
        warnings_text = "\n".join(f"  - {w}" for w in warnings) if warnings else "  None"

        response_text = f"""ENVIRONMENT STATUS:
- WSL: {wsl_status}
- Java: {java_status}
- Spark: {spark_status}
- Python venv: {python_status}
- Odibi: {odibi_status}

OVERALL: {status_text}
CRITICAL COMPONENTS: {"OK" if critical_ready else "NOT READY"}

Issues:
{issues_text}

Warnings:
{warnings_text}

Command: {result.command}
Exit Code: {result.exit_code}
Evidence Hash: {evidence.evidence_hash}
"""

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.95 if all_ready else (0.7 if critical_ready else 0.4),
            metadata={
                "environment_ready": critical_ready,
                "all_components_ready": all_ready,
                "execution_evidence": evidence,
                "parsed_status": parsed_status,
            },
        )


USER_AGENT_PROMPT = """
# User Agent

## Purpose
Execute real Odibi pipelines via ExecutionGateway and report ACTUAL results.
You MUST NOT fabricate execution outputs, data sources, or pipeline configurations.

## CRITICAL: Evidence-Based Execution (Phase 6)

You are REQUIRED to:
1. Use ExecutionGateway.run_pipeline() for all execution
2. Report ONLY what the gateway returns (stdout, stderr, exit_code)
3. Include ExecutionEvidence in your response metadata
4. Return NO_EVIDENCE status if execution cannot be performed

You are FORBIDDEN from:
- Inventing pipeline names (e.g., "user_activity_etl")
- Fabricating data sources (e.g., "hive://warehouse/customers")
- Making up execution results (e.g., "1,250,000 rows processed")
- Imagining errors or warnings not from real execution
- Creating synthetic YAML configurations that weren't executed

## What You Actually Do
1. Look for YAML configs in the workspace (context.odibi_root)
2. Run them via ExecutionGateway.run_pipeline()
3. Report the REAL stdout, stderr, exit_code
4. If no configs found or gateway unavailable → return NO_EVIDENCE

## Response Format for REAL Execution
```
EXECUTION REPORT:
Pipeline: <actual config file path>
Command: <actual command run>
Exit Code: <actual exit code>
Status: SUCCESS | FAILURE

Stdout:
<actual stdout from gateway>

Stderr:
<actual stderr from gateway>

Duration: <actual duration>s
Evidence Hash: <hash from ExecutionEvidence>
```

## Response Format for NO_EVIDENCE
```
EXECUTION REPORT:
Status: NO_EVIDENCE
Reason: <why execution could not be performed>

Possible reasons:
- No execution gateway available
- No pipeline configs found in workspace
- Permission denied for execution
- WSL not available
```

## Constraints
- MUST use ExecutionGateway - no fabrication
- MUST return ExecutionEvidence in metadata
- MUST report NO_EVIDENCE if unable to execute
- Cannot edit Odibi source code
- Cannot bypass the gateway
"""


class UserAgent(OdibiAgent):
    """Executes real Odibi pipelines via ExecutionGateway.

    Phase 6 Enforcement:
    - MUST use ExecutionGateway for all execution
    - MUST return ExecutionEvidence in metadata
    - MUST NOT fabricate execution results
    - Returns NO_EVIDENCE if execution cannot be performed

    Phase 9.F: Observation Integrity:
    - MUST detect pipeline failures by parsing output (NOT exit code alone)
    - MUST populate pipeline_results in ExecutionEvidence
    - Observer will use pipeline_results to emit issues
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=USER_AGENT_PROMPT,
            role=AgentRole.USER,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        import glob
        import os
        import sys
        from datetime import datetime

        from odibi.agents.core.evidence import ExecutionEvidence
        from odibi.agents.core.pipeline_status import create_pipeline_result_from_execution

        gateway = context.metadata.get("execution_gateway")
        cycle_config = context.metadata.get("cycle_config")

        if not gateway:
            evidence = ExecutionEvidence.no_evidence("No execution gateway available in context")
            return self._build_no_evidence_response(evidence)

        project_root = context.odibi_root
        if cycle_config and hasattr(cycle_config, "project_root"):
            project_root = cycle_config.project_root

        yaml_patterns = [
            os.path.join(project_root, "**", "odibi.yaml"),
            os.path.join(project_root, "**", "pipeline.yaml"),
            os.path.join(project_root, "**", "*.odibi.yaml"),
        ]

        config_files = []
        for pattern in yaml_patterns:
            config_files.extend(glob.glob(pattern, recursive=True))

        config_files = list(set(config_files))[:5]

        # Progress: Show discovered configs
        config_list = "\n".join(
            f"  {i}. {os.path.relpath(cfg, project_root)}" for i, cfg in enumerate(config_files, 1)
        )
        context.emit_progress(
            f"📂 Found {len(config_files)} pipeline config(s)",
            config_list,
        )
        print(f"\n  📂 Found {len(config_files)} pipeline config(s) in {project_root}")
        for i, cfg in enumerate(config_files, 1):
            rel_path = os.path.relpath(cfg, project_root)
            print(f"     {i}. {rel_path}")
        sys.stdout.flush()

        if not config_files:
            evidence = ExecutionEvidence.no_evidence(f"No pipeline configs found in {project_root}")
            return self._build_no_evidence_response(evidence)

        all_evidence = []
        report_lines = []

        for idx, config_path in enumerate(config_files, 1):
            rel_path = os.path.relpath(config_path, project_root)
            context.emit_progress(
                f"▶️ [{idx}/{len(config_files)}] Running: {rel_path}",
                "Starting pipeline execution...",
            )
            print(f"\n  ▶️  [{idx}/{len(config_files)}] Running: {rel_path}")
            sys.stdout.flush()

            started_at = datetime.now().isoformat()

            try:
                from odibi.agents.core.execution import TaskDefinition

                wsl_config_path = gateway._to_wsl_path(config_path)
                task = TaskDefinition(
                    task_type="pipeline",
                    args=["run", wsl_config_path],
                    timeout_seconds=300,
                    description=f"Run pipeline: {rel_path}",
                )

                output_lines = []

                def on_output(line: str, is_stderr: bool):
                    output_lines.append(("stderr" if is_stderr else "stdout", line))
                    prefix = "⚠️" if is_stderr else "📤"
                    line_preview = line.strip()[:100]
                    if line_preview:
                        context.emit_progress(
                            f"{prefix} {line_preview}",
                            line.strip(),
                        )
                        print(f"     {prefix} {line_preview}")
                        sys.stdout.flush()

                result = gateway.run_task_streaming(
                    task,
                    agent_permissions=self.permissions,
                    agent_role=self.role,
                    on_output=on_output,
                )
                finished_at = datetime.now().isoformat()

                evidence = result.to_evidence(started_at, finished_at)

                # Phase 9.F: Detect pipeline status by parsing output
                # Do NOT rely on exit code alone - validation failures may return exit 0
                pipeline_result = create_pipeline_result_from_execution(
                    pipeline_name=rel_path,
                    exit_code=result.exit_code,
                    stdout=result.stdout,
                    stderr=result.stderr,
                )
                evidence.add_pipeline_result(pipeline_result.to_dict())

                all_evidence.append(evidence)

                # Phase 9.F: Use parsed pipeline status, not just exit code
                status = pipeline_result.status.value
                is_success = pipeline_result.is_success()
                icon = "✅" if is_success else "❌"
                context.emit_progress(
                    f"{icon} {rel_path}: {status}",
                    f"Exit code: {result.exit_code}, Duration: {evidence.duration_seconds:.1f}s",
                )
                dur = evidence.duration_seconds
                print(
                    f"     {icon} {rel_path}: {status} — "
                    f"Exit code: {result.exit_code}, Duration: {dur:.1f}s"
                )
                sys.stdout.flush()

                report_lines.append("EXECUTION REPORT:")
                report_lines.append(f"Pipeline: {config_path}")
                report_lines.append(f"Command: {result.command}")
                report_lines.append(f"Exit Code: {result.exit_code}")
                report_lines.append(f"Pipeline Status: {status}")

                # Phase 9.F: Include failure details in report
                if not pipeline_result.is_success():
                    report_lines.append(f"Error Stage: {pipeline_result.error_stage}")
                    if pipeline_result.validation_errors:
                        report_lines.append("Validation Errors:")
                        for verr in pipeline_result.validation_errors:
                            report_lines.append(f"  - {verr.field_path}: {verr.message}")
                    if pipeline_result.runtime_error:
                        report_lines.append(f"Runtime Error: {pipeline_result.runtime_error}")

                report_lines.append("")
                report_lines.append("Stdout:")
                report_lines.append(_safe_truncate(result.stdout) or "(empty)")
                report_lines.append("")
                report_lines.append("Stderr:")
                report_lines.append(_safe_truncate(result.stderr) or "(empty)")
                report_lines.append("")
                report_lines.append(f"Duration: {evidence.duration_seconds:.2f}s")
                report_lines.append(f"Evidence Hash: {evidence.evidence_hash}")
                report_lines.append("")
                report_lines.append("---")
                report_lines.append("")

            except Exception as e:
                finished_at = datetime.now().isoformat()
                evidence = ExecutionEvidence.no_evidence(
                    f"Execution failed for {config_path}: {str(e)}"
                )
                all_evidence.append(evidence)

                print(f"     ⚠️  ERROR: {str(e)[:80]}")
                sys.stdout.flush()

                report_lines.append("EXECUTION REPORT:")
                report_lines.append(f"Pipeline: {config_path}")
                report_lines.append("Status: NO_EVIDENCE")
                report_lines.append(f"Reason: {str(e)}")
                report_lines.append("")
                report_lines.append("---")
                report_lines.append("")

        response_text = "\n".join(report_lines)

        # Phase 9.F: Create aggregated evidence with ALL pipeline results
        # The primary_evidence will contain all pipeline_results from all executions
        if all_evidence:
            primary_evidence = all_evidence[0]
            # Aggregate pipeline_results from all evidence objects
            for other_evidence in all_evidence[1:]:
                for pr in other_evidence.pipeline_results:
                    primary_evidence.add_pipeline_result(pr)
        else:
            primary_evidence = None

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.95,
            metadata={
                "mode": "evidence_based_execution",
                "execution_evidence": primary_evidence,
                "all_evidence": [e.to_dict() for e in all_evidence],
                "pipelines_executed": len(all_evidence),
                "fabrication_detected": False,
            },
        )

    def _build_no_evidence_response(self, evidence) -> AgentResponse:
        """Build response for NO_EVIDENCE case."""
        response_text = f"""EXECUTION REPORT:
Status: NO_EVIDENCE
Reason: {evidence.stderr}

No real execution was performed. This agent requires ExecutionGateway
to run actual pipelines. Fabricated results are not permitted.
"""
        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=1.0,
            metadata={
                "mode": "no_evidence",
                "execution_evidence": evidence,
                "pipelines_executed": 0,
                "fabrication_detected": False,
            },
        )


OBSERVER_AGENT_PROMPT = """
# Observer Agent

## Purpose
Read execution logs and classify ALL pain points, failures, and warnings.
You observe and report - you do NOT fix or suggest fixes.

## CRITICAL: Classification Rules

You MUST create an issue for each of the following patterns found in logs:

### Pipeline Status → Issue Type
| Log Pattern | Issue Type | Severity |
|-------------|------------|----------|
| Status: FAILURE | EXECUTION_FAILURE | HIGH |
| Status: PARTIAL | PARTIAL_EXECUTION | MEDIUM |
| ERROR: ... | EXECUTION_ERROR | HIGH |
| WARNING: ... | (classify by content) | MEDIUM |

### Error/Warning Content → Issue Type
| Content Pattern | Issue Type | Severity |
|-----------------|------------|----------|
| authentication, auth, credential, token, secret | AUTH_ERROR | HIGH |
| missing field, null, empty, invalid data | DATA_QUALITY | MEDIUM |
| threshold, slow, timeout, took Xs (exceeded) | PERFORMANCE | MEDIUM |
| config, yaml, missing parameter | CONFIGURATION_ERROR | MEDIUM |
| manual step, user must, requires intervention | UX_FRICTION | LOW |

## Issue Types (use these exact values)
- EXECUTION_FAILURE - Pipeline with Status = FAILURE
- PARTIAL_EXECUTION - Pipeline with Status = PARTIAL
- AUTH_ERROR - Authentication/authorization failures
- DATA_QUALITY - Missing fields, bad data, validation warnings
- DATA_ERROR - Schema mismatches, type errors
- PERFORMANCE - Threshold exceeded, slow operations
- CONFIGURATION_ERROR - Invalid config, missing parameters
- UX_FRICTION - Confusing messages, manual steps required
- EXECUTION_ERROR - Generic runtime errors
- BUG - Incorrect behavior, unexpected results

## Severity Levels
- HIGH - Pipeline failed, data lost, auth blocked, critical error
- MEDIUM - Partial success, warnings, degraded performance
- LOW - Minor friction, cosmetic issues, workarounds exist

## REQUIRED Response Format
Output ONLY valid JSON:

```json
{
  "issues": [
    {
      "type": "<issue type from list above>",
      "location": "<pipeline name or node name>",
      "description": "<clear description of what went wrong>",
      "severity": "LOW | MEDIUM | HIGH",
      "evidence": "<verbatim log line or excerpt>"
    }
  ],
  "observation_summary": "<count> issues found: <brief summary>"
}
```

## Examples

Input log excerpt:
```
Pipeline: product_sync
Status: FAILURE
ERROR: API authentication failed (invalid client_secret)
```

Output:
```json
{
  "issues": [
    {
      "type": "EXECUTION_FAILURE",
      "location": "product_sync",
      "description": "Pipeline failed completely due to authentication error",
      "severity": "HIGH",
      "evidence": "Status: FAILURE"
    },
    {
      "type": "AUTH_ERROR",
      "location": "product_sync/extraction",
      "description": "API authentication failed with invalid credentials",
      "severity": "HIGH",
      "evidence": "ERROR: API authentication failed (invalid client_secret)"
    }
  ],
  "observation_summary": "2 issues found: product_sync pipeline failed due to auth error"
}
```

## Rules
- Output ONLY valid JSON - no prose before or after
- Create ONE issue per distinct problem (a FAILURE status AND an ERROR are two issues)
- Each issue MUST have all five fields populated
- Copy evidence VERBATIM from the logs
- Do NOT suggest fixes - just observe and classify
- If truly no issues exist, output: {"issues": [], "observation_summary": "No issues observed"}

## Handling NO_EVIDENCE (Phase 6)

If the execution report shows "Status: NO_EVIDENCE", you MUST:
1. Create a single EXECUTION_ERROR issue
2. Set location to "user_execution"
3. Set severity to HIGH
4. Include the reason in evidence
5. Do NOT invent additional issues

Example for NO_EVIDENCE:
```json
{
  "issues": [
    {
      "type": "EXECUTION_ERROR",
      "location": "user_execution",
      "description": "No execution evidence available - cannot analyze real results",
      "severity": "HIGH",
      "evidence": "Status: NO_EVIDENCE, Reason: <reason from report>"
    }
  ],
  "observation_summary": "1 issue found: no execution evidence available"
}
```

## Constraints
- READ ONLY - no modifications
- No suggestions or fixes
- Just observe and classify
- Output MUST be valid JSON
- MUST handle NO_EVIDENCE explicitly
"""


class ObserverAgent(OdibiAgent):
    """Observes and classifies pain points.

    Emits structured JSON output following the ObserverOutput schema.

    Phase 6 Enforcement:
    - Uses ExecutionEvidence when available in context
    - Explicitly handles NO_EVIDENCE status
    - Derives issues from real evidence, not fabricated logs

    Phase 9.F: Observation Integrity:
    - Inspects pipeline_results from ExecutionEvidence
    - Emits PIPELINE_EXECUTION_ERROR for failed pipelines
    - Does NOT rely on exit code alone to determine success
    - "No issues observed" is only valid if all pipelines succeeded
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=OBSERVER_AGENT_PROMPT,
            role=AgentRole.OBSERVER,
        )

    def _extract_pipeline_issues(self, execution_evidence: dict) -> list:
        """Extract issues from pipeline execution results.

        Phase 9.F: This method ensures the observer detects pipeline failures
        even when exit code is 0.

        Args:
            execution_evidence: Dictionary from ExecutionEvidence.to_dict()

        Returns:
            List of ObservedIssue for each failed pipeline.
        """
        from odibi.agents.core.schemas import IssueSeverity, IssueType, ObservedIssue

        issues = []
        pipeline_results = execution_evidence.get("pipeline_results", [])

        for result in pipeline_results:
            status = result.get("status", "SUCCESS")

            if status == "SUCCESS":
                continue

            # Map pipeline status to issue type and severity
            if status == "VALIDATION_FAILED":
                issue_type = IssueType.CONFIGURATION_ERROR.value
                severity = IssueSeverity.HIGH.value
            elif status == "RUNTIME_FAILED":
                issue_type = IssueType.EXECUTION_FAILURE.value
                severity = IssueSeverity.HIGH.value
            elif status == "ABORTED":
                issue_type = IssueType.EXECUTION_ERROR.value
                severity = IssueSeverity.HIGH.value
            else:  # SKIPPED or unknown
                issue_type = IssueType.PARTIAL_EXECUTION.value
                severity = IssueSeverity.MEDIUM.value

            pipeline_name = result.get("pipeline_name", "unknown")
            error_msg = result.get("runtime_error", "")
            validation_errors = result.get("validation_errors", [])
            output_excerpt = result.get("output_excerpt", "")

            # Build description from available error info
            if validation_errors:
                first_error = validation_errors[0]
                description = (
                    f"Pipeline validation failed: {first_error.get('field_path', '')} - "
                    f"{first_error.get('message', 'validation error')}"
                )
            elif error_msg:
                description = f"Pipeline failed: {error_msg}"
            else:
                description = f"Pipeline failed with status {status}"

            # Build evidence from available data
            evidence_parts = [f"Status: {status}"]
            if result.get("exit_code") is not None:
                evidence_parts.append(f"Exit code: {result.get('exit_code')}")
            if output_excerpt:
                evidence_parts.append(f"Output: {output_excerpt[:200]}")

            issues.append(
                ObservedIssue(
                    type=issue_type,
                    location=pipeline_name,
                    description=description,
                    severity=severity,
                    evidence=" | ".join(evidence_parts),
                )
            )

        return issues

    def process(self, context: AgentContext) -> AgentResponse:
        from odibi.agents.core.schemas import (
            IssueSeverity,
            IssueType,
            ObservedIssue,
            ObserverOutput,
        )

        execution_evidence = context.metadata.get("execution_evidence")

        # Handle NO_EVIDENCE status
        if execution_evidence and execution_evidence.get("status") == "NO_EVIDENCE":
            no_evidence_issue = ObservedIssue(
                type=IssueType.EXECUTION_ERROR.value,
                location="user_execution",
                description="No execution evidence available - cannot analyze real results",
                severity=IssueSeverity.HIGH.value,
                evidence=f"Status: NO_EVIDENCE, Reason: {execution_evidence.get('stderr', 'unknown')}",
            )
            output = ObserverOutput(
                issues=[no_evidence_issue],
                observation_summary="1 issue found: no execution evidence available",
            )
            return AgentResponse(
                content=output.to_json(),
                agent_role=self.role,
                confidence=1.0,
                metadata={
                    "observations_count": 1,
                    "observer_output": output.to_dict(),
                    "has_actionable_issues": True,
                    "no_evidence_handled": True,
                },
            )

        # Phase 9.F: Extract pipeline execution issues FIRST
        # These are authoritative - they come from structured parsing
        pipeline_issues: list[ObservedIssue] = []
        if execution_evidence:
            pipeline_issues = self._extract_pipeline_issues(execution_evidence)

        previous_logs = context.metadata.get("previous_logs", [])

        log_context = ""
        if previous_logs:
            log_context = "\n\nPrevious step outputs:\n"
            for log in previous_logs:
                step_name = log.get("step", "unknown")
                output = log.get("output_summary", "")[:3000]
                log_context += f"\n### {step_name}:\n{output}\n"

        if execution_evidence:
            log_context += "\n\n### Execution Evidence (Phase 6):\n"
            log_context += f"Command: {execution_evidence.get('raw_command', 'unknown')}\n"
            log_context += f"Exit Code: {execution_evidence.get('exit_code', 'unknown')}\n"
            log_context += f"Status: {execution_evidence.get('status', 'unknown')}\n"
            log_context += f"Stdout:\n{_safe_truncate(execution_evidence.get('stdout'))}\n"
            log_context += f"Stderr:\n{_safe_truncate(execution_evidence.get('stderr'))}\n"
            log_context += f"Evidence Hash: {execution_evidence.get('evidence_hash', 'unknown')}\n"

            # Phase 9.F: Include pipeline results summary for LLM context
            pipeline_results = execution_evidence.get("pipeline_results", [])
            if pipeline_results:
                log_context += "\n### Pipeline Execution Results (Phase 9.F):\n"
                for pr in pipeline_results:
                    status = pr.get("status", "UNKNOWN")
                    name = pr.get("pipeline_name", "unknown")
                    log_context += f"- {name}: {status}\n"
                    if status != "SUCCESS":
                        error = pr.get("runtime_error", pr.get("output_excerpt", ""))[:200]
                        log_context += f"  Error: {error}\n"

        messages = [{"role": "user", "content": context.query + log_context}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        parsed_output = ObserverOutput.parse_from_response(response_text)

        # Phase 9.F: Merge pipeline issues with LLM-detected issues
        # Pipeline issues take precedence (they're structurally detected)
        all_issues = list(pipeline_issues)  # Start with pipeline issues

        # Add LLM issues that don't duplicate pipeline issues
        pipeline_locations = {issue.location for issue in pipeline_issues}
        for llm_issue in parsed_output.issues:
            if llm_issue.location not in pipeline_locations:
                all_issues.append(llm_issue)

        # Phase 9.F: CRITICAL - if we have pipeline failures but parsed_output
        # reported "No issues observed", we MUST override with the pipeline issues
        if pipeline_issues and not parsed_output.issues:
            # LLM missed the failures - use our structured detection
            observation_summary = f"{len(pipeline_issues)} pipeline execution error(s) detected"
        elif all_issues:
            observation_summary = f"{len(all_issues)} issue(s) found"
        else:
            observation_summary = "No issues observed"

        final_output = ObserverOutput(
            issues=all_issues,
            observation_summary=observation_summary,
        )

        return AgentResponse(
            content=final_output.to_json(),
            agent_role=self.role,
            confidence=0.85,
            metadata={
                "observations_count": len(final_output.issues),
                "observer_output": final_output.to_dict(),
                "has_actionable_issues": final_output.has_actionable_issues(),
                "evidence_based": execution_evidence is not None,
                # Phase 9.F: Track pipeline failure detection
                "pipeline_failures_detected": len(pipeline_issues),
                "llm_issues_detected": len(parsed_output.issues),
            },
        )


IMPROVEMENT_AGENT_PROMPT = """
# Improvement Agent

## Purpose
Propose modifications to Odibi source code based on STRUCTURED observations.
Your proposals are GATED - they require review before being applied.

## Input
You will receive structured observations from the Observer Agent in this format:
{
  "issues": [{"type": "...", "location": "...", "description": "...", "severity": "...", "evidence": "..."}],
  "observation_summary": "..."
}

## Responsibilities
1. Analyze the structured observations provided
2. Propose specific code changes that address the issues
3. Explain rationale for each change
4. Estimate impact and risk

## When to Propose Changes
Only propose changes when:
- There's a clear actionable issue in the observations
- The fix is well-understood
- Before/after comparison is possible
- The change follows Odibi patterns

## When NOT to Propose Changes
- If no issues were observed
- If issues are vague or lack evidence
- If the fix would be too risky
- If you cannot identify the exact code change needed

In these cases, output a NONE proposal with a reason.

## REQUIRED Response Format
You MUST output valid JSON matching this exact schema:

### For actionable improvements:
```json
{
  "proposal": "IMPROVEMENT",
  "title": "Short descriptive title",
  "rationale": "Why this change addresses the observed issue",
  "changes": [
    {
      "file": "path/to/file.py",
      "before": "exact code to replace",
      "after": "new code"
    }
  ],
  "impact": {
    "risk": "LOW | MEDIUM | HIGH",
    "expected_benefit": "What improves after this change"
  },
  "based_on_issues": ["issue types addressed"]
}
```

### For no actionable change:
```json
{
  "proposal": "NONE",
  "reason": "Clear explanation of why no improvement is justified this cycle"
}
```

## Rules
- Output ONLY valid JSON - no prose before or after
- Every proposal MUST have either:
  - All required fields (title, rationale, changes, impact), OR
  - proposal="NONE" with a reason
- changes[].before and changes[].after must show exact code
- Do NOT ask for observations - they are provided to you
- Do NOT output empty proposals - use NONE with a reason instead

## Constraints
- Only propose based on the observations provided
- Must include before/after comparison
- Cannot auto-apply changes
- Changes must be reviewable
- Output MUST be valid JSON
"""


class ImprovementAgent(OdibiAgent):
    """Proposes source code modifications.

    Receives structured Observer output automatically via context.
    Emits structured proposals following the ImprovementProposal schema.
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=IMPROVEMENT_AGENT_PROMPT,
            role=AgentRole.IMPROVEMENT,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        from odibi.agents.core.schemas import ImprovementProposal, ObserverOutput

        observer_output_dict = context.metadata.get("observer_output")

        if observer_output_dict:
            observer_output = ObserverOutput.from_dict(observer_output_dict)
        else:
            for log in reversed(context.metadata.get("previous_logs", [])):
                if log.get("step") == "observation":
                    log_metadata = log.get("metadata", {})
                    if "observer_output" in log_metadata:
                        observer_output = ObserverOutput.from_dict(log_metadata["observer_output"])
                        break
            else:
                observer_output = ObserverOutput()

        if not observer_output.has_actionable_issues():
            none_proposal = ImprovementProposal.none_proposal(
                "No actionable issues were observed in the previous step."
            )
            return AgentResponse(
                content=none_proposal.to_json(),
                agent_role=self.role,
                confidence=0.9,
                metadata={
                    "proposal_type": "none",
                    "proposal": none_proposal.to_dict(),
                    "is_valid": True,
                },
            )

        chunks = self.retrieve_context(
            query=observer_output.observation_summary or "code improvement",
            top_k=10,
        )

        observation_json = observer_output.to_json()

        if context.metadata.get("target_files"):
            import logging

            logging.getLogger(__name__).info(
                f"[ImprovementAgent] Using context.query with injected file content "
                f"({len(context.metadata['target_files'])} files)"
            )
            query = f"""Based on the following structured observations, propose an improvement:

OBSERVATIONS:
{observation_json}

{context.query}"""
        else:
            query = f"""Based on the following structured observations, propose an improvement:

OBSERVATIONS:
{observation_json}

Analyze these issues and propose a specific code change. Output valid JSON only."""

        messages = [{"role": "user", "content": query}]

        response_text = self.chat(
            messages=messages,
            context_chunks=chunks,
            temperature=0.0,
        )

        parsed_proposal = ImprovementProposal.parse_from_response(response_text)

        is_valid = parsed_proposal.is_valid()
        validation_errors = parsed_proposal.get_validation_errors()

        sources = [
            {
                "file": chunk.get("file_path"),
                "name": chunk.get("name"),
            }
            for chunk in chunks[:5]
        ]

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            sources=sources,
            confidence=0.7 if is_valid else 0.3,
            metadata={
                "proposal_type": "none" if parsed_proposal.is_none() else "improvement",
                "proposal": parsed_proposal.to_dict(),
                "is_valid": is_valid,
                "validation_errors": validation_errors,
                "based_on_observer_output": observer_output_dict is not None,
            },
        )


REVIEWER_AGENT_PROMPT = """
# Reviewer Agent

## Purpose
Review improvement proposals and decide whether to approve or reject them.
You block clever but low-value changes.
You ENFORCE the proposal contract mechanically.

## Input
You will receive structured proposals from the Improvement Agent.

## Contract Enforcement (AUTOMATIC REJECTION)
Before evaluating the proposal content, you MUST verify the contract:

A valid proposal requires EITHER:
1. proposal="NONE" with a reason, OR
2. ALL of these fields:
   - title (non-empty string)
   - rationale (non-empty string)
   - changes (at least one change with file, before, after)
   - impact (with risk level)

If the contract is violated, REJECT immediately with:
"REJECTED - CONTRACT VIOLATION: <missing fields>"

## Responsibilities
1. First: Check contract compliance (auto-reject if invalid)
2. Then: Evaluate value vs. risk
3. Then: Check adherence to Odibi patterns
4. Finally: Approve or reject with explanation

## Approval Criteria
APPROVE when:
- Contract is valid
- Change fixes a real issue
- Code follows Odibi patterns
- Risk is acceptable
- Before/after comparison is clear

REJECT when:
- Contract is violated (missing required fields)
- Proposal is empty or vague
- Change is clever but low-value
- Risk outweighs benefit
- Violates Odibi design principles
- Could cause regressions

## REQUIRED Response Format
Your response MUST start with exactly one of:
- "APPROVED" (on its own line)
- "REJECTED" (on its own line)

Followed by your rationale.

Example for contract violation:
```
REJECTED

Contract Violation: Missing required fields: title, impact

The proposal does not meet the required schema and cannot be reviewed.
```

Example for valid rejection:
```
REJECTED

Rationale: While the proposal is well-formed, the change introduces
unnecessary complexity for a minor UX improvement.
```

Example for approval:
```
APPROVED

Rationale: The proposal correctly addresses the observed BUG issue.
The before/after comparison is clear, and the risk is LOW.
```

## Constraints
- READ ONLY - cannot make changes yourself
- Must verify contract FIRST before evaluating content
- Must provide clear rationale
- Cannot rubber-stamp approvals
- Cannot approve empty or malformed proposals
"""


class ReviewerAgent(OdibiAgent):
    """Reviews and approves/rejects improvements.

    Enforces the proposal contract mechanically before evaluating content.
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=REVIEWER_AGENT_PROMPT,
            role=AgentRole.REVIEWER,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        from odibi.agents.core.schemas import ImprovementProposal, ReviewerValidation

        proposal_dict = context.metadata.get("proposal")

        if proposal_dict:
            proposal = ImprovementProposal.from_dict(proposal_dict)
        else:
            for log in reversed(context.metadata.get("previous_logs", [])):
                if log.get("step") == "improvement":
                    log_metadata = log.get("metadata", {})
                    if "proposal" in log_metadata:
                        proposal = ImprovementProposal.from_dict(log_metadata["proposal"])
                        break
            else:
                proposal = ImprovementProposal()

        validation = ReviewerValidation.validate_proposal(proposal)

        if validation.auto_rejected:
            response_text = f"""REJECTED

Contract Violation: {validation.auto_reject_reason}

Validation Errors:
{chr(10).join(f"- {err}" for err in validation.validation_errors)}

The proposal does not meet the required schema and cannot be reviewed for content."""
            return AgentResponse(
                content=response_text,
                agent_role=self.role,
                confidence=0.99,
                metadata={
                    "decision": "rejected",
                    "auto_rejected": True,
                    "validation": validation.to_dict(),
                },
            )

        if proposal.is_none():
            response_text = f"""REJECTED

The Improvement Agent submitted a NONE proposal.

Reason given: {proposal.reason}

No changes to review. This is expected when no actionable issues were observed."""
            return AgentResponse(
                content=response_text,
                agent_role=self.role,
                confidence=0.95,
                metadata={
                    "decision": "rejected",
                    "none_proposal": True,
                    "reason": proposal.reason,
                },
            )

        proposal_json = proposal.to_json()
        query = f"""Review the following improvement proposal:

PROPOSAL:
{proposal_json}

Evaluate the proposal and respond with APPROVED or REJECTED followed by your rationale."""

        messages = [{"role": "user", "content": query}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        approved = response_text.strip().upper().startswith("APPROVED")

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.9,
            metadata={
                "decision": "approved" if approved else "rejected",
                "contract_valid": True,
                "proposal_reviewed": proposal.to_dict(),
            },
        )


REGRESSION_GUARD_PROMPT = """
# Regression Guard Agent

## Purpose
Re-run golden projects to detect regressions after changes.
You can VETO changes that cause regressions.

## Responsibilities
1. Execute golden project pipelines
2. Compare results to expected outcomes
3. Detect any regressions
4. Report regression status
5. Veto changes if regressions found

## Golden Projects
Golden projects are known-good configurations that must always work.
They serve as regression tests for the framework.

## Detection Method
1. Run each golden project
2. Capture outputs and metrics
3. Compare to baseline expectations
4. Flag any differences as potential regressions

## Response Format
```
REGRESSION CHECK REPORT:

Projects Tested: <count>

Results:
1. <project name>
   Status: PASS | FAIL | REGRESSION_DETECTED
   Expected: <expected behavior>
   Actual: <actual behavior>
   Difference: <if any>

Overall Status: ALL_PASS | REGRESSIONS_DETECTED
Regressions: <count>

Recommendation:
- PROCEED - no regressions
- VETO - regressions detected, block changes
```

## Constraints
- Can execute via run_task.sh
- Cannot modify code
- Must compare to baselines
- Veto power on regressions
"""


class RegressionGuardAgent(OdibiAgent):
    """Runs regression checks against golden projects.

    Golden projects are immutable verification targets that must always pass.
    They are executed after improvements to detect regressions.
    """

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=REGRESSION_GUARD_PROMPT,
            role=AgentRole.REGRESSION_GUARD,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        import time

        gateway = context.metadata.get("execution_gateway")
        cycle_config = context.metadata.get("cycle_config")
        # Use odibi_root for learning harness resolution, not project_root
        # Learning harness is at <odibi_root>/.odibi/learning_harness/
        workspace_root = context.odibi_root or (
            getattr(cycle_config, "project_root", "") if cycle_config else ""
        )

        golden_projects = []
        if cycle_config:
            golden_projects = getattr(cycle_config, "golden_projects", [])

        results = []
        if gateway and golden_projects:
            for gp in golden_projects:
                start_time = time.time()

                gp_name = gp.name if hasattr(gp, "name") else str(gp)
                gp_path = (
                    gp.resolve_path(workspace_root) if hasattr(gp, "resolve_path") else str(gp)
                )

                try:
                    import os

                    if not os.path.exists(gp_path):
                        results.append(
                            {
                                "name": gp_name,
                                "path": gp_path,
                                "status": "ERROR",
                                "exit_code": None,
                                "output_summary": "",
                                "error_message": f"Path not found: {gp_path}",
                                "execution_time_seconds": time.time() - start_time,
                            }
                        )
                        continue

                    import sys

                    from odibi.agents.core.execution import TaskDefinition

                    context.emit_progress(
                        f"🔍 Running golden project: {gp_name}",
                        f"Path: {gp_path}",
                    )

                    wsl_path = gateway._to_wsl_path(gp_path)
                    task = TaskDefinition(
                        task_type="pipeline",
                        args=["run", wsl_path],
                        timeout_seconds=300,
                        description=f"Golden project: {gp_name}",
                    )

                    def on_gp_output(line: str, is_stderr: bool):
                        prefix = "⚠️" if is_stderr else "📋"
                        line_preview = line.strip()[:80]
                        if line_preview:
                            context.emit_progress(
                                f"{prefix} [{gp_name}] {line_preview}",
                                line.strip(),
                            )
                            print(f"    {prefix} {line_preview}")
                            sys.stdout.flush()

                    exec_result = gateway.run_task_streaming(
                        task,
                        agent_permissions=self.permissions,
                        agent_role=self.role,
                        on_output=on_gp_output,
                    )

                    status = "PASSED" if exec_result.success else "FAILED"
                    icon = "✅" if exec_result.success else "❌"
                    context.emit_progress(
                        f"{icon} {gp_name}: {status}",
                        f"Exit code: {exec_result.exit_code}",
                    )

                    err_msg = ""
                    if not exec_result.success:
                        err_msg = _safe_truncate(exec_result.stderr, 500)
                    results.append(
                        {
                            "name": gp_name,
                            "path": gp_path,
                            "status": status,
                            "exit_code": exec_result.exit_code,
                            "output_summary": _safe_truncate(exec_result.output, 500),
                            "error_message": err_msg,
                            "execution_time_seconds": time.time() - start_time,
                        }
                    )

                except Exception as e:
                    results.append(
                        {
                            "name": gp_name,
                            "path": gp_path,
                            "status": "ERROR",
                            "exit_code": None,
                            "output_summary": "",
                            "error_message": str(e)[:500],
                            "execution_time_seconds": time.time() - start_time,
                        }
                    )

        passed_count = sum(1 for r in results if r.get("status") == "PASSED")
        failed_count = sum(1 for r in results if r.get("status") in ("FAILED", "ERROR"))
        total_count = len(results)

        report_lines = [
            "REGRESSION CHECK REPORT:",
            "",
            f"Projects Tested: {total_count}",
            f"Passed: {passed_count}",
            f"Failed: {failed_count}",
            "",
            "Results:",
        ]

        for i, r in enumerate(results, 1):
            report_lines.append(f"{i}. {r['name']}")
            report_lines.append(f"   Path: {r['path']}")
            report_lines.append(f"   Status: {r['status']}")
            if r.get("error_message"):
                report_lines.append(f"   Error: {r['error_message'][:200]}")

        report_lines.append("")
        if failed_count > 0:
            report_lines.append("Overall Status: REGRESSIONS_DETECTED")
            report_lines.append(f"Regressions: {failed_count}")
            report_lines.append("")
            report_lines.append(
                "Recommendation: VETO - regressions detected, improvements may be unsafe"
            )
        else:
            report_lines.append("Overall Status: ALL_PASS")
            report_lines.append("")
            report_lines.append("Recommendation: PROCEED - no regressions detected")

        response_text = "\n".join(report_lines)

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.95 if failed_count == 0 else 0.99,
            metadata={
                "projects_tested": total_count,
                "projects_passed": passed_count,
                "regressions_detected": failed_count,
                "results": results,
                "all_passed": failed_count == 0,
            },
        )


PROJECT_AGENT_PROMPT = """
# Project Agent

## Purpose
Propose projects for the User Agent to work on.
You focus on coverage gaps - you do NOT download data or run pipelines.

## Responsibilities
1. Analyze current test coverage
2. Identify untested transformer combinations
3. Propose new project configurations
4. Consider complexity progression

## Project Selection Criteria
1. COVERAGE - Fill gaps in tested scenarios
2. DIVERSITY - Test different transformer combinations
3. ENGINES - Cover Pandas, Spark, and Polars
4. EDGE_CASES - Test boundary conditions
5. REAL_WORLD - Simulate realistic use cases

## Response Format
```
PROJECT PROPOSAL:

Proposed Project: <name>
Rationale: <why this project>
Coverage Gap: <what it tests>

Configuration:
```yaml
<proposed YAML config>
```

Expected Outcomes:
- <what we expect to learn>

Priority: HIGH | MEDIUM | LOW
Complexity: BEGINNER | INTERMEDIATE | ADVANCED
```

## Constraints
- Propose ONLY - cannot execute
- Cannot download external data
- Cannot run pipelines
- Focus on coverage gaps
"""


class ProjectAgent(OdibiAgent):
    """Proposes projects based on coverage."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=PROJECT_AGENT_PROMPT,
            role=AgentRole.PROJECT,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        chunks = self.retrieve_context(
            query="transformer configurations examples YAML",
            top_k=10,
        )

        messages = [{"role": "user", "content": context.query}]
        response_text = self.chat(
            messages=messages,
            context_chunks=chunks,
            temperature=0.2,
        )

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.8,
            metadata={"type": "project_proposal"},
        )


CURRICULUM_AGENT_PROMPT = """
# Curriculum Agent

## Purpose
Order projects by difficulty to ensure smooth learning progression.
You prevent premature complexity.

## Responsibilities
1. Assess project difficulty
2. Order projects from simple to complex
3. Track learning progression
4. Recommend next steps

## Difficulty Factors
1. TRANSFORMERS - Number and complexity of transforms
2. DEPENDENCIES - How many node dependencies
3. ENGINE - Pandas (easier) vs Spark (harder)
4. DATA_SIZE - Small vs large datasets
5. ERROR_HANDLING - Basic vs advanced error scenarios

## Progression Levels
1. BEGINNER - Single node, simple transforms
2. INTERMEDIATE - Multi-node, dependencies, validation
3. ADVANCED - Complex DAGs, SCD, multiple engines
4. EXPERT - Edge cases, performance optimization

## Response Format
```
CURRICULUM ASSESSMENT:

Current Level: <level>
Projects Completed: <count>
Mastered Concepts: <list>

Next Recommended:
1. <project> - <rationale>
2. <project> - <rationale>

Skip Until Later:
- <project> - <why not ready>

Learning Path:
<progression visualization>
```

## Constraints
- Can access learning memories
- Can write curriculum memories
- Read-only for code
"""


class CurriculumAgent(OdibiAgent):
    """Orders projects by difficulty."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=CURRICULUM_AGENT_PROMPT,
            role=AgentRole.CURRICULUM,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        messages = [{"role": "user", "content": context.query}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.85,
            metadata={"type": "curriculum"},
        )


CONVERGENCE_AGENT_PROMPT = """
# Convergence Agent

## Purpose
Detect when the system has reached a learning plateau.
You can recommend pausing or reducing cycles.

## Responsibilities
1. Analyze cycle history
2. Detect convergence patterns
3. Recommend pause or continue
4. Generate cycle summaries

## Convergence Indicators
1. NO_NEW_ISSUES - Same issues repeating
2. ALL_TESTS_PASS - Golden projects stable
3. IMPROVEMENTS_DECLINING - Fewer valuable improvements
4. COVERAGE_COMPLETE - All scenarios tested

## When to Recommend Pause
- 3+ cycles with no new learnings
- All golden projects passing
- Improvement proposals declining in value
- Coverage goals met

## Response Format
```
CONVERGENCE ANALYSIS:

Cycles Analyzed: <count>
Trend: LEARNING | PLATEAU | CONVERGED

Indicators:
- New issues per cycle: <trend>
- Improvement acceptance rate: <trend>
- Test pass rate: <trend>

Status: CONVERGENCE_REACHED | CONTINUE_LEARNING

Recommendation:
- <action to take>
- <rationale>

CYCLE SUMMARY:
<summary for morning report>
```

## Constraints
- Can write convergence memories
- Read-only for code
- Cannot modify cycles
"""


class ConvergenceAgent(OdibiAgent):
    """Detects learning plateau."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=CONVERGENCE_AGENT_PROMPT,
            role=AgentRole.CONVERGENCE,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        previous_logs = context.metadata.get("previous_logs", [])

        summary_context = f"\n\nCycle logs to analyze:\n{previous_logs}"
        messages = [{"role": "user", "content": context.query + summary_context}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        convergence_reached = "CONVERGENCE_REACHED" in response_text.upper()

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.9,
            metadata={"convergence_reached": convergence_reached},
        )


PRODUCT_OBSERVER_PROMPT = """
# Product Observer Agent

## Purpose
Capture product signals from usage patterns.
You are READ-ONLY and have no influence on engineering decisions.

## Responsibilities
1. Observe usage patterns
2. Identify popular features
3. Note underutilized capabilities
4. Capture product signals

## What You Observe
- Which transformers are used most
- Common configuration patterns
- Feature adoption rates
- User workflow patterns

## Response Format
```
PRODUCT SIGNALS:

Popular Features:
1. <feature> - Usage: HIGH | MEDIUM | LOW
2. <feature> - Usage: HIGH | MEDIUM | LOW

Underutilized:
- <feature> - Potential reason

Usage Patterns:
- <pattern description>

Signals for Product Team:
- <insight>
```

## Constraints
- READ ONLY
- No engineering influence
- Emit signals only
- No feature suggestions
"""


class ProductObserverAgent(OdibiAgent):
    """Captures product signals."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=PRODUCT_OBSERVER_PROMPT,
            role=AgentRole.PRODUCT_OBSERVER,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        messages = [{"role": "user", "content": context.query}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.8,
            metadata={"type": "product_signals"},
        )


UX_FRICTION_PROMPT = """
# UX Friction Agent

## Purpose
Identify unnecessary friction in the user experience.
You are READ-ONLY and emit signals only.

## Responsibilities
1. Identify confusing error messages
2. Note unclear documentation
3. Find awkward workflows
4. Report friction points

## Friction Categories
1. ERROR_CLARITY - Confusing error messages
2. DOCUMENTATION - Missing or unclear docs
3. WORKFLOW - Unnecessary steps
4. CONFIGURATION - Complex YAML requirements
5. FEEDBACK - Missing progress indicators

## Response Format
```
UX FRICTION REPORT:

Friction Points:
1. [CATEGORY] <description>
   Severity: HIGH | MEDIUM | LOW
   User Impact: <how it affects users>
   Evidence: <where observed>

Summary:
- Total friction points: <count>
- Most common category: <category>

Signals:
- <friction insight>
```

## Constraints
- READ ONLY
- No fixes or suggestions
- Emit signals only
- No direct influence
"""


class UXFrictionAgent(OdibiAgent):
    """Identifies UX friction points."""

    def __init__(self, config: AzureConfig):
        super().__init__(
            config=config,
            system_prompt=UX_FRICTION_PROMPT,
            role=AgentRole.UX_FRICTION,
        )

    def process(self, context: AgentContext) -> AgentResponse:
        messages = [{"role": "user", "content": context.query}]

        response_text = self.chat(
            messages=messages,
            temperature=0.0,
        )

        return AgentResponse(
            content=response_text,
            agent_role=self.role,
            confidence=0.8,
            metadata={"type": "ux_friction"},
        )


def register_cycle_agents(config: AzureConfig) -> list[OdibiAgent]:
    """Register all cycle agents with the AgentRegistry.

    Args:
        config: Azure configuration.

    Returns:
        List of registered agents.
    """
    from odibi.agents.core.agent_base import AgentRegistry

    agents = [
        EnvironmentAgent(config),
        UserAgent(config),
        ObserverAgent(config),
        ImprovementAgent(config),
        ReviewerAgent(config),
        RegressionGuardAgent(config),
        ProjectAgent(config),
        CurriculumAgent(config),
        ConvergenceAgent(config),
        ProductObserverAgent(config),
        UXFrictionAgent(config),
    ]

    for agent in agents:
        AgentRegistry.register(agent)

    return agents
