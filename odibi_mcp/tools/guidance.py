"""Task guidance system for interactive agent workflows.

Provides structured questions and defaults for each task type,
removing the need for agents to "figure out" what to ask users.
"""

from typing import Any, Dict, Literal, Optional


# Task recipes - structured guidance for common workflows
TASK_GUIDANCE = {
    "profile_data": {
        "description": "Analyze schema, stats, and sample data from a file or table",
        "questions": [
            {
                "param": "connection",
                "question": "Which connection has your data?",
                "hint": "Use map_environment to discover connections",
                "discover_with": "map_environment",
                "required": True,
            },
            {
                "param": "path",
                "question": "File path or table name?",
                "hint": "Examples: customers.csv, dbo.Sales, data/*.parquet",
                "discover_with": "profile_folder",
                "required": True,
            },
            {
                "param": "max_rows",
                "question": "How much data to sample?",
                "options": [
                    {"label": "Quick (100 rows)", "value": 100, "time": "~2 sec"},
                    {"label": "Standard (1000 rows)", "value": 1000, "time": "~10 sec"},
                    {"label": "Deep (10000 rows)", "value": 10000, "time": "~60 sec"},
                ],
                "default": 100,
                "required": False,
            },
        ],
        "next_tool": "profile_source",
        "call_template": "profile_source(connection={connection}, path={path}, max_rows={max_rows})",
    },
    "build_pipeline": {
        "description": "Generate a complete pipeline YAML using a pattern",
        "questions": [
            {
                "param": "pattern",
                "question": "Which pattern fits your use case?",
                "hint": "Use list_patterns to see all options with descriptions",
                "discover_with": "list_patterns",
                "required": True,
            },
            {
                "param": "pipeline_name",
                "question": "What should this pipeline be called?",
                "hint": "Use lowercase with underscores: customer_dimension, daily_sales",
                "required": True,
            },
            {
                "param": "source_connection",
                "question": "Where is the source data?",
                "hint": "Use map_environment to discover connections",
                "discover_with": "map_environment",
                "required": True,
            },
            {
                "param": "source_table",
                "question": "Source table/file name?",
                "hint": "Examples: dbo.Customers, raw_data.csv",
                "required": True,
            },
            {
                "param": "target_connection",
                "question": "Where should output go?",
                "default": "{source_connection}",
                "required": True,
            },
            {
                "param": "target_path",
                "question": "Output path?",
                "hint": "Examples: gold/dim_customer, output/sales.parquet",
                "default": "gold/{pipeline_name}",
                "required": True,
            },
            {
                "param": "keys",
                "question": "Business key column(s)?",
                "hint": "Which columns uniquely identify records? Use profile_source to see columns",
                "discover_with": "profile_source",
                "list": True,
                "required": "pattern in [scd2, merge]",
            },
            {
                "param": "tracked_columns",
                "question": "Which columns to track for changes? (SCD2 only)",
                "hint": "Columns that trigger new versions when they change",
                "list": True,
                "required": "pattern == scd2",
            },
            {
                "param": "natural_key",
                "question": "Natural key column? (Dimension only)",
                "hint": "Business ID column like customer_id",
                "required": "pattern == dimension",
            },
            {
                "param": "surrogate_key",
                "question": "Surrogate key column name? (Dimension only)",
                "hint": "Generated ID like customer_sk",
                "default": "{entity}_sk",
                "required": "pattern == dimension",
            },
        ],
        "next_tool": "apply_pattern_template",
        "call_template": "apply_pattern_template(pattern={pattern}, pipeline_name={pipeline_name}, ...)",
    },
    "test_pipeline": {
        "description": "Validate and test a pipeline YAML",
        "questions": [
            {
                "param": "mode",
                "question": "How should I test it?",
                "options": [
                    {
                        "label": "Validate only",
                        "value": "validate",
                        "description": "Check YAML syntax and structure (instant)",
                    },
                    {
                        "label": "Dry-run (recommended)",
                        "value": "dry-run",
                        "description": "Show execution plan without running (safe, ~5 sec)",
                    },
                    {
                        "label": "Sample execution",
                        "value": "sample",
                        "description": "Actually run on limited data (requires data, ~30 sec)",
                    },
                ],
                "default": "dry-run",
                "required": False,
            },
            {
                "param": "max_rows",
                "question": "Max rows to process? (sample mode only)",
                "default": 100,
                "required": False,
                "visible_when": "mode == sample",
            },
        ],
        "next_tool": "test_pipeline",
        "call_template": "test_pipeline(yaml_content={yaml}, mode={mode}, max_rows={max_rows})",
    },
    "inspect_output": {
        "description": "Examine pipeline output after execution",
        "questions": [
            {
                "param": "connection",
                "question": "Which connection has the output?",
                "hint": "Usually same as pipeline's write connection",
                "required": True,
            },
            {
                "param": "path",
                "question": "Output file/table path?",
                "hint": "Check the pipeline YAML's write.path",
                "required": True,
            },
            {
                "param": "max_rows",
                "question": "How many rows to inspect?",
                "options": [
                    {"label": "Quick peek (10 rows)", "value": 10},
                    {"label": "Standard (100 rows)", "value": 100},
                    {"label": "Detailed (1000 rows)", "value": 1000},
                ],
                "default": 100,
                "required": False,
            },
        ],
        "next_tool": "profile_source",
        "call_template": "profile_source(connection={connection}, path={path}, max_rows={max_rows})",
    },
    "discover_environment": {
        "description": "Explore available connections and data sources",
        "questions": [
            {
                "param": "connection",
                "question": "Which connection to explore?",
                "hint": "Use map_environment() with no args to see all connections first",
                "required": False,
            },
            {
                "param": "path",
                "question": "Specific path/folder to explore?",
                "hint": "Leave empty to see top-level, or specify like: /data/bronze/",
                "required": False,
            },
        ],
        "next_tool": "map_environment",
        "call_template": "map_environment(connection={connection}, path={path})",
    },
}


def get_task_guidance(
    task: Literal[
        "profile_data",
        "build_pipeline",
        "test_pipeline",
        "inspect_output",
        "discover_environment",
    ],
) -> Dict[str, Any]:
    """Get structured guidance for a task workflow.

    Returns questions to ask user, defaults, and next tool to call.
    Removes need for agent to "figure out" what to ask.

    Args:
        task: Type of task user wants to accomplish

    Returns:
        {
            "task": str,
            "description": str,
            "questions": [
                {
                    "param": str,
                    "question": str,
                    "options": [...],  # If multiple choice
                    "default": Any,
                    "required": bool,
                    "discover_with": str,  # Tool to call for discovery
                    "hint": str
                }
            ],
            "next_tool": str,
            "call_template": str,
            "workflow": str  # Suggested conversation flow
        }
    """
    if task not in TASK_GUIDANCE:
        return {
            "error": f"Unknown task: {task}",
            "available_tasks": list(TASK_GUIDANCE.keys()),
        }

    guidance = TASK_GUIDANCE[task].copy()
    guidance["task"] = task

    # Add workflow suggestion
    guidance["workflow"] = _generate_workflow(task, guidance)

    return guidance


def list_task_types() -> Dict[str, Any]:
    """List all available task types with descriptions.

    Use this when user asks something vague like "help me analyze data"
    or "I want to build a pipeline" - show them what's possible.
    """
    return {
        "available_tasks": [
            {"task": task, "description": meta["description"]}
            for task, meta in TASK_GUIDANCE.items()
        ],
        "usage": "Call get_task_guidance(task=X) to get detailed guidance for a specific task",
    }


def _generate_workflow(task: str, guidance: Dict[str, Any]) -> str:
    """Generate suggested conversation flow for a task."""
    questions = guidance.get("questions", [])
    next_tool = guidance.get("next_tool", "")

    workflow = "Suggested workflow:\n\n"
    workflow += f"1. Present task: '{guidance['description']}'\n"

    # Questions with discovery
    discovery_steps = []
    direct_questions = []

    for q in questions:
        if q.get("discover_with"):
            discovery_steps.append(q)
        else:
            direct_questions.append(q)

    if discovery_steps:
        workflow += "\n2. Discovery (optional but helpful):\n"
        for q in discovery_steps:
            workflow += f"   - Call {q['discover_with']}() to help user answer '{q['question']}'\n"

    workflow += "\n3. Ask user these questions:\n"
    for i, q in enumerate(questions, 1):
        required = " (REQUIRED)" if q.get("required") else ""
        default = f" [default: {q.get('default')}]" if q.get("default") else ""
        workflow += f"   {i}. {q['question']}{required}{default}\n"

        if q.get("options"):
            workflow += f"      Options: {', '.join(opt['label'] for opt in q['options'])}\n"

        if q.get("hint"):
            workflow += f"      Hint: {q['hint']}\n"

    workflow += f"\n4. Call {next_tool}() with user's answers\n"

    if task == "build_pipeline":
        workflow += "\n5. Then call test_pipeline(mode='dry-run') to validate\n"
        workflow += "6. If valid, show user the YAML and execution plan\n"

    return workflow


def get_param_defaults(task: str, pattern: Optional[str] = None) -> Dict[str, Any]:
    """Get default values for a task's parameters.

    Useful for quick execution when user says "use defaults" or "I don't care".

    Args:
        task: Task type
        pattern: Pattern name (for build_pipeline task)

    Returns:
        Dictionary of param defaults
    """
    if task not in TASK_GUIDANCE:
        return {}

    guidance = TASK_GUIDANCE[task]
    defaults = {}

    for q in guidance.get("questions", []):
        if q.get("default") is not None:
            defaults[q["param"]] = q["default"]
        elif q.get("options"):
            # Use first option as default
            defaults[q["param"]] = q["options"][0]["value"]

    return defaults
