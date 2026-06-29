"""Universal action dispatcher for Odibi MCP gateway.

This module implements the 2-tool universal gateway pattern, compressing 37+ actions
into a single dispatch surface. Based on the proven context_workbench architecture.
"""

from __future__ import annotations

import json
from typing import Any, Callable
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pydantic import BaseModel


class OdibiDispatcher:
    """Universal action dispatcher for Odibi MCP gateway.
    
    Routes action names to handler methods, provides help/discovery system,
    and manages pause/resume workflow state.
    """
    
    def __init__(self):
        """Initialize dispatcher with action registry."""
        self._actions: dict[str, Callable] = self._register_actions()
        self._lazy_services = {}  # Lazy-loaded service instances
    
    def _register_actions(self) -> dict[str, Callable]:
        """Build action registry mapping action names to handler functions.
        
        Returns:
            Dictionary mapping action name (str) to handler method (Callable)
        """
        return {
            # === WORKFLOWS ===
            "run_workflow": self._run_workflow,
            "resume_workflow": self._resume_workflow,
            "list_workflows": self._list_workflows,
            "get_workflow": self._get_workflow,
            
            # === DISCOVERY ===
            "map_environment": self._map_environment,
            "profile_source": self._profile_source,
            "profile_folder": self._profile_folder,
            
            # === INSPECTION ===
            "story_read": self._story_read,
            "node_sample": self._node_sample,
            "node_failed_rows": self._node_failed_rows,
            "lineage_graph": self._lineage_graph,
            
            # === CONSTRUCTION ===
            "list_transformers": self._list_transformers,
            "list_patterns": self._list_patterns,
            "apply_pattern_template": self._apply_pattern_template,
            "suggest_pipeline": self._suggest_pipeline,
            "create_ingestion_pipeline": self._create_ingestion_pipeline,
            
            # === VALIDATION ===
            "validate_yaml": self._validate_yaml,
            "validate_pipeline": self._validate_pipeline,
            "test_pipeline": self._test_pipeline,
            "diagnose": self._diagnose,
            
            # === TASK GUIDANCE ===
            "get_task_guidance": self._get_task_guidance,
            "list_task_types": self._list_task_types,
            
            # === ONBOARDING ===
            "onboard": self._onboard,
            "get_schema": self._get_schema,
            "search_docs": self._search_docs,
            "get_doc": self._get_doc,
            "list_docs": self._list_docs,
            "list_examples": self._list_examples,
            "get_example": self._get_example,
            "list_skills": self._list_skills,
            "get_skill": self._get_skill,
            
            # === DOWNLOAD ===
            "download_sql": self._download_sql,
            "download_table": self._download_table,
            "download_file": self._download_file,
            
            # === SESSION BUILDER ===
            "create_pipeline": self._create_pipeline,
            "add_node": self._add_node,
            "configure_read": self._configure_read,
            "configure_write": self._configure_write,
            "configure_transform": self._configure_transform,
            "get_pipeline_state": self._get_pipeline_state,
            "render_pipeline_yaml": self._render_pipeline_yaml,
            "list_sessions": self._list_sessions,
            "discard_pipeline": self._discard_pipeline,
        }
    
    def dispatch(self, action: str, *args, **kwargs) -> dict[str, Any]:
        """Execute an action by name with args.
        
        Args:
            action: Action name (e.g., 'profile_source', 'run_workflow')
            *args: Positional arguments for the action
            **kwargs: Keyword arguments for the action
            
        Returns:
            Action result as dictionary
        """
        if action not in self._actions:
            return {
                "error": f"Unknown action: {action}",
                "tip": "Run odibi_help() to see available actions",
                "valid_actions": sorted(self._actions.keys()),
                "categories": [
                    "Workflows", "Discovery", "Inspection", "Construction",
                    "Validation", "Task Guidance", "Onboarding", "Download",
                    "Session Builder"
                ]
            }
        
        try:
            result = self._actions[action](*args, **kwargs)
            # Ensure result is serializable
            return self._to_serializable(result)
        except TypeError as e:
            # Signature mismatch - provide helpful error
            import inspect
            sig = inspect.signature(self._actions[action])
            return {
                "error": f"Invalid arguments for {action}: {str(e)}",
                "action": action,
                "expected_signature": str(sig),
                "tip": f"Run odibi_help(action='{action}') for usage details"
            }
        except Exception as e:
            return {
                "error": str(e),
                "action": action,
                "tip": f"Run odibi_help(action='{action}') for usage details"
            }
    
    def help(self, category: str | None = None, action: str | None = None) -> dict[str, Any]:
        """Generate help documentation.
        
        Args:
            category: Optional category filter (e.g., "Workflows", "Discovery")
            action: Optional action name for detailed help
            
        Returns:
            Help documentation as dictionary
        """
        if action:
            return self._action_help(action)
        if category:
            return self._category_help(category)
        return self._full_help()
    
    # === HELP SYSTEM ===
    
    def _full_help(self) -> dict[str, Any]:
        """Return complete action catalog organized by category."""
        return {
            "kind": "odibi_help",
            "version": "1.0",
            "categories": [
                {
                    "name": "Workflows",
                    "description": "Multi-step deterministic recipes with pause/resume",
                    "action_count": 4,
                    "actions": ["run_workflow", "resume_workflow", "list_workflows", "get_workflow"]
                },
                {
                    "name": "Discovery",
                    "description": "Environment mapping and data profiling",
                    "action_count": 3,
                    "actions": ["map_environment", "profile_source", "profile_folder"]
                },
                {
                    "name": "Inspection",
                    "description": "Post-execution analysis of pipeline runs",
                    "action_count": 4,
                    "actions": ["story_read", "node_sample", "node_failed_rows", "lineage_graph"]
                },
                {
                    "name": "Construction",
                    "description": "Pipeline building from patterns and templates",
                    "action_count": 5,
                    "actions": ["list_transformers", "list_patterns", "apply_pattern_template", "suggest_pipeline", "create_ingestion_pipeline"]
                },
                {
                    "name": "Validation",
                    "description": "Testing, validation, and diagnostics",
                    "action_count": 4,
                    "actions": ["validate_yaml", "validate_pipeline", "test_pipeline", "diagnose"]
                },
                {
                    "name": "Task Guidance",
                    "description": "Structured Q&A for parameter collection",
                    "action_count": 2,
                    "actions": ["get_task_guidance", "list_task_types"]
                },
                {
                    "name": "Onboarding",
                    "description": "First-time setup, schema discovery, documentation",
                    "action_count": 9,
                    "actions": ["onboard", "get_schema", "search_docs", "get_doc", "list_docs", "list_examples", "get_example", "list_skills", "get_skill"]
                },
                {
                    "name": "Download",
                    "description": "Export data or generated code",
                    "action_count": 3,
                    "actions": ["download_sql", "download_table", "download_file"]
                },
                {
                    "name": "Session Builder",
                    "description": "Incremental YAML construction (stateful)",
                    "action_count": 9,
                    "actions": ["create_pipeline", "add_node", "configure_read", "configure_write", "configure_transform", "get_pipeline_state", "render_pipeline_yaml", "list_sessions", "discard_pipeline"]
                }
            ],
            "total_actions": 43,
            "usage": {
                "discovery": "odibi_help(category='Workflows')",
                "action_details": "odibi_help(action='profile_source')",
                "execution": "odibi_execute('profile_source', '{\"connection\": \"s3_raw\", \"path\": \"orders.csv\"}')"
            }
        }
    
    def _category_help(self, category: str) -> dict[str, Any]:
        """Return actions in a specific category."""
        # Action catalog organized by category
        actions_by_category = {
            "Workflows": [
                {"name": "run_workflow", "signature": "workflow_name, params=None", "description": "Execute named workflow"},
                {"name": "resume_workflow", "signature": "resume_token, inputs=None", "description": "Continue paused workflow"},
                {"name": "list_workflows", "signature": "", "description": "Available workflow names + descriptions"},
                {"name": "get_workflow", "signature": "workflow_name", "description": "Full workflow definition"},
            ],
            "Discovery": [
                {"name": "map_environment", "signature": "connection=None", "description": "List connections, databases, high-level schema"},
                {"name": "profile_source", "signature": "connection, path, max_rows=100", "description": "Schema, stats, nulls, cardinality, sample data"},
                {"name": "profile_folder", "signature": "connection, folder_path", "description": "List files with metadata (size, format, mod time)"},
            ],
            "Inspection": [
                {"name": "story_read", "signature": "pipeline, run_id=None", "description": "Execution summary, node statuses, failure counts"},
                {"name": "node_sample", "signature": "pipeline, node, limit=10", "description": "Fetch successful output rows"},
                {"name": "node_failed_rows", "signature": "pipeline, node, limit=10", "description": "Fetch quarantined rows with failure reasons"},
                {"name": "lineage_graph", "signature": "pipeline", "description": "Visual flow diagram (nodes + edges)"},
            ],
            "Construction": [
                {"name": "list_transformers", "signature": "category=None", "description": "Available transformer names + descriptions"},
                {"name": "list_patterns", "signature": "", "description": "Pipeline patterns (dimension_scd1, dimension_scd2, fact, etc.)"},
                {"name": "apply_pattern_template", "signature": "pattern, table_name, connection, source_path", "description": "Generate YAML from pattern"},
                {"name": "suggest_pipeline", "signature": "source_path, connection, intent", "description": "Smart chain recommendation based on source data"},
                {"name": "create_ingestion_pipeline", "signature": "source_path, connection, target_table", "description": "Opinionated bronze → silver ingestion"},
            ],
            "Validation": [
                {"name": "validate_yaml", "signature": "yaml_content", "description": "Config structure check (Pydantic strict validation)"},
                {"name": "validate_pipeline", "signature": "pipeline", "description": "Dry-run validation (parse YAML, check connections, validate config)"},
                {"name": "test_pipeline", "signature": "pipeline, sample_size=100", "description": "Full test with data (reads source, executes transforms, writes to temp)"},
                {"name": "diagnose", "signature": "pipeline, error_context=None", "description": "Systematic troubleshooting (validation → connection → schema → transform analysis)"},
            ],
            "Task Guidance": [
                {"name": "get_task_guidance", "signature": "task_type", "description": "Structured questions + defaults for a task"},
                {"name": "list_task_types", "signature": "", "description": "Available task types with descriptions"},
            ],
            "Onboarding": [
                {"name": "onboard", "signature": "", "description": "System overview, available categories, quick start"},
                {"name": "get_schema", "signature": "component=None", "description": "Odibi config contract (Pydantic models → JSON schema)"},
                {"name": "search_docs", "signature": "query", "description": "Full-text search across 80+ documentation files"},
                {"name": "get_doc", "signature": "doc_path", "description": "Retrieve specific documentation markdown"},
                {"name": "list_docs", "signature": "category=None", "description": "List available documentation files"},
                {"name": "list_examples", "signature": "pattern=None", "description": "List runnable example pipeline YAMLs"},
                {"name": "get_example", "signature": "pattern_name", "description": "Get worked example pipeline (YAML + explanation)"},
                {"name": "list_skills", "signature": "", "description": "List loadable Odibi skills"},
                {"name": "get_skill", "signature": "name", "description": "Load skill's full guidance by name"},
            ],
            "Download": [
                {"name": "download_sql", "signature": "pipeline", "description": "Export pipeline as SQL DDL/DML"},
                {"name": "download_table", "signature": "pipeline, node, format='csv'", "description": "Export node output as DataFrame"},
                {"name": "download_file", "signature": "pipeline, destination", "description": "Write pipeline YAML to file"},
            ],
            "Session Builder": [
                {"name": "create_pipeline", "signature": "name, engine='pandas'", "description": "Start new pipeline session"},
                {"name": "add_node", "signature": "node_name, transformer, params", "description": "Add transform node to session"},
                {"name": "configure_read", "signature": "connection, path, format=None, options=None", "description": "Set source config"},
                {"name": "configure_write", "signature": "connection, path, mode='overwrite', options=None", "description": "Set target config"},
                {"name": "configure_transform", "signature": "node_name, params", "description": "Update node config"},
                {"name": "get_pipeline_state", "signature": "", "description": "Current session state (nodes, read, write)"},
                {"name": "render_pipeline_yaml", "signature": "", "description": "Generate YAML from session state"},
                {"name": "list_sessions", "signature": "", "description": "List active pipeline builder sessions"},
                {"name": "discard_pipeline", "signature": "", "description": "Clear session, start fresh"},
            ],
        }
        
        if category not in actions_by_category:
            return {
                "error": f"Unknown category: {category}",
                "valid_categories": list(actions_by_category.keys()),
                "tip": "Run odibi_help() to see all categories"
            }
        
        return {
            "kind": "category_help",
            "category": category,
            "actions": actions_by_category[category],
            "usage_example": f"odibi_execute('{actions_by_category[category][0]['name']}', '{{...}}')"
        }
    
    def _action_help(self, action: str) -> dict[str, Any]:
        """Return detailed help for a specific action."""
        # Full action documentation with examples
        action_docs = {
            "profile_source": {
                "signature": "connection, path, max_rows=100",
                "description": "Profile a data source (CSV/Parquet/JSON/Delta). Returns schema, stats, nulls, cardinality, sample data.",
                "args": [
                    {"name": "connection", "type": "str", "required": True, "description": "Connection name (run map_environment to list)"},
                    {"name": "path", "type": "str", "required": True, "description": "File path or table name"},
                    {"name": "max_rows", "type": "int", "required": False, "default": 100, "description": "Sample size for profiling"},
                ],
                "returns": {
                    "schema": "List of columns with types, nulls, cardinality",
                    "stats": "Min/max/mean for numeric columns, top values for categorical",
                    "sample": "First N rows",
                    "findings": "Observations (e.g. '82% nulls in optional_field — normal for sparse data')",
                    "risks": "Conditional warnings (e.g. 'If order_id is meant to be unique, duplicates detected')"
                },
                "examples": [
                    {
                        "description": "Profile a CSV file",
                        "code": "odibi_execute('profile_source', '{\"connection\": \"s3_raw\", \"path\": \"orders.csv\", \"max_rows\": 1000}')"
                    },
                    {
                        "description": "Profile a Delta table",
                        "code": "odibi_execute('profile_source', '{\"connection\": \"delta_lake\", \"path\": \"catalog.schema.table\"}')"
                    }
                ]
            },
            "run_workflow": {
                "signature": "workflow_name, params=None",
                "description": "Execute a deterministic workflow with built-in retry logic, loops, and pause/resume capability.",
                "args": [
                    {"name": "workflow_name", "type": "str", "required": True, "description": "Workflow name (see list_workflows)"},
                    {"name": "params", "type": "dict", "required": False, "default": None, "description": "Workflow parameters (YAML path, pipeline name, etc.)"},
                ],
                "returns": {
                    "status": "COMPLETED, AWAITING_INPUT, or FAILED",
                    "resume_token": "If paused, token for resume_workflow",
                    "outputs": "Workflow outputs (YAML content, validation results, etc.)",
                    "prompts": "If paused, questions to ask user for next inputs"
                },
                "examples": [
                    {
                        "description": "Build and validate a pipeline",
                        "code": "odibi_execute('run_workflow', '{\"workflow_name\": \"build_and_validate\", \"params\": {}}')"
                    },
                    {
                        "description": "Debug a failed pipeline run",
                        "code": "odibi_execute('run_workflow', '{\"workflow_name\": \"debug_failed_run\", \"params\": {\"pipeline\": \"my_pipeline\"}}')"
                    }
                ]
            },
            # Add more action docs as needed...
        }
        
        if action not in action_docs:
            # Return basic info from registry if no detailed docs yet
            if action in self._actions:
                import inspect
                sig = inspect.signature(self._actions[action])
                return {
                    "kind": "action_help",
                    "action": action,
                    "signature": str(sig),
                    "description": "Documentation coming soon",
                    "tip": f"Action exists but detailed docs not yet written. Try calling it to see what it returns."
                }
            return {
                "error": f"Unknown action: {action}",
                "tip": "Run odibi_help() to see all actions"
            }
        
        return {
            "kind": "action_help",
            "action": action,
            **action_docs[action]
        }
    
    # === SERIALIZATION HELPERS ===
    
    def _to_serializable(self, obj: Any) -> Any:
        """Convert objects to JSON-serializable format."""
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, BaseModel):
            return obj.model_dump(mode="json")
        if is_dataclass(obj) and not isinstance(obj, type):
            return {k: self._to_serializable(v) for k, v in asdict(obj).items()}
        if isinstance(obj, dict):
            return {k: self._to_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._to_serializable(v) for v in obj]
        if hasattr(obj, "__dict__"):
            return {k: self._to_serializable(v) for k, v in obj.__dict__.items()}
        return str(obj)
    
    # === ACTION HANDLERS (delegate to facade tools) ===
    
    # Workflows
    def _run_workflow(self, workflow_name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute a named workflow."""
        from odibi_mcp.tools.workflows import run_workflow
        return run_workflow(workflow_name, params or {})
    
    def _resume_workflow(self, resume_token: str, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
        """Continue paused workflow."""
        from odibi_mcp.tools.workflows import resume_workflow
        return resume_workflow(resume_token, inputs or {})
    
    def _list_workflows(self) -> dict[str, Any]:
        """List available workflows."""
        from odibi_mcp.tools.workflows import list_workflows
        return list_workflows()
    
    def _get_workflow(self, workflow_name: str) -> dict[str, Any]:
        """Get workflow definition."""
        from odibi_mcp.tools.workflows import get_workflow
        return get_workflow(workflow_name)
    
    # Discovery
    def _map_environment(self, connection: str | None = None) -> dict[str, Any]:
        """List connections and environment info."""
        from odibi_mcp.tools.smart import map_environment
        return map_environment(connection)
    
    def _profile_source(self, connection: str, path: str, max_rows: int = 100) -> dict[str, Any]:
        """Profile a data source."""
        from odibi_mcp.tools.smart import profile_source
        return profile_source(connection, path, max_rows)
    
    def _profile_folder(self, connection: str, folder_path: str) -> dict[str, Any]:
        """List files in a folder."""
        from odibi_mcp.tools.smart import profile_folder
        return profile_folder(connection, folder_path)
    
    # Inspection
    def _story_read(self, pipeline: str, run_id: str | None = None) -> dict[str, Any]:
        """Read pipeline execution story."""
        from odibi_mcp.tools.story import story_read
        return story_read(pipeline, run_id)
    
    def _node_sample(self, pipeline: str, node: str, limit: int = 10) -> dict[str, Any]:
        """Sample node output."""
        from odibi_mcp.tools.story import node_sample
        return node_sample(pipeline, node, limit)
    
    def _node_failed_rows(self, pipeline: str, node: str, limit: int = 10) -> dict[str, Any]:
        """Fetch quarantined rows."""
        from odibi_mcp.tools.story import node_failed_rows
        return node_failed_rows(pipeline, node, limit)
    
    def _lineage_graph(self, pipeline: str) -> dict[str, Any]:
        """Generate lineage graph."""
        from odibi_mcp.tools.story import lineage_graph
        return lineage_graph(pipeline)
    
    # Construction
    def _list_transformers(self, category: str | None = None) -> dict[str, Any]:
        """List available transformers."""
        from odibi_mcp.tools.construction import list_transformers
        return list_transformers(category)
    
    def _list_patterns(self) -> dict[str, Any]:
        """List pipeline patterns."""
        from odibi_mcp.tools.construction import list_patterns
        return list_patterns()
    
    def _apply_pattern_template(self, pattern: str, table_name: str, connection: str, source_path: str) -> dict[str, Any]:
        """Generate YAML from pattern."""
        from odibi_mcp.tools.construction import apply_pattern_template
        return apply_pattern_template(pattern, table_name, connection, source_path)
    
    def _suggest_pipeline(self, source_path: str, connection: str, intent: str) -> dict[str, Any]:
        """Suggest pipeline based on data."""
        from odibi_mcp.tools.phase3_smart import suggest_pipeline
        return suggest_pipeline(source_path, connection, intent)
    
    def _create_ingestion_pipeline(self, source_path: str, connection: str, target_table: str) -> dict[str, Any]:
        """Create ingestion pipeline."""
        from odibi_mcp.tools.phase3_smart import create_ingestion_pipeline
        return create_ingestion_pipeline(source_path, connection, target_table)
    
    # Validation
    def _validate_yaml(self, yaml_content: str) -> dict[str, Any]:
        """Validate YAML structure."""
        from odibi_mcp.tools.yaml_builder import validate_odibi_config
        return validate_odibi_config(yaml_content)
    
    def _validate_pipeline(self, pipeline: str) -> dict[str, Any]:
        """Validate pipeline config."""
        from odibi_mcp.tools.validation import validate_pipeline
        return validate_pipeline(pipeline)
    
    def _test_pipeline(self, pipeline: str, sample_size: int = 100) -> dict[str, Any]:
        """Test pipeline with data."""
        from odibi_mcp.tools.execution import test_pipeline
        return test_pipeline(pipeline, sample_size)
    
    def _diagnose(self, pipeline: str, error_context: str | None = None) -> dict[str, Any]:
        """Diagnose pipeline issues."""
        from odibi_mcp.tools.diagnose import diagnose
        return diagnose(pipeline, error_context)
    
    # Task Guidance
    def _get_task_guidance(self, task_type: str) -> dict[str, Any]:
        """Get structured task guidance."""
        from odibi_mcp.tools.guidance import get_task_guidance
        return get_task_guidance(task_type)
    
    def _list_task_types(self) -> dict[str, Any]:
        """List available task types."""
        from odibi_mcp.tools.guidance import list_task_types
        return list_task_types()
    
    # Onboarding — delegate to the OdibiKnowledge singleton's methods.
    # (get_knowledge() returns the instance; call its methods — it does NOT take an action arg.)
    def _onboard(self) -> dict[str, Any]:
        """Get onboarding information."""
        from odibi_mcp.knowledge import get_knowledge
        return get_knowledge().onboard()

    def _get_schema(self, component: str | None = None) -> dict[str, Any]:
        """Get config schema."""
        from odibi_mcp.knowledge import get_knowledge
        return get_knowledge().get_schema(component)

    def _search_docs(self, query: str) -> dict[str, Any]:
        """Search documentation."""
        from odibi_mcp.knowledge import get_knowledge
        return {"results": get_knowledge().search_docs(query)}

    def _get_doc(self, doc_path: str) -> dict[str, Any]:
        """Get documentation file."""
        from odibi_mcp.knowledge import get_knowledge
        return get_knowledge().get_doc(doc_path)

    def _list_docs(self, category: str | None = None) -> dict[str, Any]:
        """List documentation files."""
        from odibi_mcp.knowledge import get_knowledge
        return {"docs": get_knowledge().list_docs(category)}

    def _list_examples(self, pattern: str | None = None) -> dict[str, Any]:
        """List example pipelines."""
        from odibi_mcp.knowledge import get_knowledge
        return {"examples": get_knowledge().list_examples(pattern)}

    def _get_example(self, pattern_name: str) -> dict[str, Any]:
        """Get example pipeline."""
        from odibi_mcp.knowledge import get_knowledge
        return get_knowledge().get_example(pattern_name)

    def _list_skills(self) -> dict[str, Any]:
        """List available skills."""
        from odibi_mcp.knowledge import get_knowledge
        return {"skills": get_knowledge().list_skills()}

    def _get_skill(self, name: str) -> dict[str, Any]:
        """Get skill content."""
        from odibi_mcp.knowledge import get_knowledge
        return get_knowledge().get_skill(name)
    
    # Download
    def _download_sql(self, pipeline: str) -> dict[str, Any]:
        """Export pipeline as SQL."""
        from odibi_mcp.tools.smart import download_sql
        return download_sql(pipeline)
    
    def _download_table(self, pipeline: str, node: str, format: str = "csv") -> dict[str, Any]:
        """Export node output as table."""
        from odibi_mcp.tools.smart import download_table
        return download_table(pipeline, node, format)
    
    def _download_file(self, pipeline: str, destination: str) -> dict[str, Any]:
        """Write pipeline YAML to file."""
        from odibi_mcp.tools.smart import download_file
        return download_file(pipeline, destination)
    
    # Session Builder
    def _create_pipeline(self, name: str, engine: str = "pandas") -> dict[str, Any]:
        """Create pipeline session."""
        from odibi_mcp.tools.builder import create_pipeline
        return create_pipeline(name, engine)
    
    def _add_node(self, node_name: str, transformer: str, params: dict[str, Any]) -> dict[str, Any]:
        """Add node to pipeline session."""
        from odibi_mcp.tools.builder import add_node
        return add_node(node_name, transformer, params)
    
    def _configure_read(self, connection: str, path: str, format: str | None = None, options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Configure read block."""
        from odibi_mcp.tools.builder import configure_read
        return configure_read(connection, path, format, options)
    
    def _configure_write(self, connection: str, path: str, mode: str = "overwrite", options: dict[str, Any] | None = None) -> dict[str, Any]:
        """Configure write block."""
        from odibi_mcp.tools.builder import configure_write
        return configure_write(connection, path, mode, options)
    
    def _configure_transform(self, node_name: str, params: dict[str, Any]) -> dict[str, Any]:
        """Update node configuration."""
        from odibi_mcp.tools.builder import configure_transform
        return configure_transform(node_name, params)
    
    def _get_pipeline_state(self) -> dict[str, Any]:
        """Get current pipeline session state."""
        from odibi_mcp.tools.builder import get_pipeline_state
        return get_pipeline_state()
    
    def _render_pipeline_yaml(self) -> dict[str, Any]:
        """Render pipeline YAML from session."""
        from odibi_mcp.tools.builder import render_pipeline_yaml
        return render_pipeline_yaml()
    
    def _list_sessions(self) -> dict[str, Any]:
        """List active sessions."""
        from odibi_mcp.tools.builder import list_sessions
        return list_sessions()
    
    def _discard_pipeline(self) -> dict[str, Any]:
        """Discard current pipeline session."""
        from odibi_mcp.tools.builder import discard_pipeline
        return discard_pipeline()
