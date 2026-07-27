"""Universal action dispatcher for Odibi MCP gateway.

This module implements the 2-tool universal gateway pattern, compressing 37+ actions
into a single dispatch surface. Based on the proven context_workbench architecture.
"""

from __future__ import annotations

from threading import RLock
from typing import Any, Callable
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pydantic import BaseModel

import odibi.planning as immutable_planning

try:
    from odibi_mcp.contracts.access import (
        RUNTIME_DATA_ACTIONS,
        ActionEffect,
        ApplicationIdentity,
        ManagedProjectAccess,
        PreparedRuntimeCall,
        RemoteLogicalLineageProjection,
        RemotePatternRenderProjection,
        RuntimeAccessDenied,
        prepare_remote_pattern_render,
        render_remote_logical_lineage_projection,
        sanitize_runtime_result,
    )
except ImportError:  # Flat Databricks workspace deployment
    from contracts.access import (
        RUNTIME_DATA_ACTIONS,
        ActionEffect,
        ApplicationIdentity,
        ManagedProjectAccess,
        PreparedRuntimeCall,
        RemoteLogicalLineageProjection,
        RemotePatternRenderProjection,
        RuntimeAccessDenied,
        prepare_remote_pattern_render,
        render_remote_logical_lineage_projection,
        sanitize_runtime_result,
    )

try:
    from odibi_mcp.tools.workflows import (
        get_workflow as _get_workflow_definition,
        list_workflows as _list_workflow_definitions,
        resume_workflow as _resume_workflow_execution,
        run_workflow as _run_workflow_execution,
    )
except ImportError:  # Flat Databricks workspace deployment
    from tools.workflows import (
        get_workflow as _get_workflow_definition,
        list_workflows as _list_workflow_definitions,
        resume_workflow as _resume_workflow_execution,
        run_workflow as _run_workflow_execution,
    )


ACTION_EFFECTS: dict[str, ActionEffect] = {
    # Workflows
    "run_workflow": ActionEffect.EXECUTION,
    "resume_workflow": ActionEffect.EXECUTION,
    "list_workflows": ActionEffect.PUBLIC_READ,
    "get_workflow": ActionEffect.PUBLIC_READ,
    # Discovery
    "map_environment": ActionEffect.SENSITIVE_READ,
    "profile_source": ActionEffect.SENSITIVE_READ,
    "profile_folder": ActionEffect.SENSITIVE_READ,
    # Inspection
    "story_read": ActionEffect.SENSITIVE_READ,
    "node_sample": ActionEffect.SENSITIVE_READ,
    "node_failed_rows": ActionEffect.SENSITIVE_READ,
    "lineage_graph": ActionEffect.SENSITIVE_READ,
    # Construction
    "list_transformers": ActionEffect.PUBLIC_READ,
    "list_patterns": ActionEffect.PUBLIC_READ,
    # Rendering reads and may update shared project connection context.
    "apply_pattern_template": ActionEffect.SENSITIVE_READ,
    "suggest_pipeline": ActionEffect.SENSITIVE_READ,
    "create_ingestion_pipeline": ActionEffect.SESSION_MUTATION,
    # Validation
    "validate_yaml": ActionEffect.PUBLIC_READ,
    "validate_pipeline": ActionEffect.PUBLIC_READ,
    "test_pipeline": ActionEffect.EXECUTION,
    "diagnose": ActionEffect.SENSITIVE_READ,
    # Task guidance
    "get_task_guidance": ActionEffect.PUBLIC_READ,
    "list_task_types": ActionEffect.PUBLIC_READ,
    # Onboarding
    "onboard": ActionEffect.PUBLIC_READ,
    "get_schema": ActionEffect.PUBLIC_READ,
    "search_docs": ActionEffect.PUBLIC_READ,
    "get_doc": ActionEffect.SENSITIVE_READ,
    "list_docs": ActionEffect.PUBLIC_READ,
    "list_examples": ActionEffect.PUBLIC_READ,
    "get_example": ActionEffect.SENSITIVE_READ,
    "list_skills": ActionEffect.PUBLIC_READ,
    "get_skill": ActionEffect.SENSITIVE_READ,
    # Download
    "download_sql": ActionEffect.FILE_WRITE,
    "download_table": ActionEffect.FILE_WRITE,
    "download_file": ActionEffect.FILE_WRITE,
    # Session builder
    "create_pipeline": ActionEffect.SESSION_MUTATION,
    "add_node": ActionEffect.SESSION_MUTATION,
    "configure_read": ActionEffect.SESSION_MUTATION,
    "configure_write": ActionEffect.SESSION_MUTATION,
    "configure_transform": ActionEffect.SESSION_MUTATION,
    "get_pipeline_state": ActionEffect.SENSITIVE_READ,
    "render_pipeline_yaml": ActionEffect.SENSITIVE_READ,
    "list_sessions": ActionEffect.SENSITIVE_READ,
    "discard_pipeline": ActionEffect.SESSION_MUTATION,
}

_RUNTIME_ACCESS_MESSAGES = {
    "PROJECT_SCOPE_REQUIRED": "A valid managed project scope is required for this action",
    "INVALID_RUNTIME_ARGUMENT": "Runtime data arguments are invalid",
    "PATH_SCOPE_REQUIRED": "A valid contained data path is required for this action",
    "PRIVACY_LIMIT_REQUIRED": "A valid bounded privacy limit is required for this action",
    "DOWNLOAD_FORMAT_REQUIRED": "The requested download format is not allowed",
    "EXPORT_SCOPE_REQUIRED": "A valid controlled export destination is required",
    "PHYSICAL_REFERENCES_DISABLED": "Remote physical references are unavailable",
    "REMOTE_WORKFLOW_DISABLED": "This workflow is unavailable over the remote transport",
    "REMOTE_RENDERING_DISABLED": "This rendering action is unavailable over the remote transport",
    "REMOTE_RENDER_PROJECTION_REQUIRED": "The remote-safe rendering projection is required",
    "PROJECTED_RENDER_FAILED": "The remote-safe template could not be rendered",
    "LOGICAL_PROJECTION_UNAVAILABLE": "The logical lineage projection is unavailable",
    "RUNTIME_DATA_UNAVAILABLE": "Runtime data is unavailable",
}

_REMOTE_SAFE_WORKFLOWS = frozenset({"validate_yaml_simple"})
_REMOTE_DISABLED_RENDERING_ACTIONS = frozenset(
    {"create_ingestion_pipeline", "render_pipeline_yaml"}
)
_RUNTIME_CONTEXT_LOCK = RLock()


def _validate_legacy_sample_size(sample_size: int) -> None:
    """Validate the deprecated ignored direct-planner compatibility parameter."""
    if type(sample_size) is not int:
        raise TypeError("sample_size must be an integer")
    if not 1 <= sample_size <= 1000:
        raise ValueError("sample_size must be between 1 and 1000")


class OdibiDispatcher:
    """Universal action dispatcher for Odibi MCP gateway.

    Routes action names to handler methods, provides help/discovery system,
    and manages pause/resume workflow state.
    """

    def __init__(self, managed_access: ManagedProjectAccess | None = None):
        """Initialize dispatcher with action registry."""
        self._actions: dict[str, Callable] = self._register_actions()
        registered = set(self._actions)
        classified = set(ACTION_EFFECTS)
        if registered != classified:
            missing = sorted(registered - classified)
            stale = sorted(classified - registered)
            raise RuntimeError(f"Action effect policy mismatch: missing={missing}, stale={stale}")
        self._lazy_services = {}  # Lazy-loaded service instances
        self._managed_access = managed_access

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

    def dispatch(
        self,
        action: str,
        *args,
        application_identity: ApplicationIdentity | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Execute an action by name with args.

        Args:
            action: Action name (e.g., 'profile_source', 'run_workflow')
            *args: Positional arguments for the action
            application_identity: Transport-authenticated principal, when available
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
                    "Workflows",
                    "Discovery",
                    "Inspection",
                    "Construction",
                    "Validation",
                    "Task Guidance",
                    "Onboarding",
                    "Download",
                    "Session Builder",
                ],
            }

        effect = ACTION_EFFECTS[action]
        if effect is not ActionEffect.PUBLIC_READ:
            if not isinstance(application_identity, ApplicationIdentity):
                return {
                    "error": "Application identity is required for this action",
                    "code": "AUTHORIZATION_REQUIRED",
                    "action": action,
                    "effect": effect.value,
                }
            if not application_identity.authorizes(effect):
                return {
                    "error": "Application identity is not authorized for this action",
                    "code": "FORBIDDEN",
                    "action": action,
                    "effect": effect.value,
                }

        is_remote_identity = (
            isinstance(application_identity, ApplicationIdentity)
            and application_identity.subject != "trusted-local"
        )
        is_remote_lineage = is_remote_identity and action == "lineage_graph"
        remote_pattern_projection: RemotePatternRenderProjection | None = None
        if is_remote_identity and action == "apply_pattern_template":
            try:
                remote_pattern_projection = prepare_remote_pattern_render(args, kwargs)
            except RuntimeAccessDenied as error:
                return self._runtime_access_error(action, error.code)
            except Exception:
                return self._runtime_access_error(action, "REMOTE_RENDER_PROJECTION_REQUIRED")
        elif is_remote_identity and action in _REMOTE_DISABLED_RENDERING_ACTIONS:
            return self._runtime_access_error(action, "REMOTE_RENDERING_DISABLED")
        if is_remote_identity and action in {"run_workflow", "resume_workflow"}:
            if args:
                return self._runtime_access_error(action, "INVALID_RUNTIME_ARGUMENT")
            workflow_name = kwargs.get("workflow_name")
            if action == "resume_workflow" or (
                action == "run_workflow" and workflow_name not in _REMOTE_SAFE_WORKFLOWS
            ):
                return self._runtime_access_error(action, "REMOTE_WORKFLOW_DISABLED")

        prepared_runtime_call: PreparedRuntimeCall | None = None
        remote_lineage_projection: RemoteLogicalLineageProjection | None = None
        if action in RUNTIME_DATA_ACTIONS and is_remote_identity:
            if args:
                return self._runtime_access_error(action, "INVALID_RUNTIME_ARGUMENT")
            try:
                access = self._managed_access or ManagedProjectAccess.from_environment()
                if is_remote_lineage and type(access) is not ManagedProjectAccess:
                    return self._runtime_access_error(action, "LOGICAL_PROJECTION_UNAVAILABLE")
                prepared = access.prepare(action, kwargs)
                if is_remote_lineage:
                    if (
                        type(prepared) is not PreparedRuntimeCall
                        or prepared.action != action
                        or type(prepared.logical_lineage) is not RemoteLogicalLineageProjection
                    ):
                        return self._runtime_access_error(action, "LOGICAL_PROJECTION_UNAVAILABLE")
                    remote_lineage_projection = prepared.logical_lineage
                else:
                    kwargs = prepared.kwargs
                prepared_runtime_call = prepared
            except RuntimeAccessDenied as error:
                return self._runtime_access_error(action, error.code)
            except Exception:
                code = (
                    "LOGICAL_PROJECTION_UNAVAILABLE"
                    if is_remote_lineage
                    else "PROJECT_SCOPE_REQUIRED"
                )
                return self._runtime_access_error(action, code)

        try:
            with _RUNTIME_CONTEXT_LOCK:
                previous_context = None
                if prepared_runtime_call is not None and not is_remote_lineage:
                    previous_context = self._bind_runtime_context(prepared_runtime_call)
                try:
                    if remote_pattern_projection is not None:
                        result = self._render_remote_pattern_projection(remote_pattern_projection)
                    elif is_remote_lineage:
                        assert remote_lineage_projection is not None
                        result = render_remote_logical_lineage_projection(remote_lineage_projection)
                    else:
                        result = self._actions[action](*args, **kwargs)
                    # Ensure result is serializable
                    serialized = self._to_serializable(result)
                    if prepared_runtime_call is not None and not is_remote_lineage:
                        return sanitize_runtime_result(serialized, prepared_runtime_call)
                    return serialized
                finally:
                    if prepared_runtime_call is not None and not is_remote_lineage:
                        self._restore_runtime_context(previous_context)
        except RuntimeAccessDenied as error:
            if is_remote_lineage:
                return self._runtime_access_error(action, "LOGICAL_PROJECTION_UNAVAILABLE")
            return self._runtime_access_error(action, error.code)
        except TypeError as e:
            if remote_pattern_projection is not None:
                return self._runtime_access_error(action, "PROJECTED_RENDER_FAILED")
            if is_remote_lineage:
                return self._runtime_access_error(action, "LOGICAL_PROJECTION_UNAVAILABLE")
            if prepared_runtime_call is not None:
                return self._runtime_access_error(action, "INVALID_RUNTIME_ARGUMENT")
            # Signature mismatch - provide helpful error
            import inspect

            sig = inspect.signature(self._actions[action])
            return {
                "error": f"Invalid arguments for {action}: {str(e)}",
                "action": action,
                "expected_signature": str(sig),
                "tip": f"Run odibi_help(action='{action}') for usage details",
            }
        except Exception as error:
            if remote_pattern_projection is not None:
                return self._runtime_access_error(action, "PROJECTED_RENDER_FAILED")
            if is_remote_lineage:
                return self._runtime_access_error(action, "LOGICAL_PROJECTION_UNAVAILABLE")
            if prepared_runtime_call is not None:
                return {
                    "error": "Runtime data is unavailable",
                    "code": "RUNTIME_DATA_UNAVAILABLE",
                    "action": action,
                }
            return {
                "error": str(error),
                "action": action,
                "tip": f"Run odibi_help(action='{action}') for usage details",
            }

    @staticmethod
    def _runtime_access_error(action: str, code: str) -> dict[str, Any]:
        """Return a stable denial without echoing caller or host values."""
        return {
            "error": _RUNTIME_ACCESS_MESSAGES.get(
                code, "Runtime data access is unavailable for this action"
            ),
            "code": code,
            "action": action,
        }

    @staticmethod
    def _bind_runtime_context(prepared: PreparedRuntimeCall):
        """Bind the exact validated snapshot without initializing connections."""
        try:
            from odibi_mcp.context import (
                MCPProjectContext,
                get_project_context,
                set_project_context,
            )
        except ImportError:  # Flat Databricks workspace deployment
            from context import MCPProjectContext, get_project_context, set_project_context

        current = get_project_context()
        snapshot = prepared.validated_config_snapshot()
        set_project_context(MCPProjectContext.from_config_snapshot(prepared.config_path, snapshot))
        return current

    @staticmethod
    def _restore_runtime_context(previous_context) -> None:
        """Restore the process-global context while still holding the dispatcher lock."""
        try:
            from odibi_mcp.context import set_project_context
        except ImportError:  # Flat Databricks workspace deployment
            from context import set_project_context

        set_project_context(previous_context)

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
                    "actions": [
                        "run_workflow",
                        "resume_workflow",
                        "list_workflows",
                        "get_workflow",
                    ],
                },
                {
                    "name": "Discovery",
                    "description": "Environment mapping and data profiling",
                    "action_count": 3,
                    "actions": ["map_environment", "profile_source", "profile_folder"],
                },
                {
                    "name": "Inspection",
                    "description": "Post-execution analysis of pipeline runs",
                    "action_count": 4,
                    "actions": ["story_read", "node_sample", "node_failed_rows", "lineage_graph"],
                },
                {
                    "name": "Construction",
                    "description": "Pipeline building from patterns and templates",
                    "action_count": 5,
                    "actions": [
                        "list_transformers",
                        "list_patterns",
                        "apply_pattern_template",
                        "suggest_pipeline",
                        "create_ingestion_pipeline",
                    ],
                },
                {
                    "name": "Validation",
                    "description": "Testing, validation, and diagnostics",
                    "action_count": 4,
                    "actions": ["validate_yaml", "validate_pipeline", "test_pipeline", "diagnose"],
                },
                {
                    "name": "Task Guidance",
                    "description": "Structured Q&A for parameter collection",
                    "action_count": 2,
                    "actions": ["get_task_guidance", "list_task_types"],
                },
                {
                    "name": "Onboarding",
                    "description": "First-time setup, schema discovery, documentation",
                    "action_count": 9,
                    "actions": [
                        "onboard",
                        "get_schema",
                        "search_docs",
                        "get_doc",
                        "list_docs",
                        "list_examples",
                        "get_example",
                        "list_skills",
                        "get_skill",
                    ],
                },
                {
                    "name": "Download",
                    "description": "Export data or generated code",
                    "action_count": 3,
                    "actions": ["download_sql", "download_table", "download_file"],
                },
                {
                    "name": "Session Builder",
                    "description": "Incremental YAML construction (stateful)",
                    "action_count": 9,
                    "actions": [
                        "create_pipeline",
                        "add_node",
                        "configure_read",
                        "configure_write",
                        "configure_transform",
                        "get_pipeline_state",
                        "render_pipeline_yaml",
                        "list_sessions",
                        "discard_pipeline",
                    ],
                },
            ],
            "total_actions": 43,
            "action_effects": {
                action: effect.value for action, effect in sorted(ACTION_EFFECTS.items())
            },
            "usage": {
                "discovery": "odibi_help(category='Workflows')",
                "action_details": "odibi_help(action='profile_source')",
                "execution": 'odibi_execute(\'profile_source\', \'{"connection": "s3_raw", "path": "orders.csv"}\')',
            },
        }

    def _category_help(self, category: str) -> dict[str, Any]:
        """Return actions in a specific category."""
        # Action catalog organized by category
        actions_by_category = {
            "Workflows": [
                {
                    "name": "run_workflow",
                    "signature": "workflow_name, params=None",
                    "description": "Execute named workflow",
                },
                {
                    "name": "resume_workflow",
                    "signature": "resume_token, inputs=None",
                    "description": "Continue paused workflow",
                },
                {
                    "name": "list_workflows",
                    "signature": "",
                    "description": "Available workflow names + descriptions",
                },
                {
                    "name": "get_workflow",
                    "signature": "workflow_name",
                    "description": "Full workflow definition",
                },
            ],
            "Discovery": [
                {
                    "name": "map_environment",
                    "signature": "connection=None",
                    "description": "List connections, databases, high-level schema",
                },
                {
                    "name": "profile_source",
                    "signature": "connection, path, max_rows=100",
                    "description": "Schema, stats, nulls, cardinality, sample data",
                },
                {
                    "name": "profile_folder",
                    "signature": "project, connection, folder_path, pattern='*', max_files=20",
                    "description": "List files with metadata (size, format, mod time)",
                },
            ],
            "Inspection": [
                {
                    "name": "story_read",
                    "signature": "pipeline, run_id=None",
                    "description": "Execution summary, node statuses, failure counts",
                },
                {
                    "name": "node_sample",
                    "signature": "pipeline, node, limit=10",
                    "description": "Fetch successful output rows",
                },
                {
                    "name": "node_failed_rows",
                    "signature": "pipeline, node, limit=10",
                    "description": "Fetch quarantined rows with failure reasons",
                },
                {
                    "name": "lineage_graph",
                    "signature": "pipeline",
                    "description": "Visual flow diagram (nodes + edges)",
                },
            ],
            "Construction": [
                {
                    "name": "list_transformers",
                    "signature": "category=None",
                    "description": "Available transformer names + descriptions",
                },
                {
                    "name": "list_patterns",
                    "signature": "",
                    "description": "Pipeline patterns (dimension_scd1, dimension_scd2, fact, etc.)",
                },
                {
                    "name": "apply_pattern_template",
                    "signature": "pattern, table_name, connection, source_path",
                    "description": "Generate YAML from pattern",
                },
                {
                    "name": "suggest_pipeline",
                    "signature": "source_path, connection, intent",
                    "description": "Smart chain recommendation based on source data",
                },
                {
                    "name": "create_ingestion_pipeline",
                    "signature": "source_path, connection, target_table",
                    "description": "Opinionated bronze → silver ingestion",
                },
            ],
            "Validation": [
                {
                    "name": "validate_yaml",
                    "signature": "yaml_content",
                    "description": "Config structure check (Pydantic strict validation)",
                },
                {
                    "name": "validate_pipeline",
                    "signature": "pipeline",
                    "description": "Dry-run validation (parse YAML, check connections, validate config)",
                },
                {
                    "name": "test_pipeline",
                    "signature": "pipeline",
                    "description": "Immutable bounded logical plan (schema 1.0); only status=planned is success",
                },
                {
                    "name": "diagnose",
                    "signature": "pipeline, error_context=None",
                    "description": "Systematic troubleshooting (validation → connection → schema → transform analysis)",
                },
            ],
            "Task Guidance": [
                {
                    "name": "get_task_guidance",
                    "signature": "task_type",
                    "description": "Structured questions + defaults for a task",
                },
                {
                    "name": "list_task_types",
                    "signature": "",
                    "description": "Available task types with descriptions",
                },
            ],
            "Onboarding": [
                {
                    "name": "onboard",
                    "signature": "",
                    "description": "System overview, available categories, quick start",
                },
                {
                    "name": "get_schema",
                    "signature": "component=None",
                    "description": "Odibi config contract (Pydantic models → JSON schema)",
                },
                {
                    "name": "search_docs",
                    "signature": "query",
                    "description": "Full-text search across 80+ documentation files",
                },
                {
                    "name": "get_doc",
                    "signature": "doc_path",
                    "description": "Retrieve specific documentation markdown",
                },
                {
                    "name": "list_docs",
                    "signature": "category=None",
                    "description": "List available documentation files",
                },
                {
                    "name": "list_examples",
                    "signature": "pattern=None",
                    "description": "List runnable example pipeline YAMLs",
                },
                {
                    "name": "get_example",
                    "signature": "pattern_name",
                    "description": "Get worked example pipeline (YAML + explanation)",
                },
                {
                    "name": "list_skills",
                    "signature": "",
                    "description": "List loadable Odibi skills",
                },
                {
                    "name": "get_skill",
                    "signature": "name",
                    "description": "Load skill's full guidance by name",
                },
            ],
            "Download": [
                {
                    "name": "download_sql",
                    "signature": "project, connection, query, filename, limit=1000",
                    "description": "Export a bounded read-only SQL result to the controlled export root",
                },
                {
                    "name": "download_table",
                    "signature": "project, connection, table, filename, limit=1000",
                    "description": "Export a bounded table result to the controlled export root",
                },
                {
                    "name": "download_file",
                    "signature": "project, connection, source_path, filename",
                    "description": "Copy a bounded storage object to the controlled export root",
                },
            ],
            "Session Builder": [
                {
                    "name": "create_pipeline",
                    "signature": "pipeline_name, layer='gold'",
                    "description": "Start new pipeline session; returns session_id",
                },
                {
                    "name": "add_node",
                    "signature": "session_id, node_name, depends_on=None",
                    "description": "Add a node (depends_on nodes must already exist)",
                },
                {
                    "name": "configure_read",
                    "signature": "session_id, node_name, connection, format, table=None, path=None, query=None, options=None",
                    "description": "Set a node's read block",
                },
                {
                    "name": "configure_write",
                    "signature": "session_id, node_name, connection, format, path=None, table=None, mode='overwrite', keys=None, partition_by=None, options=None",
                    "description": "Set a node's write block",
                },
                {
                    "name": "configure_transform",
                    "signature": "session_id, node_name, steps",
                    "description": "Set a node's transform steps",
                },
                {
                    "name": "get_pipeline_state",
                    "signature": "session_id",
                    "description": "Current session state (nodes, read, write)",
                },
                {
                    "name": "render_pipeline_yaml",
                    "signature": "session_id",
                    "description": "Validate and render YAML from session state",
                },
                {
                    "name": "list_sessions",
                    "signature": "",
                    "description": "List active pipeline builder sessions",
                },
                {
                    "name": "discard_pipeline",
                    "signature": "session_id",
                    "description": "Discard session without rendering",
                },
            ],
        }

        if category not in actions_by_category:
            return {
                "error": f"Unknown category: {category}",
                "valid_categories": list(actions_by_category.keys()),
                "tip": "Run odibi_help() to see all categories",
            }

        actions = [
            {**item, "effect": ACTION_EFFECTS[item["name"]].value}
            for item in actions_by_category[category]
        ]
        return {
            "kind": "category_help",
            "category": category,
            "actions": actions,
            "usage_example": f"odibi_execute('{actions_by_category[category][0]['name']}', '{{...}}')",
        }

    def _action_help(self, action: str) -> dict[str, Any]:
        """Return detailed help for a specific action."""
        # Full action documentation with examples
        action_docs = {
            "profile_source": {
                "signature": "connection, path, max_rows=100",
                "description": "Profile a data source (CSV/Parquet/JSON/Delta). Returns schema, stats, nulls, cardinality, sample data.",
                "args": [
                    {
                        "name": "connection",
                        "type": "str",
                        "required": True,
                        "description": "Connection name (run map_environment to list)",
                    },
                    {
                        "name": "path",
                        "type": "str",
                        "required": True,
                        "description": "File path or table name",
                    },
                    {
                        "name": "max_rows",
                        "type": "int",
                        "required": False,
                        "default": 100,
                        "description": "Sample size for profiling",
                    },
                ],
                "returns": {
                    "schema": "List of columns with types, nulls, cardinality",
                    "stats": "Min/max/mean for numeric columns, top values for categorical",
                    "sample": "First N rows",
                    "findings": "Observations (e.g. '82% nulls in optional_field — normal for sparse data')",
                    "risks": "Conditional warnings (e.g. 'If order_id is meant to be unique, duplicates detected')",
                },
                "examples": [
                    {
                        "description": "Profile a CSV file",
                        "code": 'odibi_execute(\'profile_source\', \'{"connection": "s3_raw", "path": "orders.csv", "max_rows": 1000}\')',
                    },
                    {
                        "description": "Profile a Delta table",
                        "code": 'odibi_execute(\'profile_source\', \'{"connection": "delta_lake", "path": "catalog.schema.table"}\')',
                    },
                ],
            },
            "run_workflow": {
                "signature": "workflow_name, params=None",
                "description": "Execute a deterministic workflow with built-in retry logic, loops, and pause/resume capability.",
                "args": [
                    {
                        "name": "workflow_name",
                        "type": "str",
                        "required": True,
                        "description": "Workflow name (see list_workflows)",
                    },
                    {
                        "name": "params",
                        "type": "dict",
                        "required": False,
                        "default": None,
                        "description": "Workflow parameters (YAML path, pipeline name, etc.)",
                    },
                ],
                "returns": {
                    "status": "COMPLETED, AWAITING_INPUT, or FAILED",
                    "resume_token": "If paused, token for resume_workflow",
                    "outputs": "Workflow outputs (YAML content, validation results, etc.)",
                    "prompts": "If paused, questions to ask user for next inputs",
                },
                "examples": [
                    {
                        "description": "Build and validate a pipeline",
                        "code": 'odibi_execute(\'run_workflow\', \'{"workflow_name": "build_and_validate", "params": {}}\')',
                    },
                    {
                        "description": "Debug a failed pipeline run",
                        "code": 'odibi_execute(\'run_workflow\', \'{"workflow_name": "debug_failed_run", "params": {"pipeline": "my_pipeline"}}\')',
                    },
                ],
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
                    "effect": ACTION_EFFECTS[action].value,
                    "signature": str(sig),
                    "description": "Documentation coming soon",
                    "tip": "Action exists but detailed docs not yet written. Try calling it to see what it returns.",
                }
            return {
                "error": f"Unknown action: {action}",
                "tip": "Run odibi_help() to see all actions",
            }

        return {
            "kind": "action_help",
            "action": action,
            "effect": ACTION_EFFECTS[action].value,
            **action_docs[action],
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
    def _run_workflow(
        self, workflow_name: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Execute a named workflow."""
        return _run_workflow_execution(workflow_name, params or {})

    def _resume_workflow(
        self, resume_token: str, inputs: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Continue paused workflow."""
        return _resume_workflow_execution(resume_token, inputs or {})

    def _list_workflows(self) -> dict[str, Any]:
        """List available workflows."""
        return _list_workflow_definitions()

    def _get_workflow(self, workflow_name: str) -> dict[str, Any]:
        """Get workflow definition."""
        return _get_workflow_definition(workflow_name)

    # Discovery
    def _map_environment(
        self,
        connection: str | None = None,
        path: str = "",
        pattern: str = "",
        limit: int = 500,
    ) -> dict[str, Any]:
        """List connections and environment info."""
        try:
            from odibi_mcp.tools.smart import map_environment
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import map_environment

        return map_environment(
            connection=connection,
            path=path,
            pattern=pattern,
            limit=limit,
        )

    def _profile_source(self, connection: str, path: str, max_rows: int = 100) -> dict[str, Any]:
        """Profile a data source."""
        try:
            from odibi_mcp.tools.smart import profile_source
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import profile_source

        return profile_source(
            connection=connection,
            path=path,
            max_attempts=5,
            use_cache=False,
            sample_rows=max_rows,
        )

    def _profile_folder(
        self,
        connection: str,
        folder_path: str,
        pattern: str = "*",
        max_files: int = 50,
    ) -> dict[str, Any]:
        """List files in a folder."""
        try:
            from odibi_mcp.tools.smart import profile_folder
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import profile_folder

        return profile_folder(
            connection=connection,
            folder_path=folder_path,
            pattern=pattern,
            max_files=max_files,
        )

    # Inspection
    def _story_read(self, pipeline: str, run_id: str | None = None) -> dict[str, Any]:
        """Read pipeline execution story."""
        try:
            from odibi_mcp.tools.story import story_read
        except ImportError:  # Flat Databricks workspace deployment
            from tools.story import story_read

        run_selector = {"run_id": run_id} if run_id is not None else None
        return story_read(pipeline=pipeline, run_selector=run_selector)

    def _node_sample(self, pipeline: str, node: str, limit: int = 10) -> dict[str, Any]:
        """Sample node output."""
        try:
            from odibi_mcp.tools.story import node_sample
        except ImportError:  # Flat Databricks workspace deployment
            from tools.story import node_sample

        return node_sample(pipeline=pipeline, node=node, limit=limit)

    def _node_failed_rows(self, pipeline: str, node: str, limit: int = 10) -> dict[str, Any]:
        """Fetch quarantined rows."""
        try:
            from odibi_mcp.tools.story import node_failed_rows
        except ImportError:  # Flat Databricks workspace deployment
            from tools.story import node_failed_rows

        return node_failed_rows(pipeline=pipeline, node=node, limit=limit)

    def _lineage_graph(self, pipeline: str) -> dict[str, Any]:
        """Generate lineage graph."""
        try:
            from odibi_mcp.tools.story import lineage_graph
        except ImportError:  # Flat Databricks workspace deployment
            from tools.story import lineage_graph

        return lineage_graph(pipeline=pipeline)

    # Construction
    def _list_transformers(self, category: str | None = None) -> dict[str, Any]:
        """List available transformers."""
        from tools.construction import list_transformers

        return list_transformers(category)

    def _list_patterns(self) -> dict[str, Any]:
        """List pipeline patterns."""
        from tools.construction import list_patterns

        return list_patterns()

    def _render_remote_pattern_projection(
        self, projection: RemotePatternRenderProjection
    ) -> dict[str, Any]:
        """Render only the data-free projection prepared at the dispatch boundary."""
        try:
            from odibi_mcp.tools.render import render_remote_pattern_projection
        except ImportError:  # Flat Databricks workspace deployment
            from tools.render import render_remote_pattern_projection

        return render_remote_pattern_projection(projection)

    def _apply_pattern_template(
        self, pattern: str, table_name: str, connection: str, source_path: str
    ) -> dict[str, Any]:
        """Generate YAML from pattern."""
        from tools.construction import apply_pattern_template

        return apply_pattern_template(pattern, table_name, connection, source_path)

    def _suggest_pipeline(self, source_path: str, connection: str, intent: str) -> dict[str, Any]:
        """Suggest pipeline based on data."""
        try:
            from odibi_mcp.tools.phase3_smart import suggest_pipeline
        except ImportError:  # Flat Databricks workspace deployment
            from tools.phase3_smart import suggest_pipeline

        return suggest_pipeline(
            source_path=source_path,
            connection=connection,
            intent=intent,
            sample_rows=100,
        )

    def _create_ingestion_pipeline(
        self, source_path: str, connection: str, target_table: str
    ) -> dict[str, Any]:
        """Create ingestion pipeline."""
        from tools.phase3_smart import create_ingestion_pipeline

        return create_ingestion_pipeline(source_path, connection, target_table)

    # Validation
    def _validate_yaml(self, yaml_content: str) -> dict[str, Any]:
        """Validate YAML structure."""
        from tools.yaml_builder import validate_odibi_config

        return validate_odibi_config(yaml_content)

    def _validate_pipeline(self, pipeline: str) -> dict[str, Any]:
        """Validate pipeline config."""
        from tools.validation import validate_pipeline

        return validate_pipeline(pipeline)

    def _test_pipeline(self, pipeline: str, sample_size: int = 100) -> dict[str, object]:
        """Return the shared immutable logical plan for supplied YAML text."""
        _validate_legacy_sample_size(sample_size)
        return immutable_planning.plan_pipeline_yaml(pipeline).to_dict()

    def _diagnose(self, pipeline: str, error_context: str | None = None) -> dict[str, Any]:
        """Diagnose pipeline issues."""
        from tools.diagnose import diagnose

        return diagnose(pipeline, error_context)

    # Task Guidance
    def _get_task_guidance(self, task_type: str) -> dict[str, Any]:
        """Get structured task guidance."""
        from tools.guidance import get_task_guidance

        return get_task_guidance(task_type)

    def _list_task_types(self) -> dict[str, Any]:
        """List available task types."""
        from tools.guidance import list_task_types

        return list_task_types()

    # Onboarding — delegate to the OdibiKnowledge singleton's methods.
    # (get_knowledge() returns the instance; call its methods — it does NOT take an action arg.)
    @staticmethod
    def _import_knowledge():
        """Import get_knowledge from the knowledge module.

        Works both as a package import (``odibi_mcp.knowledge``) for tests/pip
        installs and as a bare import (``knowledge``) for flat Databricks
        workspace deployments.
        """
        try:
            from odibi_mcp.knowledge import get_knowledge
        except ImportError:
            from knowledge import get_knowledge
        return get_knowledge

    def _onboard(self) -> dict[str, Any]:
        """Get onboarding information."""
        return self._import_knowledge()().onboard()

    def _get_schema(self, component: str | None = None) -> dict[str, Any]:
        """Get config schema."""
        return self._import_knowledge()().get_schema(component)

    def _search_docs(self, query: str) -> dict[str, Any]:
        """Search documentation."""
        return {"results": self._import_knowledge()().search_docs(query)}

    def _get_doc(self, doc_path: str) -> dict[str, Any]:
        """Get documentation file."""
        return self._import_knowledge()().get_doc(doc_path)

    def _list_docs(self, category: str | None = None) -> dict[str, Any]:
        """List documentation files."""
        return {"docs": self._import_knowledge()().list_docs(category)}

    def _list_examples(self, pattern: str | None = None) -> dict[str, Any]:
        """List example pipelines."""
        return {"examples": self._import_knowledge()().list_examples(pattern)}

    def _get_example(self, pattern_name: str) -> dict[str, Any]:
        """Get example pipeline."""
        return self._import_knowledge()().get_example(pattern_name)

    def _list_skills(self) -> dict[str, Any]:
        """List available skills."""
        return {"skills": self._import_knowledge()().list_skills()}

    def _get_skill(self, name: str) -> dict[str, Any]:
        """Get skill content."""
        return self._import_knowledge()().get_skill(name)

    # Download
    def _download_sql(
        self,
        connection: str,
        query: str,
        output_path: str,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Export a bounded SQL result."""
        try:
            from odibi_mcp.tools.smart import download_sql
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import download_sql

        return download_sql(
            connection=connection,
            query=query,
            output_path=output_path,
            limit=limit,
            max_bytes=10 * 1024 * 1024,
            exclusive=True,
            report_truncation=True,
        )

    def _download_table(
        self,
        connection: str,
        table: str,
        output_path: str,
        limit: int = 1000,
    ) -> dict[str, Any]:
        """Export a bounded table result."""
        try:
            from odibi_mcp.tools.smart import download_table
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import download_table

        return download_table(
            connection=connection,
            table=table,
            output_path=output_path,
            limit=limit,
            max_bytes=10 * 1024 * 1024,
            exclusive=True,
            report_truncation=True,
        )

    def _download_file(
        self,
        connection: str,
        source_path: str,
        output_path: str,
    ) -> dict[str, Any]:
        """Copy a bounded storage object to the controlled export root."""
        try:
            from odibi_mcp.tools.smart import download_file
        except ImportError:  # Flat Databricks workspace deployment
            from tools.smart import download_file

        return download_file(
            connection=connection,
            source_path=source_path,
            output_path=output_path,
            max_bytes=10 * 1024 * 1024,
            exclusive=True,
        )

    # Session Builder
    def _create_pipeline(self, pipeline_name: str, layer: str = "gold") -> dict[str, Any]:
        """Create a pipeline builder session. Returns a session_id for subsequent calls."""
        from tools.builder import create_pipeline

        return create_pipeline(pipeline_name, layer)

    def _add_node(
        self, session_id: str, node_name: str, depends_on: list[str] | None = None
    ) -> dict[str, Any]:
        """Add a node to the pipeline session."""
        from tools.builder import add_node

        return add_node(session_id, node_name, depends_on)

    def _configure_read(
        self,
        session_id: str,
        node_name: str,
        connection: str,
        format: str,
        table: str | None = None,
        path: str | None = None,
        query: str | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Configure a node's read block."""
        from tools.builder import configure_read

        return configure_read(
            session_id,
            node_name,
            connection,
            format,
            table=table,
            path=path,
            query=query,
            options=options,
        )

    def _configure_write(
        self,
        session_id: str,
        node_name: str,
        connection: str,
        format: str,
        path: str | None = None,
        table: str | None = None,
        mode: str = "overwrite",
        keys: list[str] | None = None,
        partition_by: list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Configure a node's write block."""
        from tools.builder import configure_write

        return configure_write(
            session_id,
            node_name,
            connection,
            format,
            path=path,
            table=table,
            mode=mode,
            keys=keys,
            partition_by=partition_by,
            options=options,
        )

    def _configure_transform(
        self, session_id: str, node_name: str, steps: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Configure a node's transform steps."""
        from tools.builder import configure_transform

        return configure_transform(session_id, node_name, steps)

    def _get_pipeline_state(self, session_id: str) -> dict[str, Any]:
        """Get current pipeline builder session state."""
        from tools.builder import get_pipeline_state

        return get_pipeline_state(session_id)

    def _render_pipeline_yaml(self, session_id: str) -> dict[str, Any]:
        """Validate and render the session's pipeline YAML."""
        from tools.builder import render_pipeline_yaml

        return render_pipeline_yaml(session_id)

    def _list_sessions(self) -> dict[str, Any]:
        """List active sessions."""
        from tools.builder import list_sessions

        return list_sessions()

    def _discard_pipeline(self, session_id: str) -> dict[str, Any]:
        """Discard a builder session without rendering."""
        from tools.builder import discard_pipeline

        return discard_pipeline(session_id)
