# Odibi MCP Universal Gateway

Odibi MCP exposes two gateway tools: `odibi_execute(action, args_json=None)` performs a
registered action and `odibi_help(action=...)` returns the current inventory,
signatures/parameters, and effect classes. Use dynamic help for those facts rather than
a static action catalog. This guide—not `odibi_help`—is the authority for transport-route
policy.

```text
odibi_help()
odibi_help(action="test_pipeline")
odibi_execute("validate_yaml", '{"yaml_content":"..."}')
odibi_execute("test_pipeline", '{"pipeline":"..."}')
```

Follow the canonical [operation safety ladder](../features/planning.md#operation-safety-ladder).
Only planner `status == "planned"` succeeds; `unresolved` and `invalid` fail closed.
Although `test_pipeline` currently plans immutably, its authorization class remains
`execution`.

## Five separate trust boundaries

| Boundary | Exact current meaning | Does not prove |
| --- | --- | --- |
| **HTTP Bearer identity** | `_http_application_identity()` reads HTTP `Authorization`; `authenticate_bearer_identity()` exactly compares configured `ODIBI_MCP_AUTH_TOKEN` and creates one application identity. | Browser-origin trust, TLS, Internet isolation, per-user/provider identity, managed-project membership, or action authorization by name. |
| **CORS browser policy** | `ODIBI_MCP_CORS_ORIGINS` is an exact bounded allowlist for explicit browser Origins; unset denies explicit Origins before dispatch. No-Origin requests continue to identity/dispatcher controls. | Authentication or protection from non-browser/no-Origin clients. |
| **Listener bind** | Root/package app manifests and the manual default listen on `0.0.0.0:8000`. | Public Internet reachability, TLS, proxy identity, or safety; operators must assess their network/front proxy. |
| **Action authorization** | Each registered action maps to `public_read`, `sensitive_read`, `execution`, `file_write`, or `session_mutation`. Non-public effects require an `ApplicationIdentity` granting that effect before dispatch. | A universally read-only service, action-specific business RBAC, or remote project/path validity. An immutable implementation does not automatically change its registered effect. |
| **Managed-project runtime-data scope** | For non-trusted-local identities, only current `RUNTIME_DATA_ACTIONS` receive `ManagedProjectAccess` preparation for exact project/config/path/export policy before runtime-data effects. | A universal one-project rule for every public read, session action, workflow, or trusted-local call, or live provider permissions. |

`ActionEffect`, `ACTION_EFFECTS`, and `RUNTIME_DATA_ACTIONS` are authoritative. Discover
the current inventory, signatures/parameters, and effect classes with `odibi_help()`;
apply the route policy documented below.

## Operator environment

| Variable | Current use |
| --- | --- |
| `ODIBI_MCP_AUTH_TOKEN` | Exact HTTP Bearer credential. |
| `ODIBI_MCP_CORS_ORIGINS` | Comma-separated exact browser-Origin allowlist. |
| `ODIBI_MCP_PROJECT` | Managed project name required from remote runtime-data callers. |
| `ODIBI_MCP_PROJECT_ROOT` | Operator-owned root bounding managed project paths. |
| `ODIBI_CONFIG` | Operator-owned managed Odibi configuration path. |
| `ODIBI_MCP_EXPORT_ROOT` | Optional operator-owned root bounding exports. |

The last four names are the exact inputs read by
`ManagedProjectAccess.from_environment`; callers cannot supply replacement roots.

## Transport routes

- **HTTP:** exact Bearer identity and effect authorization, then managed preparation
  where applicable.
- **stdio via `odibi_mcp.mcp_server`:** no HTTP identity, so only public reads are available.
- **Explicit in-process bootstrap:** `trusted-local` preserves existing trusted behavior;
  this is not evidence of remote behavior.

Static reads include `search_docs`, `get_schema`, `list_patterns`, and
`list_transformers`. Runtime inspection includes `map_environment`, `story_read`,
`node_sample`, `node_failed_rows`, and `lineage_graph`. Validation uses `validate_yaml`
or `validate_pipeline`; planning uses `test_pipeline`. Trusted/local workflow execution
uses `run_workflow` or `resume_workflow` only where current route policy permits.

Install with `pip install "odibi[mcp]"`. Run stdio with `python -m odibi_mcp`, or HTTP
with `python -m odibi_mcp.databricks_app`. Review
[Security](https://github.com/henryodibi11/Odibi/blob/main/SECURITY.md) and
immutable [planning](../features/planning.md) before exposing or executing the service.
