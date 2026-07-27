# Immutable Pipeline Planning

Odibi's immutable planner creates a bounded logical graph from caller-supplied YAML
text. It is selected before `PipelineManager.from_yaml`, eager runtime imports,
extension discovery, connection/provider setup, and runtime construction.

Only `status == "planned"` is success. `unresolved` is a fail-closed result: a logical
graph was projected, but one or more meanings depend on excluded extension, provider,
or runtime state. `invalid` means no trustworthy plan was produced.

## Package API

```python
from odibi.planning import plan_pipeline_bytes, plan_pipeline_yaml

response = plan_pipeline_yaml(yaml_text)
if response.status != "planned":
    handle_bounded_diagnostics(response.diagnostics)

canonical_json = response.to_json()
response_dict = response.to_dict()
```

`plan_pipeline_yaml` accepts an exact `str`; `plan_pipeline_bytes` accepts exact UTF-8
`bytes`. Callers may supply a nominal `PlanningLimits` instance to lower limits, never
raise the compiled ceilings. Both functions return a frozen `PlanningResponse`.

## CLI

```bash
cat config.yaml | odibi plan --stdin --format json
```

The selector is deliberately stdin-only. It does not accept a path, `--env`, project
root, node/pipeline selector, resume option, or any runtime facility. Standard output
contains exactly one canonical JSON response followed by one line feed.

| Exit | Meaning |
| ---: | --- |
| `0` | `status == "planned"` |
| `2` | invalid arguments, UTF-8, YAML, schema, dependency, or limit |
| `3` | `status == "unresolved"` |
| `4` | sanitized internal or serialization failure |

## MCP and workflows

Authorized MCP `test_pipeline` calls use the same package planner when the compatibility
mode is `"dry-run"` (the default). `validate_yaml_runnable` is a deprecated planning
alias. Their planning results equal `PlanningResponse.to_dict()` with no adapter
envelope. Existing `max_rows` and dispatcher `sample_size` inputs remain accepted and
bounded for one compatibility cycle, but are deprecated, ignored, and omitted from the
response.

Authorization remains separate from immutability:

- HTTP planning still requires the exact configured Bearer credential.
- Explicit trusted-local dispatcher/bootstrap planning remains available.
- Stdio planning without HTTP identity remains denied.
- The remote workflow allowlist remains exactly `validate_yaml_simple`, whose response
  remains validation-only rather than planner schema `1.0`.
- Trusted-local planning workflows branch to success only for `status == "planned"`.

## Schema 1.0

Every package, CLI, MCP direct, and trusted-local workflow planning result has exactly
this logical shape:

```json
{
  "diagnostics": [],
  "plan": {
    "name": "demo",
    "pipelines": [
      {
        "edges": [{"source": "source", "target": "target"}],
        "name": "example",
        "nodes": [
          {
            "depends_on": [],
            "kind": "read",
            "name": "source",
            "resolution": "resolved"
          },
          {
            "depends_on": ["source"],
            "kind": "write",
            "name": "target",
            "resolution": "resolved"
          }
        ]
      }
    ]
  },
  "schema_version": "1.0",
  "status": "planned",
  "truncated": false
}
```

Pipeline and node declaration order is preserved. Dependencies, derived edges, and
diagnostics have deterministic canonical ordering. Identifiers are Unicode NFC
normalized and reject physical-path/URI-like forms. No physical read/write value,
connection/provider configuration, credential, runtime detail, process output,
exception text, timing, or host metadata is projected.

## Hard bounds

| Resource | Maximum |
| --- | ---: |
| UTF-8 input | 1,048,576 bytes |
| YAML documents | exactly 1 |
| YAML nesting depth | 32 |
| YAML scalar/sequence/mapping nodes | 20,000 |
| YAML aliases | 64 |
| Recursive aliases or merge keys | 0 |
| Pipelines | 128 |
| Nodes per pipeline | 1,024 |
| Nodes total | 4,096 |
| Edges total | 16,384 |
| Emitted diagnostics | 256 |
| Logical identifier | 128 Unicode code points |
| Other emitted string | 512 Unicode code points |
| Canonical JSON response | 2,097,152 UTF-8 bytes |

Diagnostics beyond the cap are retained deterministically and set `truncated` to true.
An oversized response is replaced in full by one fixed `RESPONSE_LIMIT_EXCEEDED`
invalid response; a partial graph is never emitted.

## Boundary and non-goals

The planner uses only in-memory caller text, the Python standard library, and PyYAML.
It does not inspect a path or ambient project/temp files, create scratch, load `.env`,
execute transforms/plugins/entry points, construct connections/providers/credentials,
start Spark, inspect catalogs/state/HWM, generate stories/docs/lineage, emit alerts or
telemetry, touch destinations, launch subprocesses, or create network clients. It does
not mutate CWD, environment, `sys.path`, `sys.modules`, registries, logging, active
sessions, the filesystem, or external state.

This is logical planning, not sandboxing arbitrary Python and not runtime semantic
validation. Extension-, provider-, and runtime-dependent meaning produces fixed typed
`unresolved` diagnostics without executing the excluded facility.

## Legacy runtime simulation is different

`PipelineManager.from_yaml(...)`, `PipelineManager.run(dry_run=True)`, and
`odibi run PATH --dry-run` remain callable for compatibility as **legacy late runtime
simulation**. They may load `.env`, project/installed code, providers/credentials,
engines, catalogs/state, logging/telemetry, stories/lineage, alerts, and surrounding
lifecycle effects. They are not immutable planning or security boundaries, and planner
failure never falls back to them.

### Bounded legacy-documentation follow-up

The immutable planner and directly linked CLI/MCP guidance use the contract above. A
repository audit also found historical material that still uses `dry-run` as shorthand
for “validate,” “no writes,” or “without executing.” Those statements describe only
ordinary node-operation skipping and must not be interpreted as a process-wide safety
guarantee. The bounded documentation follow-up is to relabel legacy pipeline dry-run
claims in these source documents and their generated MCP corpus copies without changing
the separate catalog/system cleanup-preview commands:

- `docs/visuals/run_lifecycle.md`
- `docs/guides/{cli_master_guide,mcp_guide}.md`
- `docs/features/{pipelines,engines}.md`
- `docs/reference/{cheatsheet,developer_cheatsheet}.md`
- `docs/{ODIBI_DEEP_CONTEXT.md,playbook/README.md,journeys/senior-data-engineer.md}`
- `odibi_mcp/tools/execute.py` and `odibi/utils/error_suggestions.py`

Until that bounded wording follow-up lands, this page and `odibi run --help` are the
authoritative distinction: legacy runtime simulation can cause surrounding effects;
immutable planning requires caller-supplied text through the dedicated planner.
