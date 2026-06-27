---
name: odibi
description: "Start here for Odibi — the declarative YAML→star-schema pipeline framework. Covers what Odibi is, the canonical discover→author→validate→run→inspect workflow, the context-workbench handoff, and which MCP tool to use for each step."
---

# odibi Skill

Odibi is a **declarative data-pipeline framework**: you describe a pipeline in YAML
(connections + pipelines + nodes), and Odibi runs it on Pandas, Spark, or Polars to
produce validated star-schema (dimension/fact) tables, plus a story/catalog audit layer.

Config is validated by **strict Pydantic models** (`odibi/config.py`). Unknown/misspelled
keys are a **hard error with a did-you-mean hint** — you cannot silently typo a field.

## When to Load This Skill

- You are about to build, run, or debug an Odibi pipeline and need orientation.
- You have a data source and need to get it into a star schema.
- You don't know which Odibi MCP tool or sub-skill to reach for.

## The Canonical Workflow

```
1. DISCOVER     → onboard / bootstrap_context        "What is this framework + project?"
2. PROFILE SRC  → context-workbench tools OR          "What's in my source data?"
                  profile_source / map_environment
3. AUTHOR YAML  → get_schema (the config contract)    "Write the pipeline."
                  + apply_pattern_template + get_example
4. VALIDATE     → validate_yaml (fix did-you-mean)    "Is the YAML legal?"
5. TEST/RUN     → test_pipeline                        "Run it."
6. INSPECT      → node_sample, story                   "Look at outputs."
```

Each step has a dedicated skill — load it when you reach that step:

| Step | Load skill | Key MCP tools |
|---|---|---|
| Author connections | `add-a-connection` | `get_schema`, `validate_yaml` |
| Author nodes/transforms/patterns | `pipeline-yaml-authoring` | `get_schema`, `list_transformers`, `list_patterns`, `apply_pattern_template`, `get_example`, `validate_yaml` |
| Validation / quarantine / gates | `validation-workflow` | `get_validation_rules`, `validate_yaml` |
| Pick an engine | `engine-parity` | `list_transformers`, `explain` |
| Run on Databricks | `databricks-notebook-protocol` | — |

## context-workbench ↔ Odibi Handoff

The two tools feed each other. **Use context-workbench (`cw(...)`) to understand source
data; use Odibi to author and run the pipeline.**

| Use context-workbench to... | Use Odibi to... |
|---|---|
| Profile a file/table (`profile_table`, `microscope`) | Author the pipeline YAML |
| Check a join before writing it (`pre_join`) | Encode that join as a `join` transformer step |
| Diff a new file vs last month (`diff`, `coerce_check`) | Configure incremental read + validation |
| Inspect/QA pipeline outputs (`profile_table`, `quality`) | Run the pipeline (`test_pipeline`) |

Typical flow: `cw("profile_table", src)` → decide grain/keys/cleaning → author Odibi YAML →
`validate_yaml` → `test_pipeline` → `cw("profile_table", output)` to QA the result.

> Odibi also has its own light profilers (`profile_source`, `map_environment`) for when
> context-workbench isn't connected. For deep source investigation, prefer context-workbench.

## Discovery Tools (start of any session)

- `onboard` — orient on Odibi itself: what it is, core concepts, where things live.
- `bootstrap_context` — load the current project's context (connections, pipelines).
- `list_skills` / `get_skill` — list and load these procedural skills.
- `search_docs` / `get_doc` / `list_docs` — search/read the rich docs.
- `list_examples` / `get_example` — runnable example pipelines.

## Mental Model of an Odibi Config

A project is one tree (see `pipeline-yaml-authoring` for the full contract):

```yaml
project: my_project
engine: pandas              # pandas | spark | polars (default: pandas)
connections: { ... }        # named sources/targets (see add-a-connection)
pipelines:                  # list of pipelines, each with nodes
  - pipeline: build_gold
    layer: gold
    nodes: [ ... ]          # read → transformer/transform → validation → write
story:   { connection: ..., path: ... }   # mandatory: audit/story output
system:  { connection: ..., path: ... }   # mandatory: system catalog
```

`story` and `system` are **mandatory** at the project level. Always include them.

## Golden Rules

1. **Never guess a field name.** Call `get_schema` for the contract; the validator
   hard-errors on unknown keys.
2. **Always `validate_yaml` before running.** Fix every did-you-mean error first.
3. **`transformer:` runs patterns AND heavy transformers** (e.g. `scd2`, `merge`,
   `dimension`, `fact`). Lighter step chains go in `transform: { steps: [...] }`.
4. **Profile before you author.** Don't design a schema you haven't looked at.
5. **Pandas is the fully-tested engine.** Spark is implemented; Polars is partial —
   see `engine-parity` before choosing.
