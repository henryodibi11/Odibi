# Odibi — Assistant Instructions

You are working with **Odibi**, a declarative YAML→star-schema data-pipeline framework
(Pandas/Spark/Polars engines, strict Pydantic config, patterns, validation/quarantine/gates,
a simulation engine, and a story/catalog layer).

**Odibi is rich. Do not improvise from memory — use the tools.** The config is validated by
strict models that hard-error on unknown/misspelled keys, so guessing field names fails fast.

## Start here

1. **Load the `odibi` skill first** — `get_skill("odibi")` (or read `.assistant/skills/odibi/SKILL.md`).
   It gives you the canonical workflow and a map of which skill/tool to use for each step.
2. Then load the step-specific skill when you reach it (`pipeline-yaml-authoring`,
   `add-a-connection`, `validation-workflow`, `engine-parity`, `databricks-notebook-protocol`).

## The canonical workflow

`discover → profile source → author YAML → validate → test/run → inspect`

- **discover**: `onboard`, `bootstrap_context`
- **profile**: context-workbench tools (preferred) or `profile_source` / `map_environment`
- **author**: `get_schema` (the contract), `list_transformers`, `list_patterns`,
  `apply_pattern_template`, `get_example`
- **validate**: `validate_yaml` — fix every did-you-mean error before running
- **run**: `test_pipeline`
- **inspect**: `node_sample`, story tools

## context-workbench ↔ Odibi

Use **context-workbench** to understand and QA data (profile, `pre_join`, schema diff,
inspect outputs); use **Odibi** to author and run the pipeline. They feed each other —
profile the source in context-workbench, then encode what you learned as Odibi YAML.

## Finding things

- `list_skills` / `get_skill` — these procedural skills
- `search_docs` / `get_doc` / `list_docs` — the full Odibi documentation
- `list_examples` / `get_example` — runnable example pipelines
- `get_schema` — the exact, always-current config contract (generated from the models)
