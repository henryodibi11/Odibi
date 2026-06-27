# Odibi — Agent Entry Point

If you are an AI agent working in this repo, **read [`.assistant/.assistant_instructions.md`](.assistant/.assistant_instructions.md) first** — it orients you on Odibi and the canonical workflow.

- **Skills** (load on demand): [`.assistant/skills/`](.assistant/skills/) — start with `odibi`, then the step-specific skill (`pipeline-yaml-authoring`, `add-a-connection`, `validation-workflow`, `engine-parity`, `databricks-notebook-protocol`).
- **Connected via the Odibi MCP instead of the repo?** Call `onboard` first, then `get_skill`, `get_schema`, `search_docs`, `get_doc`, `list_examples`, `get_example`. Same content, served from the package.
- **Docs:** [`docs/`](docs/) · **Examples:** [`examples/`](examples/) · **Config contract:** `get_schema` (generated from the Pydantic models, always current).

Use **context-workbench** to profile/understand source data and inspect outputs; use **Odibi** to author and run the pipeline.
