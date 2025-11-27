# Gap Analysis V8: Enterprise Maturity & Developer Experience

**Objective:** Elevate Odibi from a flexible library to an enterprise-grade platform ("Day 6").

## 1. Governance (The "Lineage" Gap)

**Current State:**
- Odibi tracks execution metadata (`steps`, `sql_hash`, `schema`) and logs it to the console or `state.json`.
- There is no standard way to export this metadata to enterprise catalogs like DataHub, Marquez, or Atlan.

**The Gap:**
- Lack of **OpenLineage** integration.
- Enterprise data platforms require standard lineage events (`RUN_START`, `RUN_COMPLETE`, `DATASET_INPUT`, `DATASET_OUTPUT`).

**Implementation Plan:**
1.  **Create `odibi.lineage` module:**
    -   Implement `OpenLineageAdapter` class using `openlineage-python` library.
    -   Map Odibi `NodeConfig` to OpenLineage `Job`.
    -   Map Odibi `NodeResult` to OpenLineage `RunEvent`.
2.  **Integrate Hooks:**
    -   **Pipeline Level:** In `odibi/pipeline.py`, emit `START` when pipeline begins and `COMPLETE`/`FAIL` when it ends.
    -   **Node Level:** In `odibi/node.py`, emit `START` before `_execute_read_phase` and `COMPLETE` after `_execute_write_phase`.
    -   **Facets:** Extract schema from `NodeResult.schema` to populate OpenLineage `SchemaDatasetFacet`.
3.  **Configuration:**
    -   Add `lineage` section to `project.yaml` (e.g., `url`, `namespace`).

## 2. Observability (The "UI" Gap)

**Current State:**
- Users view results via:
    -   Console logs.
    -   Static HTML files in `stories/`.
    -   Raw JSON in `.odibi/state.json`.

**The Gap:**
- No centralized, dynamic interface to browse pipeline history.
- Debugging requires digging through file systems.

**Implementation Plan:**
1.  **Develop `odibi ui` command:**
    -   Use **FastAPI** as the lightweight backend.
    -   Use **Jinja2** + **HTMX** (or just simple HTML/JS) for a zero-build frontend.
2.  **Features:**
    -   **Dashboard:** List recent runs (from `.odibi/` or catalog backend).
    -   **Story Browser:** Serve the static HTML stories dynamically.
    -   **Config Viewer:** Read-only view of the effective `odibi.yaml`.
3.  **Architecture:**
    -   `odibi/ui/app.py`: FastAPI app.
    -   `odibi/ui/templates/`: HTML templates.
    -   Reads strictly from `stories/` directory and `catalog` (if available).

## 3. Productionization (The "Ops" Gap)

**Current State:**
- `odibi init` simply copies a YAML template.
- Users must manually figure out how to dockerize or deploy Odibi to CI/CD.

**The Gap:**
- No standard project scaffolding for production.
- No automated CI/CD workflow generation.

**Implementation Plan:**
1.  **Enhance `odibi init`:**
    -   Prompt for project type (Local, Azure, Databricks).
2.  **Generate Scaffolding:**
    -   **`Dockerfile`**: Standard Python image installing `odibi`.
    -   **`.github/workflows/ci.yaml`**: GitHub Action to run `odibi test` and `odibi run --dry-run`.
    -   **`.dockerignore`** and **`.gitignore`**.
    -   **`README.md`**: Populated with project name and run instructions.

## 4. Developer Experience (DX)

**Current State:**
- `odibi test` exists but is limited to basic unit tests.
- Troubleshooting connection issues involves running a full pipeline and waiting for it to fail.

**The Gap:**
- **Health Checks:** No quick way to validate environment health (connections, dependencies).
- **Test DX:** `odibi test` could be more powerful (e.g., snapshot testing).

**Implementation Plan:**
1.  **Implement `odibi doctor`:**
    -   **Check Dependencies:** Verify `duckdb`, `pyspark` (if needed), `pandas` versions.
    -   **Check Connections:** Attempt `test_connection()` on all defined connections in `odibi.yaml`.
    -   **Check Config:** Run `odibi validate`.
2.  **Enhance `odibi test`:**
    -   Add support for **Snapshot Testing**: `odibi test --snapshot` to auto-generate expected data from current result (like Jest).

---

# Implementation Priorities (Day 6)

1.  **Priority 1: `odibi doctor`**
    -   Low hanging fruit, high value for debugging environment issues immediately.
2.  **Priority 2: OpenLineage Integration**
    -   Critical for "Enterprise Maturity". Enables integration with DataHub/Marquez.
3.  **Priority 3: `odibi ui`**
    -   "Wow" factor. Makes the tool accessible to non-engineers (viewing stories).
4.  **Priority 4: Enhanced `odibi init`**
    -   Important for adoption, but manual setup is currently possible.
