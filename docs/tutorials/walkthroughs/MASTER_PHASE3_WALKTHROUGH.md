# Odibi Phase 3 Internals: A Guided Walkthrough

**Version:** v1.3.0-alpha.5-phase3  
**Date:** November 2025  
**Focus:** CLI Tools, Story Engine, and Azure SQL

---

## 1. Introduction

Phase 3 represents a major shift in Odibi's maturity. While Phase 1 and 2 focused on the core execution engines (Pandas/Spark) and storage (ADLS/Delta), Phase 3 focuses on **Developer Experience** and **Observability**.

This walkthrough guides you through the internal architecture of the new features delivered in Phase 3:
1.  **The CLI Engine:** How the new `odibi` command works.
2.  **Story Generation:** How we track metadata and generate reports.
3.  **Azure SQL Connector:** Production-ready database integration.

---

## 2. The CLI Architecture (`odibi.cli`)

The Command Line Interface is the new entry point for all Odibi operations. It replaces ad-hoc Python scripts with a structured, subcommand-based architecture.

### Entry Point (`main.py`)
The `odibi` command maps to `odibi.cli.main:main`. It uses Python's `argparse` to dispatch control to specific submodules.

```python
# odibi/cli/main.py structure
def main():
    parser = argparse.ArgumentParser(...)
    subparsers = parser.add_subparsers(...)

    # Subcommands
    run_parser = subparsers.add_parser("run")      # -> run_command()
    validate_parser = subparsers.add_parser("validate") # -> validate_command()
    doctor_parser = subparsers.add_parser("doctor")  # -> doctor_command()
    graph_parser = subparsers.add_parser("graph")    # -> graph_command()
```

### Key Commands

#### `odibi run`
Executes a pipeline. It instantiates the `PipelineManager`, loads the config, and runs the graph.
*   **New Feature:** `--dry-run` mode calculates the execution graph and validates connections without processing data.

#### `odibi doctor`
A diagnostic tool that acts as a linter for your data environment.
*   **Checks:**
    *   YAML syntax validity.
    *   Connection reachability (e.g., can we actually talk to ADLS?).
    *   Environment variable presence (e.g., `AZURE_CLIENT_SECRET`).
    *   Package dependencies (e.g., is `pyspark` installed?).

#### `odibi graph`
Visualizes the DAG (Directed Acyclic Graph) of your pipeline.
*   **Formats:** ASCII (terminal-friendly), DOT (Graphviz), Mermaid (Markdown).
*   **Internals:** Builds the execution plan using `networkx` (or internal graph logic) and traverses it to generate the visual representation.

---

## 3. The Story Engine (`odibi.story`)

The "Story" is Odibi's term for a rich execution report. It turns a pipeline run into a readable narrative.

### Metadata Tracking (`metadata.py`)
We introduced `NodeExecutionMetadata` to track granular details for every node:

```python
@dataclass
class NodeExecutionMetadata:
    node_name: str
    status: str          # success/failed/skipped
    duration: float

    # Data Observability
    rows_in: int
    rows_out: int
    rows_change_pct: float

    # Schema Evolution
    schema_in: List[str]
    schema_out: List[str]
    columns_added: List[str]    # Tracked automatically
    columns_removed: List[str]  # Tracked automatically
```

### Generator Logic (`generator.py`)
The `StoryGenerator` class takes a dictionary of `NodeResult` objects and renders a Markdown file.

**Key Features:**
1.  **Schema Diffing:** It compares `schema_in` vs `schema_out` to automatically list columns added or removed by a transformation.
2.  **Sample Data:** Captures the first `N` rows (default: 10) of output for debugging.
3.  **Templating:** While currently Python-based string building, it is designed to support Jinja2 templates in Phase 4.

**Output Location:** `stories/<pipeline_name>_<timestamp>.md`

---

## 4. Azure SQL Connector (`odibi.connections.azure_sql`)

Phase 3 completes the Azure story by adding full SQL Server support.

### Architecture
The `AzureSQL` class wraps `SQLAlchemy` and `pyodbc` to provide a robust interface.

### Authentication Modes
We support two distinct authentication strategies for production security:

1.  **Managed Identity (`aad_msi`):**
    *   The default for Databricks and Azure VMs.
    *   Uses `Authentication=ActiveDirectoryMsi` in the ODBC string.
    *   **No passwords required** in config.

2.  **SQL Authentication (`sql`):**
    *   Traditional Username/Password.
    *   Used for local development or legacy systems.

### Connection Pooling
To ensure performance, the connector uses SQLAlchemy's connection pool:
```python
self._engine = create_engine(
    connection_url,
    pool_pre_ping=True,  # Auto-reconnect if connection drops
    pool_recycle=3600,   # Recycle connections every hour
)
```

### Operations
*   **`read_table`**: Reads an entire table into a DataFrame.
*   **`read_sql`**: Executes arbitrary SELECT queries.
*   **`write_table`**: Uses `fast_executemany` (via `method="multi"`) for high-performance inserts.

---

## 5. Summary & Next Steps

Phase 3 has successfully transformed Odibi from a "script runner" into a "framework" with tooling, visibility, and robust connectivity.

### Ready for Phase 4
With the transparency layer complete, we are now ready to tackle **Phase 4: Performance & Production Hardening**. The stories we generate today will be the baseline for measuring the performance improvements of tomorrow.
