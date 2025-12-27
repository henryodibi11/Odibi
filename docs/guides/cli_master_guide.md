# Odibi CLI: Zero to Hero
> **Ultimate Cheatsheet & Reference (v2.4.0)**

The Command Line Interface (CLI) is your primary tool for managing Odibi projects.

---

## 🟢 Level 1: The Basics

### 1. Create a New Pipeline File
Generate a "Master Kitchen Sink" reference file with all features enabled.
```bash
odibi create my_pipeline.yaml
```

### 2. Run a Pipeline
Execute the pipeline defined in your YAML file.
```bash
odibi run my_pipeline.yaml
```

**Common Flags:**
*   `--dry-run`: Simulate execution (don't write data).
*   `--resume`: Resume from the last failure (skips successful nodes).
*   `--env prod`: Load production environment variables.

---

## 🟡 Level 2: Intermediate (Management)

### 1. Initialize a Full Project
Don't just create a file; create a full folder structure with best practices (Bronze/Silver/Gold layers).
```bash
# Creates folder 'my_project' with organized subfolders
odibi init-pipeline my_project --template kitchen-sink
```

### 2. Validate Configuration
Check if your YAML is valid before running it.
```bash
odibi validate my_pipeline.yaml
```

### 3. Visualize Dependencies
Generate a dependency graph to understand flow.
```bash
# ASCII Art (Default)
odibi graph my_pipeline.yaml

# Mermaid Diagram (for Markdown)
odibi graph my_pipeline.yaml --format mermaid
```

---

## 🔴 Level 3: Hero (Advanced Tools)

### 1. Deep Diff (Compare Runs)
Did a pipeline run suddenly output fewer rows? Use `story diff` to compare two runs.
```bash
# List available runs
odibi story list

# Compare two story JSON files
odibi story diff stories/runs/20231027_120000.json stories/runs/20231027_120500.json
```
*Output: Shows execution time differences, row count changes, and success rates.*

### 2. Manage Secrets
Securely manage local secrets for your pipelines.
```bash
# Initialize secrets store (creates .env.template)
odibi secrets init

# Validate all secrets are configured
odibi secrets validate
```

---

## 🧠 Level 4: System Catalog (The Brain)

### Query the System Catalog
The System Catalog stores metadata about all your runs, pipelines, nodes, and state. Query it without manually reading Delta tables.

```bash
# List recent runs
odibi catalog runs config.yaml

# Filter by pipeline and status
odibi catalog runs config.yaml --pipeline my_etl --status SUCCESS --days 14

# List registered pipelines
odibi catalog pipelines config.yaml

# List nodes (optionally filter by pipeline)
odibi catalog nodes config.yaml --pipeline my_etl

# View HWM state checkpoints
odibi catalog state config.yaml

# Get execution statistics
odibi catalog stats config.yaml --days 30
```

**Catalog Subcommands:**
| Subcommand | Description |
| :--- | :--- |
| `runs` | List execution runs from `meta_runs` |
| `pipelines` | List registered pipelines from `meta_pipelines` |
| `nodes` | List registered nodes from `meta_nodes` |
| `state` | List HWM state checkpoints from `meta_state` |
| `tables` | List registered assets from `meta_tables` |
| `metrics` | List metrics definitions from `meta_metrics` |
| `patterns` | List pattern compliance from `meta_patterns` |
| `stats` | Show execution statistics (success rate, avg duration, etc.) |

**Common Flags:**
* `--format json`: Output as JSON instead of ASCII table
* `--pipeline <name>`: Filter by pipeline name
* `--days <n>`: Show data from last N days (default: 7)
* `--limit <n>`: Limit number of results (default: 20)

---

## 🔍 Level 5: Schema & Lineage Tracking

### Schema Version History
Track how table schemas evolve over time.

```bash
# View schema history for a table
odibi schema history silver/customers --config config.yaml

# Compare two schema versions
odibi schema diff silver/customers --config config.yaml --from-version 3 --to-version 5

# Output as JSON
odibi schema history silver/customers --config config.yaml --format json
```

**Example Output:**
```
Schema History: silver/customers
================================================================================
Version    Captured At            Changes
--------------------------------------------------------------------------------
v5         2024-01-30 10:15:00    +loyalty_tier
v4         2024-01-15 08:30:00    ~email (VARCHAR→STRING)
v3         2024-01-01 12:00:00    -legacy_id
v2         2023-12-15 09:00:00    +created_at, +updated_at
v1         2023-12-01 10:00:00    Initial schema (12 columns)
```

### Cross-Pipeline Lineage
Trace data dependencies across pipelines.

```bash
# Trace upstream sources
odibi lineage upstream gold/customer_360 --config config.yaml

# Trace downstream consumers
odibi lineage downstream bronze/customers_raw --config config.yaml

# Impact analysis - what would be affected by changes?
odibi lineage impact bronze/customers_raw --config config.yaml
```

**Example Output (upstream):**
```
Upstream Lineage: gold/customer_360
============================================================
gold/customer_360
└── silver/dim_customers (silver_pipeline.process_customers)
    └── bronze/customers_raw (bronze_pipeline.ingest_customers)
```

**Example Output (impact):**
```
⚠️  Impact Analysis: bronze/customers_raw
============================================================

Changes to bronze/customers_raw would affect:

  Affected Tables:
    - silver/dim_customers (pipeline: silver_pipeline)
    - gold/customer_360 (pipeline: gold_pipeline)
    - gold/churn_features (pipeline: ml_pipeline)

  Summary:
    Total: 3 downstream table(s) in 2 pipeline(s)
```

**Schema Subcommands:**
| Subcommand | Description |
| :--- | :--- |
| `history` | View schema version history for a table |
| `diff` | Compare two schema versions |

**Lineage Subcommands:**
| Subcommand | Description |
| :--- | :--- |
| `upstream` | Trace upstream sources of a table |
| `downstream` | Trace downstream consumers of a table |
| `impact` | Impact analysis for schema changes |

**Common Flags:**
* `--config <path>`: Path to YAML config file (required)
* `--depth <n>`: Maximum depth to traverse (default: 3)
* `--format json`: Output as JSON
* `--limit <n>`: Limit results (schema history only)

---

## 📄 Command Reference

| Command | Description |
| :--- | :--- |
| `run` | Execute a pipeline. |
| `create` | Create a single YAML config file. |
| `init-pipeline` | Scaffold a full project directory. |
| `validate` | Check YAML syntax and logic. |
| `graph` | Visualize pipeline dependencies. |
| `story` | Manage and compare execution reports (`generate`, `diff`, `list`). |
| `secrets` | Manage local secure secrets (`init`, `validate`). |
| `catalog` | Query System Catalog (`runs`, `pipelines`, `nodes`, `state`, `stats`). |
| `schema` | Schema version tracking (`history`, `diff`). |
| `lineage` | Cross-pipeline lineage (`upstream`, `downstream`, `impact`). |
| `init-vscode` | Setup VS Code environment. |
