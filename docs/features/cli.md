# Command-Line Interface

The Odibi CLI provides a comprehensive set of commands for running pipelines, managing configurations, exploring lineage, and querying the System Catalog.

## Overview

The Odibi CLI is your primary tool for:
- **Pipeline execution**: Run, validate, and monitor data pipelines
- **Configuration management**: Validate and scaffold YAML configs
- **Catalog queries**: Explore runs, nodes, and execution metadata
- **Lineage exploration**: Trace upstream/downstream dependencies
- **Schema tracking**: View schema history and compare versions

## Commands

### odibi run

Execute a pipeline from a YAML configuration file.

```bash
odibi run config.yaml
```

#### Options

| Flag | Description |
|------|-------------|
| `--env` | Environment to use (default: `development`) |
| `--dry-run` | Simulate execution without writing data |
| `--resume` | Resume from last failure (skip successful nodes) |
| `--parallel` | Run independent nodes in parallel |
| `--workers` | Number of worker threads for parallel execution (default: 4) |
| `--on-error` | Override error handling: `fail_fast`, `fail_later`, `ignore` |

#### Examples

```bash
# Basic execution
odibi run my_pipeline.yaml

# Production run with parallel execution
odibi run my_pipeline.yaml --env production --parallel --workers 8

# Test without writing data
odibi run my_pipeline.yaml --dry-run

# Resume a failed run
odibi run my_pipeline.yaml --resume
```

### odibi validate

Validate a YAML configuration file for syntax and logical errors.

```bash
odibi validate config.yaml
```

Validation checks include:
- YAML syntax
- Required fields
- Connection references
- Transform function existence
- Node dependency cycles

#### Example Output

```
[OK] Config is valid
```

Or with errors:

```
[!] Pipeline 'process_orders' Errors:
  - Node 'transform_orders' references unknown connection: missing_db
  - Circular dependency detected: nodeA -> nodeB -> nodeA

[X] Validation failed
```

### odibi catalog

Query the System Catalog for execution metadata, registered pipelines, and statistics.

```bash
odibi catalog <command> config.yaml [options]
```

#### Subcommands

| Subcommand | Description |
|------------|-------------|
| `runs` | List execution runs from `meta_runs` |
| `pipelines` | List registered pipelines from `meta_pipelines` |
| `nodes` | List registered nodes from `meta_nodes` |
| `state` | List HWM state checkpoints from `meta_state` |
| `tables` | List registered assets from `meta_tables` |
| `metrics` | List metrics definitions from `meta_metrics` |
| `patterns` | List pattern compliance from `meta_patterns` |
| `stats` | Show execution statistics |

#### Common Options

| Flag | Description |
|------|-------------|
| `--format`, `-f` | Output format: `table` (default) or `json` |
| `--pipeline`, `-p` | Filter by pipeline name |
| `--days`, `-d` | Show data from last N days (default: 7) |
| `--limit`, `-l` | Maximum number of results (default: 20) |
| `--status`, `-s` | Filter by status: `SUCCESS`, `FAILED`, `RUNNING` |

#### Examples

```bash
# List recent runs
odibi catalog runs config.yaml

# Filter runs by pipeline and status
odibi catalog runs config.yaml --pipeline my_etl --status FAILED --days 14

# List all registered pipelines
odibi catalog pipelines config.yaml

# View nodes for a specific pipeline
odibi catalog nodes config.yaml --pipeline silver_pipeline

# Check HWM state checkpoints
odibi catalog state config.yaml

# Get execution statistics
odibi catalog stats config.yaml --days 30

# Output as JSON
odibi catalog runs config.yaml --format json
```

#### Example Output

```
run_id     | pipeline_name | node_name      | status  | rows  | duration_ms | timestamp
-----------+---------------+----------------+---------+-------+-------------+---------------------
abc123     | bronze_etl    | ingest_orders  | SUCCESS | 15420 | 3250        | 2024-01-30 10:15:00
def456     | bronze_etl    | ingest_custo...| SUCCESS | 8932  | 2100        | 2024-01-30 10:14:00

Showing 2 runs from the last 7 days.
```

### odibi lineage

Explore cross-pipeline data lineage and perform impact analysis.

```bash
odibi lineage <command> <table> --config config.yaml [options]
```

#### Subcommands

| Subcommand | Description |
|------------|-------------|
| `upstream` | Trace upstream sources of a table |
| `downstream` | Trace downstream consumers of a table |
| `impact` | Impact analysis for schema changes |

#### Options

| Flag | Description |
|------|-------------|
| `--config` | Path to YAML config file (required) |
| `--depth` | Maximum depth to traverse (default: 3) |
| `--format` | Output format: `tree` (default) or `json` |

#### Examples

```bash
# Trace upstream sources
odibi lineage upstream gold/customer_360 --config config.yaml

# Trace downstream consumers
odibi lineage downstream bronze/customers_raw --config config.yaml

# Impact analysis
odibi lineage impact bronze/customers_raw --config config.yaml --depth 5

# Output as JSON
odibi lineage upstream gold/customer_360 --config config.yaml --format json
```

#### Example Output (upstream)

```
Upstream Lineage: gold/customer_360
============================================================
gold/customer_360
└── silver/dim_customers (silver_pipeline.process_customers)
    └── bronze/customers_raw (bronze_pipeline.ingest_customers)
```

#### Example Output (impact)

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

### odibi schema

Track schema version history and compare schema changes over time.

```bash
odibi schema <command> <table> --config config.yaml [options]
```

#### Subcommands

| Subcommand | Description |
|------------|-------------|
| `history` | Show schema version history for a table |
| `diff` | Compare two schema versions |

#### Options

| Flag | Description |
|------|-------------|
| `--config` | Path to YAML config file (required) |
| `--limit` | Maximum versions to show (default: 10) |
| `--format` | Output format: `table` (default) or `json` |
| `--from-version` | Source version number (for diff) |
| `--to-version` | Target version number (for diff) |

#### Examples

```bash
# View schema history
odibi schema history silver/customers --config config.yaml

# Compare specific versions
odibi schema diff silver/customers --config config.yaml --from-version 3 --to-version 5

# Output history as JSON
odibi schema history silver/customers --config config.yaml --format json
```

#### Example Output (history)

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

#### Example Output (diff)

```
Schema Diff: silver/customers
From v3 → v5
============================================================
+ loyalty_tier                   STRING               (added in v5)
~ email                          VARCHAR → STRING
- legacy_id                      INTEGER              (removed in v5)
  customer_id                    INTEGER              (unchanged)
  name                           STRING               (unchanged)
```

## Global Options

These options are available for all commands:

| Flag | Description |
|------|-------------|
| `--log-level` | Set logging verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`) |

```bash
# Enable debug logging
odibi run config.yaml --log-level DEBUG
```

## Examples

### Complete Workflow

```bash
# 1. Validate configuration
odibi validate my_pipeline.yaml

# 2. Dry run to test logic
odibi run my_pipeline.yaml --dry-run

# 3. Execute pipeline
odibi run my_pipeline.yaml --env production --parallel

# 4. Check execution results
odibi catalog runs my_pipeline.yaml --days 1

# 5. View statistics
odibi catalog stats my_pipeline.yaml --pipeline bronze_etl
```

### Debugging a Failed Pipeline

```bash
# Check recent failures
odibi catalog runs config.yaml --status FAILED --limit 10

# Resume from failure
odibi run config.yaml --resume

# Enable verbose logging
odibi run config.yaml --log-level DEBUG
```

### Schema Change Impact Assessment

```bash
# Check schema history before making changes
odibi schema history bronze/customers_raw --config config.yaml

# Assess downstream impact
odibi lineage impact bronze/customers_raw --config config.yaml

# After changes, verify schema was captured
odibi schema history bronze/customers_raw --config config.yaml --limit 1
```

### Monitoring Pipeline Health

```bash
# Daily stats check
odibi catalog stats config.yaml --days 7

# Find problematic nodes
odibi catalog runs config.yaml --status FAILED --days 30

# Check state for incremental loads
odibi catalog state config.yaml --pipeline my_incremental_etl
```

## Related

- [Getting Started](../tutorials/getting_started.md) - Getting started with Odibi
- [CLI Master Guide](../guides/cli_master_guide.md) - Comprehensive CLI reference
- [System Catalog](catalog.md) - Catalog metadata details
- [YAML Schema Reference](../reference/yaml_schema.md) - Configuration reference
