# 🖥️ Master CLI Guide

The Odibi CLI (`odibi`) is your control center. This guide covers every command you need for daily development and debugging.

---

## 1. `odibi run` (Execute)

Runs your pipeline. This is the command you will use 90% of the time.

```bash
# Run everything
odibi run odibi.yaml

# Run in "Dry Run" mode (Safest way to test)
# validates connections and prints SQL without executing
odibi run odibi.yaml --dry-run

# Run specific pipeline (if your file has multiple)
odibi run odibi.yaml --pipeline my_etl_pipeline
```

**Options:**
*   `--parallel <n>`: Run `n` nodes in parallel (default: 1).
*   `--env <name>`: Load a specific environment config (e.g., `production`).

---

## 2. `odibi stress` (Chaos Engineering)

Odibi includes a "Chaos Monkey" for data. It generates random, messy data to test if your pipeline is robust.

**Why use it?**
To find bugs *before* production. Does your SQL handle negative numbers? Dates like "2025-02-30"? Null IDs?

```bash
# Generate 10 random datasets and run the pipeline against them
odibi stress odibi.yaml --runs 10
```

If a run fails, Odibi saves the "killer input" to a `stress_failures/` folder so you can reproduce the crash.

---

## 3. `odibi doctor` (Debug Environment)

If you are getting weird errors ("Module not found", "Connection refused"), run the doctor.

```bash
odibi doctor odibi.yaml
```

**It checks:**
*   ✅ Python version compatibility.
*   ✅ Installed libraries (Spark, Azure, etc.).
*   ✅ Network connectivity to configured databases.
*   ✅ Validity of the YAML file.

---

## 4. `odibi init-pipeline` (New Project)

Start fresh with best practices.

```bash
odibi init-pipeline my_new_project --template local-medallion
```

**Available Templates:**
*   `local-medallion`: Pandas-based, local folders for Bronze/Silver/Gold. Best for starting out.
*   `azure-delta`: Spark-based, configured for ADLS + Delta Lake.
*   `reference-lite`: Minimal example for quick tests.

## 5. `odibi generate-project` (Scaffold from Data)

Don't write boilerplate.

```bash
odibi generate-project --input ./raw_csvs --output ./new_project
```

This command:
1.  Scans your input files (CSVs, Parquet).
2.  Infers schema (columns, types).
3.  Generates a complete `odibi.yaml` project structure.
4.  Writes starting SQL transformation files.

---

## 6. `odibi secrets` (Secret Management)

Manage local secrets securely.

```bash
# Create a template .env file with all secrets used in your YAML
odibi secrets init odibi.yaml
```

## 7. `odibi graph` (Visualization)

Visualize your pipeline dependencies.

```bash
# Show ASCII graph
odibi graph odibi.yaml
```

---

## 8. `odibi story` (Reporting)

Manage the execution reports ("Stories").

```bash
# View the latest run report in your browser
odibi story view --latest

# List all past runs
odibi story list

# Compare two runs (Diff)
# Great for seeing how a code change affected the data
odibi story diff run_1.json run_2.json
```
