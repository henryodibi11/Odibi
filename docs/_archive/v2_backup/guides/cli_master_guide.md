# Master CLI Guide

The Odibi CLI is your control center for data engineering. This guide covers every command in detail, with practical examples for daily use.

## ⚡ Command Overview

| Command | Purpose | Frequency |
|---------|---------|-----------|
| `odibi run` | Executes pipelines. | Daily |
| `odibi validate` | Checks syntax without running. | Pre-commit |
| `odibi doctor` | Debugs environment & connections. | Setup / Error |
| `odibi stress` | Fuzz tests your pipeline against random data. | QA / CI |
| `odibi generate-project` | Creates new projects from data files. | Once per project |
| `odibi story` | Manages execution reports. | Post-run |

---

## 1. Running Pipelines (`odibi run`)

The core command. By default, it runs all nodes in sequence.

### Basic Usage
```bash
odibi run config.yaml
```

### Dry Run (Safety First)
Always run with `--dry-run` first. It validates connections and SQL syntax without moving data.
```bash
odibi run config.yaml --dry-run
```

### Parallel Execution
Speed up execution by running independent nodes in parallel.
```bash
# Run up to 4 nodes at once
odibi run config.yaml --parallel 4
```

### Overriding Variables
You can inject environment variables or override config values at runtime.
```bash
# Override output path
odibi run config.yaml --var output_path=s3://bucket/prod
```

### Log Levels
Debug issues with verbose logging.
```bash
odibi run config.yaml --log-level DEBUG
```

---

## 2. Debugging (`odibi doctor`)

If something feels wrong, call the doctor. It checks:
1.  **Environment:** Python version, Spark install, Java home.
2.  **Connections:** Can we reach Azure? Is the SQL server up?
3.  **Config:** Are there circular dependencies?

```bash
odibi doctor config.yaml
```

**Pro Tip:** Run this in your CI/CD pipeline to fail fast if secrets are missing.

---

## 3. Stress Testing (`odibi stress`)

Odibi includes a "Chaos Monkey" for data. It downloads real-world datasets (or generates random junk) and throws them at your pipeline to see if it breaks.

### Quick Fuzz
Test against 5 random CSVs from the local `data/` folder.
```bash
odibi stress config.yaml --runs 5
```

### The "Gauntlet"
Download datasets from Kaggle and stress test.
```bash
odibi stress config.yaml --source kaggle --dataset "netflix-shows"
```

### Output
If a run fails, Odibi saves the "killer input" to `stress_failures/`.
*   **Action:** Add this file to your test suite to prevent regression.

---

## 4. Scaffolding (`odibi generate-project`)

Don't write YAML by hand. Point Odibi at a folder of CSVs, and it will build the project for you.

```bash
odibi generate-project --input ./raw_data --output ./my_new_project
```

**What it does:**
1.  Analyzes CSV headers and types.
2.  Creates `odibi.yaml` with Bronze (Raw) and Silver (Cleaned) layers.
3.  Generates SQL transformations for cleaning (trimming spaces, casting types).
4.  Sets up the directory structure.

---

## 5. Managing Stories (`odibi story`)

Every run generates a "Story" (a JSON report with metadata, schema changes, and sample rows).

### List Stories
```bash
odibi story list
```

### View a Story
Renders the story as an HTML page and opens it in your browser.
```bash
odibi story view --latest
```

### Export
Convert stories to Markdown for GitHub/GitLab comments.
```bash
odibi story export --format markdown --output report.md
```
