# 💻 Odibi CLI: Zero to Hero
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

### 4. Generate VS Code Setup
Configure VS Code for optimal Odibi development (IntelliSense, schema validation).
```bash
odibi init-vscode
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

### 2. Doctor (Debugging)
Something weird happening? The doctor checks your environment, dependencies, and connections.
```bash
odibi doctor my_pipeline.yaml
```
*Checks: Python version, library dependencies, connection connectivity, secret availability.*

### 3. Generate Project from Template
Advanced scaffolding for different use cases.
```bash
odibi generate-project --type basic --output my_new_project
```

### 4. Diagnostics
Run deep diagnostics on your installation.
```bash
odibi diagnostics
```

---

## 📄 Command Reference

| Command | Description |
| :--- | :--- |
| `run` | Execute a pipeline. |
| `create` | Create a single YAML config file. |
| `init-pipeline` | Scaffold a full project directory. |
| `validate` | Check YAML syntax and logic. |
| `doctor` | Diagnose environment and connection issues. |
| `graph` | Visualize pipeline dependencies. |
| `story` | Manage and compare execution reports (`generate`, `diff`, `list`). |
| `secrets` | Manage local secure secrets (`init`, `validate`). |
| `schema` | Generate JSON schema for VS Code intellisense. |
| `diagnostics` | Run system diagnostics. |
| `init-vscode` | Setup VS Code environment. |
