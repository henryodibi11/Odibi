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

### 2. Manage Secrets
Securely manage local secrets for your pipelines.
```bash
# Initialize secrets store
odibi secrets init

# Set a secret
odibi secrets set MY_API_KEY value123
```

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
| `init-vscode` | Setup VS Code environment. |
