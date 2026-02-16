# AI Assistant Setup Guide

This guide explains how to set up AI coding assistants (Continue, Cline, Amp) to work with odibi using MCP (Model Context Protocol) servers.

## Overview

Odibi includes an MCP server (`odibi-knowledge`) that gives AI assistants perfect knowledge of:
- All 52+ transformers and their signatures
- All 6 DWH patterns (Dimension, Fact, SCD2, Merge, Aggregation, Date Dimension)
- Exact YAML pipeline structure
- Framework documentation (2,300+ lines)

## Installation

### 1. Install odibi with MCP support

```bash
# Clone the repository
git clone https://github.com/henryodibi11/Odibi.git
cd odibi

# Create virtual environment
python -m venv .venv

# Activate (Windows)
.venv\Scripts\activate

# Activate (Linux/Mac)
source .venv/bin/activate

# Install with MCP support
pip install -e ".[mcp]"

# Or with RAG (semantic codebase search)
pip install -e ".[mcp-rag]"
```

### 2. Verify installation

```bash
python -m odibi_mcp.server --help
```

## AI Assistant Configuration

### Continue (Recommended for BYOK/Azure OpenAI)

Continue uses your own API keys (Azure OpenAI, OpenAI, etc.).

**Step 1: Create user config** (`~/.continue/config.yaml`):

```yaml
models:
  - name: Azure GPT-4o
    provider: azure
    model: gpt-4o
    apiBase: https://YOUR-RESOURCE.openai.azure.com/
    apiKey: YOUR-AZURE-KEY
    apiVersion: "2024-02-15-preview"
    engine: YOUR-DEPLOYMENT-NAME
```

**Step 2: Open odibi workspace**

The `.continuerc.json` in the odibi root auto-loads:
- odibi-knowledge MCP (your framework docs)
- filesystem, git, memory, fetch MCPs
- context7 (up-to-date library docs)
- sequential-thinking (complex reasoning)

### Cline

Cline also supports BYOK. Configure MCPs in VS Code settings or `cline_mcp_settings.json`:

```json
{
  "odibi-knowledge": {
    "command": "python",
    "args": ["-m", "odibi_mcp.server"],
    "cwd": "/path/to/odibi"
  }
}
```

The `.clinerules` file in the odibi root provides framework guidance.

### Amp

Amp is a managed service (no BYOK). Add MCP in VS Code settings:

```json
"amp.mcpServers": {
  "odibi-knowledge": {
    "command": "python",
    "args": ["-m", "odibi_mcp.server"],
    "cwd": "/path/to/odibi"
  }
}
```

The `AGENTS.md` file in the odibi root provides framework guidance.

## Available MCP Servers

### Included in odibi workspace config

| MCP | Purpose | Requires |
|-----|---------|----------|
| **odibi-knowledge** | Framework patterns, signatures, docs | `pip install -e ".[mcp]"` |
| **filesystem** | File read/write/search | npx (auto-downloads) |
| **git** | Git operations | npx (auto-downloads) |
| **memory** | Persistent context across sessions | npx (auto-downloads) |
| **sequential-thinking** | Complex reasoning | npx (auto-downloads) |
| **fetch** | Fetch web pages | npx (auto-downloads) |
| **context7** | Up-to-date library docs | npx (auto-downloads) |

### odibi-knowledge Tools (15 total)

**Bootstrap Tool:**
| Tool | Description |
|------|-------------|
| `bootstrap_context` | **CALL FIRST** - Auto-gather full project context |

**Discovery Tools:**
| Tool | Description |
|------|-------------|
| `map_environment` | Scout a connection to understand what exists |
| `profile_source` | Self-correcting profiler for files/tables |
| `profile_folder` | Batch profile all files in a folder |

**Knowledge Tools:**
| Tool | Description |
|------|-------------|
| `list_transformers` | List all 52+ transformers |
| `list_patterns` | List all 6 DWH patterns |
| `list_connections` | List all connection types |
| `explain(name)` | Get detailed docs for any feature |
| `get_validation_rules` | All validation rule types with examples |

**Validation & Debug Tools:**
| Tool | Description |
|------|-------------|
| `validate_yaml` | Validate YAML before saving |
| `diagnose_error` | Diagnose odibi errors and get fix suggestions |

**Story/Sample Tools:**
| Tool | Description |
|------|-------------|
| `story_read` | Get pipeline run status |
| `node_sample` | Get node output sample |
| `node_failed_rows` | Get validation failures |

**Lineage Tools:**
| Tool | Description |
|------|-------------|
| `lineage_graph` | Full pipeline lineage graph |

> **Note:** For YAML structure reference, see `docs/reference/yaml_schema.md`. For codebase questions, use grep on the `docs/` folder.

## Usage Tips

### Prompt Examples

```
# Explore data before building pipelines
"Use map_environment to see what's in my connection"
"Profile the customers.csv file"

# Get pattern/transformer info before writing YAML
"What patterns are available for tracking historical changes?"
"Explain the scd2 pattern"

# Use context7 for library docs
"How do I use PySpark window functions? use context7"

# Complex reasoning
"Plan the implementation of a new SCD2 pattern variant"
```

### Auto-invoke Rules

Add to your AI assistant's rules to auto-use odibi-knowledge:

```
When working with odibi code or YAML configs, always use the odibi-knowledge
MCP tools first: list_transformers, list_patterns, explain, validate_yaml.
Reference docs/reference/yaml_schema.md for YAML structure.
```

## Troubleshooting

### MCP server not starting

```bash
# Check if odibi_mcp is installed
python -c "import odibi_mcp; print('OK')"

# Test the server directly
python -m odibi_mcp.server
```

### "Module not found" errors

```bash
# Reinstall with MCP support
pip install -e ".[mcp]"
```

### npx MCPs not downloading

```bash
# Ensure Node.js is installed
node --version  # Should be v18+

# Clear npx cache if needed
npx clear-npx-cache
```

## Multi-Machine Setup

The odibi workspace config files are portable:

| File | Contains | Travels with git |
|------|----------|------------------|
| `.continuerc.json` | MCP configs, rules reference | ✅ Yes |
| `.continue/rules.md` | Framework guidance | ✅ Yes |
| `.clinerules` | Cline guidance | ✅ Yes |
| `AGENTS.md` | Amp guidance | ✅ Yes |

**Per-machine setup (one time):**

1. Clone odibi repo
2. `pip install -e ".[mcp]"`
3. Create `~/.continue/config.yaml` with your API keys

Everything else auto-loads from the workspace.
