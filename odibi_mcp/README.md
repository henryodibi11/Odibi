# Odibi Knowledge MCP Server

MCP (Model Context Protocol) server that provides AI tools (Cline, Claude Desktop, Cursor) with **perfect retrieval** of odibi knowledge.

## Tools (13 total)

### Structured (Deterministic - Perfect Retrieval)

| Tool | What it returns |
|------|-----------------|
| `list_transformers` | All 52 transformers with params |
| `list_patterns` | All 6 DWH patterns |
| `list_connections` | All 7 connection types |
| `explain(name)` | Full docs for any transformer/pattern/connection |
| `get_transformer_signature` | **Exact** custom transformer code pattern |
| `get_yaml_structure` | **Exact** YAML pipeline structure |

### Documentation (137 markdown files accessible)

| Tool | What it returns |
|------|-----------------|
| `get_deep_context` | Full ODIBI_DEEP_CONTEXT.md (2299 lines, 62KB) |
| `get_doc(path)` | Any doc by path (e.g., `docs/patterns/scd2.md`) |
| `list_docs(category)` | List docs by category (patterns, tutorials, guides, features, reference, examples, context) |
| `search_docs(query)` | Search all docs for a keyword |

### Semantic (RAG - 6402 indexed chunks)

| Tool | Purpose |
|------|---------|
| `query_codebase(question)` | Semantic search over indexed codebase |
| `reindex(force=False)` | Rebuild the vector index |
| `get_index_stats` | Check index status |

## Installation

### For Cline

Add to your `cline_mcp_settings.json`:

```json
{
  "mcpServers": {
    "odibi-knowledge": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "D:/odibi",
      "disabled": false
    }
  }
}
```

### For Claude Desktop

Add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "odibi-knowledge": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "D:/odibi"
    }
  }
}
```

## Usage Examples

Once configured, AI tools can call:

```
# Get exact patterns (deterministic)
list_transformers           → All 52 transformers with params
get_yaml_structure          → Exact YAML template
get_transformer_signature   → Exact function signature

# Access documentation
get_deep_context            → Full 2299-line framework docs
get_doc("patterns/scd2.md") → SCD2 pattern documentation
search_docs("validation")   → Find all docs mentioning validation
list_docs(category="guides") → List all guide documents

# Explain anything
explain("scd2")             → Pattern + transformer docs for SCD2
explain("local")            → Connection type documentation
```

## What This Solves

AI tools like Cline don't auto-embed repository context. This MCP server provides:

1. **Perfect retrieval** for common patterns (transformer signatures, YAML structure)
2. **Full documentation access** (137 markdown files, 62KB deep context)
3. **Semantic search** for exploratory questions (6402 indexed code chunks)

## RAG Setup (Optional)

Semantic search requires:

```bash
pip install chromadb sentence-transformers
```

Then:
```
reindex(force=True)
query_codebase("how does validation work?")
```
