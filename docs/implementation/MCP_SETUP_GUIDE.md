# Using Odibi MCP Tools with AI Assistants

## Quick Start

Your MCP server is ready at: `odibi_mcp/server.py`

**Available in:**
- ✅ **Amp** (this assistant)
- ✅ **Cline** (VS Code extension)
- ✅ **Continue** (VS Code extension)
- ✅ **Claude Desktop**
- ✅ Any MCP-compatible client

---

## Setup for Amp (Easiest)

**Amp can connect directly!** Just configure MCP in VS Code settings.

### 1. Create MCP config file

**Location:** `.vscode/settings.json` or global settings

```json
{
  "amp.mcpServers": {
    "odibi": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "d:/odibi",
      "env": {
        "ODIBI_CONFIG": "d:/odibi/exploration.yaml"
      }
    }
  }
}
```

### 2. Restart Amp

Amp will connect to your MCP server and expose all 15 tools!

---

## Setup for Cline

### 1. Create `cline_mcp_settings.json`

**Location:** `d:/odibi/cline_mcp_settings.json`

```json
{
  "mcpServers": {
    "odibi": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "d:/odibi",
      "env": {
        "ODIBI_CONFIG": "d:/odibi/exploration.yaml"
      }
    }
  }
}
```

### 2. Configure Cline

In VS Code:
1. Open Cline settings
2. Point to `cline_mcp_settings.json`
3. Restart Cline

---

## Setup for Continue

### 1. Update `~/.continue/config.json`

```json
{
  "mcpServers": [
    {
      "name": "odibi",
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "d:/odibi",
      "env": {
        "ODIBI_CONFIG": "d:/odibi/exploration.yaml"
      }
    }
  ]
}
```

### 2. Restart Continue

---

## Setup for Claude Desktop

### 1. Edit Claude config

**Location:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "odibi": {
      "command": "python",
      "args": ["-m", "odibi_mcp.server"],
      "cwd": "d:/odibi",
      "env": {
        "ODIBI_CONFIG": "d:/odibi/exploration.yaml"
      }
    }
  }
}
```

### 2. Restart Claude Desktop

---

## Test MCP Server Manually

### 1. Run the server
```bash
cd d:/odibi
python -m odibi_mcp.server
```

You should see:
```
Starting MCP server...
Server ready
```

### 2. Test with MCP Inspector

```bash
npx @modelcontextprotocol/inspector python -m odibi_mcp.server
```

Opens web UI to test tools interactively.

---

## Example Usage with AI Assistant

Once connected, you can ask:

### Example 1: Simple Pipeline
```
You: "Build me a customer dimension pipeline"

AI (uses MCP tools):
1. Calls list_patterns() → sees 6 patterns
2. Selects "dimension" pattern  
3. Calls apply_pattern_template(
     pattern="dimension",
     pipeline_name="dim_customer",
     source_table="dbo.Customer",
     natural_key="customer_id",
     surrogate_key="customer_sk"
   )
4. Returns valid YAML ✅

You: Save this to dim_customer.yaml and run it!
```

### Example 2: Complex Multi-Node Pipeline
```
You: "Build a pipeline that:
      1. Loads orders from SQL
      2. Cleans duplicates
      3. Joins with customers
      4. Aggregates by month"

AI (uses Phase 2 builder):
1. Calls create_pipeline("order_processing")
2. Calls add_node("load_orders")
3. Calls configure_read/write for node 1
4. Calls add_node("clean_orders", depends_on=["load_orders"])
5. Calls configure_transform with deduplicate
... (6 more calls)
8. Calls render_pipeline_yaml()
9. Returns complete 4-node DAG ✅
```

### Example 3: Smart Suggestion
```
You: "I have a SQL table dbo.Employee with 50k rows"

AI:
1. Calls profile_source("db", "dbo.Employee")
2. Calls suggest_pipeline(profile)
3. AI: "I suggest 'dimension' pattern (75% confidence)"
4. Calls apply_pattern_template with suggested params
5. Returns YAML ✅
```

### Example 4: Bulk Ingestion
```
You: "Ingest all tables from Sales schema"

AI:
1. Calls map_environment("sales_db")
2. Finds: Orders, OrderLines, Customers, Products
3. Calls create_ingestion_pipeline with 4 tables
4. Returns 4-node parallel ingestion pipeline ✅
```

---

## Available Tools (What AI Can Do)

### Discovery (existing tools)
- `map_environment` - Explore connections
- `profile_source` - Get schema/samples
- `profile_folder` - Batch profiling
- `download_*` - Download data locally

### Construction (new tools)
**Phase 1 (simple):**
- `list_transformers` - See 56+ transformers
- `list_patterns` - See 6 patterns
- `apply_pattern_template` - Generate pipeline YAML
- `validate_pipeline` - Validate YAML

**Phase 2 (complex):**
- `create_pipeline` - Start session
- `add_node` - Add nodes
- `configure_read/write/transform` - Configure nodes
- `render_pipeline_yaml` - Finalize
- `list_sessions`, `discard_pipeline` - Manage sessions

**Phase 3 (smart):**
- `suggest_pipeline` - Auto-select pattern
- `create_ingestion_pipeline` - Bulk multi-table

### Diagnostics (existing)
- `diagnose` - Troubleshoot issues
- `story_read` - View run history
- `lineage_graph` - See data flow

---

## Verification

### Test MCP Server Starts
```bash
cd d:/odibi
python -c "import asyncio; from odibi_mcp import server; tools = asyncio.run(server.list_tools()); print(f'Server has {len(tools)} tools')"
```

Should output: `Server has 19 tools`

### Test Tool Call
```bash
python -c "from odibi_mcp.tools.construction import list_patterns; import json; print(json.dumps(list_patterns(), indent=2))"
```

Should return 6 patterns.

---

## What to Tell AI After Connecting

```
"You now have access to Odibi MCP tools!

To build pipelines:
1. Use list_patterns to see available patterns
2. Use apply_pattern_template to generate YAML
3. For complex pipelines, use create_pipeline + add_node + configure_*

Never write YAML manually - always use the tools!
All tools validate inputs, so you can't create invalid configs."
```

---

## Troubleshooting

**Server won't start:**
- Check Python path: `python --version` (need 3.9+)
- Check odibi installs: `pip list | grep odibi`
- Check .env file has valid OPENAI_API_KEY (if using AI features)

**Tools not showing:**
- Restart AI assistant
- Check MCP config path is correct
- Check `cwd` points to d:/odibi

**Tools return errors:**
- Check ODIBI_CONFIG environment variable
- Check exploration.yaml has valid connections
- Read error messages (they're structured with fixes)

---

## Next Steps

1. **Connect Amp/Cline/Continue** using config above
2. **Ask AI to build a pipeline** (use examples above)
3. **Watch it work!** AI calls tools, generates valid YAML
4. **Save and run** the generated YAML

Your AI assistant now has 15 tools to build Odibi pipelines without ever writing YAML manually!
