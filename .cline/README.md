# Cline Configuration for Odibi

## Setup

1. **Install Cline extension** in VS Code

2. **Configure API key** in Cline settings:
   - Recommended: **Claude Sonnet** (best for data work)
   - Cheap option: **Claude Haiku**
   - Or: **GPT-4o-mini** (tested with 100% success!)

3. **Point Cline to this config:**
   - Cline Settings → MCP Settings File → Browse to `.cline/config.json`

4. **Reload VS Code**

## Model Recommendations

**Best for Odibi:**
- **Claude 3.5 Sonnet** - Excellent with tools, great reasoning
- **GPT-4o-mini** - Tested with 100% success, very cheap
- **Claude Haiku** - Cheapest Claude, still good

**Our testing:** GPT-4o-mini works perfectly (100% valid YAML, no hallucination)

## Usage

Just ask simple questions:
```
"What odibi patterns are available?"
"Build a customer dimension pipeline"
"Profile my ADLS connection"
```

Cline will:
1. Call MCP tools automatically
2. Read from 13 auto-loaded resources
3. Generate valid YAML
4. No long prompts needed from you!

## What's Available

**MCP Tools:** 19 (15 construction + 4 discovery)
**Resources:** 13 documentation files (auto-loaded)
**Knowledge:** Complete odibi capabilities

## Testing

Ask Cline:
```
"List odibi patterns"
```

Should call `list_patterns()` tool and show 6 patterns.

## Configuration

The MCP server is configured to:
- Load from: `d:/odibi/exploration.yaml`
- Run from: `d:/odibi`
- Auto-load: 13 resources
- Expose: 19 tools

**Ready to use!**
