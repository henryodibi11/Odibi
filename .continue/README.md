# Continue Configuration for Odibi

## Setup

1. **Install Continue extension** in VS Code

2. **Add API keys** to `config.json`:
   - Gemini (FREE): Get from https://makersuite.google.com/app/apikey
   - OpenAI: Get from https://platform.openai.com/api-keys
   - Anthropic: Get from https://console.anthropic.com/

3. **For local models (FREE):**
   ```bash
   # Install Ollama
   ollama pull llama3:70b
   ollama pull starcoder2:3b
   ```

4. **Reload VS Code**

## Usage

### Simple Prompts (No Long Instructions!)
```
"What odibi patterns are available?"
"Build a customer dimension pipeline"
"Profile my ADLS sales data"
"Generate a 5-table ingestion pipeline"
```

### Custom Commands
- `/odibi-discover` - Discover data sources
- `/odibi-profile` - Profile a source
- `/odibi-build` - Build a pipeline

## What Continue Can Do

With odibi MCP tools, Continue can:
- ✅ Discover connections (`map_environment`)
- ✅ Profile data sources (`profile_source`)
- ✅ Suggest patterns (`suggest_pipeline`)
- ✅ Generate pipelines (`apply_pattern_template`)
- ✅ Build complex multi-node DAGs (builder tools)
- ✅ Validate YAML (`validate_pipeline`)
- ✅ Run diagnostics (`diagnose`)

## Testing

Ask Continue:
```
"List available odibi patterns"
```

Continue should call `list_patterns()` MCP tool and show 6 patterns.

If it works → MCP is connected! ✅

## Model Recommendations

### For Odibi Work

**Best (FREE):**
- Gemini Flash - Fast, free, works well with tools

**Good (Cheap):**
- GPT-4o-mini - $0.15/1M tokens, excellent with our tools (tested!)
- Claude Haiku - $0.25/1M tokens

**Local (FREE but slow):**
- Llama 3 70B - Requires beefy machine
- Codestral - Good for code

**Our tests used GPT-4o-mini with 100% success rate!**

## Troubleshooting

**MCP tools not showing:**
- Check Continue settings point to this config.json
- Reload VS Code
- Check MCP server starts: `python -m odibi_mcp.server`

**Tools return errors:**
- Set ODIBI_CONFIG env var or add to config
- Check exploration.yaml has valid connections

## Resources Auto-Loaded

Continue automatically loads 13 odibi documentation resources:
- Quick Reference (views, modes, gotchas)
- AI Instructions (workflows)
- Complete Capabilities (all features)
- Pattern guides
- Deep context (2,200 lines)

**No long prompts needed!** Continue has all the knowledge.
