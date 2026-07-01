# Agent Migration Guide: Odibi MCP Universal Gateway

## 🎯 Overview

The Odibi MCP connector now uses a **universal gateway pattern** that compresses 43 actions into 2 tools:
- `odibi_execute(action, args)` - Execute any action
- `odibi_help(category, action)` - Discover actions

This guide shows agents how to transition from the old 30-tool pattern to the new gateway.

---

## 🚀 Quick Start

### Before You Start
```python
# OLD: Check available tools
tools = list_available_tools()
# Returns: 30+ odibi_* tools

# NEW: Discover actions
help_result = odibi_help()
# Returns: Catalog of 43 actions in 9 categories
```

### Basic Pattern
```python
# OLD: Direct tool calls
result = profile_source(connection="s3", path="data.csv")
result = run_workflow(workflow_name="build_and_validate", params={})

# NEW: Gateway pattern
result = odibi_execute('profile_source', '{"connection": "s3", "path": "data.csv"}')
result = odibi_execute('run_workflow', '{"workflow_name": "build_and_validate", "params": {}}')
```

**Key Change:** Actions are now strings, args are JSON-encoded strings.

---

## 📚 Discovery Workflow

### Step 1: Get Full Catalog
```python
catalog = odibi_help()

# Returns:
{
  "kind": "odibi_help",
  "version": "1.0",
  "categories": [
    {
      "name": "Workflows",
      "description": "Multi-step deterministic recipes",
      "action_count": 4,
      "actions": ["run_workflow", "resume_workflow", ...]
    },
    # ... 8 more categories
  ],
  "total_actions": 43
}
```

### Step 2: Explore Category
```python
workflows = odibi_help(category='Workflows')

# Returns:
{
  "kind": "category_help",
  "category": "Workflows",
  "actions": [
    {
      "name": "run_workflow",
      "signature": "workflow_name, params=None",
      "description": "Execute named workflow"
    },
    # ... 3 more actions
  ]
}
```

### Step 3: Get Action Details
```python
details = odibi_help(action='profile_source')

# Returns:
{
  "kind": "action_help",
  "action": "profile_source",
  "signature": "connection, path, max_rows=100",
  "description": "Profile a data source...",
  "args": [
    {"name": "connection", "type": "str", "required": true, ...},
    {"name": "path", "type": "str", "required": true, ...},
    {"name": "max_rows", "type": "int", "required": false, "default": 100}
  ],
  "returns": {...},
  "examples": [...]
}
```

---

## 🔄 Common Migration Patterns

### Pattern 1: Data Profiling
```python
# OLD
env = map_environment(connection="s3_raw")
profile = profile_source(connection="s3_raw", path="orders.csv", max_rows=1000)

# NEW
env = odibi_execute('map_environment', '{"connection": "s3_raw"}')
profile = odibi_execute('profile_source', '{"connection": "s3_raw", "path": "orders.csv", "max_rows": 1000}')
```

### Pattern 2: Workflow Execution
```python
# OLD
workflows = list_workflows()
result = run_workflow(workflow_name="build_and_validate", params={})

# NEW
workflows = odibi_execute('list_workflows')
result = odibi_execute('run_workflow', '{"workflow_name": "build_and_validate", "params": {}}')
```

### Pattern 3: Pipeline Inspection
```python
# OLD
story = story_read(pipeline="etl_daily", run_id="2025-06-28-01")
sample = node_sample(pipeline="etl_daily", node="transform_orders", limit=10)

# NEW
story = odibi_execute('story_read', '{"pipeline": "etl_daily", "run_id": "2025-06-28-01"}')
sample = odibi_execute('node_sample', '{"pipeline": "etl_daily", "node": "transform_orders", "limit": 10}')
```

### Pattern 4: Session Building
```python
# OLD
create_pipeline(name="customer_dim", engine="pandas")
add_node(node_name="dedupe", transformer="deduplicate", params={"key": "customer_id"})
yaml = render_pipeline_yaml()

# NEW
odibi_execute('create_pipeline', '{"name": "customer_dim", "engine": "pandas"}')
odibi_execute('add_node', '{"node_name": "dedupe", "transformer": "deduplicate", "params": {"key": "customer_id"}}')
yaml = odibi_execute('render_pipeline_yaml')
```

---

## 🎓 Best Practices

### 1. Always Start with Discovery
```python
# ✅ GOOD: Discover first
help_result = odibi_help()
# Then execute based on what's available

# ❌ BAD: Assume action exists
result = odibi_execute('unknown_action', '{}')
```

### 2. Use Category Help for Related Actions
```python
# ✅ GOOD: Explore category when multiple actions needed
discovery_actions = odibi_help(category='Discovery')
for action in discovery_actions['actions']:
    print(f"Available: {action['name']} - {action['description']}")

# ❌ BAD: Call help() for every single action
```

### 3. Handle Errors Gracefully
```python
result = odibi_execute('profile_source', '{"connection": "s3"}')

if 'error' in result:
    # Error includes helpful context
    print(f"Error: {result['error']}")
    print(f"Tip: {result.get('tip', 'N/A')}")
    
    # Some errors include suggestions
    if 'expected_signature' in result:
        print(f"Expected: {result['expected_signature']}")
```

### 4. Use Action Help for Complex Actions
```python
# When unsure about parameters
details = odibi_help(action='suggest_pipeline')
# Review args, examples, returns before calling
```

---

## ⚡ Performance Tips

### 1. Lazy Bootstrap
The dispatcher is instantiated on first use - no startup penalty.

### 2. Compact JSON
Args are JSON-encoded strings. Keep payloads minimal:
```python
# ✅ GOOD: Minimal JSON
odibi_execute('profile_source', '{"connection":"s3","path":"data.csv"}')

# ❌ UNNECESSARY: Pretty JSON with whitespace
odibi_execute('profile_source', '{
  "connection": "s3",
  "path": "data.csv"
}')
```

### 3. Batch Discovery
```python
# ✅ GOOD: One help call for category
category_help = odibi_help(category='Discovery')

# ❌ BAD: Multiple action-specific help calls
help1 = odibi_help(action='map_environment')
help2 = odibi_help(action='profile_source')
help3 = odibi_help(action='profile_folder')
```

---

## 🔍 Debugging

### Unknown Action
```python
result = odibi_execute('nonexistent_action', '{}')

# Returns:
{
  "error": "Unknown action: nonexistent_action",
  "tip": "Run odibi_help() to see available actions",
  "valid_actions": [...],  # All 43 actions
  "categories": [...]      # 9 categories
}
```

### Invalid Args
```python
result = odibi_execute('profile_source', '{"wrong_param": "value"}')

# Returns:
{
  "error": "Invalid arguments for profile_source: ...",
  "action": "profile_source",
  "expected_signature": "connection, path, max_rows=100",
  "tip": "Run odibi_help(action='profile_source') for usage details"
}
```

### Malformed JSON
```python
result = odibi_execute('profile_source', '{bad json}')

# Returns:
{
  "error": "Invalid JSON in args: ...",
  "args_received": "{bad json}"
}
```

---

## 📋 Action Quick Reference

### Workflows (4)
- `run_workflow` - Execute workflow
- `resume_workflow` - Continue paused
- `list_workflows` - List available
- `get_workflow` - Get definition

### Discovery (3)
- `map_environment` - List connections
- `profile_source` - Profile data
- `profile_folder` - List files

### Inspection (4)
- `story_read` - Execution summary
- `node_sample` - Success rows
- `node_failed_rows` - Failed rows
- `lineage_graph` - Flow diagram

### Construction (5)
- `list_transformers` - List transformers
- `list_patterns` - List patterns
- `apply_pattern_template` - Generate YAML
- `suggest_pipeline` - Smart suggestion
- `create_ingestion_pipeline` - Bronze→silver

### Validation (4)
- `validate_yaml` - Structure check
- `validate_pipeline` - Dry-run
- `test_pipeline` - Full test
- `diagnose` - Troubleshoot

### Task Guidance (2)
- `get_task_guidance` - Structured Q&A
- `list_task_types` - Available tasks

### Onboarding (9)
- `onboard` - System overview
- `get_schema` - Config contract
- `search_docs` - Search docs
- `get_doc` - Get doc
- `list_docs` - List docs
- `list_examples` - List examples
- `get_example` - Get example
- `list_skills` - List skills
- `get_skill` - Load skill

### Download (3)
- `download_sql` - Export SQL
- `download_table` - Export data
- `download_file` - Write YAML

### Session Builder (9)
- `create_pipeline` - Start session
- `add_node` - Add node
- `configure_read` - Set source
- `configure_write` - Set target
- `configure_transform` - Update node
- `get_pipeline_state` - Get state
- `render_pipeline_yaml` - Generate YAML
- `list_sessions` - List sessions
- `discard_pipeline` - Clear session

---

## ✅ Checklist for Agent Developers

- [ ] Replace direct tool calls with `odibi_execute(action, args)`
- [ ] Use `odibi_help()` for discovery instead of listing tools
- [ ] Encode args as JSON strings
- [ ] Handle error responses with `error` key
- [ ] Use category help when working with related actions
- [ ] Use action help when unsure about parameters
- [ ] Test error cases (unknown action, invalid args, bad JSON)

---

## 🎉 Benefits

**Before (30+ tools):**
- 30+ tool names to remember
- Overwhelming tool list
- Hard to discover capabilities
- Approaching MCP limit

**After (2 tools):**
- ✅ 2 tool names (execute, help)
- ✅ Discovery via help system
- ✅ 43 actions available
- ✅ Room to grow
- ✅ Consistent interface

---

## 📞 Support

For questions or issues:
1. Run `odibi_help()` for full catalog
2. Run `odibi_help(action='<name>')` for specific action
3. Check error messages - they include helpful tips
4. Review examples in action help

**Happy building!** 🚀
