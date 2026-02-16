# Odibi Pipeline Skill

## The Rule
**Use MCP tools. Generate YAML, don't write it manually.**

## New Pipeline Sequence

### 1. Bootstrap
```
bootstrap_context()
```
Do this FIRST. No exceptions.

### 2. Explore
```
map_environment("connection_name")
map_environment("connection_name", "subfolder")
```

### 3. Profile
```
profile_source("connection_name", "path/to/data.csv")
```

### 4. Generate
```
generate_bronze_node(profile_result)
```

### 5. Save
```
create_file("pipelines/pipeline_name.yaml", yaml_content)
```

### 6. Validate
```
run_command("python -m odibi run pipelines/X.yaml --dry-run")
```

### 7. Run
```
run_command("python -m odibi run pipelines/X.yaml")
```

## Debug Sequence

### 1. See Failure
```
run_command("python -m odibi story last")
```

### 2. Get Help
```
debug_issue("error message", "what I was doing")
```

### 3. Fix
```
read_file("pipeline.yaml")
edit_file("pipeline.yaml", old_config, new_config)
```

### 4. Retry
```
run_command("python -m odibi run X.yaml")
```

## YAML Rules

### MUST USE:
- `read:` and `write:` (not source/sink)
- `format:` in every read/write block
- Underscores in node names

### MUST NOT:
- Write YAML from scratch (use generate_bronze_node)
- Use dashes in node names
- Skip format field

## Quick Reference

### Get transformer help
```
explain("transformer_name")
list_transformers()
```

### Get pattern help
```
explain("scd2")
explain("dimension")
list_patterns()
```

### Check environment
```
diagnose()
```

## DON'T

- ❌ Write YAML manually
- ❌ Guess YAML structure
- ❌ Skip validation
- ❌ Ask "does this look right?"

## DO

- ✅ Use generate_bronze_node()
- ✅ Always validate with --dry-run
- ✅ Fix errors and retry
- ✅ Report success/failure
