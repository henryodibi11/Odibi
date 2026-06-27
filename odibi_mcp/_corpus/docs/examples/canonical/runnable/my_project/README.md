# my_project

A data engineering project built with [Odibi](https://github.com/henryodibi11/Odibi).

## Quick Start (Golden Path)

```bash
# 1. Validate your config
odibi validate odibi.yaml

# 2. Run the pipeline
odibi run odibi.yaml

# 3. View the execution story
odibi story last
```

## Project Structure

- `odibi.yaml` - Pipeline configuration
- `mcp_config.yaml` - MCP settings for AI assistant integration
- `sample_data/` - Source CSV files
- `data/` - Output data (created on first run)

## Debugging

If a pipeline fails, Odibi shows you exactly what to do next:

```bash
# View the story for a failed node
odibi story last --node <node_name>

# Visualize the dependency graph
odibi graph odibi.yaml

# Check environment health
odibi doctor
```

## Learn More

- [Golden Path Guide](https://henryodibi11.github.io/Odibi/golden_path/)
- [YAML Schema Reference](https://henryodibi11.github.io/Odibi/reference/yaml_schema/)
- [Patterns (SCD2, Fact, Dimension)](https://henryodibi11.github.io/Odibi/patterns/)
