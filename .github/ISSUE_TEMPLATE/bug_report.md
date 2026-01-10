---
name: Bug Report
about: Report a bug to help us improve Odibi
title: '[BUG] '
labels: bug
assignees: ''
---

## Environment

- **Odibi version:** (run `pip show odibi`)
- **Python version:** (run `python --version`)
- **OS:** Windows / Linux / macOS / WSL
- **Engine:** Pandas / Spark / Polars

## Describe the Bug

A clear description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Create this YAML config:
```yaml
# paste minimal config here
```

2. Run this command:
```bash
odibi run config.yaml
```

3. See error

## Expected Behavior

What you expected to happen.

## Actual Behavior

What actually happened. Include the full error message and traceback:

```
paste error here
```

## Minimal Reproducible Example

If possible, provide the smallest YAML config that reproduces the issue:

```yaml
project: test
engine: pandas

connections:
  local:
    type: local
    base_path: ./data

pipelines:
  - pipeline: test
    nodes:
      - name: test_node
        # ... minimal config to reproduce
```

## Additional Context

Any other context about the problem (screenshots, logs, related issues).
