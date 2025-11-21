# Story Generation Demo

This example demonstrates ODIBI's storytelling capabilities.

## How to Run

1. Generate sample data:
   ```bash
   python examples/story_demo/setup_data.py
   ```

2. Run the pipeline:
   ```bash
   python -m odibi.cli run examples/story_demo/project.yaml
   ```

3. View the generated story in `examples/story_demo/outputs/stories/`.
   It will contain:
   - Execution metadata (Git commit, version)
   - Schema changes (added columns)
   - Data samples
   - Configuration snapshot
