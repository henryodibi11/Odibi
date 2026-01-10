import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Map template names to their relative paths in the repo
# Templates align with docs/golden_path.md canonical examples
TEMPLATE_MAP = {
    "hello": "docs/examples/canonical/runnable/01_hello_world.yaml",
    "scd2": "docs/examples/canonical/runnable/03_scd2_dimension.yaml",
    "star-schema": "docs/examples/canonical/runnable/04_fact_table.yaml",
}

# Template descriptions for interactive prompt
TEMPLATE_DESCRIPTIONS = {
    "hello": "Hello World - Simple CSV read/write (start here)",
    "scd2": "SCD Type 2 - Slowly Changing Dimension pattern",
    "star-schema": "Star Schema - Full dimensional model with fact table",
}


def add_init_parser(subparsers):
    """Add arguments for init-pipeline command."""
    parser = subparsers.add_parser(
        "init-pipeline",
        aliases=["create", "init", "generate-project"],
        help="Initialize a new Odibi project from a template",
    )
    parser.add_argument("name", help="Name of the project directory to create")
    parser.add_argument(
        "--template",
        choices=list(TEMPLATE_MAP.keys()),
        default=None,
        help="Template to use (default: prompt user)",
    )
    # Add --force to overwrite existing directory
    parser.add_argument(
        "--force", action="store_true", help="Overwrite existing directory if it exists"
    )


def init_pipeline_command(args):
    """Execute the init-pipeline command."""
    project_name = args.name
    template_name = args.template
    force = args.force

    # Interactive Prompt
    if template_name is None:
        print("\nSelect a project template:")
        templates = list(TEMPLATE_MAP.keys())
        for i, t in enumerate(templates):
            desc = TEMPLATE_DESCRIPTIONS.get(t, t)
            print(f"  {i + 1}. {t:<12} - {desc}")

        try:
            choice = input(f"\nEnter number (default: 1 [{templates[0]}]): ").strip()
            if not choice:
                template_name = templates[0]
            else:
                idx = int(choice) - 1
                if 0 <= idx < len(templates):
                    template_name = templates[idx]
                else:
                    logger.error("Invalid selection.")
                    return 1
        except (ValueError, EOFError, KeyboardInterrupt):
            # Fallback for non-interactive
            template_name = "hello"
            logger.info(f"Using default template: {template_name}")

    # 1. Determine Target Path
    target_dir = Path(os.getcwd()) / project_name

    if target_dir.exists():
        if not force:
            logger.error(f"Directory '{project_name}' already exists. Use --force to overwrite.")
            return 1
        else:
            logger.warning(f"Overwriting existing directory '{project_name}'...")
            shutil.rmtree(target_dir)

    # 2. Find Template File
    # Assuming we are running from within the installed package or repo
    # Try to find the repo root relative to this file
    # This file is in odibi/cli/init_pipeline.py
    # Repo root is ../../../

    current_file = Path(__file__).resolve()
    repo_root = current_file.parent.parent.parent

    template_rel_path = TEMPLATE_MAP[template_name]
    source_path = repo_root / template_rel_path

    if not source_path.exists():
        # Fallback: check if we are installed and templates are packaged (not likely in this env but good practice)
        # For now, just fail if not found in repo structure
        logger.error(f"Template file not found at: {source_path}")
        logger.error(
            "Ensure you are running Odibi from the repository root or templates are correctly installed."
        )
        return 1

    # 3. Create Project Structure
    try:
        os.makedirs(target_dir)

        # Copy the template to odibi.yaml and fix paths
        target_file = target_dir / "odibi.yaml"

        # Read template, fix relative paths for standalone project
        with open(source_path, "r") as f:
            template_content = f.read()

        # Fix paths: ../sample_data -> ./sample_data (for standalone projects)
        template_content = template_content.replace("../sample_data", "./sample_data")

        with open(target_file, "w") as f:
            f.write(template_content)

        # Create standard directories
        os.makedirs(target_dir / "data", exist_ok=True)
        os.makedirs(target_dir / "data/raw", exist_ok=True)
        os.makedirs(target_dir / "logs", exist_ok=True)
        os.makedirs(target_dir / ".github/workflows", exist_ok=True)

        # Copy sample data from canonical examples
        sample_data_dir = repo_root / "docs/examples/canonical/sample_data"
        if sample_data_dir.exists():
            target_sample_dir = target_dir / "sample_data"
            shutil.copytree(sample_data_dir, target_sample_dir)
            logger.debug(f"Copied sample data to: {target_sample_dir}")

        # Create Dockerfile
        dockerfile_content = """FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g., for pyodbc)
# RUN apt-get update && apt-get install -y unixodbc-dev

# Install Odibi
RUN pip install odibi[all]

# Copy project files
COPY . /app

# Default command
CMD ["odibi", "run", "odibi.yaml"]
"""
        with open(target_dir / "Dockerfile", "w") as f:
            f.write(dockerfile_content)

        # Create GitHub CI Workflow
        ci_yaml_content = """name: Odibi CI

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3

    - name: Set up Python 3.11
      uses: actions/setup-python@v3
      with:
        python-version: "3.11"

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install odibi[all] pytest

    - name: Check Health (Doctor)
      run: odibi doctor

    - name: Validate Configuration
      run: odibi validate odibi.yaml

    - name: Run Unit Tests
      run: odibi test

    # Optional: Dry Run
    # - name: Dry Run Pipeline
    #   run: odibi run odibi.yaml --dry-run
"""
        with open(target_dir / ".github/workflows/ci.yaml", "w") as f:
            f.write(ci_yaml_content)

        # Create .dockerignore
        with open(target_dir / ".dockerignore", "w") as f:
            f.write("data/\nlogs/\n.git/\n__pycache__/\n*.pyc\n")

        # Create .gitignore
        with open(target_dir / ".gitignore", "w") as f:
            f.write("data/\nlogs/\n__pycache__/\n*.pyc\n.odibi/\nstories/\n")

        # Generate README.md
        readme_content = f"""# {project_name}

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
"""
        with open(target_dir / "README.md", "w") as f:
            f.write(readme_content)

        # Print golden path next steps
        print(f"\n✅ Created project: {project_name}")
        print(f"   Template: {template_name} ({TEMPLATE_DESCRIPTIONS.get(template_name, '')})")
        print(f"\n📁 Location: {target_dir}")
        print("\n🚀 Golden Path - Next Steps:")
        print(f"   cd {project_name}")
        print("   odibi validate odibi.yaml   # Check config")
        print("   odibi run odibi.yaml        # Run pipeline")
        print("   odibi story last            # View results")

        return 0

    except Exception as e:
        logger.error(f"Failed to create project: {str(e)}")
        return 1
