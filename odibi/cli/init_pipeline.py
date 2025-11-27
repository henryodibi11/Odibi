import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Map template names to their relative paths in the repo
TEMPLATE_MAP = {
    "kitchen": "examples/templates/kitchen_sink.odibi.yaml",
    "full": "examples/templates/template_full.yaml",
    "local": "examples/templates/simple_local.yaml",
    "local-medallion": "examples/templates/simple_local.yaml",
    "azure": "examples/templates/azure_spark.yaml",
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
            print(f"  {i + 1}. {t}")

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
            template_name = "local"
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

        # Copy the template to odibi.yaml
        target_file = target_dir / "odibi.yaml"
        shutil.copy2(source_path, target_file)

        # Create standard directories
        os.makedirs(target_dir / "data", exist_ok=True)
        os.makedirs(target_dir / "data/raw", exist_ok=True)
        os.makedirs(target_dir / "logs", exist_ok=True)
        os.makedirs(target_dir / ".github/workflows", exist_ok=True)

        # Create sample data for local template
        if template_name in ["local", "local-medallion"]:
            with open(target_dir / "data/raw/sample_data.csv", "w") as f:
                f.write("id,name,value,updated_at\n")
                f.write("1,Item A,100,2023-01-01 10:00:00\n")
                f.write("2,Item B,200,2023-01-01 11:00:00\n")
                f.write("1,Item A (Old),90,2023-01-01 09:00:00\n")

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

## Getting Started

### Prerequisites
- Python 3.9+
- Odibi (`pip install odibi`)

### Project Structure
- `odibi.yaml`: Main pipeline configuration
- `data/`: Local data storage
- `stories/`: Execution reports (HTML)

### Commands

**1. Validate Configuration**
```bash
odibi validate odibi.yaml
```

**2. Check Health**
```bash
odibi doctor
```

**3. Run Pipeline**
```bash
odibi run odibi.yaml
```

**4. View Results**
```bash
odibi ui
```

## CI/CD
A GitHub Actions workflow is included in `.github/workflows/ci.yaml` that validates the project on every push.
"""
        with open(target_dir / "README.md", "w") as f:
            f.write(readme_content)

        logger.info(f"Created new project '{project_name}' using '{template_name}' template.")
        logger.info(f"Location: {target_dir}")
        logger.info(f"Next step: cd {project_name} && odibi run odibi.yaml")

        return 0

    except Exception as e:
        logger.error(f"Failed to create project: {str(e)}")
        return 1
