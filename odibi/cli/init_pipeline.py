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
        aliases=["create", "init"],
        help="Initialize a new Odibi project from a template",
    )
    parser.add_argument("name", help="Name of the project directory to create")
    parser.add_argument(
        "--template",
        choices=list(TEMPLATE_MAP.keys()),
        default="local",
        help="Template to use (default: local)",
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
        os.makedirs(target_dir / "logs", exist_ok=True)

        logger.info(f"Created new project '{project_name}' using '{template_name}' template.")
        logger.info(f"Location: {target_dir}")
        logger.info(f"Next step: cd {project_name} && odibi run odibi.yaml")

        return 0

    except Exception as e:
        logger.error(f"Failed to create project: {str(e)}")
        return 1
