"""Run command implementation."""

import importlib.util
import sys
from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.utils.logging import logger


def load_extensions(path: Path):
    """Load python extensions (transforms.py, plugins.py) from path."""
    # Add path to sys.path to handle imports within the extensions
    if str(path) not in sys.path:
        sys.path.append(str(path))

    for name in ["transforms.py", "plugins.py"]:
        file_path = path / name
        if file_path.exists():
            try:
                module_name = file_path.stem
                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = module
                    spec.loader.exec_module(module)
                    logger.info(f"Loaded extension: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to load {name}: {e}")


def run_command(args):
    """Execute pipeline from config file."""
    try:
        config_path = Path(args.config).resolve()

        # Load extensions from config dir
        load_extensions(config_path.parent)

        # Load extensions from parent dir (e.g. if config is in config/)
        load_extensions(config_path.parent.parent)

        # Load extensions from CWD (if different)
        if config_path.parent != Path.cwd():
            load_extensions(Path.cwd())

        manager = PipelineManager.from_yaml(args.config, env=args.env)
        results = manager.run(dry_run=args.dry_run, resume_from_failure=args.resume)

        # Check results for failures
        failed = False
        if isinstance(results, dict):
            # Multiple pipelines
            for result in results.values():
                if result.failed:
                    failed = True
                    break
        else:
            # Single pipeline
            if results.failed:
                failed = True

        if failed:
            logger.error("Pipeline execution failed")
            return 1
        else:
            logger.info("Pipeline completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1
