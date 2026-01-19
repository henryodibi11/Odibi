"""Extension loading utilities."""

import importlib.util
import sys
from pathlib import Path

from odibi.utils.logging import logger


def load_extensions(path: Path) -> None:
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
                logger.warning(f"Failed to load {name}: {e}", exc_info=True)
