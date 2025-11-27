"""Run command implementation."""

from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger


def run_command(args):
    """Execute pipeline from config file."""
    try:
        config_path = Path(args.config).resolve()
        project_root = config_path.parent

        # Change CWD to config directory to resolve relative paths consistently
        import os

        original_cwd = os.getcwd()
        os.chdir(project_root)
        logger.debug(f"Changed working directory to: {project_root}")

        try:
            # Load extensions from config dir (which is now CWD)
            load_extensions(project_root)

            manager = PipelineManager.from_yaml(config_path.name, env=args.env)
            results = manager.run(
                dry_run=args.dry_run,
                resume_from_failure=args.resume,
                parallel=args.parallel,
                max_workers=args.workers,
                on_error=args.on_error,
            )
        finally:
            # Restore CWD
            os.chdir(original_cwd)

        # Check results for failures
        failed = False
        if isinstance(results, dict):
            # Multiple pipelines
            for result in results.values():
                if result.failed:
                    failed = True
                    logger.error(f"Pipeline '{result.pipeline_name}' failed")
                    for node_name in result.failed:
                        node_res = result.node_results.get(node_name)
                        if node_res and node_res.error:
                            logger.error(f"Node '{node_name}' error: {node_res.error}")

                            # Unbury Suggestions
                            error_obj = node_res.error
                            suggestions = getattr(error_obj, "suggestions", [])

                            if not suggestions and hasattr(error_obj, "original_error"):
                                suggestions = getattr(error_obj.original_error, "suggestions", [])

                            if suggestions:
                                logger.info("💡 Suggestions:")
                                for suggestion in suggestions:
                                    logger.info(f"   - {suggestion}")
                    break
        else:
            # Single pipeline
            if results.failed:
                failed = True
                logger.error(f"Pipeline '{results.pipeline_name}' failed")
                for node_name in results.failed:
                    node_res = results.node_results.get(node_name)
                    if node_res and node_res.error:
                        logger.error(f"Node '{node_name}' error: {node_res.error}")

                        # Unbury Suggestions
                        error_obj = node_res.error
                        suggestions = getattr(error_obj, "suggestions", [])

                        if not suggestions and hasattr(error_obj, "original_error"):
                            suggestions = getattr(error_obj.original_error, "suggestions", [])

                        if suggestions:
                            logger.info("Suggestions:")
                            for suggestion in suggestions:
                                logger.info(f"   - {suggestion}")

        if failed:
            logger.error("Pipeline execution failed")
            return 1
        else:
            logger.info("Pipeline completed successfully")
            return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return 1
