"""Run command implementation."""

from odibi.pipeline import PipelineManager
from odibi.utils.logging import logger


def run_command(args):
    """Execute pipeline from config file."""
    try:
        manager = PipelineManager.from_yaml(args.config)
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
