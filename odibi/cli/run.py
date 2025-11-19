"""Run command implementation."""

from odibi.pipeline import PipelineManager


def run_command(args):
    """Execute pipeline from config file."""
    try:
        manager = PipelineManager.from_yaml(args.config)
        results = manager.run(dry_run=args.dry_run)

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
            print("\n❌ Pipeline execution failed")
            return 1
        else:
            print("\n✅ Pipeline completed successfully")
            return 0

    except Exception as e:
        print(f"Pipeline failed: {e}")
        return 1
