"""Run command implementation."""
from odibi.pipeline import PipelineManager


def run_command(args):
    """Execute pipeline from config file."""
    try:
        manager = PipelineManager.from_yaml(args.config)
        results = manager.run()
        print("Pipeline completed successfully")
        return 0
    except Exception as e:
        print(f"Pipeline failed: {e}")
        return 1
