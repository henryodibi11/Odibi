"""Validate command implementation."""


def validate_command(args):
    """Validate config file."""
    try:
        # Load and validate YAML using PipelineManager (which handles env vars + registry)
        from odibi.pipeline import PipelineManager

        # Check if we should look for transforms.py
        # PipelineManager.from_yaml handles loading transforms.py automatically
        manager = PipelineManager.from_yaml(args.config)

        # Iterate over pipelines and validate logic/params
        all_valid = True
        for name, pipeline in manager._pipelines.items():
            results = pipeline.validate()
            if not results["valid"]:
                all_valid = False
                print(f"\n[!] Pipeline '{name}' Errors:")
                for err in results["errors"]:
                    print(f"  - {err}")

            if results["warnings"]:
                print(f"\n[?] Pipeline '{name}' Warnings:")
                for warn in results["warnings"]:
                    print(f"  - {warn}")

        if all_valid:
            print("\n[OK] Config is valid")
            return 0
        else:
            print("\n[X] Validation failed")
            return 1

    except Exception as e:
        print(f"\n[X] Config validation failed: {e}")
        return 1
