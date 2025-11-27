import shutil
import sys
from pathlib import Path

import pandas as pd

# Add root to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from odibi.pipeline import PipelineManager

BASE_DIR = Path("./tests/pandas_validation/data")


def setup_data():
    if BASE_DIR.exists():
        shutil.rmtree(BASE_DIR)
    BASE_DIR.mkdir(parents=True)

    # Create input.csv
    df = pd.DataFrame({"col1": [1, 2, 3, 4], "col2": ["a", "b", "c", "d"]})
    df.to_csv(BASE_DIR / "input.csv", index=False)
    print("Created input.csv")


def run_test():
    config_path = "./tests/pandas_validation/pipeline_config.yaml"

    try:
        print("Initializing PipelineManager...")
        manager = PipelineManager.from_yaml(config_path)

        print("Running pipeline...")
        results = manager.run()

        # Check results
        if isinstance(results, dict):
            res = results["validation_flow"]
        else:
            res = results

        if res.failed:
            print(f"Pipeline FAILED. Failed nodes: {res.failed}")
            return False

        print("Pipeline completed successfully.")

        # Verify Output
        output_path = BASE_DIR / "output.csv"
        if not output_path.exists():
            print("FAIL: output.csv not found")
            return False

        out_df = pd.read_csv(output_path)
        print("Output DataFrame:")
        print(out_df)

        # Verify transformations
        # 1. Filter > 1: Should have 2, 3, 4 (3 rows)
        if len(out_df) != 3:
            print(f"FAIL: Expected 3 rows, got {len(out_df)}")
            return False

        # 2. Privacy: col2 should be redacted
        if not all(out_df["col2"] == "[REDACTED]"):
            print("FAIL: Privacy redaction failed")
            return False

        print("PASS: Runtime validation successful")
        return True

    except Exception as e:
        print(f"FAIL: Exception occurred: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    setup_data()
    success = run_test()
    sys.exit(0 if success else 1)
