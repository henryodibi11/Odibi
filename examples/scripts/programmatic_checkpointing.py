import os

import pandas as pd

from odibi.config import NodeConfig, PipelineConfig, ReadConfig, WriteConfig
from odibi.connections import LocalConnection
from odibi.pipeline import Pipeline

# 1. Define a simple pipeline programmatically
pipeline_config = PipelineConfig(
    pipeline="demo_pipeline",
    nodes=[
        NodeConfig(
            name="generate_data",
            # In a real scenario, this would be a read/transform
            # Here we simulate it by reading a file we create on the fly
            read=ReadConfig(connection="local", format="csv", path="input.csv"),
            write=WriteConfig(connection="local", format="csv", path="intermediate.csv"),
        ),
        NodeConfig(
            name="process_data",
            depends_on=["generate_data"],
            write=WriteConfig(connection="local", format="csv", path="final.csv"),
        ),
    ],
)

# Setup dummy data
pd.DataFrame({"id": [1, 2, 3]}).to_csv("data/input.csv", index=False)

# 2. Initialize Pipeline
# We need a basic ProjectConfig to satisfy PipelineManager, or just create Pipeline directly
pipeline = Pipeline(
    pipeline_config=pipeline_config,
    connections={
        "local": {"type": "local", "base_path": "data"}
    },  # Simplified dict for Direct init
    # Note: In actual usage, you'd use proper Connection objects or PipelineManager
)

pipeline.connections = {"local": LocalConnection(base_path="data")}

print("--- Run 1: Full Execution ---")
results1 = pipeline.run(resume_from_failure=False)
print(f"Completed: {results1.completed}")
# Output: ['generate_data', 'process_data']

print("\n--- Run 2: Resume (Nothing changed) ---")
# Since Run 1 was successful, resume should skip everything that was written
results2 = pipeline.run(resume_from_failure=True)
print(f"Completed: {results2.completed}")
# Check if skipped
skipped_nodes = [n for n, r in results2.node_results.items() if r.metadata.get("skipped")]
print(f"Skipped nodes: {skipped_nodes}")
# Output: ['generate_data', 'process_data'] (both skipped/restored)

print("\n--- Run 3: Force failure and Resume ---")
# Let's simulate a failure in the second node by corrupting the state or config?
# Or cleaner: assume 'generate_data' succeeded (which it did in Run 1),
# but 'process_data' failed (hypothetically).
# If we re-run with resume, 'generate_data' should be skipped.

# To simulate this programmatically without breaking code, we can just rely on the fact
# that 'generate_data' was successful in Run 1/2.
# If we were to clear the 'final.csv' but keep 'intermediate.csv' and state,
# logic dictates we might still skip if state says success.
# Note: Checkpointing relies on .odibi/state.json.
# It assumes if state says success, it is success.
# It attempts to restore (read file).

# Let's delete the final output but keep intermediate
if os.path.exists("data/final.csv"):
    os.remove("data/final.csv")

# Currently, my implementation checks state.json first.
# If state says success, it tries to restore.
# If restore fails (file missing), it re-runs.
# So if I deleted final.csv, 'process_data' restore should fail, and it should re-run.
# 'generate_data' restore should succeed (intermediate.csv exists), so it skips.

results3 = pipeline.run(resume_from_failure=True)
print(f"Completed: {results3.completed}")

# Check specifics
gen_result = results3.node_results["generate_data"]
proc_result = results3.node_results["process_data"]

print(f"Generate Data Skipped? {gen_result.metadata.get('skipped', False)}")
print(f"Process Data Skipped? {proc_result.metadata.get('skipped', False)}")
