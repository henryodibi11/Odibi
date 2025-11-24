import os
import sys
import shutil
import json
import yaml
import pandas as pd
from odibi.pipeline import PipelineManager
from odibi.diagnostics import get_delta_diff, diff_runs
from odibi.transformations.registry import get_registry
from odibi.story.metadata import PipelineStoryMetadata, NodeExecutionMetadata
from examples.diagnostics_demo.logic import enrich_customers

# Setup paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
sys.path.append(ROOT_DIR)  # Add d:/odibi to path so we can import examples.diagnostics_demo

DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(DATA_DIR, exist_ok=True)
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Create initial data
df_v0 = pd.DataFrame(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "segment": ["A", "B", "A"]}
)
df_v0.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)

# Config
config_path = os.path.join(BASE_DIR, "pipeline.odibi.yaml")
project_config = {
    "project": "diagnostics_demo",
    "version": "0.1.0",
    "story": {"connection": "local", "path": "examples/diagnostics_demo/stories"},
    "engine": "pandas",
    "pipelines": [
        {
            "pipeline": "diagnostics_demo",
            "nodes": [
                {
                    "name": "raw_customers",
                    "read": {
                        "connection": "local",
                        "path": os.path.join(DATA_DIR, "customers.csv"),
                        "format": "csv",
                    },
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "customers_delta"),
                        "format": "delta",
                        "mode": "overwrite",  # Use overwrite for first run
                    },
                },
                {
                    "name": "enriched_customers",
                    "depends_on": ["raw_customers"],
                    "transformer": "examples.diagnostics_demo.logic.enrich_customers",
                    "write": {
                        "connection": "local",
                        "path": os.path.join(OUTPUT_DIR, "enriched_delta"),
                        "format": "delta",
                        "mode": "overwrite",
                    },
                },
            ],
        }
    ],
    "connections": {"local": {"type": "local", "base_path": "."}},
}

# We need to write this config to file because PipelineManager.from_yaml reads file
with open(config_path, "w") as f:
    yaml.dump(project_config, f)

# Register transformer manually since we are not using plugins system for this demo script
get_registry().register("examples.diagnostics_demo.logic.enrich_customers", enrich_customers)

print("--- Run 1 (Baseline) ---")
manager = PipelineManager.from_yaml(config_path)
results_v1 = manager.run()

if results_v1.failed:
    print(f"Run 1 Failed! Node Results: {results_v1.failed}")
    # Print error for failed node
    for failed in results_v1.failed:
        res = results_v1.node_results[failed]
        try:
            # Force safe printing
            msg = str(res.error)
            print(f"Error in {failed}: {msg.encode('ascii', 'replace').decode()}")
        except Exception:
            print("Could not print error message due to encoding issues.")
    exit(1)

# Verify Delta Metadata in Story
node_res = results_v1.node_results["enriched_customers"]
print(f"Run 1 Node Status: {node_res.success}")
if node_res.metadata.get("delta_info"):
    print(f"Run 1 Delta Version: {node_res.metadata['delta_info'].version}")
    print(f"Run 1 SQL: {node_res.metadata.get('executed_sql')}")
else:
    print("WARNING: No Delta Info found in metadata!")

# --- Modify Data ---
df_v1 = pd.DataFrame({"id": [4, 5], "name": ["David", "Eve"], "segment": ["B", "C"]})
# Append to CSV (simulating new data arrival)
# But for this test, we overwrite CSV with new data and append to Delta
# Wait, if I overwrite CSV, the node will read new data.
# The node mode is 'overwrite' in config above. Let's change it to 'append' for Run 2.
df_v1.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)  # New batch

# Update config to append
project_config["pipelines"][0]["nodes"][0]["write"]["mode"] = "append"
project_config["pipelines"][0]["nodes"][1]["write"]["mode"] = "append"
with open(config_path, "w") as f:
    yaml.dump(project_config, f)

print("\n--- Run 2 (New Data) ---")
manager = PipelineManager.from_yaml(config_path)
results_v2 = manager.run()

node_res_v2 = results_v2.node_results["enriched_customers"]
print(f"Run 2 Delta Version: {node_res_v2.metadata['delta_info'].version}")

# --- Diagnostics ---
print("\n--- Diagnostics: Delta Diff ---")
delta_path = os.path.join(OUTPUT_DIR, "enriched_delta")
diff = get_delta_diff(table_path=delta_path, version_a=0, version_b=1)
print(f"Rows changed: {diff.rows_change}")
print(f"Schema added: {diff.schema_added}")
print(f"Operations: {diff.operations_between}")
if diff.sample_added:
    print("Sample Added Rows:")
    print(json.dumps(diff.sample_added, default=str, indent=2))
if diff.sample_removed:
    print("Sample Removed Rows:")
    print(json.dumps(diff.sample_removed, default=str, indent=2))

print("\n--- Diagnostics: Run Diff ---")
# Need to load stories to diff them properly, but we have results objects which contain metadata logic
# But diff_runs expects PipelineStoryMetadata objects.
# The PipelineResults object has a story_path, but not the metadata object directly.
# However, we can reconstruct or mock it for this test, or read the generated story json?
# ODIBI generates HTML stories. The metadata is internal during run.
# Actually, let's look at diff_runs signature. It takes PipelineStoryMetadata.
# PipelineResults doesn't store that.
# We should probably expose the metadata object in results for programmatic access.
# For now, I will manually construct wrappers to test the logic.


# Helper to convert NodeResult to Metadata (simplification for test)
def res_to_meta(res):
    m = NodeExecutionMetadata(
        node_name=res.node_name,
        operation="test",
        status="success",
        duration=res.duration,
        rows_out=res.rows_processed,
        executed_sql=res.metadata.get("executed_sql", []),
        delta_info=res.metadata.get("delta_info"),  # This is already DeltaWriteInfo object?
        # Wait, in node.py I assign DeltaWriteInfo object to metadata["delta_info"].
    )
    return m


meta_v1 = PipelineStoryMetadata(pipeline_name="diagnostics_demo")
for n in results_v1.completed:
    meta_v1.nodes.append(res_to_meta(results_v1.node_results[n]))

meta_v2 = PipelineStoryMetadata(pipeline_name="diagnostics_demo")
for n in results_v2.completed:
    meta_v2.nodes.append(res_to_meta(results_v2.node_results[n]))

run_diff = diff_runs(meta_v1, meta_v2)
print("Run Diff Results:")
for node, d in run_diff.items():
    print(f"Node: {node}")
    print(f"  Rows drift: {d.rows_change_diff}")
    print(f"  Delta Version: {d.delta_version_change}")
    if d.sql_diff:
        print("  SQL Logic Changed!")

print("\nDone.")
