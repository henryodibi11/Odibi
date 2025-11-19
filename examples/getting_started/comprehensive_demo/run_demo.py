import sys
import os
from pathlib import Path

# Add project root to path so we can import odibi if running from source
# (Not strictly necessary if odibi is installed via pip, but good for this repo context)
project_root = Path(__file__).resolve().parents[3]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from odibi.pipeline import PipelineManager
# IMPORT TRANSFORMS: This registers the @transform functions!
import transforms

def run():
    print("🚀 Starting Comprehensive Demo Pipeline...")
    
    # Initialize Manager
    # It automatically finds project.yaml in the current directory if not specified,
    # but we'll be explicit.
    config_path = "project.yaml"
    
    if not os.path.exists(config_path):
        print(f"❌ Error: {config_path} not found!")
        return

    manager = PipelineManager.from_yaml(config_path)
    
    # List available pipelines
    print(f"📋 Found pipelines: {manager.list_pipelines()}")
    
    # Run the pipeline
    results = manager.run("payroll_processing")
    
    # Print Summary
    if results.failed:
        print(f"\n❌ Pipeline Failed!")
        print(f"   Failed nodes: {results.failed}")
    else:
        print(f"\n✅ Pipeline Success!")
        print(f"   Duration: {results.duration:.2f}s")
        print(f"   Story generated at: {results.story_path}")
        
        # Let's verify the output exists
        output_file = Path("output/department_stats.csv")
        if output_file.exists():
            print("\n📊 Output (Department Stats):")
            with open(output_file, 'r') as f:
                print(f.read())

if __name__ == "__main__":
    run()
