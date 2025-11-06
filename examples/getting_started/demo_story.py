"""Demo script to show story generation."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import yaml
from odibi.pipeline import Pipeline
from odibi.config import PipelineConfig
from odibi.connections import LocalConnection

# Import transforms to register them
import transforms

def main():
    """Run pipeline and generate story."""
    
    print("=" * 60)
    print("ODIBI Story Generator Demo")
    print("=" * 60)
    print()
    
    # Load pipeline config
    with open('pipelines/transform.yaml') as f:
        pipeline_yaml = yaml.safe_load(f)
    
    pipeline_config = PipelineConfig(**pipeline_yaml)
    
    # Setup connections
    connections = {
        "local_data": LocalConnection(base_path="./data"),
        "local_output": LocalConnection(base_path="./output")
    }
    
    # Create pipeline with story generation
    pipeline = Pipeline(
        pipeline_config,
        connections=connections,
        generate_story=True,  # ← Enable stories!
        story_config={
            "max_sample_rows": 5,
            "output_path": "./stories/"
        }
    )
    
    print(f"Pipeline: {pipeline_config.pipeline}")
    print(f"Nodes: {[node.name for node in pipeline_config.nodes]}")
    print()
    
    # Run pipeline
    print("Running pipeline...")
    print()
    results = pipeline.run()
    
    # Show results
    print("=" * 60)
    print("Results")
    print("=" * 60)
    print(f"Completed: {results.completed}")
    print(f"Failed: {results.failed}")
    print(f"Skipped: {results.skipped}")
    print(f"Duration: {results.duration:.2f}s")
    print()
    
    # Show story location
    if results.story_path:
        print("=" * 60)
        print("Story Generated!")
        print("=" * 60)
        print(f"Story saved to: {results.story_path}")
        print()
        print("Preview (first 30 lines):")
        print("-" * 60)
        
        with open(results.story_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:30], 1):
                print(line.rstrip())
        
        if len(lines) > 30:
            print(f"\n... and {len(lines) - 30} more lines")
        
        print("-" * 60)
        print()
        print(f"Open the full story: {results.story_path}")
    else:
        print("No story generated")

if __name__ == "__main__":
    main()
