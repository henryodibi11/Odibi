"""Test the diagnose tool."""

import sys
import os

sys.path.insert(0, ".")
os.environ["ODIBI_CONFIG"] = "exploration.yaml"
os.environ["ODIBI_PROJECTS_DIR"] = "projects"

from odibi_mcp.tools.diagnose import diagnose, diagnose_path
import json

print("=== Running diagnose() ===")
result = diagnose()
print(f"Status: {result.status}")
print(f"Summary: {result.summary}")
print(f"Environment: {json.dumps(result.environment, indent=2)}")
print(f"Paths: {json.dumps(result.paths, indent=2)}")
print(f"Connections: {json.dumps(result.connections, indent=2)}")
print(f"Issues: {result.issues}")
print(f"Suggestions: {result.suggestions}")

print("\n=== Running diagnose_path('projects/data/_stories') ===")
path_result = diagnose_path("projects/data/_stories")
print(json.dumps(path_result, indent=2))
