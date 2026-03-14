#!/usr/bin/env python3
"""Fix all ChemE course YAML files to match Odibi requirements.

Applies:
1. Add project/story/system fields
2. Change name: to pipeline: in pipelines
3. Add format/path/mode to write sections
4. Ensure connection references are correct
"""

import re
from pathlib import Path
import yaml


def fix_yaml_file(filepath: Path):
    """Fix a single YAML file."""
    print(f"Fixing: {filepath}")

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Parse as dict to extract needed info
    try:
        data = yaml.safe_load(content)
    except:
        print("  ⚠️  Could not parse YAML, skipping")
        return False

    # Check if already has project field
    if "project" in data:
        print("  [SKIP] Already has project field")
        return False

    # Extract name if it exists
    project_name = data.get("name", filepath.stem)

    # Build new header
    new_header = f"""project: {project_name}
engine: pandas

connections:"""

    # Find connections section and add story/system after it
    if "connections:" in content:
        # Replace "name:" with "project:" if it exists
        content = re.sub(r"^name:\s*(.+)$", r"project: \1", content, flags=re.MULTILINE)

        # Find end of connections block and add story/system
        connections_match = re.search(r"(connections:.*?)\n\n(pipelines:)", content, re.DOTALL)
        if connections_match:
            connections_block = connections_match.group(1)
            # Determine connection name (first connection)
            conn_match = re.search(r"connections:\s*\n\s+(\w+):", content)
            if conn_match:
                conn_name = conn_match.group(1)

                story_system = f"""
story:
  connection: {conn_name}
  path: stories

system:
  connection: {conn_name}
  path: .odibi/catalog
"""
                content = content.replace(
                    connections_match.group(0),
                    connections_block + story_system + "\n" + connections_match.group(2),
                )

    # Fix pipeline name key
    content = re.sub(r"pipelines:\s*\n\s+-\s+name:", "pipelines:\n  - pipeline:", content)

    # Add write format/path/mode if missing
    if "write:" in content and "format:" not in content:
        # Find write sections and add format
        content = re.sub(
            r"(write:\s*\n\s+connection:\s+(\w+))(\s*\n)",
            r"\1\n          format: parquet\n          path: data.parquet\n          mode: overwrite\3",
            content,
        )

    # Write back
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print("  [OK] Fixed!")
    return True


def main():
    """Fix all YAML files in cheme_course examples."""
    base_path = Path(__file__).parent.parent / "examples" / "cheme_course"

    yaml_files = list(base_path.rglob("*.yaml"))
    print(f"Found {len(yaml_files)} YAML files\n")

    fixed_count = 0
    for filepath in sorted(yaml_files):
        if fix_yaml_file(filepath):
            fixed_count += 1

    print(f"\n[OK] Fixed {fixed_count}/{len(yaml_files)} files")
    print(f"[SKIP] {len(yaml_files) - fixed_count} files already correct or skipped")


if __name__ == "__main__":
    main()
