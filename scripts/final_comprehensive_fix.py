"""Comprehensive fix for all ChemE course YAML files."""

from pathlib import Path


def fix_yaml(filepath):
    """Fix a YAML file completely."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # Remove duplicate write sections (caused by previous scripts)
    import re

    # Find write sections and remove duplicates
    write_pattern = r"(\s+write:\s*\n\s+connection:\s+\w+\s*\n\s+format:.*?\n\s+path:.*?\n\s+mode:.*?\n)\s+format:"
    while re.search(write_pattern, content):
        content = re.sub(write_pattern, r"\1", content)

    # Ensure all write sections have format/path/mode
    write_incomplete = r"(write:\s*\n\s+connection:\s+(\w+))(\s*\n(?!\s+format:))"

    def add_write_details(match):
        indent = "          "  # Standard indent for write properties
        return f"{match.group(1)}\n{indent}format: parquet\n{indent}path: data.parquet\n{indent}mode: overwrite{match.group(3)}"

    content = re.sub(write_incomplete, add_write_details, content)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return True


# Process all files
base = Path(r"D:\odibi\examples\cheme_course")
count = 0
for yaml_file in sorted(base.rglob("*.yaml")):
    try:
        fix_yaml(yaml_file)
        count += 1
        print(f"[{count:2d}] Fixed: {yaml_file.relative_to(base)}")
    except Exception as e:
        print(f"[XX] Error: {yaml_file.name} - {e}")

print(f"\nProcessed {count} files!")
