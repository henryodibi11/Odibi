"""Final fix for all YAML files - add write format/path/mode."""

from pathlib import Path
import re


def fix_write_section(filepath):
    """Add format/path/mode to write section if missing."""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    fixed_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        fixed_lines.append(line)

        # If this line has "write:" check next lines
        if re.match(r"\s+write:\s*$", line):
            # Check if format already exists in next few lines
            has_format = False
            for j in range(i + 1, min(i + 5, len(lines))):
                if "format:" in lines[j]:
                    has_format = True
                    break

            # If no format, add it after connection line
            if not has_format and i + 1 < len(lines) and "connection:" in lines[i + 1]:
                indent = len(lines[i]) - len(lines[i].lstrip())
                conn_indent = len(lines[i + 1]) - len(lines[i + 1].lstrip())

                # Add connection line
                fixed_lines.append(lines[i + 1])

                # Add format/path/mode with same indent as connection
                space = " " * conn_indent
                fixed_lines.append(f"{space}format: parquet\n")
                fixed_lines.append(f"{space}path: data.parquet\n")
                fixed_lines.append(f"{space}mode: overwrite\n")

                i += 2  # Skip the connection line we already added
                continue

        i += 1

    # Write back
    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(fixed_lines)


# Fix all files
base = Path(r"D:\odibi\examples\cheme_course")
for yaml_file in base.rglob("*.yaml"):
    try:
        fix_write_section(yaml_file)
        print(f"Fixed: {yaml_file.name}")
    except Exception as e:
        print(f"Error fixing {yaml_file.name}: {e}")

print("\nAll files processed!")
