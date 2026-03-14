"""Validate and fix all ChemE course YAML files."""

import yaml
from pathlib import Path
import re


def validate_yaml(filepath):
    """Check if YAML file is valid."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            yaml.safe_load(f)
        return True, None
    except Exception as e:
        return False, str(e)


def fix_escaped_chars(filepath):
    """Fix escaped newlines and other issues."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # Fix escaped newlines (backtick-n from PowerShell)
    content = content.replace("`n", "\n")
    content = content.replace("\\n", "\n")

    # Fix write sections - ensure proper indentation
    # Pattern: write: followed by connection on same/next line
    content = re.sub(r"write:\s*\n\s+connection:", "write:\n          connection:", content)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)


def main():
    base = Path(r"D:\odibi\examples\cheme_course")

    print("Validating and fixing all YAML files...\n")

    total = 0
    fixed = 0
    working = 0

    for yaml_file in sorted(base.rglob("*.yaml")):
        total += 1
        rel_path = yaml_file.relative_to(base)

        # Check if valid
        valid, error = validate_yaml(yaml_file)

        if not valid:
            print(f"[FIX] {rel_path}")
            print(f"      Error: {error[:80]}...")

            # Try to fix
            fix_escaped_chars(yaml_file)

            # Validate again
            valid_after, error_after = validate_yaml(yaml_file)
            if valid_after:
                print("      [OK] Fixed!")
                fixed += 1
                working += 1
            else:
                print(f"      [ERROR] Still broken: {error_after[:60]}...")
        else:
            print(f"[ OK] {rel_path}")
            working += 1

    print(f"\n{'=' * 60}")
    print(f"Total files: {total}")
    print(f"Working: {working}")
    print(f"Fixed: {fixed}")
    print(f"Still broken: {total - working}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
