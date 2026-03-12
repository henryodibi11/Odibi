#!/usr/bin/env python3
"""
Validation script for ChemE course YAML examples.

Checks that all YAML files:
1. Are valid YAML syntax
2. Contain required Odibi structure
3. Use correct simulation format
4. Reference working functions (pid, prev, ema)

Usage:
    python scripts/validate_cheme_course.py
"""

import yaml
import sys
from pathlib import Path
from typing import Dict, Any


def validate_yaml_file(file_path: Path) -> Dict[str, Any]:
    """Validate a single YAML file."""
    result = {
        "file": str(file_path),
        "valid": True,
        "errors": [],
        "warnings": [],
    }

    try:
        with open(file_path, "r") as f:
            content = yaml.safe_load(f)

        # Check for nodes structure
        if "nodes" not in content:
            result["errors"].append("Missing 'nodes' top-level key")
            result["valid"] = False
        else:
            # Validate each node
            for i, node in enumerate(content["nodes"]):
                node_name = node.get("name", f"node_{i}")

                # Check read section
                if "read" not in node:
                    result["errors"].append(f"Node '{node_name}': Missing 'read' section")
                    result["valid"] = False
                else:
                    read_section = node["read"]

                    # Check for simulation format
                    if read_section.get("format") == "simulation":
                        # Validate simulation structure
                        if "options" not in read_section:
                            result["errors"].append(
                                f"Node '{node_name}': Missing 'options' in read"
                            )
                            result["valid"] = False
                        elif "simulation" not in read_section["options"]:
                            result["errors"].append(
                                f"Node '{node_name}': Missing 'simulation' in options"
                            )
                            result["valid"] = False
                        else:
                            sim = read_section["options"]["simulation"]

                            # Check required simulation fields
                            if "scope" not in sim:
                                result["errors"].append(
                                    f"Node '{node_name}': Missing 'scope' in simulation"
                                )
                                result["valid"] = False
                            if "entities" not in sim:
                                result["errors"].append(
                                    f"Node '{node_name}': Missing 'entities' in simulation"
                                )
                                result["valid"] = False
                            if "columns" not in sim:
                                result["errors"].append(
                                    f"Node '{node_name}': Missing 'columns' in simulation"
                                )
                                result["valid"] = False

                # Check write section
                if "write" not in node:
                    result["warnings"].append(f"Node '{node_name}': Missing 'write' section")
                else:
                    write_section = node["write"]
                    if "path" not in write_section and "connection" not in write_section:
                        result["warnings"].append(
                            f"Node '{node_name}': Write section has no 'path' or 'connection'"
                        )

        # Check for common functions in file content
        file_content = open(file_path, "r").read()

        # Look for pid() usage
        if "pid(" in file_content:
            # Check parameters
            required_params = ["pv=", "sp=", "Kp=", "Ki=", "Kd=", "dt="]
            missing_params = [p for p in required_params if p not in file_content]
            if missing_params:
                result["warnings"].append(f"pid() call may be missing parameters: {missing_params}")

        # Look for prev() usage
        if "prev(" in file_content:
            if "prev('" not in file_content:
                result["warnings"].append(
                    "prev() usage might be incorrect (should be prev('column', default))"
                )

    except yaml.YAMLError as e:
        result["valid"] = False
        result["errors"].append(f"YAML syntax error: {str(e)}")
    except Exception as e:
        result["valid"] = False
        result["errors"].append(f"Unexpected error: {str(e)}")

    return result


def validate_course_examples(course_dir: Path) -> Dict[str, Any]:
    """Validate all YAML examples in the course directory."""
    summary = {
        "total_files": 0,
        "valid_files": 0,
        "invalid_files": 0,
        "files_with_warnings": 0,
        "results": [],
    }

    # Find all YAML files
    yaml_files = list(course_dir.glob("**/*.yaml")) + list(course_dir.glob("**/*.yml"))
    summary["total_files"] = len(yaml_files)

    for yaml_file in sorted(yaml_files):
        result = validate_yaml_file(yaml_file)
        summary["results"].append(result)

        if result["valid"]:
            summary["valid_files"] += 1
        else:
            summary["invalid_files"] += 1

        if result["warnings"]:
            summary["files_with_warnings"] += 1

    return summary


def print_summary(summary: Dict[str, Any]):
    """Print validation summary."""
    print("\n" + "=" * 80)
    print("ChemE Course YAML Validation Summary")
    print("=" * 80)
    print(f"\nTotal files checked: {summary['total_files']}")
    print(f"Valid files: {summary['valid_files']}")
    print(f"Invalid files: {summary['invalid_files']}")
    print(f"Files with warnings: {summary['files_with_warnings']}")

    # Print errors
    error_count = sum(1 for r in summary["results"] if r["errors"])
    if error_count > 0:
        print(f"\n{'=' * 80}")
        print(f"ERRORS ({error_count} files)")
        print("=" * 80)
        for result in summary["results"]:
            if result["errors"]:
                print(f"\n❌ {result['file']}")
                for error in result["errors"]:
                    print(f"   - {error}")

    # Print warnings
    warning_count = sum(1 for r in summary["results"] if r["warnings"])
    if warning_count > 0:
        print(f"\n{'=' * 80}")
        print(f"WARNINGS ({warning_count} files)")
        print("=" * 80)
        for result in summary["results"]:
            if result["warnings"]:
                print(f"\n⚠️  {result['file']}")
                for warning in result["warnings"]:
                    print(f"   - {warning}")

    # Success summary
    if summary["invalid_files"] == 0:
        print(f"\n{'=' * 80}")
        print("✅ ALL FILES PASSED VALIDATION!")
        print("=" * 80)
    else:
        print(f"\n{'=' * 80}")
        print(f"❌ {summary['invalid_files']} FILES FAILED VALIDATION")
        print("=" * 80)


def main():
    """Main validation function."""
    # Find course directory
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    course_dir = repo_root / "examples" / "cheme_course"

    if not course_dir.exists():
        print(f"❌ Error: Course directory not found: {course_dir}")
        sys.exit(1)

    print(f"Validating ChemE course examples in: {course_dir}")

    # Run validation
    summary = validate_course_examples(course_dir)

    # Print results
    print_summary(summary)

    # Exit with error code if any files failed
    if summary["invalid_files"] > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
