"""Test all 38 simulation patterns from the docs/simulation/patterns/ directory.

Extracts YAML configs from markdown files and runs each one through PipelineManager.
"""

import logging
import os
import re
import shutil
import sys
import tempfile
import traceback

import yaml

# Suppress verbose odibi logging
logging.disable(logging.WARNING)


def extract_yaml_blocks(md_path):
    """Extract all YAML code blocks from a markdown file."""
    with open(md_path, "r", encoding="utf-8") as f:
        content = f.read()

    pattern_re = re.compile(
        r"## Pattern (\d+): (.+?) \{#pattern-\d+\}\s*\n"
        r".*?"
        r"```yaml\n(.*?)```",
        re.DOTALL,
    )

    results = []
    for match in pattern_re.finditer(content):
        num = int(match.group(1))
        title = match.group(2).strip()
        yaml_text = match.group(3)
        results.append((num, title, yaml_text))

    return results


def run_pattern(num, title, yaml_text, tmp_dir):
    """Run a single pattern's YAML config and return success/failure."""
    from odibi.pipeline import PipelineManager

    config = yaml.safe_load(yaml_text)

    # Override all paths to use tmp_dir
    if "connections" in config:
        for conn_name, conn_cfg in config["connections"].items():
            if conn_cfg.get("type") == "local":
                conn_cfg["base_path"] = tmp_dir

    if "story" in config:
        config["story"]["connection"] = list(config["connections"].keys())[0]

    if "system" in config:
        config["system"]["connection"] = list(config["connections"].keys())[0]

    # Fix validation quarantine connections
    if "pipelines" in config:
        for pipeline in config["pipelines"]:
            for node in pipeline.get("nodes", []):
                if "validation" in node:
                    val = node["validation"]
                    if "quarantine" in val:
                        val["quarantine"]["connection"] = list(
                            config["connections"].keys()
                        )[0]

    # Write config to temp file
    config_path = os.path.join(tmp_dir, f"pattern_{num}.yaml")
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False)

    # Create required subdirectories
    for subdir in ["stories", "bronze", "silver", "quarantine"]:
        os.makedirs(os.path.join(tmp_dir, subdir), exist_ok=True)

    # Run all pipelines in the config
    manager = PipelineManager.from_yaml(config_path)
    results = manager.run()

    # Check for node failures (PipelineManager.run returns PipelineResults)
    if hasattr(results, 'failed') and results.failed:
        raise RuntimeError(
            f"Pipeline completed but {len(results.failed)} node(s) failed: "
            f"{', '.join(results.failed)}"
        )

    return True


def safe_cleanup(path):
    """Clean up temp dir, ignoring Windows file lock errors."""
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def main():
    patterns_dir = os.path.join(
        os.path.dirname(__file__), "..", "docs", "simulation", "patterns"
    )
    patterns_dir = os.path.abspath(patterns_dir)

    md_files = [
        "foundations.md",
        "process_engineering.md",
        "energy_utilities.md",
        "manufacturing.md",
        "environmental.md",
        "healthcare.md",
        "business_it.md",
        "data_engineering.md",
    ]

    all_patterns = []
    for md_file in md_files:
        md_path = os.path.join(patterns_dir, md_file)
        if not os.path.exists(md_path):
            print(f"  WARNING: {md_file} not found, skipping")
            continue
        patterns = extract_yaml_blocks(md_path)
        print(f"  {md_file}: found {len(patterns)} patterns")
        all_patterns.extend(
            (md_file, num, title, yaml_text)
            for num, title, yaml_text in patterns
        )

    print(f"\nTotal patterns found: {len(all_patterns)}")
    print("=" * 70)

    passed = []
    failed = []

    for md_file, num, title, yaml_text in all_patterns:
        tmp_dir = tempfile.mkdtemp()
        try:
            print(f"  [{num:2d}] {title:<45s} ", end="", flush=True)
            run_pattern(num, title, yaml_text, tmp_dir)
            print("PASS")
            passed.append(num)
        except Exception as e:
            print("FAIL")
            tb_lines = traceback.format_exception(type(e), e, e.__traceback__)
            err_msg = str(e)[:200]
            print(f"       Error: {err_msg}")
            failed.append((num, title, err_msg))
        finally:
            safe_cleanup(tmp_dir)

    print("=" * 70)
    print(f"Results: {len(passed)} passed, {len(failed)} failed out of {len(all_patterns)}")

    if passed:
        print(f"  Passed: {', '.join(str(p) for p in sorted(passed))}")
    if failed:
        print(f"\n  FAILURES:")
        for num, title, err in failed:
            print(f"    Pattern {num}: {title}")
            print(f"      {err}")

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
