"""Comprehensive test for ALL 41 MCP tools against real project data."""

import sys
import os
from pathlib import Path

# Ensure correct path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "_archive"))

# On Windows, use TF-IDF fallback for embeddings to avoid PyTorch DLL issues
if sys.platform == "win32":
    os.environ["ODIBI_USE_TFIDF_EMBEDDINGS"] = "1"

# Load .env file from IPS_S_Curve project
from dotenv import load_dotenv

load_dotenv(r"C:\Users\henry\Downloads\IPS_S_Curve\.env")

# Set the project config
os.environ["ODIBI_CONFIG"] = r"C:\Users\henry\Downloads\IPS_S_Curve\ips_s_curve.yaml"

import asyncio
import json
from odibi_mcp.server import list_tools, call_tool

# Test configuration for IPS_S_Curve project
PROJECT_PIPELINE = "bronze"  # Main pipeline name
PROJECT_NODE = "scurve_metadata"  # A known node from bronze.yaml
PROJECT_CONNECTION = "goat_datalake"  # ADLS connection


class TestResult:
    def __init__(self, tool_name: str, status: str, message: str, data: any = None):
        self.tool_name = tool_name
        self.status = status  # PASS, FAIL, SKIP, WARN
        self.message = message
        self.data = data

    def __repr__(self):
        icons = {"PASS": "✓", "FAIL": "✗", "SKIP": "⊘", "WARN": "⚠"}
        return f"{icons.get(self.status, '?')} [{self.status}] {self.tool_name}: {self.message}"


async def test_tool(tool_name: str, args: dict, validator=None) -> TestResult:
    """Test a single tool and return the result."""
    try:
        result = await call_tool(tool_name, args)
        if not result:
            return TestResult(tool_name, "FAIL", "No result returned")
        text = result[0].text

        # Try to parse as JSON
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            data = text

        # Check for explicit errors
        if isinstance(data, dict) and "error" in data:
            error_msg = data.get("error") or "Unknown error"
            data.get("code", "")
            # Skip certain expected errors
            if error_msg and "No project context" in error_msg:
                return TestResult(tool_name, "SKIP", f"No project context: {error_msg}")
            if error_msg and (
                "catalog writes disabled" in error_msg.lower()
                or "skip_catalog_writes" in error_msg.lower()
            ):
                return TestResult(tool_name, "WARN", f"Catalog disabled: {error_msg}", data)
            # "No failed rows" is expected behavior when validation passes
            if error_msg and "no failed rows" in error_msg.lower():
                return TestResult(
                    tool_name, "PASS", f"No failed rows (expected): {error_msg}", data
                )
            if error_msg and error_msg != "Unknown error":
                return TestResult(tool_name, "FAIL", f"Error: {error_msg}", data)

        # Custom validation if provided
        if validator:
            is_valid, msg = validator(data)
            if not is_valid:
                return TestResult(tool_name, "FAIL", msg, data)

        # Summarize result
        if isinstance(data, list):
            return TestResult(tool_name, "PASS", f"{len(data)} items", data)
        elif isinstance(data, dict):
            keys = list(data.keys())[:5]
            return TestResult(tool_name, "PASS", f"Keys: {keys}", data)
        elif isinstance(data, str):
            return TestResult(tool_name, "PASS", f"{len(data)} chars", data)
        else:
            return TestResult(tool_name, "PASS", "OK", data)

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        return TestResult(
            tool_name, "FAIL", f"Exception: {type(e).__name__}: {str(e)[:100]}", {"traceback": tb}
        )


async def run_all_tests():
    """Run comprehensive tests for all 41 tools."""
    print("=" * 70)
    print("ODIBI MCP SERVER - COMPREHENSIVE TOOL TESTING")
    print("Project: IPS_S_Curve")
    print(f"Config: {os.environ.get('ODIBI_CONFIG')}")
    print("=" * 70)
    print()

    # Initialize context
    from odibi_mcp.context import initialize_from_env

    ctx = initialize_from_env()
    if ctx:
        print(f"✓ Project loaded: {ctx.project_name}")
        print(f"✓ Connections: {list(ctx.connections.keys())}")
        pipelines = ctx.get_pipelines()
        print(f"✓ Pipelines found: {len(pipelines)}")
        for p in pipelines:
            pname = p.get("pipeline", p.get("name", "unknown"))
            source = p.get("_source", "inline")
            print(f"  - {pname} (from: {Path(source).name if source != 'inline' else 'inline'})")
    else:
        print("✗ No project context loaded!")
        return

    print()

    # List all tools first
    tools = await list_tools()
    print(f"Total tools registered: {len(tools)}")
    print()

    results = []

    # ============ KNOWLEDGE TOOLS (21 tools) ============
    print("─" * 70)
    print("KNOWLEDGE TOOLS")
    print("─" * 70)

    # Helper to unwrap _context wrapper
    def unwrap(d):
        if isinstance(d, dict) and "_context" in d and "result" in d:
            return d["result"]
        return d

    knowledge_tests = [
        (
            "list_transformers",
            {},
            lambda d: (
                isinstance(unwrap(d), list) and len(unwrap(d)) > 0,
                "Expected non-empty list",
            ),
        ),
        (
            "list_patterns",
            {},
            lambda d: (
                isinstance(unwrap(d), list) and len(unwrap(d)) >= 6,
                "Expected at least 6 patterns",
            ),
        ),
        (
            "list_connections",
            {},
            lambda d: (
                isinstance(unwrap(d), list) and len(unwrap(d)) > 0,
                "Expected non-empty list",
            ),
        ),
        (
            "explain",
            {"name": "dimension"},
            lambda d: (
                isinstance(unwrap(d), dict) and "name" in unwrap(d),
                "Expected dict with 'name'",
            ),
        ),
        (
            "explain",
            {"name": "add_column"},
            lambda d: (isinstance(unwrap(d), dict), "Expected dict"),
        ),
        ("get_transformer_signature", {}, None),
        ("get_yaml_structure", {}, None),
        (
            "get_deep_context",
            {},
            lambda d: (isinstance(d, str) and len(d) > 1000, "Expected long doc string"),
        ),
        ("get_index_stats", {}, None),
        (
            "list_docs",
            {},
            lambda d: (isinstance(d, list) and len(d) > 0, "Expected non-empty list"),
        ),
        ("list_docs", {"category": "patterns"}, None),
        ("search_docs", {"query": "SCD2"}, None),
        ("get_doc", {"doc_path": "docs/patterns/scd2.md"}, None),
        ("validate_yaml", {"yaml_content": "project: test\nengine: pandas"}, None),
        ("diagnose_error", {"error_message": "KeyError: 'column_name'"}, None),
        ("get_example", {"pattern_name": "dimension"}, None),
        ("suggest_pattern", {"use_case": "track customer changes over time"}, None),
        ("get_engine_differences", {}, None),
        ("get_validation_rules", {}, None),
        (
            "generate_transformer",
            {"name": "test_transform", "params": [], "description": "Test"},
            None,
        ),
        (
            "generate_pipeline_yaml",
            {
                "project_name": "test_project",
                "input_path": "data/input",
                "input_format": "csv",
                "output_path": "data/output",
                "output_format": "delta",
            },
            None,
        ),
    ]

    for tool_name, args, validator in knowledge_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # Test query_codebase separately (uses keyword fallback on Windows)
    r = await test_tool("query_codebase", {"question": "how does SCD2 work?", "k": 3}, None)
    results.append(r)
    print(r)

    # ============ STORY TOOLS (3 tools) ============
    print()
    print("─" * 70)
    print("STORY TOOLS")
    print("─" * 70)

    story_tests = [
        ("story_read", {"pipeline": "bronze"}, None),
        ("story_read", {"pipeline": "bronze", "run_id": "latest"}, None),
        ("node_describe", {"pipeline": "bronze", "node": "scurve_metadata"}, None),
    ]

    for tool_name, args, validator in story_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # Test story_diff requires two runs - check if we have them
    story_result = await call_tool("story_read", {"pipeline": "bronze"})
    try:
        story_data = json.loads(story_result[0].text)
        if isinstance(story_data, dict) and "runs" in story_data and len(story_data["runs"]) >= 2:
            run_ids = [r.get("run_id") for r in story_data["runs"][:2]]
            r = await test_tool(
                "story_diff", {"pipeline": "bronze", "run_a": run_ids[0], "run_b": run_ids[1]}, None
            )
        else:
            r = TestResult("story_diff", "SKIP", "Fewer than 2 runs available for diff")
    except Exception as e:
        r = TestResult("story_diff", "SKIP", f"Could not get runs: {e}")
    results.append(r)
    print(r)

    # ============ SAMPLE TOOLS (3 tools) ============
    print()
    print("─" * 70)
    print("SAMPLE TOOLS")
    print("─" * 70)

    sample_tests = [
        ("node_sample", {"pipeline": "bronze", "node": "scurve_metadata", "max_rows": 5}, None),
        ("node_sample_in", {"pipeline": "bronze", "node": "scurve_metadata", "max_rows": 5}, None),
        (
            "node_failed_rows",
            {"pipeline": "bronze", "node": "scurve_metadata", "max_rows": 5},
            None,
        ),
    ]

    for tool_name, args, validator in sample_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # ============ CATALOG TOOLS (4 tools) ============
    print()
    print("─" * 70)
    print("CATALOG TOOLS (expect WARN - catalog writes disabled locally)")
    print("─" * 70)

    catalog_tests = [
        ("node_stats", {"pipeline": "bronze", "node": "scurve_metadata"}, None),
        ("pipeline_stats", {"pipeline": "bronze"}, None),
        ("failure_summary", {"pipeline": "bronze", "max_failures": 10}, None),
        ("schema_history", {"pipeline": "bronze", "node": "scurve_metadata"}, None),
    ]

    for tool_name, args, validator in catalog_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # ============ LINEAGE TOOLS (3 tools) ============
    print()
    print("─" * 70)
    print("LINEAGE TOOLS")
    print("─" * 70)

    lineage_tests = [
        ("lineage_upstream", {"pipeline": "bronze", "node": "scurve_metadata", "depth": 2}, None),
        ("lineage_downstream", {"pipeline": "bronze", "node": "scurve_metadata", "depth": 2}, None),
        ("lineage_graph", {"pipeline": "bronze", "include_external": True}, None),
    ]

    for tool_name, args, validator in lineage_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # ============ SCHEMA TOOLS (2 tools) ============
    print()
    print("─" * 70)
    print("SCHEMA TOOLS")
    print("─" * 70)

    schema_tests = [
        ("list_outputs", {"pipeline": "bronze"}, None),
        ("output_schema", {"pipeline": "bronze", "output_name": "scurve_metadata"}, None),
    ]

    for tool_name, args, validator in schema_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # ============ DISCOVERY TOOLS (5 tools) ============
    print()
    print("─" * 70)
    print("DISCOVERY TOOLS")
    print("─" * 70)

    discovery_tests = [
        (
            "list_files",
            {"connection": "goat_datalake", "path": "IPS_S_Curve", "pattern": "*"},
            None,
        ),
        (
            "preview_source",
            {"connection": "goat_ips", "path": "S_Curve_Data.csv", "max_rows": 5},
            None,
        ),
        ("infer_schema", {"connection": "goat_ips", "path": "S_Curve_Data.csv"}, None),
        # SQL Server tests - may fail if network unavailable
        ("list_tables", {"connection": "goat_sql", "schema": "dbo", "pattern": "*"}, None),
        (
            "describe_table",
            {"connection": "goat_sql", "table": "IPS_S_Curve", "schema": "dbo"},
            None,
        ),
    ]

    for tool_name, args, validator in discovery_tests:
        r = await test_tool(tool_name, args, validator)
        results.append(r)
        print(r)

    # ============ SUMMARY ============
    print()
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]
    skipped = [r for r in results if r.status == "SKIP"]
    warned = [r for r in results if r.status == "WARN"]

    print(f"✓ PASSED:  {len(passed)}")
    print(f"⚠ WARNED:  {len(warned)} (expected - catalog disabled)")
    print(f"⊘ SKIPPED: {len(skipped)}")
    print(f"✗ FAILED:  {len(failed)}")
    print("─" * 30)
    print(f"TOTAL:     {len(results)}")
    print()

    if failed:
        print("FAILED TESTS:")
        for r in failed:
            print(f"  ✗ {r.tool_name}: {r.message}")
            if r.data:
                if isinstance(r.data, dict) and "traceback" in r.data:
                    print(f"    Traceback:\n{r.data['traceback']}")
                else:
                    print(f"    Data: {json.dumps(r.data, indent=2, default=str)[:300]}")

    print()
    success_rate = (len(passed) + len(warned)) / len(results) * 100
    print(f"Success Rate: {success_rate:.1f}%")

    return results


if __name__ == "__main__":
    asyncio.run(run_all_tests())
