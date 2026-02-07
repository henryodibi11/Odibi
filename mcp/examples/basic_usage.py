"""
Odibi MCP - Basic Usage Examples

This file demonstrates how to use the MCP tools programmatically.
"""

from odibi_mcp.contracts.access import AccessContext, ConnectionPolicy
from odibi_mcp.contracts.selectors import DEFAULT_RUN_SELECTOR
from odibi_mcp.tools.story import story_read, story_diff
from odibi_mcp.tools.catalog import node_stats, failure_summary
from odibi_mcp.tools.lineage import lineage_upstream


# Example 1: Read pipeline story
def example_read_story():
    """Read metadata for a pipeline run."""
    result = story_read(
        pipeline="sales_pipeline",
        run_selector=DEFAULT_RUN_SELECTOR,
    )
    print(f"Pipeline: {result.pipeline}")
    print(f"Status: {result.status}")
    print(f"Duration: {result.duration_seconds}s")
    print(f"Nodes: {len(result.nodes)}")


# Example 2: Compare two runs
def example_compare_runs():
    """Compare two pipeline runs."""
    diff = story_diff(
        pipeline="sales_pipeline",
        run_a="run_001",
        run_b="run_002",
    )
    print(f"Row count change: {diff.row_count_diff}")
    print(
        f"Schema changes: {len(diff.schema_diff.added)} added, {len(diff.schema_diff.removed)} removed"
    )


# Example 3: Get node statistics
def example_node_stats():
    """Get statistics for a specific node."""
    stats = node_stats(
        pipeline="sales_pipeline",
        node="transform_sales",
    )
    print(f"Run count: {stats.run_count}")
    print(f"Success rate: {(1 - stats.failure_rate) * 100:.1f}%")
    print(f"Avg duration: {stats.avg_duration_seconds}s")


# Example 4: Investigate failures
def example_failure_summary():
    """Get summary of recent failures."""
    failures = failure_summary(pipeline="sales_pipeline")
    print(f"Total failures: {failures.total_failures}")
    for error_type, count in failures.by_error_type.items():
        print(f"  {error_type}: {count}")


# Example 5: Trace lineage
def example_lineage():
    """Trace data lineage for a node."""
    upstream = lineage_upstream(
        pipeline="sales_pipeline",
        node="final_output",
        depth=3,
    )
    print(f"Source: {upstream.source.logical_name}")
    print(f"Upstream dependencies: {len(upstream.related)}")


# Example 6: Access control setup
def example_access_control():
    """Configure access control for MCP."""
    ctx = AccessContext(
        authorized_projects={"sales_project", "marketing_project"},
        environment="production",
        connection_policies={
            "adls_main": ConnectionPolicy(
                connection="adls_main",
                allowed_path_prefixes=["/data/bronze/sales/", "/data/silver/"],
                denied_path_prefixes=["/data/bronze/pii/"],
                allow_physical_refs=False,
            )
        },
        physical_refs_enabled=False,
    )

    # Check if a project is authorized
    try:
        ctx.check_project("sales_project")
        print("Sales project: authorized")
    except PermissionError:
        print("Sales project: denied")

    # Check if a path is allowed
    policy = ctx.connection_policies["adls_main"]
    print(f"/data/bronze/sales/file.csv: {policy.is_path_allowed('/data/bronze/sales/file.csv')}")
    print(f"/data/bronze/pii/file.csv: {policy.is_path_allowed('/data/bronze/pii/file.csv')}")


if __name__ == "__main__":
    print("=== Odibi MCP Examples ===\n")

    print("1. Read Story")
    example_read_story()
    print()

    print("2. Compare Runs")
    example_compare_runs()
    print()

    print("3. Node Stats")
    example_node_stats()
    print()

    print("4. Failure Summary")
    example_failure_summary()
    print()

    print("5. Lineage")
    example_lineage()
    print()

    print("6. Access Control")
    example_access_control()
