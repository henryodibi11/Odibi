"""
SQL Operation
=============

Execute SQL queries on DataFrames using DuckDB.
"""

from odibi.transformations import transformation
from odibi.transformations.templates import purpose_detail_result


@transformation("sql", category="querying", tags=["query", "filter", "transform"])
def sql(df, query, table_name="df"):
    """
    Execute SQL query on DataFrame.

    Args:
        df: Input DataFrame
        query: SQL query string (use table_name as table reference)
        table_name: Name to use for the DataFrame in SQL (default: "df")

    Returns:
        Query result as DataFrame

    Example:
        Input DataFrame (df):
        | ID   | Value |
        |------|-------|
        | A    | 100   |
        | B    | 80    |

        Query: "SELECT * FROM df WHERE Value > 90"

        Output:
        | ID   | Value |
        |------|-------|
        | A    | 100   |
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("DuckDB is required for SQL operations. Install with: pip install duckdb")

    # Execute query using DuckDB
    result = duckdb.query(query).df()
    return result


@sql.explain
def explain(query, table_name="df", **context):
    """Generate context-aware explanation for SQL operation."""
    # Parse query type
    query_upper = query.strip().upper()
    if query_upper.startswith("SELECT"):
        operation_type = "query and filter"
    elif "GROUP BY" in query_upper:
        operation_type = "aggregate"
    elif "JOIN" in query_upper:
        operation_type = "combine"
    else:
        operation_type = "transform"

    # Extract key elements from query
    details = [
        f"Table reference: `{table_name}`",
        f"Operation type: {operation_type}",
        "Uses DuckDB SQL engine for high performance",
    ]

    # Add query preview (first 100 chars)
    query_preview = query.strip()
    if len(query_preview) > 100:
        query_preview = query_preview[:97] + "..."
    details.append(f"Query: `{query_preview}`")

    return purpose_detail_result(
        purpose=f"Execute SQL query to {operation_type} data",
        details=details,
        result="Filtered/transformed dataset matching SQL criteria",
    )
