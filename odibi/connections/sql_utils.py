"""SQL connection utilities and format detection."""

# All format strings that indicate a SQL database connection
SQL_FORMATS = frozenset(
    {
        "sql",
        "sql_server",
        "azure_sql",
        "postgres",
        "postgresql",
    }
)


def is_sql_format(fmt: str) -> bool:
    """Check if a format string represents a SQL database connection.

    Args:
        fmt: The format string from pipeline configuration

    Returns:
        True if the format is a SQL database type
    """
    return fmt in SQL_FORMATS
