import pandas as pd

from odibi.context import Context


def enrich_customers(context: Context, current: pd.DataFrame, **kwargs):
    """
    Enrich customer data with timestamp and filter by ID.
    This is a test transformation for diagnostics demo.
    """
    # Simulate a SQL transformation logic tracking
    # Note: In context.sql(), the current dataframe is aliased as 'df'
    context.sql("SELECT * FROM df WHERE id > 0")

    # Add a column
    current["enriched_at"] = pd.Timestamp.now()
    return current
