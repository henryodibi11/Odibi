"""Sample transform functions for getting started example."""

from odibi import transform
import pandas as pd


@transform
def calculate_revenue(context, source: str):
    """Calculate total revenue from sales data.
    
    Args:
        source: Name of sales DataFrame in context
        
    Returns:
        DataFrame with revenue column added
    """
    df = context.get(source)
    df = df.copy()
    df['revenue'] = df['quantity'] * df['price']
    return df


@transform
def filter_by_category(context, source: str, category: str):
    """Filter sales by product category.
    
    Args:
        source: Name of DataFrame in context
        category: Category to filter (e.g., 'Electronics')
        
    Returns:
        Filtered DataFrame
    """
    df = context.get(source)
    return df[df['category'] == category]


@transform
def enrich_with_customer_data(context, sales_table: str, customers_table: str):
    """Join sales with customer information.
    
    Args:
        sales_table: Name of sales DataFrame
        customers_table: Name of customers DataFrame
        
    Returns:
        Enriched DataFrame with customer info
    """
    sales = context.get(sales_table)
    customers = context.get(customers_table)
    
    # Merge on customer_id
    enriched = sales.merge(customers, on='customer_id', how='left')
    
    return enriched


@transform
def aggregate_by_product(context, source: str):
    """Aggregate sales by product.
    
    Args:
        source: Name of DataFrame to aggregate
        
    Returns:
        DataFrame grouped by product with totals
    """
    df = context.get(source)
    
    aggregated = df.groupby('product').agg({
        'quantity': 'sum',
        'revenue': 'sum'
    }).reset_index()
    
    aggregated.columns = ['product', 'total_quantity', 'total_revenue']
    
    return aggregated
