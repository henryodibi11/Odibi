import pandas as pd

from odibi.registry import transform


@transform
def flatten_exchange_rates(context, current):
    """Flatten Frankfurter API nested response.

    Expects DataFrame where index is date and 'rates' column contains dicts.
    """
    df = current

    # Basic validation
    if df.empty:
        return pd.DataFrame(columns=["date", "usd", "eur"])

    # The API response read by pd.read_json likely put dates in index
    # and 'rates' column has {'USD': x, 'EUR': y} dicts.

    # Reset index to make date a column
    # Note: If index name is None, reset_index names it 'index'
    df_reset = df.reset_index().rename(columns={"index": "date"})

    # Extract USD and EUR from 'rates' column
    # We use json_normalize-like logic but simple apply(pd.Series) works for simple dicts
    if "rates" in df_reset.columns:
        rates_df = df_reset["rates"].apply(pd.Series)
        # Combine date with rates
        final_df = pd.concat([df_reset[["date"]], rates_df], axis=1)
    else:
        # Fallback if structure is different
        return df

    # Normalize columns
    final_df.columns = [str(c).lower() for c in final_df.columns]

    # Ensure columns exist (fill 0 or null if missing)
    for col in ["usd", "eur"]:
        if col not in final_df.columns:
            final_df[col] = None

    return final_df
