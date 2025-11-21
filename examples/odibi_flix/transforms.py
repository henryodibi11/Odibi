from odibi.registry import transform


@transform
def explode_genres(context, current):
    """Explode comma-separated genres into rows."""
    df = current

    if "genres" not in df.columns:
        return df

    # Standardize nulls (IMDB uses \N)
    df["genres"] = df["genres"].replace(r"\\N", None, regex=True)

    # Split and explode
    # We only keep relevant columns to avoid massive duplication of memory if not needed
    # But for "Silver" usually we keep all.

    df["genres"] = df["genres"].str.split(",")
    exploded = df.explode("genres")

    return exploded.dropna(subset=["genres"])
