from odibi.registry import transform


@transform
def flatten_events(context, current):
    """Flatten StatsBomb event objects."""
    df = current.copy()

    def extract_name(val):
        if isinstance(val, dict):
            return val.get("name")
        return val

    # Key columns to flatten
    cols = ["type", "team", "player", "position", "play_pattern", "possession_team"]

    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(extract_name)

    return df


@transform
def extract_shots(context, current):
    """Extract shot details."""
    df = current

    # Filter shots
    shots = df[df["type"] == "Shot"].copy()

    # Extract shot stats from 'shot' column (dict)
    if "shot" in shots.columns:
        # outcome
        shots["outcome"] = shots["shot"].apply(
            lambda x: x.get("outcome", {}).get("name") if isinstance(x, dict) else None
        )
        # xg
        shots["xg"] = shots["shot"].apply(
            lambda x: x.get("statsbomb_xg") if isinstance(x, dict) else 0.0
        )

    return shots
