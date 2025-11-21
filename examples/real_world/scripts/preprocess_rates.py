import json
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "data/raw"
INPUT_FILE = RAW_DIR / "exchange_rates_2016_2018.json"
OUTPUT_FILE = RAW_DIR / "exchange_rates_flat.csv"

def flatten_rates():
    if not INPUT_FILE.exists():
        print(f"[ERROR] Input file not found: {INPUT_FILE}")
        return

    print(f"Reading {INPUT_FILE}...")
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    rates = data.get("rates", {})
    print(f"Found {len(rates)} daily records.")

    rows = []
    for date_str, values in rates.items():
        row = {
            "date": date_str,
            "usd": values.get("USD"),
            "eur": values.get("EUR")
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    print(f"Writing {len(df)} rows to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    print("[OK] Done.")

if __name__ == "__main__":
    flatten_rates()
