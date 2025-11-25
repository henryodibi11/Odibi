import argparse
import sys
from pathlib import Path

import pandas as pd
import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

OLIST_DATASET = "olistbr/brazilian-ecommerce"
OLIST_URL = f"https://www.kaggle.com/datasets/{OLIST_DATASET}"
FRANKFURTER_API = "https://api.frankfurter.app"

REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "product_category_name_translation.csv",
]


def setup_directories():
    """Create necessary directories."""
    print(f"Creating directories in {DATA_DIR}...")
    for d in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def check_olist_data():
    """Check if Olist data exists."""
    missing = []
    for f in REQUIRED_FILES:
        if not (RAW_DIR / f).exists():
            missing.append(f)

    if not missing:
        print("[OK] Olist dataset found.")
        return True

    # Fallback to manual instructions
    print("\n" + "=" * 60)
    print("[STOP] MISSING DATASETS")
    print("=" * 60)
    print("We need the Olist E-Commerce dataset to run this showcase.")
    print(f"Missing files: {len(missing)}/{len(REQUIRED_FILES)}")
    print("\nPlease download the dataset from Kaggle:")
    print(f"URL: {OLIST_URL}")
    print("\nAction:")
    print(1, "Download the archive.zip")
    print(2, f"Extract the CSV files into: {RAW_DIR.absolute()}")
    print("=" * 60 + "\n")
    return False


def fetch_exchange_rates():
    """Fetch historical exchange rates (BRL -> USD/EUR)."""
    print("Fetching exchange rates from Frankfurter API...")
    output_file = RAW_DIR / "exchange_rates_2016_2018.json"

    if output_file.exists():
        print("[OK] Exchange rates already cached.")
        return

    # Olist data range approx: 2016-09-01 to 2018-10-17
    start_date = "2016-09-01"
    end_date = "2018-10-31"

    url = f"{FRANKFURTER_API}/{start_date}..{end_date}?from=BRL&to=USD,EUR"

    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        data = response.json()

        # Save raw JSON
        import json

        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

        print(f"[OK] Saved {len(data['rates'])} days of exchange rates.")

    except Exception as e:
        print(f"[ERROR] Failed to fetch exchange rates: {e}")


def explode_data(multiplier=10):
    """Duplicate data to simulate 'Big Data'."""
    print(f"\n[INFO] BIG DATA MODE: Exploding dataset by {multiplier}x...")

    for f in REQUIRED_FILES:
        file_path = RAW_DIR / f
        # Only explode large transactional tables
        if "orders" in f or "items" in f or "payments" in f or "reviews" in f:
            print(f"  Processing {f}...")

            # Read in chunks to save memory during read
            # But for multiplication we need to append

            output_path = RAW_DIR / f"large_{f}"
            if output_path.exists():
                print(f"  Skipping {f} (already exists)")
                continue

            df = pd.read_csv(file_path)
            original_len = len(df)

            # Write header
            df.to_csv(output_path, index=False, mode="w")

            # Append duplicates
            for i in range(multiplier - 1):
                # We could modify IDs here to make them unique,
                # but for performance testing purely on volume, pure duplication is often enough
                # to test JOIN/GROUP BY speed (though cardinality remains same).
                # To truly test cardinality, we'd need to suffix IDs.

                # Let's suffix string IDs
                temp_df = df.copy()

                # Identify ID columns
                id_cols = [c for c in temp_df.columns if c.endswith("_id")]

                for col in id_cols:
                    temp_df[col] = temp_df[col] + f"_{i}"

                temp_df.to_csv(output_path, index=False, header=False, mode="a")
                print(f"    Append {i + 1}/{multiplier - 1}...", end="\r")

            print(f"\n  [OK] {f}: {original_len} -> {original_len * multiplier} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup data for OdibiStore showcase")
    parser.add_argument(
        "--explode", action="store_true", help="Duplicate data 10x for performance testing"
    )
    args = parser.parse_args()

    setup_directories()
    if check_olist_data():
        fetch_exchange_rates()

        if args.explode:
            explode_data(multiplier=10)
        else:
            print("\n[INFO] Skipped Big Data explosion. Run with --explode to generate 10x volume.")
    else:
        sys.exit(1)
