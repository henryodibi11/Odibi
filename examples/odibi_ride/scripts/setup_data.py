import requests
import argparse
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"

# NYC TLC Data (Yellow Taxi)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def setup_directories():
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def download_file(year, month):
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    output_path = RAW_DIR / filename

    if output_path.exists():
        print(f"[SKIP] {filename} already exists")
        return

    print(f"Downloading {filename}...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[OK] Downloaded {filename}")
    except Exception as e:
        print(f"[ERROR] Failed to download {filename}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--months", type=int, default=1, help="Number of months to download (starting Jan 2023)"
    )
    args = parser.parse_args()

    setup_directories()

    print(f"Setting up data in {RAW_DIR}...")

    # Download Jan 2023 onwards
    for i in range(args.months):
        download_file(2023, i + 1)
