import gzip
from pathlib import Path

import requests

BASE_URL = "https://datasets.imdbws.com"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FILES = ["title.basics.tsv.gz", "title.principals.tsv.gz", "name.basics.tsv.gz"]


def download_sample(filename, rows=10000):
    url = f"{BASE_URL}/{filename}"
    output_file = DATA_DIR / filename.replace(".gz", "")  # save as .tsv

    if output_file.exists():
        print(f"[SKIP] {output_file} exists")
        return

    print(f"Downloading sample from {filename}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            # Stream GZIP decompression
            with (
                gzip.open(r.raw, "rt", encoding="utf-8") as f_in,
                open(output_file, "w", encoding="utf-8") as f_out,
            ):
                for i, line in enumerate(f_in):
                    f_out.write(line)
                    if i >= rows:
                        break
        print(f"[OK] Saved {rows} rows to {output_file}")
    except Exception as e:
        print(f"[ERROR] Failed {filename}: {e}")


if __name__ == "__main__":
    print("Setting up OdibiFlix data...")
    for f in FILES:
        download_sample(f, rows=5000)  # 5k rows enough for testing
