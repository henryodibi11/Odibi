import time
from pathlib import Path

import requests

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = ["aapl.us", "msft.us", "spy.us"]


def download_ticker(ticker):
    url = f"https://stooq.com/q/d/l/?s={ticker}&i=d"
    output_file = DATA_DIR / f"{ticker}.csv"

    if output_file.exists():
        print(f"[SKIP] {ticker} already exists")
        return

    print(f"Downloading {ticker} from Stooq...")
    try:
        r = requests.get(url)
        r.raise_for_status()
        content = r.text

        if "No data" in content or len(content) < 100:
            print(f"[WARN] No data for {ticker}")
            return

        with open(output_file, "w") as f:
            f.write(content)
        print(f"[OK] Downloaded {ticker}")
        time.sleep(1)

    except Exception as e:
        print(f"[ERROR] Failed {ticker}: {e}")


if __name__ == "__main__":
    print("Fetching REAL financial data from Stooq...")
    for t in TICKERS:
        download_ticker(t)
