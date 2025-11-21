import requests
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 2018 World Cup Final (France vs Croatia)
URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/8658.json"
OUTPUT = DATA_DIR / "match_8658.json"

if not OUTPUT.exists():
    print(f"Downloading {OUTPUT.name}...")
    try:
        r = requests.get(URL)
        r.raise_for_status()
        with open(OUTPUT, "wb") as f:
            f.write(r.content)
        print("Success.")
    except Exception as e:
        print(f"Error: {e}")
else:
    print("Data already exists.")
