from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Custom "weather" format: DATE|TEMP|HUMIDITY
data = """2025-01-01|25.5|60
2025-01-02|26.0|55
2025-01-03|24.8|65
"""

with open(DATA_DIR / "station_1.weather", "w") as f:
    f.write(data)
print(f"Created {DATA_DIR / 'station_1.weather'}")
