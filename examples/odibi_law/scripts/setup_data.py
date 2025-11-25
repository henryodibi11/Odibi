from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

BILLS = [
    {
        "bill_id": "hr-2025-001",
        "title": "Future Data Act",
        "text": "SECTION 1. SHORT TITLE. This Act may be cited as the 'Future Data Act'. SEC. 2. DATA QUALITY. All data pipelines must be tested thoroughly using real-world scenarios. SEC 3. OBSERVABILITY. All pipelines must emit telemetry.",
    },
    {
        "bill_id": "s-2025-042",
        "title": "AI Transparency Act",
        "text": "SECTION 1. IDENTIFICATION. Artificial Intelligence agents must disclose their nature to human users. SEC 2. LIABILITY. Developers are responsible for agent actions.",
    },
]

pd.DataFrame(BILLS).to_csv(DATA_DIR / "bills.csv", index=False)
print(f"Created {DATA_DIR / 'bills.csv'}")
