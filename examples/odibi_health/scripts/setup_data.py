import pandas as pd
import random
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def generate_patients(n=100):
    data = []
    for i in range(n):
        data.append(
            {
                "patient_id": f"P-{i:04d}",
                "name": f"Patient {i}",
                "ssn": f"{random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(1000, 9999)}",
                "phone": f"555-01{i:02d}",
                "email": f"patient{i}@example.com",
                "diagnosis_code": "A01",
                "bill_amount": random.randint(100, 50000),
            }
        )

    df = pd.DataFrame(data)
    df.to_csv(DATA_DIR / "patients.csv", index=False)
    print(f"Generated {n} records at {DATA_DIR / 'patients.csv'}")


if __name__ == "__main__":
    generate_patients()
