import os
import pandas as pd
import numpy as np

# Create data directory
os.makedirs("examples/story_demo/data", exist_ok=True)

# Generate sample data
np.random.seed(42)
dates = pd.date_range(start="2025-01-01", periods=5)
data = []

for date in dates:
    for _ in range(10):  # 10 tx per day
        data.append({
            "transaction_id": f"TX_{np.random.randint(1000, 9999)}",
            "transaction_date": date.strftime("%Y-%m-%d"),
            "amount": round(np.random.uniform(10, 200), 2),
            "customer_id": f"C{np.random.randint(1, 20)}"
        })

df = pd.DataFrame(data)
df.to_csv("examples/story_demo/data/transactions.csv", index=False)
print("Generated examples/story_demo/data/transactions.csv")
