import pandas as pd

df = pd.read_parquet("projects/data/bronze/mm60_na.parquet")
print("Columns:", df.columns.tolist())
print(f"Shape: {df.shape}")
print("\nFirst 3 rows:")
print(df.head(3))
