import deltalake

# Path to the Delta table

delta_table_path = "./data/bronze/ksb1_na1_q1"

# Load the Delta table
table = deltalake.DeltaTable(delta_table_path)

# Print the table schema
print("Schema:")
print(table.schema())

# Convert the table to a Pandas DataFrame
df = table.to_pandas()

# Show first 10 rows
print("\nFirst 10 rows:")
print(df.head(10))
