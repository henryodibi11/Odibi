import sys
from pathlib import Path
import pandas as pd

# --- Import Odibi ---
try:
    from odibi.engine.pandas_engine import PandasEngine
    from odibi.connections.local_dbfs import LocalDBFS
    from odibi.context import PandasContext
except ImportError:
    print("❌ Odibi not found. Run: pip install -e .")
    sys.exit(1)


def main():
    print("\n-> Starting Local Pandas Playground...")

    # Initialize Engine
    engine = PandasEngine()
    print("-> Pandas Engine Initialized")

    # Define a local connection
    project_root = Path(__file__).parent.parent.absolute()
    data_dir = project_root / "data" / "playground_pandas"
    data_dir.mkdir(parents=True, exist_ok=True)

    conn = LocalDBFS(root=str(data_dir))
    print(f"-> Storage Root: {conn.root}")

    # --- 1. Create Data ---
    print("\n1. Creating Data...")
    data = {
        "id": [1, 2, 3],
        "name": ["Widget A", "Widget B", "Service X"],
        "price": [19.99, 29.99, 100.00],
        "category": ["Hardware", "Hardware", "Service"],
    }
    df = pd.DataFrame(data)
    print(df)

    # --- 2. Write Data (Parquet) ---
    print("\n2. Writing Parquet...")
    engine.write(df, connection=conn, format="parquet", path="products.parquet", mode="overwrite")
    print("   -> Write complete.")

    # --- 3. Read Data Back ---
    print("\n3. Reading Parquet...")
    df_read = engine.read(connection=conn, format="parquet", path="products.parquet")
    print(f"   -> Read {len(df_read)} rows.")
    print(df_read)

    # --- 4. SQL Transformation (DuckDB) ---
    print("\n4. Running SQL Transformation (DuckDB)...")
    # Note: PandasEngine.execute_sql uses DuckDB/PandasSQL

    try:
        context = PandasContext()
        context.register("source_data", df_read)
        result = engine.execute_sql(
            """
            SELECT
                category,
                count(*) as count,
                avg(price) as avg_price
            FROM source_data
            GROUP BY category
            """,
            context=context,
        )
        print(result)
    except ImportError:
        print("⚠️  DuckDB/PandasSQL not installed. Skipping SQL test.")

    print("\n-> Pandas Playground run successful!")


if __name__ == "__main__":
    main()
