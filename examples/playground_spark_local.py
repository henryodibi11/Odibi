import os
import sys
from pathlib import Path

# --- 1. Setup Environment for Local Spark ---
project_root = Path(__file__).parent.parent.absolute()
tools_dir = project_root / "tools" / "hadoop"

if tools_dir.exists():
    print(f"-> Found local Hadoop tools at: {tools_dir}")
    os.environ["HADOOP_HOME"] = str(tools_dir)
    # Add bin to PATH so hadoop.dll is found
    os.environ["PATH"] += os.pathsep + str(tools_dir / "bin")

    # Fix for "Python worker failed to connect back"
    if "PYSPARK_PYTHON" not in os.environ:
        print(f"-> Using Python executable: {sys.executable}")
        os.environ["PYSPARK_PYTHON"] = sys.executable

    if "PYSPARK_DRIVER_PYTHON" not in os.environ:
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
else:
    print("-> Local Hadoop tools not found. Spark might fail on Windows if HADOOP_HOME is not set.")

# --- 2. Import Odibi ---
try:
    from odibi.engine.spark_engine import SparkEngine
    from odibi.connections.local_dbfs import LocalDBFS
except ImportError:
    print("X Spark libraries not found. Run: pip install '.[spark]'")
    sys.exit(1)


def main():
    print("\n-> Starting Local Spark Playground...")

    # Initialize Engine
    # On first run, this might take a moment to start the JVM
    engine = SparkEngine()
    print(f"-> Spark Session Created: {engine.spark.version}")

    # Define a local connection
    # We'll use a temporary directory for data
    data_dir = project_root / "data" / "playground"
    data_dir.mkdir(parents=True, exist_ok=True)

    conn = LocalDBFS(root=str(data_dir))
    print(f"-> Storage Root: {conn.root}")

    # --- 3. Create Data ---
    print("\n1. Creating Data...")
    data = [
        {"id": 1, "name": "Widget A", "price": 19.99, "category": "Hardware"},
        {"id": 2, "name": "Widget B", "price": 29.99, "category": "Hardware"},
        {"id": 3, "name": "Service X", "price": 100.00, "category": "Service"},
    ]

    # Create Spark DataFrame from list of dicts
    df = engine.spark.createDataFrame(data)
    df.show()

    # --- 4. Write Data (Parquet) ---
    print("2. Writing Parquet...")
    engine.write(df, connection=conn, format="parquet", path="products.parquet", mode="overwrite")
    print("   -> Write complete.")

    # --- 5. Read Data Back ---
    print("3. Reading Parquet...")
    df_read = engine.read(connection=conn, format="parquet", path="products.parquet")
    print(f"   -> Read {df_read.count()} rows.")
    df_read.show()

    # --- 6. SQL Transformation ---
    print("4. Running SQL Transformation...")
    # Register temp view implicitly by passing dict to execute_sql?
    # No, execute_sql takes a context dict of {name: df}

    result = engine.execute_sql(
        """
        SELECT
            category,
            count(*) as count,
            avg(price) as avg_price
        FROM source_data
        GROUP BY category
        """,
        context={"source_data": df_read},
    )
    result.show()

    print("\n-> Playground run successful! Local Spark is working.")


if __name__ == "__main__":
    main()
