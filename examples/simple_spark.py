import os
import sys

# Setup Hadoop for Windows
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tools_dir = os.path.join(project_root, "tools", "hadoop")
if os.path.exists(tools_dir):
    os.environ["HADOOP_HOME"] = tools_dir
    os.environ["PATH"] += os.pathsep + os.path.join(tools_dir, "bin")

# Point to current python
# Must be set BEFORE importing pyspark.sql
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession  # noqa: E402

print(f"Python: {sys.executable}")
print(f"Hadoop Home: {os.environ.get('HADOOP_HOME')}")

spark = SparkSession.builder.appName("SimpleTest").master("local[*]").getOrCreate()

print("Spark Session created")

data = [("A", 1), ("B", 2)]
df = spark.createDataFrame(data, ["letter", "number"])
df.show()
print("Done")
