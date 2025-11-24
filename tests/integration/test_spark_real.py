import pytest


@pytest.mark.extras
def test_spark_real_session():
    """Test that a real SparkSession can be created and run a simple job."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark not installed")

    try:
        # Use local[1] to run with a single thread, which is easier for CI
        spark = (
            SparkSession.builder.appName("OdibiIntegrationTest")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        data = [("Alice", 1), ("Bob", 2)]
        df = spark.createDataFrame(data, ["name", "id"])

        assert df.count() == 2
        rows = df.collect()
        assert rows[0]["name"] == "Alice"

        spark.stop()
    except RuntimeError as e:
        if "Only remote Spark sessions using Databricks Connect are supported" in str(e):
            pytest.skip("Databricks Connect installed but not configured")
        else:
            pytest.fail(f"Failed to start or use real Spark session: {e}")
    except Exception as e:
        # Skip on Windows if Hadoop home is missing (common dev environment issue)
        import sys

        err_msg = str(e)
        if sys.platform == "win32":
            # Windows failures are almost always due to winutils/hadoop missing
            # Check for common symptoms or just skip if it looks like environment issue
            if (
                "HADOOP_HOME" in err_msg
                or "Job aborted" in err_msg
                or "Python worker failed" in err_msg
            ):
                pytest.skip(
                    f"Skipping Spark test on Windows (likely HADOOP_HOME/winutils issue): {e}"
                )
            else:
                pytest.fail(f"Failed to start or use real Spark session: {e}")
        else:
            pytest.fail(f"Failed to start or use real Spark session: {e}")
