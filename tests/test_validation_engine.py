import pandas as pd
import pytest

from odibi.config import (
    AcceptedValuesTest,
    CustomSQLTest,
    NotNullTest,
    RangeTest,
    RegexMatchTest,
    RowCountTest,
    TestType,
    UniqueTest,
    ValidationAction,
    ValidationConfig,
)
from odibi.validation.engine import Validator


def test_validation_config_loading():
    config = ValidationConfig(
        mode="fail",
        tests=[
            {"type": "not_null", "columns": ["id"]},
            {"type": "range", "column": "age", "min": 0, "max": 120},
        ],
    )
    assert config.mode == ValidationAction.FAIL
    assert len(config.tests) == 2
    assert isinstance(config.tests[0], NotNullTest)
    assert isinstance(config.tests[1], RangeTest)


def test_pandas_validation():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", None],
            "age": [25, 30, 150, 20],
            "email": ["alice@example.com", "bob", "charlie@example.com", "dave@example.com"],
            "status": ["active", "inactive", "pending", "deleted"],
        }
    )

    validator = Validator()

    # 1. Not Null
    config = ValidationConfig(tests=[NotNullTest(type=TestType.NOT_NULL, columns=["name"])])
    failures = validator.validate(df, config)
    assert len(failures) == 1
    assert "Column 'name' contains 1 NULLs" in failures[0]

    # 2. Unique
    # id is unique
    config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["id"])])
    assert len(validator.validate(df, config)) == 0

    # status is unique? No, let's add a duplicate
    df_dup = pd.concat(
        [
            df,
            pd.DataFrame(
                {"id": [5], "name": ["Dup"], "age": [20], "email": ["x"], "status": ["active"]}
            ),
        ]
    )
    config = ValidationConfig(tests=[UniqueTest(type=TestType.UNIQUE, columns=["status"])])
    failures = validator.validate(df_dup, config)
    assert len(failures) == 1
    assert "Column 'status' is not unique" in failures[0]

    # 3. Accepted Values
    config = ValidationConfig(
        tests=[
            AcceptedValuesTest(
                type=TestType.ACCEPTED_VALUES,
                column="status",
                values=["active", "inactive", "pending"],
            )
        ]
    )
    failures = validator.validate(df, config)
    # 'deleted' is invalid
    assert len(failures) == 1
    assert "invalid values" in failures[0]

    # 4. Row Count
    config = ValidationConfig(tests=[RowCountTest(type=TestType.ROW_COUNT, min=10)])
    failures = validator.validate(df, config)
    assert len(failures) == 1
    assert "Row count 4 < min 10" in failures[0]

    # 5. Range
    config = ValidationConfig(tests=[RangeTest(type=TestType.RANGE, column="age", min=0, max=100)])
    failures = validator.validate(df, config)
    # 150 is > 100
    assert len(failures) == 1
    assert "out of range" in failures[0]

    # 6. Regex
    config = ValidationConfig(
        tests=[
            RegexMatchTest(
                type=TestType.REGEX_MATCH, column="email", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"
            )
        ]
    )
    failures = validator.validate(df, config)
    # "bob" fails
    assert len(failures) == 1
    assert "does not match pattern" in failures[0]

    # 7. Custom SQL
    config = ValidationConfig(
        tests=[CustomSQLTest(type=TestType.CUSTOM_SQL, condition="age < 100")]
    )
    failures = validator.validate(df, config)
    assert len(failures) == 1
    assert "Custom check" in failures[0]


try:
    import os
    import sys

    from pyspark.sql import SparkSession

    @pytest.fixture(scope="module")
    def spark():
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        return SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    def test_spark_validation(spark):
        data = [
            (1, "Alice", 25, "alice@example.com", "active"),
            (2, "Bob", 30, "bob", "inactive"),
            (3, "Charlie", 150, "charlie@example.com", "pending"),
            (4, None, 20, "dave@example.com", "deleted"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "email", "status"])

        validator = Validator()

        # Config with multiple tests
        config = ValidationConfig(
            tests=[
                NotNullTest(type=TestType.NOT_NULL, columns=["name"]),
                RangeTest(type=TestType.RANGE, column="age", min=0, max=100),
                RegexMatchTest(
                    type=TestType.REGEX_MATCH, column="email", pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$"
                ),
                AcceptedValuesTest(
                    type=TestType.ACCEPTED_VALUES,
                    column="status",
                    values=["active", "inactive", "pending"],
                ),
            ]
        )

        try:
            failures = validator.validate(df, config)
            # 1. name=None (id=4)
            # 2. age=150 (id=3)
            # 3. email='bob' (id=2)
            # 4. status='deleted' (id=4)

            assert len(failures) == 4

            # Check message content roughly
            joined = " ".join(failures)
            assert "NULLs" in joined
            assert "out of range" in joined
            assert "does not match pattern" in joined
            assert "invalid values" in joined
        except Exception as e:
            import sys

            if sys.platform == "win32" and (
                "Python worker failed" in str(e) or "Job aborted" in str(e)
            ):
                pytest.skip(f"Skipping Spark test on Windows due to environment issue: {e}")
            raise e

except ImportError:
    pass
