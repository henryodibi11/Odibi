"""Quick test of HWM implementation."""

# Test 1: Config imports
try:
    from odibi.config import WriteConfig, WriteMode

    print("[OK] Config imports successful")
except Exception as e:
    print(f"[FAIL] Config import failed: {e}")
    exit(1)

# Test 2: WriteConfig with first_run_query
try:
    config = WriteConfig(
        connection="test_conn",
        format="delta",
        path="test/path",
        mode=WriteMode.APPEND,
        first_run_query="SELECT * FROM source",
    )
    assert config.first_run_query == "SELECT * FROM source"
    print("[OK] WriteConfig first_run_query field works")
except Exception as e:
    print(f"[FAIL] WriteConfig test failed: {e}")
    exit(1)

# Test 3: Node imports
try:
    from odibi.node import Node

    print("[OK] Node import successful")
except Exception as e:
    print(f"[FAIL] Node import failed: {e}")
    exit(1)

# Test 4: Engine base method
try:
    from odibi.engine.base import Engine

    # Check table_exists is in signature

    methods = [m for m in dir(Engine) if not m.startswith("_")]
    assert "table_exists" in methods
    print("[OK] Engine.table_exists() defined")
except Exception as e:
    print(f"[FAIL] Engine base test failed: {e}")
    exit(1)

# Test 5: Spark engine implementation
try:
    from odibi.engine.spark_engine import SparkEngine

    methods = [m for m in dir(SparkEngine) if not m.startswith("_")]
    assert "table_exists" in methods
    print("[OK] SparkEngine.table_exists() implemented")
except Exception as e:
    print(f"[FAIL] Spark engine test failed: {e}")
    exit(1)

# Test 6: Pandas engine implementation
try:
    from odibi.engine.pandas_engine import PandasEngine

    methods = [m for m in dir(PandasEngine) if not m.startswith("_")]
    assert "table_exists" in methods
    print("[OK] PandasEngine.table_exists() implemented")
except Exception as e:
    print(f"[FAIL] Pandas engine test failed: {e}")
    exit(1)

# Test 7: Node methods
try:
    from odibi.node import Node

    methods = dir(Node)
    assert "_determine_write_mode" in methods
    print("[OK] Node._determine_write_mode() added")
except Exception as e:
    print(f"[FAIL] Node methods test failed: {e}")
    exit(1)

print("\n[SUCCESS] All HWM implementation tests passed!")
