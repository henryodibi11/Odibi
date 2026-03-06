"""Comprehensive tests for Phase 3 smart chaining and templates."""

from dotenv import load_dotenv
from pathlib import Path
import sys

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.tools.phase3_smart import suggest_pipeline, create_ingestion_pipeline
from odibi_mcp.tools.validation import validate_pipeline

tests_passed = 0
tests_failed = 0
test_details = []


def test(name):
    """Test decorator."""

    def decorator(func):
        def wrapper():
            global tests_passed, tests_failed, test_details
            try:
                print(f"\n{'=' * 80}")
                print(f"TEST: {name}")
                print("=" * 80)
                result = func()
                if result:
                    tests_passed += 1
                    test_details.append(("PASS", name))
                    print(f"[OK] PASSED: {name}")
                else:
                    tests_failed += 1
                    test_details.append(("FAIL", name))
                    print(f"[FAIL] FAILED: {name}")
                return result
            except Exception as e:
                tests_failed += 1
                test_details.append(("ERROR", name, str(e)))
                print(f"[ERROR] {name}: {e}")
                import traceback

                traceback.print_exc()
                return False

        return wrapper

    return decorator


@test("suggest_pipeline: dimension pattern for low-volume keyed data")
def test_suggest_dimension():
    """Test that suggest_pipeline recommends dimension for reference data."""
    profile = {
        "connection": "crm",
        "path": "dbo.Customer",
        "candidate_keys": ["customer_id"],
        "row_count_estimate": 10000,
        "schema": {
            "columns": [
                {"name": "customer_id", "dtype": "int", "cardinality": "unique"},
                {"name": "name", "dtype": "string"},
                {"name": "email", "dtype": "string"},
            ]
        },
    }

    result = suggest_pipeline(profile)

    assert result["suggested_pattern"] == "dimension"
    assert result["confidence"] > 0.5
    assert "ready_for" in result
    assert "apply_pattern_template" in result["ready_for"]

    # Check ready_for params
    params = result["ready_for"]["apply_pattern_template"]
    assert params["pattern"] == "dimension"
    assert "natural_key" in params
    assert "surrogate_key" in params

    print(f"  [OK] Suggested: {result['suggested_pattern']} (confidence: {result['confidence']})")
    print(f"  Reason: {result['reason']}")
    return True


@test("suggest_pipeline: fact pattern for high-volume data")
def test_suggest_fact():
    """Test that suggest_pipeline recommends fact for high-volume transactional data."""
    profile = {
        "connection": "sales",
        "path": "dbo.Orders",
        "candidate_keys": ["order_id"],
        "row_count_estimate": 5000000,
        "schema": {
            "columns": [
                {"name": "order_id", "dtype": "int", "cardinality": "unique"},
                {"name": "customer_id", "dtype": "int"},
                {"name": "order_date", "dtype": "datetime"},
                {"name": "amount", "dtype": "decimal"},
            ]
        },
    }

    result = suggest_pipeline(profile)

    assert result["suggested_pattern"] == "fact"
    assert result["confidence"] > 0.7

    params = result["ready_for"]["apply_pattern_template"]
    assert params["pattern"] == "fact"
    assert "keys" in params

    print(f"  [OK] Suggested: {result['suggested_pattern']} (confidence: {result['confidence']})")
    print(f"  Reason: {result['reason']}")
    return True


@test("suggest_pipeline: fact pattern for timestamp columns")
def test_suggest_fact_timestamps():
    """Test that timestamp columns trigger fact pattern."""
    profile = {
        "connection": "logs",
        "path": "events.parquet",
        "candidate_keys": [],
        "row_count_estimate": None,
        "schema": {
            "columns": [
                {"name": "event_id", "dtype": "string"},
                {"name": "created_at", "dtype": "timestamp"},
                {"name": "event_type", "dtype": "string"},
            ]
        },
    }

    result = suggest_pipeline(profile)

    assert result["suggested_pattern"] == "fact"
    assert len(result["timestamp_columns_found"]) > 0

    print(f"  [OK] Timestamp columns → {result['suggested_pattern']} pattern")
    return True


@test("create_ingestion_pipeline: 2 tables")
def test_ingestion_2_tables():
    """Test bulk ingestion with 2 tables."""
    result = create_ingestion_pipeline(
        pipeline_name="bronze_erp",
        source_connection="erp_db",
        target_connection="warehouse",
        tables=[
            {"schema": "dbo", "table": "Orders", "keys": ["order_id"]},
            {"schema": "dbo", "table": "Customers", "keys": ["customer_id"]},
        ],
    )

    assert result["valid"], f"Errors: {result.get('errors')}"
    assert result["node_count"] == 2
    assert len(result["tables_ingested"]) == 2

    # Validate YAML
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation errors: {validation.get('errors')}"

    print("  [OK] Generated 2-node ingestion pipeline")
    print(f"  Nodes: {result['tables_ingested']}")
    return True


@test("create_ingestion_pipeline: 5 tables with WHERE clauses")
def test_ingestion_5_tables():
    """Test bulk ingestion with 5 tables and WHERE clauses."""
    result = create_ingestion_pipeline(
        pipeline_name="bronze_sales",
        source_connection="sales_db",
        target_connection="lake",
        tables=[
            {"schema": "Sales", "table": "Orders", "keys": ["OrderID"]},
            {"schema": "Sales", "table": "OrderLines", "keys": ["OrderLineID"]},
            {"schema": "Inventory", "table": "Products", "keys": ["ProductID"]},
            {"schema": "HR", "table": "Employees", "keys": ["EmployeeID"], "where": "IsActive = 1"},
            {
                "schema": "Customer",
                "table": "Customers",
                "keys": ["CustomerID"],
                "where": "Country = 'USA'",
            },
        ],
        layer="bronze",
        mode="append_once",
    )

    assert result["valid"]
    assert result["node_count"] == 5

    # Validate
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"]

    print("  [OK] Generated 5-node pipeline with WHERE clauses")
    print(f"  Nodes: {result['node_count']}")
    return True


@test("create_ingestion_pipeline: empty tables list")
def test_ingestion_empty():
    """Test that empty tables list is rejected."""
    result = create_ingestion_pipeline(
        pipeline_name="test", source_connection="db", target_connection="lake", tables=[]
    )

    assert not result["valid"]
    assert len(result["errors"]) > 0

    print("  [OK] Empty tables list rejected")
    return True


@test("End-to-end: profile → suggest → generate")
def test_e2e_workflow():
    """Test complete workflow from profile to YAML generation."""
    # Simulate profile_source output
    mock_profile = {
        "connection": "hr_db",
        "path": "dbo.Employee",
        "format": "sql",
        "candidate_keys": ["emp_id"],
        "candidate_watermarks": ["modified_date"],
        "row_count_estimate": 25000,
        "schema": {
            "columns": [
                {"name": "emp_id", "dtype": "int", "cardinality": "unique"},
                {"name": "name", "dtype": "string"},
                {"name": "department", "dtype": "string"},
                {"name": "salary", "dtype": "decimal"},
                {"name": "hire_date", "dtype": "date"},
                {"name": "modified_date", "dtype": "datetime"},
            ]
        },
    }

    # Step 1: Suggest pattern
    print("\n  [STEP 1] Get pattern suggestion...")
    suggestion = suggest_pipeline(mock_profile)

    pattern = suggestion["suggested_pattern"]
    confidence = suggestion["confidence"]

    print(f"    Suggested: {pattern} (confidence: {confidence})")
    print(f"    Reason: {suggestion['reason']}")

    # Step 2: Use ready_for params to generate YAML
    print("\n  [STEP 2] Generate YAML with suggested params...")
    from odibi_mcp.tools.construction import apply_pattern_template

    params = suggestion["ready_for"]["apply_pattern_template"]
    result = apply_pattern_template(**params)

    assert result["valid"], f"Generation failed: {result.get('errors')}"

    print("    [OK] YAML generated")

    # Step 3: Validate
    print("\n  [STEP 3] Validate generated YAML...")
    validation = validate_pipeline(result["yaml"])

    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print(f"    {validation['summary']}")

    print("\n  [OK] Complete workflow: profile → suggest → generate → validate")
    return True


@test("AI Agent: Use suggest_pipeline")
def test_ai_agent_suggestion():
    """Test that AI agent can use suggest_pipeline."""
    client = OpenAI()

    mock_profile = {
        "candidate_keys": ["product_id"],
        "row_count_estimate": 5000,
        "schema": {
            "columns": [
                {"name": "product_id", "dtype": "int", "cardinality": "unique"},
                {"name": "product_name", "dtype": "string"},
                {"name": "price", "dtype": "decimal"},
            ]
        },
    }

    suggestion = suggest_pipeline(mock_profile)

    print("\n  Profile analyzed")
    print(
        f"  AI sees: pattern={suggestion['suggested_pattern']}, confidence={suggestion['confidence']}"
    )

    # Ask AI if it agrees
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": f"""A tool suggested using '{suggestion["suggested_pattern"]}' pattern for product data with 5,000 rows.

Reason: {suggestion["reason"]}

Do you agree? Answer in 1 sentence.""",
            }
        ],
        temperature=0.3,
        max_tokens=50,
    )

    ai_response = response.choices[0].message.content
    print(f"  AI says: {ai_response}")

    print("  [OK] AI agent can consume suggestion")
    return True


def print_summary():
    """Print test summary."""
    print("\n" + "=" * 80)
    print("PHASE 3 TEST SUMMARY")
    print("=" * 80)

    total = tests_passed + tests_failed
    percentage = (tests_passed / total * 100) if total > 0 else 0

    print(f"\nTotal Tests: {total}")
    print(f"Passed: {tests_passed}")
    print(f"Failed: {tests_failed}")
    print(f"Success Rate: {percentage:.1f}%")

    print("\nDetailed Results:")
    for result in test_details:
        status = result[0]
        name = result[1]
        symbol = "[OK]" if status == "PASS" else "[FAIL]"
        print(f"  {symbol} {name}")

    if tests_failed == 0:
        print("\n" + "=" * 80)
        print("[SUCCESS] ALL PHASE 3 TESTS PASSED!")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("[FAIL] SOME TESTS FAILED")
        print("=" * 80)

    return tests_failed == 0


if __name__ == "__main__":
    print("Starting Phase 3 test suite...\n")

    test_suggest_dimension()
    test_suggest_fact()
    test_suggest_fact_timestamps()
    test_ingestion_2_tables()
    test_ingestion_5_tables()
    test_ingestion_empty()
    test_e2e_workflow()
    test_ai_agent_suggestion()

    success = print_summary()
    sys.exit(0 if success else 1)
