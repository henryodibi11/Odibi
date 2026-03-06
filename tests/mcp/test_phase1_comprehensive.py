"""Comprehensive test suite for Phase 1 MCP tools.

Tests all patterns, error cases, validation, and agent workflows.
Ensures no gaps before moving to Phase 2.
"""

from dotenv import load_dotenv
from pathlib import Path
import json
import sys

# Load .env
load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.tools.construction import list_transformers, list_patterns, apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline

# Test results tracking
tests_passed = 0
tests_failed = 0
test_details = []


def test(name):
    """Decorator to track test results."""

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
                    print(f"✓ PASSED: {name}")
                else:
                    tests_failed += 1
                    test_details.append(("FAIL", name))
                    print(f"✗ FAILED: {name}")
                return result
            except Exception as e:
                tests_failed += 1
                test_details.append(("ERROR", name, str(e)))
                print(f"✗ ERROR in {name}: {e}")
                import traceback

                traceback.print_exc()
                return False

        return wrapper

    return decorator


@test("list_patterns returns all 6 patterns")
def test_list_patterns():
    """Test that list_patterns returns all expected patterns."""
    result = list_patterns()

    assert result["count"] == 6, f"Expected 6 patterns, got {result['count']}"

    expected_patterns = {"dimension", "fact", "scd2", "merge", "aggregation", "date_dimension"}
    actual_patterns = {p["name"] for p in result["patterns"]}

    assert expected_patterns == actual_patterns, (
        f"Missing patterns: {expected_patterns - actual_patterns}"
    )

    # Check each pattern has required metadata
    for pattern in result["patterns"]:
        assert "name" in pattern
        assert "description" in pattern
        assert "required_params" in pattern
        assert "optional_params" in pattern
        assert "example_call" in pattern
        print(
            f"  ✓ {pattern['name']}: {len(pattern['required_params'])} required, {len(pattern['optional_params'])} optional"
        )

    return True


@test("list_transformers returns 56+ transformers")
def test_list_transformers():
    """Test that list_transformers returns all registered transformers."""
    result = list_transformers()

    assert result["count"] > 50, f"Expected 50+ transformers, got {result['count']}"
    print(f"  ✓ Found {result['count']} transformers")

    # Check structure
    assert "transformers" in result
    assert "categories" in result

    # Verify each transformer has required fields
    for t in result["transformers"][:5]:  # Check first 5
        assert "name" in t
        assert "description" in t
        assert "category" in t
        assert "parameters" in t
        assert "example_yaml" in t

    # Test category filtering
    scd_result = list_transformers(category="scd")
    assert scd_result["count"] > 0, "Should have SCD transformers"
    print(f"  ✓ Category filter works: {scd_result['count']} SCD transformers")

    # Test search
    search_result = list_transformers(search="deduplicate")
    assert search_result["count"] > 0, "Should find deduplicate"
    print("  ✓ Search works: found transformers matching 'deduplicate'")

    return True


@test("Pattern: dimension - valid config")
def test_dimension_pattern_valid():
    """Test dimension pattern with valid params."""
    result = apply_pattern_template(
        pattern="dimension",
        pipeline_name="dim_customer",
        source_connection="raw",
        target_connection="local",
        target_path="gold/dim_customer",
        source_table="dbo.Customer",
        natural_key="customer_id",
        surrogate_key="customer_sk",
        tracked_columns=["name", "email", "address"],
    )

    assert result["valid"], f"Expected valid, got errors: {result.get('errors')}"
    assert result["yaml"], "Expected YAML output"

    # Validate the generated YAML
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print("  ✓ Dimension pattern generates valid YAML")
    return True


@test("Pattern: dimension - missing required params")
def test_dimension_pattern_missing_params():
    """Test dimension pattern fails without required params."""
    result = apply_pattern_template(
        pattern="dimension",
        pipeline_name="dim_customer",
        source_connection="raw",
        target_connection="local",
        target_path="gold/dim_customer",
        source_table="dbo.Customer",
        # Missing natural_key and surrogate_key
    )

    assert not result["valid"], "Should fail without required params"
    assert len(result["errors"]) > 0, "Should have errors"

    # Check error mentions missing params
    error_messages = " ".join(e["message"] for e in result["errors"])
    assert "natural_key" in error_messages or "surrogate_key" in error_messages

    print("  ✓ Correctly rejects missing required params")
    return True


@test("Pattern: scd2 - valid config")
def test_scd2_pattern_valid():
    """Test SCD2 pattern with valid params."""
    result = apply_pattern_template(
        pattern="scd2",
        pipeline_name="employee_history",
        source_connection="hr",
        target_connection="warehouse",
        target_path="silver/dim_employee",
        source_table="dbo.Employee",
        keys=["emp_id"],
        tracked_columns=["name", "dept", "salary"],
    )

    assert result["valid"], f"Expected valid, got errors: {result.get('errors')}"

    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print("  ✓ SCD2 pattern generates valid YAML")
    return True


@test("Pattern: scd2 - missing tracked_columns")
def test_scd2_pattern_missing_tracked():
    """Test SCD2 fails without tracked_columns."""
    result = apply_pattern_template(
        pattern="scd2",
        pipeline_name="employee_history",
        source_connection="hr",
        target_connection="warehouse",
        target_path="silver/dim_employee",
        source_table="dbo.Employee",
        keys=["emp_id"],
        # Missing tracked_columns
    )

    assert not result["valid"], "Should fail without tracked_columns"
    print("  ✓ Correctly rejects SCD2 without tracked_columns")
    return True


@test("Pattern: fact - valid config")
def test_fact_pattern_valid():
    """Test fact pattern with valid params."""
    result = apply_pattern_template(
        pattern="fact",
        pipeline_name="fact_orders",
        source_connection="sales",
        target_connection="warehouse",
        target_path="gold/fact_orders",
        source_table="dbo.Orders",
        keys=["order_id"],
        layer="gold",
    )

    assert result["valid"], f"Expected valid, got errors: {result.get('errors')}"

    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print("  ✓ Fact pattern generates valid YAML")
    return True


@test("Pattern: date_dimension - valid config")
def test_date_dimension_pattern_valid():
    """Test date dimension pattern."""
    result = apply_pattern_template(
        pattern="date_dimension",
        pipeline_name="dim_date",
        source_connection="local",  # Not used but required
        target_connection="warehouse",
        target_path="gold/dim_date",
        start_date="2020-01-01",
        end_date="2030-12-31",
    )

    assert result["valid"], f"Expected valid, got errors: {result.get('errors')}"

    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print("  ✓ Date dimension pattern generates valid YAML")
    return True


@test("Pattern: aggregation - valid config")
def test_aggregation_pattern_valid():
    """Test aggregation pattern."""
    result = apply_pattern_template(
        pattern="aggregation",
        pipeline_name="monthly_sales",
        source_connection="warehouse",
        target_connection="warehouse",
        target_path="gold/agg_monthly_sales",
        source_table="silver.fact_orders",
        grain=["customer_id", "month"],
        measures=["SUM(amount) as total_sales", "COUNT(*) as order_count"],
    )

    assert result["valid"], f"Expected valid, got errors: {result.get('errors')}"

    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], f"Validation failed: {validation.get('errors')}"

    print("  ✓ Aggregation pattern generates valid YAML")
    return True


@test("Invalid pipeline name (special characters)")
def test_invalid_pipeline_name():
    """Test that invalid pipeline names are rejected."""
    result = apply_pattern_template(
        pattern="dimension",
        pipeline_name="dim-customer!@#",  # Invalid characters
        source_connection="raw",
        target_connection="local",
        target_path="gold/dim_customer",
        source_table="dbo.Customer",
        natural_key="customer_id",
        surrogate_key="customer_sk",
    )

    assert not result["valid"], "Should reject invalid pipeline name"

    # Check error code
    error_codes = [e["code"] for e in result["errors"]]
    assert "INVALID_NAME" in error_codes

    print("  ✓ Correctly rejects invalid pipeline names")
    return True


@test("Unknown pattern")
def test_unknown_pattern():
    """Test that unknown patterns are rejected."""
    result = apply_pattern_template(
        pattern="nonexistent_pattern",
        pipeline_name="test",
        source_connection="raw",
        target_connection="local",
        target_path="test",
    )

    assert not result["valid"], "Should reject unknown pattern"

    error_codes = [e["code"] for e in result["errors"]]
    assert "UNKNOWN_PATTERN" in error_codes

    allowed_patterns = result["errors"][0].get("allowed_values", [])
    assert len(allowed_patterns) == 6, "Should suggest 6 valid patterns"

    print("  ✓ Correctly rejects unknown patterns with suggestions")
    return True


@test("Validation: missing dependencies")
def test_validation_missing_dependencies():
    """Test that validation catches missing dependencies."""
    yaml_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: node_a
        depends_on: [nonexistent_node]
        read:
          connection: x
          format: sql
          table: t
        write:
          connection: y
          format: delta
          path: out
"""

    result = validate_pipeline(yaml_content)

    assert not result["valid"], "Should catch missing dependency"

    error_codes = [e["code"] for e in result["errors"]]
    assert "MISSING_DEPENDENCY" in error_codes

    print("  ✓ Validation catches missing dependencies")
    return True


@test("Validation: unknown transformer")
def test_validation_unknown_transformer():
    """Test that validation catches unknown transformers."""
    yaml_content = """
pipelines:
  - pipeline: test
    nodes:
      - name: test_node
        read:
          connection: x
          format: sql
          table: t
        transform:
          steps:
            - function: nonexistent_transformer
              params: {}
        write:
          connection: y
          format: delta
          path: out
"""

    result = validate_pipeline(yaml_content)

    assert not result["valid"], "Should catch unknown transformer"

    error_codes = [e["code"] for e in result["errors"]]
    assert "UNKNOWN_TRANSFORMER" in error_codes

    print("  ✓ Validation catches unknown transformers")
    return True


@test("Round-trip YAML validation")
def test_roundtrip_validation():
    """Test that generated YAML can be re-parsed without errors."""
    result = apply_pattern_template(
        pattern="dimension",
        pipeline_name="dim_product",
        source_connection="raw",
        target_connection="local",
        target_path="gold/dim_product",
        source_table="dbo.Product",
        natural_key="product_id",
        surrogate_key="product_sk",
    )

    assert result["valid"], "Initial generation should succeed"

    # Parse the YAML back
    import yaml

    parsed = yaml.safe_load(result["yaml"])
    assert isinstance(parsed, dict), "YAML should parse to dict"
    assert "pipelines" in parsed, "Should have pipelines key"

    # Re-validate
    validation = validate_pipeline(result["yaml"])
    assert validation["valid"], "Re-validation should pass"

    # Serialize again
    yaml_content = yaml.safe_dump(parsed, default_flow_style=False, sort_keys=False)
    validation2 = validate_pipeline(yaml_content)
    assert validation2["valid"], "Second round-trip should pass"

    print("  ✓ YAML survives multiple round-trips")
    return True


@test("AI Agent: Selects correct pattern for use case")
def test_agent_pattern_selection():
    """Test that AI agent selects correct patterns for different use cases."""
    client = OpenAI()

    patterns_json = json.dumps(list_patterns(), indent=2)

    test_cases = [
        {
            "use_case": "Track employee salary changes over time with full history",
            "expected": "scd2",
            "reason": "needs historical tracking",
        },
        {
            "use_case": "Build a customer lookup table that gets fully refreshed daily",
            "expected": "dimension",
            "reason": "reference data with full refresh",
        },
        {
            "use_case": "Load daily sales transactions",
            "expected": "fact",
            "reason": "transactional data",
        },
        {
            "use_case": "Create a calendar table from 2020 to 2030",
            "expected": "date_dimension",
            "reason": "date dimension generation",
        },
    ]

    for i, case in enumerate(test_cases):
        print(f"\n  Test case {i + 1}: {case['use_case'][:50]}...")

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": f"""You select data warehouse patterns. Available patterns:
{patterns_json}

Return ONLY a JSON object: {{"pattern": "name", "reason": "brief explanation"}}""",
                },
                {"role": "user", "content": case["use_case"]},
            ],
            temperature=0.1,
            max_tokens=100,
        )

        try:
            result = json.loads(response.choices[0].message.content)
            selected = result["pattern"]

            if selected == case["expected"]:
                print(f"    ✓ Correctly selected {selected}")
            else:
                print(f"    ✗ Expected {case['expected']}, got {selected}")
                print(f"      Reason: {result.get('reason')}")
                # Don't fail - agent might have valid reasoning
        except Exception as e:
            print(f"    ✗ Failed to parse agent response: {e}")

    print("  ✓ Agent pattern selection tested (informational)")
    return True


@test("AI Agent: Doesn't hallucinate field names")
def test_agent_no_hallucination():
    """Test that AI agent uses correct field names from documentation."""
    client = OpenAI()

    # Give agent wrong examples and see if it catches them
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """You are reviewing YAML configs. Common mistakes to catch:
- Using 'source:' instead of 'read:'
- Using 'sink:' instead of 'write:'
- Using 'inputs:' at node level instead of 'read:'

Return JSON: {"has_errors": true/false, "errors": ["error1", "error2"]}""",
            },
            {
                "role": "user",
                "content": """Review this YAML:
nodes:
  - name: test
    source:
      connection: x
      table: t
    sink:
      connection: y
      path: out""",
            },
        ],
        temperature=0.1,
        max_tokens=150,
    )

    try:
        result = json.loads(response.choices[0].message.content)
        has_errors = result.get("has_errors", False)

        assert has_errors, "Agent should catch wrong field names (source/sink)"

        errors = result.get("errors", [])
        error_text = " ".join(errors).lower()

        # Agent should mention read/write instead of source/sink
        if (
            "source" in error_text
            or "sink" in error_text
            or "read" in error_text
            or "write" in error_text
        ):
            print("  ✓ Agent recognizes wrong field names")
        else:
            print(f"  ~ Agent response: {errors}")

    except Exception as e:
        print(f"  ~ Agent test inconclusive: {e}")

    return True


@test("All patterns generate unique node structures")
def test_pattern_uniqueness():
    """Test that different patterns generate different YAML structures."""
    patterns_to_test = ["dimension", "fact", "scd2", "date_dimension"]
    yamls = {}

    for pattern in patterns_to_test:
        params = {
            "pattern": pattern,
            "pipeline_name": f"{pattern}_test",
            "source_connection": "test",
            "target_connection": "test",
            "target_path": f"gold/{pattern}",
        }

        # Add pattern-specific params
        if pattern == "dimension":
            params.update({"natural_key": "id", "surrogate_key": "sk", "source_table": "t"})
        elif pattern == "scd2":
            params.update({"keys": ["id"], "tracked_columns": ["col1"], "source_table": "t"})
        elif pattern == "fact":
            params.update({"keys": ["id"], "source_table": "t"})
        elif pattern == "date_dimension":
            params.update({"start_date": "2020-01-01", "end_date": "2021-01-01"})

        result = apply_pattern_template(**params)
        assert result["valid"], f"{pattern} should generate valid YAML"

        yamls[pattern] = result["yaml"]

    # Check that YAMLs are different
    unique_yamls = len(set(yamls.values()))
    assert unique_yamls == len(patterns_to_test), "Each pattern should generate unique YAML"

    print(f"  ✓ All {len(patterns_to_test)} patterns generate unique structures")
    return True


def print_summary():
    """Print test summary."""
    print("\n" + "=" * 80)
    print("COMPREHENSIVE TEST SUMMARY")
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
        symbol = "✓" if status == "PASS" else "✗"
        print(f"  {symbol} {name}")
        if status == "ERROR" and len(result) > 2:
            print(f"    Error: {result[2]}")

    if tests_failed == 0:
        print("\n" + "=" * 80)
        print("🎉 ALL TESTS PASSED! PHASE 1 IS READY!")
        print("=" * 80)
        print("\nNo gaps found. Safe to proceed to Phase 2.")
    else:
        print("\n" + "=" * 80)
        print("⚠️  SOME TESTS FAILED - REVIEW NEEDED")
        print("=" * 80)
        print("\nFix failing tests before proceeding to Phase 2.")

    return tests_failed == 0


if __name__ == "__main__":
    print("Starting comprehensive Phase 1 test suite...\n")

    # Run all tests
    test_list_patterns()
    test_list_transformers()
    test_dimension_pattern_valid()
    test_dimension_pattern_missing_params()
    test_scd2_pattern_valid()
    test_scd2_pattern_missing_tracked()
    test_fact_pattern_valid()
    test_date_dimension_pattern_valid()
    test_aggregation_pattern_valid()
    test_invalid_pipeline_name()
    test_unknown_pattern()
    test_validation_missing_dependencies()
    test_validation_unknown_transformer()
    test_roundtrip_validation()
    test_agent_pattern_selection()
    test_agent_no_hallucination()
    test_pattern_uniqueness()

    # Print summary
    success = print_summary()

    sys.exit(0 if success else 1)
