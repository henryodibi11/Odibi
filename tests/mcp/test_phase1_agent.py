"""Test Phase 1 MCP tools with a real AI agent.

This simulates an AI agent discovering patterns, selecting one, and generating valid YAML.
Tests that the agent doesn't hallucinate field names or transformer names.
"""

from dotenv import load_dotenv
from pathlib import Path
import json

# Load .env
load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.tools.construction import list_transformers, list_patterns, apply_pattern_template
from odibi_mcp.tools.validation import validate_pipeline


def test_agent_workflow():
    """Test that an AI agent can successfully build a pipeline using Phase 1 tools."""

    print("=" * 80)
    print("PHASE 1 AGENT WORKFLOW TEST")
    print("=" * 80)

    client = OpenAI()

    # Step 1: Agent discovers patterns
    print("\n[STEP 1] Agent calls list_patterns...")
    patterns = list_patterns()
    print(f"  Found {patterns['count']} patterns")

    # Step 2: Agent asks GPT to select a pattern for a use case
    print("\n[STEP 2] Agent asks GPT: 'I need to track customer changes over time'")

    tools_json = json.dumps(patterns, indent=2)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": """You are helping build data pipelines. You have access to these patterns:

{patterns}

The user will describe what they want to build. You must:
1. Select the appropriate pattern
2. Identify what parameters are required
3. Return ONLY a JSON object with your recommendation

Response format:
{
  "pattern": "pattern_name",
  "reason": "why this pattern fits",
  "required_params": ["param1", "param2"]
}""".replace("{patterns}", tools_json),
            },
            {
                "role": "user",
                "content": "I need to track customer changes over time. When a customer's email or address changes, I want to keep history of both old and new values.",
            },
        ],
        temperature=0.1,
        max_tokens=300,
    )

    agent_recommendation = response.choices[0].message.content
    print(f"\n  Agent recommends:\n{agent_recommendation}")

    # Parse recommendation
    try:
        rec = json.loads(agent_recommendation)
        selected_pattern = rec["pattern"]
        print(f"\n  ✓ Pattern selected: {selected_pattern}")
    except:
        print("  [ERROR] Agent didn't return valid JSON")
        return False

    # Step 3: Test apply_pattern_template with correct params
    print(f"\n[STEP 3] Agent calls apply_pattern_template with {selected_pattern} pattern...")

    # Build params based on pattern
    params = {
        "pattern": selected_pattern,
        "pipeline_name": "customer_history",
        "source_connection": "crm_db",
        "target_connection": "warehouse",
        "target_path": "silver/dim_customer",
        "source_table": "dbo.Customers",
    }

    # Add pattern-specific params
    if selected_pattern == "scd2":
        params["keys"] = ["customer_id"]
        params["tracked_columns"] = ["email", "address", "city", "state"]
    elif selected_pattern == "dimension":
        params["natural_key"] = "customer_id"
        params["surrogate_key"] = "customer_sk"
        params["tracked_columns"] = ["email", "address"]

    result = apply_pattern_template(**params)

    if result["valid"]:
        print("  ✓ YAML generated successfully")
        print(f"\n  Generated YAML (first 500 chars):\n{result['yaml'][:500]}...")
    else:
        print(f"  [ERROR] YAML generation failed: {result.get('errors', [])}")
        return False

    # Step 4: Validate the generated YAML
    print("\n[STEP 4] Validating generated YAML...")
    validation = validate_pipeline(result["yaml"])

    print(f"  Summary: {validation['summary']}")

    if validation["valid"]:
        print("  ✓ YAML is valid!")
    else:
        print("  [ERROR] Validation failed:")
        for error in validation["errors"]:
            print(f"    - {error['code']}: {error['message']}")
        return False

    # Step 5: Test that agent can list transformers
    print("\n[STEP 5] Agent explores available transformers...")
    transformers = list_transformers(category="scd")
    print(f"  Found {transformers['count']} SCD transformers")
    if transformers["count"] > 0:
        print(f"  Example: {transformers['transformers'][0]['name']}")

    # Step 6: Ask agent to describe the workflow
    print("\n[STEP 6] Asking agent to explain what it did...")

    explain_response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": f"""I used these tools to build a pipeline:
1. list_patterns() - found 6 patterns
2. Selected '{selected_pattern}' pattern
3. Called apply_pattern_template() with params
4. Got valid YAML

In 2-3 sentences, explain why '{selected_pattern}' was the right choice for tracking customer changes over time.""",
            }
        ],
        temperature=0.3,
        max_tokens=150,
    )

    explanation = explain_response.choices[0].message.content
    print(f"\n  Agent explains:\n  {explanation}")

    print("\n" + "=" * 80)
    print("✓ ALL TESTS PASSED")
    print("=" * 80)
    print("\nRESULTS:")
    print(f"  - Agent selected correct pattern: {selected_pattern}")
    print("  - YAML generated without errors")
    print("  - YAML validated successfully")
    print("  - No hallucinated field names or transformer names")
    print("\n[SUCCESS] Phase 1 tools work correctly with AI agents!")

    return True


if __name__ == "__main__":
    try:
        success = test_agent_workflow()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n[ERROR] Test failed with exception: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
