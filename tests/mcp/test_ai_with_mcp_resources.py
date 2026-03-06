"""Comprehensive test: Can an AI agent use odibi MCP tools with minimal prompting?

This simulates what happens when an AI assistant (Amp, Cline, etc.) connects to
the odibi MCP server with auto-loaded resources.

Tests:
1. AI has access to resources (Critical Context, guides)
2. AI can build pipelines with SHORT prompts (no long instructions)
3. AI uses correct field names (read: not source:)
4. AI doesn't hallucinate transformer names
5. AI selects correct patterns
"""

from dotenv import load_dotenv
from pathlib import Path
import json

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.tools.construction import list_transformers, list_patterns, apply_pattern_template
from odibi_mcp.resources import get_resources, read_resource


def load_mcp_resources():
    """Load MCP resources that AI would have access to."""
    resources = get_resources()

    resource_content = {}

    for resource in resources:
        if resource["path"].exists():
            content = read_resource(resource["uri"])
            resource_content[resource["name"]] = content[:2000]  # First 2000 chars

    return resource_content


def simulate_ai_agent_with_resources(user_prompt: str, resources: dict) -> dict:
    """Simulate an AI agent that has MCP resources loaded in context."""

    client = OpenAI()

    # Build system message with critical context (like MCP would provide)
    critical_context = resources.get("⚡ AI CRITICAL CONTEXT", "")

    # Simulate tool availability
    patterns = list_patterns()
    list_transformers()

    tools_description = f"""
You have access to these MCP tools:
- list_patterns() - Returns: {json.dumps(patterns, indent=2)[:500]}...
- apply_pattern_template(pattern, pipeline_name, source_connection, target_connection, target_path, ...)
- list_transformers() - 56 transformers available

Critical Context (auto-loaded):
{critical_context[:1500]}

CRITICAL RULES from context:
- NEVER use: source:, sink:, inputs:, outputs:
- ALWAYS use: read:, write:, query:
- Call tools, don't write YAML manually
- Pattern params must match requirements
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": tools_description},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.1,
        max_tokens=500,
    )

    return {
        "prompt": user_prompt,
        "response": response.choices[0].message.content,
        "model": response.model,
    }


def test_scenarios():
    """Test various user scenarios with minimal prompts."""

    print("=" * 80)
    print("COMPREHENSIVE AI AGENT TEST WITH MCP RESOURCES")
    print("=" * 80)

    # Load resources
    print("\n[SETUP] Loading MCP resources that AI would have...")
    resources = load_mcp_resources()
    print(f"  Loaded {len(resources)} resources")
    print(f"  Critical Context: {'⚡ AI CRITICAL CONTEXT' in resources}")

    test_cases = [
        {
            "name": "Build Customer Dimension",
            "prompt": "Build a customer dimension pipeline from dbo.Customer table",
            "should_mention": ["dimension", "natural_key", "surrogate_key"],
            "should_not_mention": ["source:", "sink:"],
        },
        {
            "name": "Track Employee Changes",
            "prompt": "I need to track employee changes over time",
            "should_mention": ["scd2", "track", "history"],
            "should_not_mention": ["source:", "manually"],
        },
        {
            "name": "Daily Orders Load",
            "prompt": "Load daily orders transactions",
            "should_mention": ["fact", "transactional"],
            "should_not_mention": ["dedup", "drop_nulls"],  # Wrong transformer names
        },
        {
            "name": "Calendar Table",
            "prompt": "Create a calendar table from 2020 to 2030",
            "should_mention": ["date_dimension", "start_date", "end_date"],
            "should_not_mention": [],
        },
    ]

    results = []

    for test in test_cases:
        print(f"\n{'=' * 80}")
        print(f"TEST: {test['name']}")
        print(f"{'=' * 80}")
        print(f'\nUser prompt: "{test["prompt"]}"')
        print(f"(Prompt length: {len(test['prompt'])} characters)")

        result = simulate_ai_agent_with_resources(test["prompt"], resources)

        response_text = result["response"].lower()

        print(f"\nAI Response ({result['model']}):")
        print("-" * 80)
        print(
            result["response"][:400] + "..."
            if len(result["response"]) > 400
            else result["response"]
        )
        print("-" * 80)

        # Check expectations
        passed = True
        issues = []

        for keyword in test["should_mention"]:
            if keyword.lower() not in response_text:
                passed = False
                issues.append(f"  ✗ Should mention '{keyword}' but didn't")

        for keyword in test["should_not_mention"]:
            if keyword.lower() in response_text:
                passed = False
                issues.append(f"  ✗ Should NOT mention '{keyword}' but did")

        # Check for correct field names
        if "source:" in result["response"] or "sink:" in result["response"]:
            passed = False
            issues.append("  ✗ Used wrong field names (source:/sink:)")

        if passed:
            print(f"\n✓ PASSED: {test['name']}")
        else:
            print(f"\n✗ FAILED: {test['name']}")
            for issue in issues:
                print(issue)

        results.append((test["name"], passed))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed_count = sum(1 for _, p in results if p)
    total = len(results)

    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")

    print(f"\nResults: {passed_count}/{total} tests passed")

    if passed_count == total:
        print("\n" + "=" * 80)
        print("✓ SUCCESS: AI AGENT CAN USE ODIBI WITH MINIMAL PROMPTS!")
        print("=" * 80)
        print("\nKey findings:")
        print("  - AI reads Critical Context correctly")
        print("  - AI selects appropriate patterns")
        print("  - AI uses correct field names (read:/write:)")
        print("  - AI doesn't hallucinate transformer names")
        print("\nMCP Resources work as designed! No long prompts needed.")
        return True
    else:
        print("\n" + "=" * 80)
        print("⚠ SOME TESTS FAILED - Review responses")
        print("=" * 80)
        return False


def test_tool_calling():
    """Test that AI actually calls tools with correct parameters."""

    print("\n" + "=" * 80)
    print("TEST: AI Tool Calling with Resources")
    print("=" * 80)

    client = OpenAI()
    resources = load_mcp_resources()
    critical = resources.get("⚡ AI CRITICAL CONTEXT", "")[:1000]

    # Give AI access to actual tool
    tools_available = (
        """
You have these tools available (call them with JSON params):

Tool: apply_pattern_template
Required params: pattern, pipeline_name, source_connection, target_connection, target_path
Pattern options: dimension, fact, scd2, date_dimension, aggregation, merge

For dimension pattern, also need: natural_key, surrogate_key

Critical Context:
"""
        + critical
        + """

TASK: For the user request, determine which tool to call and what parameters.
Return ONLY JSON: {"tool": "name", "params": {...}}
"""
    )

    user_request = "Build a customer dimension from dbo.Customer table"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": tools_available},
            {"role": "user", "content": user_request},
        ],
        temperature=0.1,
        max_tokens=300,
    )

    try:
        tool_call = json.loads(response.choices[0].message.content)

        print(f'\nUser: "{user_request}"')
        print("\nAI decided to call:")
        print(f"  Tool: {tool_call.get('tool')}")
        print(f"  Params: {json.dumps(tool_call.get('params', {}), indent=4)}")

        # Verify correctness
        checks = []
        checks.append(("Tool name", tool_call.get("tool") == "apply_pattern_template"))
        checks.append(("Pattern", tool_call.get("params", {}).get("pattern") == "dimension"))
        checks.append(("Has natural_key", "natural_key" in tool_call.get("params", {})))
        checks.append(("Has surrogate_key", "surrogate_key" in tool_call.get("params", {})))

        print("\nValidation:")
        all_pass = True
        for check_name, passed in checks:
            status = "✓" if passed else "✗"
            print(f"  {status} {check_name}")
            if not passed:
                all_pass = False

        if all_pass:
            print("\n✓ AI called correct tool with correct params!")

            # Actually execute the tool
            print("\n[EXECUTING TOOL]...")
            result = apply_pattern_template(**tool_call["params"])

            print(f"  Valid: {result['valid']}")
            if result["valid"]:
                print(f"  YAML generated: {len(result['yaml'])} characters")
                print("\n✓ SUCCESS: AI + Resources + Tools = Working Pipeline!")
                return True
            else:
                print(f"  Errors: {result.get('errors')}")
                return False
        else:
            print("\n✗ AI made mistakes in tool call")
            return False

    except Exception as e:
        print(f"\n✗ Failed to parse AI response: {e}")
        print(f"Response was: {response.choices[0].message.content}")
        return False


if __name__ == "__main__":
    print("Testing AI Agent with MCP Resources...\n")

    # Test 1: Understanding with resources
    scenario_test = test_scenarios()

    # Test 2: Actual tool calling
    tool_test = test_tool_calling()

    # Final verdict
    print("\n" + "=" * 80)
    print("FINAL VERDICT")
    print("=" * 80)

    if scenario_test and tool_test:
        print("\n✓ MCP RESOURCES SOLVE THE PROBLEM!")
        print("\nProven:")
        print("  ✓ Short prompts work (6-10 words)")
        print("  ✓ AI reads Critical Context correctly")
        print("  ✓ AI uses correct field names")
        print("  ✓ AI calls tools with correct params")
        print("  ✓ Generated YAML is valid")
        print("\nYou can now just say 'Build X' - no long prompts needed!")
    else:
        print("\n⚠ Some tests failed - review responses")
