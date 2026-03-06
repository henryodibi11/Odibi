"""Comprehensive test campaign for AI agent knowledge of ALL odibi features.

Tests that AI agents can:
1. Use basic patterns correctly
2. Know engine-specific features (views, temp tables, etc.)
3. Handle advanced write modes
4. Configure incremental loading
5. Use validation and quarantine
6. Work with Delta Lake features
7. Handle non-obvious gotchas

This validates that MCP resources contain complete knowledge.
"""

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.resources import get_resources, read_resource

# Test categories
test_results = {
    "basic": [],
    "engine_specific": [],
    "advanced_modes": [],
    "validation": [],
    "non_obvious": [],
}


def load_ai_context():
    """Load the context AI would have from MCP resources."""
    resources = get_resources()

    context_parts = []

    # Load critical resources
    priority_resources = [
        "⚡ AI INSTRUCTIONS (READ FIRST)",
        "⚡ Critical Field Names & Rules",
        "Complete Capabilities Reference",
    ]

    for resource in resources:
        if resource["name"] in priority_resources:
            content = read_resource(resource["uri"])
            # Load FULL content (real MCP loads everything)
            context_parts.append(f"=== {resource['name']} ===\n{content}\n")

    return "\n\n".join(context_parts)


def test_ai_knowledge(
    category: str, test_name: str, prompt: str, expected_keywords: list, avoid_keywords: list = []
):
    """Test if AI knows a specific capability."""

    print(f"\n{'=' * 80}")
    print(f"TEST [{category.upper()}]: {test_name}")
    print("=" * 80)
    print(f'Prompt: "{prompt}"')

    client = OpenAI()
    context = load_ai_context()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": f"""You are an odibi expert. Answer based on this knowledge:

{context}

Be specific and concise. Answer in 2-3 sentences when possible.""",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.1,
        max_tokens=400,
    )

    answer = response.choices[0].message.content
    print(f"\nAI Answer:\n{answer[:300]}...")

    # Check expectations
    passed = True
    for keyword in expected_keywords:
        if keyword.lower() not in answer.lower():
            print(f"  [X] Missing expected: '{keyword}'")
            passed = False

    for keyword in avoid_keywords:
        if keyword.lower() in answer.lower():
            print(f"  [X] Should avoid: '{keyword}'")
            passed = False

    if passed:
        print("\n  [OK] PASSED")
    else:
        print("\n  [FAIL] FAILED")

    test_results[category].append((test_name, passed))
    return passed


def run_campaign():
    """Run comprehensive test campaign."""

    print("=" * 80)
    print("ODIBI AI KNOWLEDGE TEST CAMPAIGN")
    print("=" * 80)
    print("\nLoading MCP resources that AI would have access to...")

    resources = get_resources()
    print(f"  Resources available: {len(resources)}")

    # CATEGORY 1: Basic Features
    print("\n" + "=" * 80)
    print("CATEGORY 1: BASIC FEATURES")
    print("=" * 80)

    test_ai_knowledge(
        "basic",
        "Can Spark query views?",
        "Can odibi query database views with Spark engine?",
        expected_keywords=["yes", "view", "table"],
        avoid_keywords=["cannot", "not supported"],
    )

    test_ai_knowledge(
        "basic",
        "Upsert requires keys",
        "What happens if I use mode: upsert without specifying keys?",
        expected_keywords=["error", "fail", "required", "keys"],
        avoid_keywords=[],
    )

    test_ai_knowledge(
        "basic",
        "Node name rules",
        "Can I use 'my-node' as a node name?",
        expected_keywords=["no", "underscore", "alphanumeric", "my_node"],
        avoid_keywords=["yes", "fine", "ok"],
    )

    # CATEGORY 2: Engine-Specific
    print("\n" + "=" * 80)
    print("CATEGORY 2: ENGINE-SPECIFIC FEATURES")
    print("=" * 80)

    test_ai_knowledge(
        "engine_specific",
        "Spark temp views",
        "In Spark engine, how can I reference output from a previous node?",
        expected_keywords=["temp view", "spark.sql", "node_name"],
        avoid_keywords=[],
    )

    test_ai_knowledge(
        "engine_specific",
        "Pandas SQL capability",
        "Can Pandas engine run SQL queries over DataFrames?",
        expected_keywords=["yes", "duckdb", "context.sql"],
        avoid_keywords=["cannot"],
    )

    test_ai_knowledge(
        "engine_specific",
        "Polars SCD2 limitation",
        "Does Polars engine support the SCD2 transformer?",
        expected_keywords=["no", "not supported", "spark", "pandas"],
        avoid_keywords=["yes", "works"],
    )

    # CATEGORY 3: Advanced Modes
    print("\n" + "=" * 80)
    print("CATEGORY 3: ADVANCED WRITE MODES")
    print("=" * 80)

    test_ai_knowledge(
        "advanced_modes",
        "Append_once for Bronze",
        "What write mode is recommended for Bronze layer ingestion?",
        expected_keywords=["append_once", "idempotent", "keys"],
        avoid_keywords=["overwrite", "append (without"],
    )

    test_ai_knowledge(
        "advanced_modes",
        "SQL Server merge strategy",
        "How do I do upsert to SQL Server target?",
        expected_keywords=["merge", "merge_keys", "sql"],
        avoid_keywords=[],
    )

    # CATEGORY 4: Validation
    print("\n" + "=" * 80)
    print("CATEGORY 4: VALIDATION & QUARANTINE")
    print("=" * 80)

    test_ai_knowledge(
        "validation",
        "Quarantine configuration",
        "If I set on_fail: quarantine, what else do I need to configure?",
        expected_keywords=["quarantine", "connection", "path"],
        avoid_keywords=[],
    )

    test_ai_knowledge(
        "validation",
        "Contracts vs Validation",
        "What's the difference between contracts and validation?",
        expected_keywords=["pre-transform", "post-transform", "contracts", "before"],
        avoid_keywords=[],
    )

    # CATEGORY 5: Non-Obvious Features
    print("\n" + "=" * 80)
    print("CATEGORY 5: NON-OBVIOUS FEATURES")
    print("=" * 80)

    test_ai_knowledge(
        "non_obvious",
        "Unknown member rows",
        "What does unknown_member: true do in dimension pattern?",
        expected_keywords=["sk=0", "orphan", "fact"],
        avoid_keywords=[],
    )

    test_ai_knowledge(
        "non_obvious",
        "Variable substitution",
        "Can I use date variables in odibi YAML?",
        expected_keywords=["yes", "${date", "variable"],
        avoid_keywords=["cannot"],
    )

    test_ai_knowledge(
        "non_obvious",
        "Cross-pipeline references",
        "How do I reference output from another pipeline?",
        expected_keywords=["$pipeline", "node", "reference"],
        avoid_keywords=[],
    )

    test_ai_knowledge(
        "non_obvious",
        "First-run behavior",
        "What happens on first run when target doesn't exist with incremental loading?",
        expected_keywords=["full load", "skip", "filter"],
        avoid_keywords=["error", "fail"],
    )

    # Print summary
    print("\n" + "=" * 80)
    print("CAMPAIGN SUMMARY")
    print("=" * 80)

    for category, tests in test_results.items():
        passed = sum(1 for _, p in tests if p)
        total = len(tests)
        pct = (passed / total * 100) if total > 0 else 0

        print(f"\n{category.upper()}: {passed}/{total} ({pct:.0f}%)")
        for name, p in tests:
            status = "[OK]" if p else "[FAIL]"
            print(f"  {status} {name}")

    # Overall
    all_tests = [t for tests in test_results.values() for t in tests]
    total_passed = sum(1 for _, p in all_tests if p)
    total_tests = len(all_tests)
    overall_pct = (total_passed / total_tests * 100) if total_tests > 0 else 0

    print(f"\n{'=' * 80}")
    print(f"OVERALL: {total_passed}/{total_tests} ({overall_pct:.0f}%)")
    print("=" * 80)

    if overall_pct >= 80:
        print("\n[SUCCESS] AI has comprehensive odibi knowledge!")
        print(f"  Resources cover {overall_pct:.0f}% of tested capabilities")
        print("  AI can use advanced features effectively")
    elif overall_pct >= 60:
        print("\n[PARTIAL] AI has basic knowledge but missing advanced features")
        print(f"  {100 - overall_pct:.0f}% of capabilities need better documentation")
    else:
        print("\n[INSUFFICIENT] Resources need significant improvement")
        print(f"  Only {overall_pct:.0f}% of capabilities covered")

    return overall_pct >= 80


if __name__ == "__main__":
    success = run_campaign()
    exit(0 if success else 1)
