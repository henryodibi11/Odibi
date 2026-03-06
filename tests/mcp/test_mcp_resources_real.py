"""Test AI with ACTUAL MCP resource loading (not simulated).

This uses the real MCP server resource mechanism to test AI agent knowledge.
"""

from dotenv import load_dotenv
from pathlib import Path
import asyncio

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp import server


async def load_real_mcp_resources():
    """Load resources exactly as MCP server would provide them."""
    resources = await server.list_resources()

    resource_content = {}

    for resource in resources:
        # Read using MCP's read_resource (returns string directly)
        content = await server.read_resource(resource.uri)
        # MCP returns the actual content string
        resource_content[resource.name] = content if isinstance(content, str) else str(content)

    return resource_content


def test_with_real_resources():
    """Test AI knowledge with actual MCP resources."""

    print("=" * 80)
    print("TESTING WITH REAL MCP RESOURCE LOADING")
    print("=" * 80)

    # Load resources via real MCP mechanism
    print("\n[LOADING] Using actual MCP server.read_resource()...")
    resources = asyncio.run(load_real_mcp_resources())

    print(f"  Loaded {len(resources)} resources\n")

    # Verify resources loaded fully
    for name in list(resources.keys())[:5]:
        content_len = (
            len(resources[name]) if isinstance(resources[name], str) else len(str(resources[name]))
        )
        print(f"    - {name}: {content_len} chars")

    # Build comprehensive context from priority resources
    priority_names = [
        "⚡ QUICK REFERENCE (READ FIRST)",
        "⚡ AI INSTRUCTIONS",
        "Complete Capabilities Reference",
    ]

    context_parts = []
    total_chars = 0

    for name in priority_names:
        if name in resources:
            content = resources[name]
            context_parts.append(f"=== {name} ===\n{content}\n")
            total_chars += len(content)
            print(f"  + {name}: {len(content)} chars")

    full_context = "\n\n".join(context_parts)
    print(f"\nTotal context: {total_chars} characters")

    # Test critical knowledge
    test_cases = [
        {
            "question": "Can odibi with Spark engine query database views?",
            "must_contain": ["yes", "view"],
            "must_not_contain": ["cannot", "not supported"],
            "category": "Engine Features",
        },
        {
            "question": "What happens if I use mode: upsert without keys in options?",
            "must_contain": ["fail", "error", "required"],
            "must_not_contain": ["works", "fine"],
            "category": "Write Modes",
        },
        {
            "question": "Can I name a node 'my-node' with a hyphen?",
            "must_contain": ["no", "underscore", "my_node"],
            "must_not_contain": ["yes", "fine"],
            "category": "Naming Rules",
        },
        {
            "question": "In Spark, how do I reference the output from a previous node?",
            "must_contain": ["temp view", "spark.sql", "node_name"],
            "must_not_contain": [],
            "category": "Spark Temp Views",
        },
        {
            "question": "What write mode should I use for Bronze layer ingestion?",
            "must_contain": ["append_once", "idempotent"],
            "must_not_contain": [],
            "category": "Bronze Best Practice",
        },
        {
            "question": "What does unknown_member: true do?",
            "must_contain": ["sk=0", "orphan"],
            "must_not_contain": [],
            "category": "Unknown Member",
        },
        {
            "question": "Can I use date variables like ${date:-7d} in YAML?",
            "must_contain": ["yes", "${date"],
            "must_not_contain": ["cannot", "not supported"],
            "category": "Variables",
        },
        {
            "question": "What's the difference between contracts and validation?",
            "must_contain": ["pre-transform", "post-transform"],
            "must_not_contain": [],
            "category": "Validation Types",
        },
    ]

    client = OpenAI()
    results = []

    for i, test in enumerate(test_cases, 1):
        print(f"\n{'=' * 80}")
        print(f"TEST {i}/{len(test_cases)}: {test['category']}")
        print("=" * 80)
        print(f"Question: {test['question']}")

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": f"""You are an odibi expert. Use this knowledge to answer:

{full_context}

Answer concisely (2-3 sentences max).""",
                },
                {"role": "user", "content": test["question"]},
            ],
            temperature=0.1,
            max_tokens=200,
        )

        answer = response.choices[0].message.content
        answer_lower = answer.lower()

        print(f"\nAnswer: {answer}")

        # Validate
        passed = True

        for keyword in test["must_contain"]:
            if keyword.lower() not in answer_lower:
                print(f"  [MISS] Expected '{keyword}'")
                passed = False

        for keyword in test["must_not_contain"]:
            if keyword.lower() in answer_lower:
                print(f"  [BAD] Should not contain '{keyword}'")
                passed = False

        if passed:
            print("  [OK] PASSED")
        else:
            print("  [FAIL] FAILED")

        results.append((test["category"], passed))

    # Summary
    print("\n" + "=" * 80)
    print("REAL MCP RESOURCE TEST RESULTS")
    print("=" * 80)

    passed_count = sum(1 for _, p in results if p)
    total = len(results)
    percentage = (passed_count / total * 100) if total > 0 else 0

    print(f"\nPassed: {passed_count}/{total} ({percentage:.0f}%)\n")

    for category, passed in results:
        status = "[OK]" if passed else "[FAIL]"
        print(f"  {status} {category}")

    print("\n" + "=" * 80)

    if percentage >= 80:
        print("[SUCCESS] AI has sufficient odibi knowledge!")
        print(f"  {percentage:.0f}% of critical features covered")
        print("  Ready for production use")
    elif percentage >= 60:
        print("[GOOD] AI has good knowledge, minor gaps remain")
        print(f"  {percentage:.0f}% coverage - acceptable for launch")
    else:
        print("[NEEDS WORK] Knowledge gaps exist")
        print(f"  Only {percentage:.0f}% coverage")
        print("  Review failed tests and enhance resources")

    print("=" * 80)

    return percentage >= 60


if __name__ == "__main__":
    success = test_with_real_resources()
    exit(0 if success else 1)
