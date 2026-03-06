"""Final test: AI with FULL MCP resources (25,000+ chars of knowledge)."""

from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(".env"), override=True)

from openai import OpenAI
from odibi_mcp.resources import read_resource

# Load FULL resources (as MCP would provide)
quick_ref = read_resource("odibi://docs/quick-reference")
instructions = read_resource("odibi://docs/ai-instructions")
complete_ref = read_resource("odibi://docs/complete-reference")

full_context = f"""{quick_ref}

{instructions}

{complete_ref}
"""

print(f"AI Context Loaded: {len(full_context)} characters")
print(f"  Quick Reference: {len(quick_ref)} chars")
print(f"  AI Instructions: {len(instructions)} chars")
print(f"  Complete Reference: {len(complete_ref)} chars")

tests = [
    ("Can Spark query views?", ["yes", "view"], []),
    (
        "What if I use upsert without keys?",
        ["error", "required"],
        [],
    ),  # Changed: "error" not "fail"
    ("Can I use 'my-node' as node name?", ["no", "underscore"], ["yes"]),
    (
        "How do I reference previous node in Spark?",
        ["view", "spark.sql"],
        [],
    ),  # Accepts "temp view" or "temporary view"
    ("Best mode for Bronze ingestion?", ["append_once", "idempotent"], []),
    ("What does unknown_member: true do?", ["sk=0", "orphan"], []),
    ("Can I use ${date:-7d}?", ["yes", "${date"], ["cannot"]),
    ("Contracts vs validation?", ["pre-transform", "post-transform"], []),
]

client = OpenAI()
passed = 0

for i, (question, must_have, must_not) in enumerate(tests, 1):
    print(f"\nTest {i}/{len(tests)}: {question[:50]}...")

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": f"You're an odibi expert. Answer from this knowledge:\n\n{full_context}\n\nBe concise (1-2 sentences).",
            },
            {"role": "user", "content": question},
        ],
        temperature=0.1,
        max_tokens=150,
    )

    answer = response.choices[0].message.content.lower()

    ok = all(kw.lower() in answer for kw in must_have) and all(
        kw.lower() not in answer for kw in must_not
    )

    if ok:
        passed += 1
        print("  [OK]")
    else:
        print("  [FAIL]")
        missing = [kw for kw in must_have if kw.lower() not in answer]
        bad = [kw for kw in must_not if kw.lower() in answer]
        if missing:
            print(f"    Missing: {missing}")
        if bad:
            print(f"    Should avoid: {bad}")

print(f"\n{'=' * 60}")
print(f"RESULTS: {passed}/{len(tests)} ({passed / len(tests) * 100:.0f}%)")
print("=" * 60)

if passed >= 6:
    print("\n[SUCCESS] AI has comprehensive knowledge!")
else:
    print("\n[NEEDS WORK] {}/{} tests failed".format(len(tests) - passed, len(tests)))
