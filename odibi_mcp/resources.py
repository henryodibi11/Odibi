"""MCP Resources - Auto-attached documentation for AI agents.

Resources are automatically loaded into AI context, so agents always have
access to odibi knowledge without you writing long prompts.
"""

from pathlib import Path


def get_resources():
    """Return list of MCP resources (documentation for AI)."""

    docs_dir = Path(__file__).parent.parent / "docs"

    resources = []

    # QUICK REFERENCE - Ultra concise, load first
    quick_ref = docs_dir / "AI_QUICK_REFERENCE.md"
    if quick_ref.exists():
        resources.append(
            {
                "uri": "odibi://docs/quick-reference",
                "name": "⚡ QUICK REFERENCE (READ FIRST)",
                "description": "Ultra-concise. Views=YES, upsert needs keys, node names alphanumeric_underscore, incremental modes, validation, Delta features. 1-min read.",
                "mimeType": "text/markdown",
                "path": quick_ref,
            }
        )

    # CRITICAL INSTRUCTIONS - Tool usage
    ai_instructions = docs_dir / "AI_INSTRUCTIONS.md"
    if ai_instructions.exists():
        resources.append(
            {
                "uri": "odibi://docs/ai-instructions",
                "name": "⚡ AI INSTRUCTIONS",
                "description": "How to use MCP tools. Be proactive, use defaults, generate YAML immediately.",
                "mimeType": "text/markdown",
                "path": ai_instructions,
            }
        )

    # CRITICAL CONTEXT - Field names and rules
    critical_context = docs_dir / "AI_CRITICAL_CONTEXT.md"
    if critical_context.exists():
        resources.append(
            {
                "uri": "odibi://docs/critical-context",
                "name": "⚡ Critical Field Names & Rules",
                "description": "Never use source:/sink:. Always use read:/write:. Write mode requirements.",
                "mimeType": "text/markdown",
                "path": critical_context,
            }
        )

    # Complete capabilities reference
    complete_ref = docs_dir / "AI_COMPLETE_REFERENCE.md"
    if complete_ref.exists():
        resources.append(
            {
                "uri": "odibi://docs/complete-reference",
                "name": "Complete Capabilities Reference",
                "description": "ALL odibi features: engine-specific, advanced patterns, write modes, incremental, validation, Delta Lake, non-obvious gotchas.",
                "mimeType": "text/markdown",
                "path": complete_ref,
            }
        )

    # Core framework documentation (deep dive)
    resources.append(
        {
            "uri": "odibi://docs/deep-context",
            "name": "Odibi Deep Context (2,200 lines)",
            "description": "Complete framework documentation. Use for deep dives when Complete Reference isn't enough.",
            "mimeType": "text/markdown",
            "path": docs_dir / "ODIBI_DEEP_CONTEXT.md",
        }
    )

    # Quick reference
    resources.append(
        {
            "uri": "odibi://docs/cheatsheet",
            "name": "Odibi Cheat Sheet",
            "description": "Quick reference for common tasks, patterns, and CLI commands.",
            "mimeType": "text/markdown",
            "path": docs_dir / "reference" / "cheatsheet.md",
        }
    )

    # Pattern guides
    pattern_docs = [
        ("dimension", "Dimension Pattern Guide"),
        ("fact", "Fact Pattern Guide"),
        ("scd2", "SCD2 Pattern Guide"),
        ("merge", "Merge Pattern Guide"),
        ("aggregation", "Aggregation Pattern Guide"),
    ]

    for pattern, title in pattern_docs:
        pattern_file = docs_dir / "patterns" / f"{pattern}.md"
        if pattern_file.exists():
            resources.append(
                {
                    "uri": f"odibi://docs/patterns/{pattern}",
                    "name": title,
                    "description": f"How to use the {pattern} pattern with examples.",
                    "mimeType": "text/markdown",
                    "path": pattern_file,
                }
            )

    # MCP setup guide
    resources.append(
        {
            "uri": "odibi://docs/mcp-setup",
            "name": "MCP Tools Usage Guide",
            "description": "How to use MCP tools to build pipelines. Includes workflows and examples.",
            "mimeType": "text/markdown",
            "path": Path(__file__).parent.parent / "MCP_SETUP_GUIDE.md",
        }
    )

    # Agent guides
    agent_guide = Path(__file__).parent.parent / "AGENTS.md"
    if agent_guide.exists():
        resources.append(
            {
                "uri": "odibi://docs/agent-guide",
                "name": "AI Agent Guide (Framework)",
                "description": "General instructions for AI assistants working with odibi codebase.",
                "mimeType": "text/markdown",
                "path": agent_guide,
            }
        )

    # AI-specific MCP usage guide
    ai_mcp_guide = docs_dir / "guides" / "ai-agent-guide.md"
    if ai_mcp_guide.exists():
        resources.append(
            {
                "uri": "odibi://docs/ai-mcp-guide",
                "name": "AI Agent MCP Usage Guide",
                "description": "How AI agents should use MCP tools effectively. READ THIS FIRST for tool usage patterns!",
                "mimeType": "text/markdown",
                "path": ai_mcp_guide,
            }
        )

    return resources


def read_resource(uri: str) -> str:
    """Read a resource by URI."""
    resources = get_resources()

    for resource in resources:
        if resource["uri"] == uri:
            path = resource["path"]
            if path.exists():
                return path.read_text(encoding="utf-8")
            else:
                return f"Resource not found: {path}"

    return f"Unknown resource: {uri}"
