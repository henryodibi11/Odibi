"""
Explanation Templates
=====================

Helper functions for generating common explanation patterns.
"""

from typing import List, Dict


def purpose_detail_result(purpose: str, details: List[str], result: str) -> str:
    """
    Generate standard Purpose/Details/Result explanation.

    Args:
        purpose: One-line purpose statement
        details: List of detail bullet points
        result: One-line result statement

    Returns:
        Formatted markdown explanation
    """
    details_text = "\n".join(f"- {detail}" for detail in details)

    return f"""**Purpose:**
{purpose}

**Details:**
{details_text}

**Result:**
{result}
"""


def with_formula(purpose: str, details: List[str], formula: str, result: str) -> str:
    """
    Generate explanation with formula.

    Useful for calculations and transformations with mathematical operations.

    Args:
        purpose: Purpose statement
        details: Detail bullet points
        formula: Formula or calculation
        result: Result statement

    Returns:
        Markdown with formula section
    """
    details_text = "\n".join(f"- {detail}" for detail in details)

    return f"""**Purpose:**
{purpose}

**Details:**
{details_text}

**Formula:**
```
{formula}
```

**Result:**
{result}
"""


def table_explanation(
    purpose: str,
    details: List[str],
    table_data: List[Dict[str, str]],
    result: str,
) -> str:
    """
    Generate explanation with embedded table.

    Args:
        purpose: Purpose statement
        details: Detail bullet points
        table_data: List of dicts with column headers as keys
        result: Result statement

    Returns:
        Markdown with table
    """
    if not table_data:
        return purpose_detail_result(purpose, details, result)

    # Generate markdown table
    headers = list(table_data[0].keys())
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

    data_rows = []
    for row in table_data:
        values = [str(row.get(h, "")) for h in headers]
        data_rows.append("| " + " | ".join(values) + " |")

    table = "\n".join([header_row, separator_row] + data_rows)

    details_text = "\n".join(f"- {detail}" for detail in details)

    return f"""**Purpose:**
{purpose}

**Details:**
{details_text}

{table}

**Result:**
{result}
"""


def simple_explanation(purpose: str, result: str) -> str:
    """
    Generate simple one-liner explanation.

    Args:
        purpose: What the transformation does
        result: What it produces

    Returns:
        Simple markdown explanation
    """
    return f"""**{purpose}**

→ {result}
"""
