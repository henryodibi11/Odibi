"""Diagram rendering tools.

Render Mermaid diagrams for visualizing architecture, flows, etc.
"""

import base64
import re
from dataclasses import dataclass
from typing import Optional



@dataclass
class DiagramResult:
    """Result of diagram rendering."""

    success: bool
    code: str
    image_url: Optional[str] = None
    svg: Optional[str] = None
    error: Optional[str] = None


def render_mermaid(
    code: str,
    output_format: str = "svg",
    theme: str = "dark",
) -> DiagramResult:
    """Render a Mermaid diagram.

    Uses mermaid.ink API for rendering.

    Args:
        code: Mermaid diagram code.
        output_format: Output format ("svg" or "png").
        theme: Theme ("dark", "default", "forest", "neutral").

    Returns:
        DiagramResult with rendered diagram.
    """
    try:
        code = code.strip()
        if not code:
            return DiagramResult(
                success=False,
                code=code,
                error="Empty diagram code",
            )

        diagram_types = [
            "graph",
            "flowchart",
            "sequenceDiagram",
            "classDiagram",
            "stateDiagram",
            "erDiagram",
            "gantt",
            "pie",
            "gitGraph",
            "journey",
            "mindmap",
            "timeline",
        ]

        has_valid_type = any(
            code.strip().startswith(dt) for dt in diagram_types
        )
        if not has_valid_type:
            return DiagramResult(
                success=False,
                code=code,
                error=f"Invalid diagram type. Must start with one of: {', '.join(diagram_types[:5])}...",
            )

        encoded = base64.urlsafe_b64encode(code.encode()).decode()
        base_url = "https://mermaid.ink"

        if output_format == "svg":
            url = f"{base_url}/svg/{encoded}?theme={theme}"
        else:
            url = f"{base_url}/img/{encoded}?theme={theme}"

        return DiagramResult(
            success=True,
            code=code,
            image_url=url,
        )

    except Exception as e:
        return DiagramResult(
            success=False,
            code=code,
            error=str(e),
        )


def render_mermaid_local(code: str) -> DiagramResult:
    """Render Mermaid diagram locally using mmdc CLI if available.

    Args:
        code: Mermaid diagram code.

    Returns:
        DiagramResult with SVG content.
    """
    import subprocess
    import tempfile
    from pathlib import Path

    try:
        result = subprocess.run(
            ["mmdc", "--version"],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0:
            return render_mermaid(code)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return render_mermaid(code)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "diagram.mmd"
            output_file = Path(tmpdir) / "diagram.svg"

            input_file.write_text(code)

            result = subprocess.run(
                [
                    "mmdc",
                    "-i", str(input_file),
                    "-o", str(output_file),
                    "-t", "dark",
                    "-b", "transparent",
                ],
                capture_output=True,
                timeout=30,
            )

            if result.returncode != 0:
                error = result.stderr.decode() if result.stderr else "Unknown error"
                return DiagramResult(
                    success=False,
                    code=code,
                    error=f"mmdc failed: {error}",
                )

            svg_content = output_file.read_text()

            return DiagramResult(
                success=True,
                code=code,
                svg=svg_content,
            )

    except Exception:
        return render_mermaid(code)


def format_diagram(result: DiagramResult) -> str:
    """Format diagram result for display in chat.

    Args:
        result: DiagramResult to format.

    Returns:
        Markdown with embedded diagram.
    """
    if not result.success:
        return f"""❌ **Diagram Error:** {result.error}

```mermaid
{result.code}
```
"""

    if result.svg:
        return f"""📊 **Diagram:**

{result.svg}

<details>
<summary>View code</summary>

```mermaid
{result.code}
```
</details>
"""

    if result.image_url:
        return f"""📊 **Diagram:**

![Diagram]({result.image_url})

<details>
<summary>View code</summary>

```mermaid
{result.code}
```
</details>
"""

    return f"""```mermaid
{result.code}
```
"""


def extract_mermaid_blocks(text: str) -> list[str]:
    """Extract Mermaid code blocks from text.

    Args:
        text: Text containing mermaid code blocks.

    Returns:
        List of Mermaid code strings.
    """
    pattern = r"```mermaid\s*(.*?)\s*```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
    return matches


def create_flowchart(
    title: str,
    nodes: list[dict],
    edges: list[dict],
    direction: str = "TD",
) -> str:
    """Helper to create a flowchart programmatically.

    Args:
        title: Chart title (used as comment).
        nodes: List of {"id": str, "label": str, "shape": "rect"|"round"|"diamond"}.
        edges: List of {"from": str, "to": str, "label": str}.
        direction: "TD" (top-down), "LR" (left-right), etc.

    Returns:
        Mermaid flowchart code.
    """
    lines = [f"flowchart {direction}"]

    shape_map = {
        "rect": ("[", "]"),
        "round": ("(", ")"),
        "diamond": ("{", "}"),
        "circle": ("((", "))"),
    }

    for node in nodes:
        node_id = node["id"]
        label = node.get("label", node_id)
        shape = node.get("shape", "rect")
        left, right = shape_map.get(shape, ("[", "]"))
        lines.append(f"    {node_id}{left}{label}{right}")

    for edge in edges:
        from_id = edge["from"]
        to_id = edge["to"]
        label = edge.get("label", "")

        if label:
            lines.append(f"    {from_id} -->|{label}| {to_id}")
        else:
            lines.append(f"    {from_id} --> {to_id}")

    return "\n".join(lines)


def create_sequence_diagram(
    title: str,
    participants: list[str],
    messages: list[dict],
) -> str:
    """Helper to create a sequence diagram programmatically.

    Args:
        title: Diagram title.
        participants: List of participant names.
        messages: List of {"from": str, "to": str, "message": str, "type": "sync"|"async"}.

    Returns:
        Mermaid sequence diagram code.
    """
    lines = ["sequenceDiagram"]

    for p in participants:
        lines.append(f"    participant {p}")

    for msg in messages:
        from_p = msg["from"]
        to_p = msg["to"]
        message = msg.get("message", "")
        msg_type = msg.get("type", "sync")

        if msg_type == "async":
            lines.append(f"    {from_p}->>+{to_p}: {message}")
        else:
            lines.append(f"    {from_p}->>{to_p}: {message}")

    return "\n".join(lines)
