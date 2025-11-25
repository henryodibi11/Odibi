"""Introspection tool for generating API documentation."""

import inspect
import pkgutil
import importlib
import sys
from pathlib import Path
from typing import List, Optional, Any

from pydantic import BaseModel, Field


# --- Data Models ---


class ParameterDoc(BaseModel):
    name: str
    type_hint: str
    default: Optional[str] = None


class FieldDoc(BaseModel):
    name: str
    type_hint: str
    required: bool
    default: Optional[str] = None
    description: Optional[str] = None


class FunctionDoc(BaseModel):
    name: str
    signature: str
    summary: Optional[str]
    parameters: List[ParameterDoc]
    source_line: int


class ClassDoc(BaseModel):
    name: str
    summary: Optional[str]
    docstring: Optional[str]
    methods: List[FunctionDoc]
    fields: List[FieldDoc] = Field(default_factory=list)
    source_line: int


class ModuleDoc(BaseModel):
    name: str
    path: str
    summary: Optional[str]
    classes: List[ClassDoc]
    functions: List[FunctionDoc]
    submodules: List["ModuleDoc"] = Field(default_factory=list)


# --- Extraction Logic ---


def get_docstring(obj: Any) -> Optional[str]:
    """Extract the full docstring."""
    return inspect.getdoc(obj)


def get_summary(obj: Any) -> Optional[str]:
    """Extract the first line of the docstring."""
    doc = get_docstring(obj)
    if not doc:
        return None
    return doc.split("\n")[0].strip()


def format_type_hint(annotation: Any) -> str:
    """Format a type hint as a string."""
    if annotation is inspect.Parameter.empty:
        return "Any"
    if isinstance(annotation, str):
        return annotation
    if hasattr(annotation, "__name__"):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def get_parameters(func: Any) -> List[ParameterDoc]:
    """Extract parameters from a function."""
    try:
        sig = inspect.signature(func)
    except ValueError:
        return []

    params = []
    for name, param in sig.parameters.items():
        if name == "self":
            continue

        default = None
        if param.default is not inspect.Parameter.empty:
            default = str(param.default)

        params.append(
            ParameterDoc(
                name=name,
                type_hint=format_type_hint(param.annotation),
                default=default,
            )
        )
    return params


def get_pydantic_fields(cls: Any) -> List[FieldDoc]:
    """Extract fields from a Pydantic model."""
    fields = []

    # Pydantic V2
    if hasattr(cls, "model_fields"):
        for name, field in cls.model_fields.items():
            # Get type annotation
            type_hint = "Any"
            if field.annotation is not None:
                type_hint = format_type_hint(field.annotation)

            # Check required/default
            required = field.is_required()
            default = None
            if not required:
                # Try to get default value representation
                if field.default is not None:  # Pydantic V2 specific checks might be needed
                    default = str(field.default)

            fields.append(
                FieldDoc(
                    name=name,
                    type_hint=type_hint,
                    required=required,
                    default=default,
                    description=field.description,
                )
            )
    # Pydantic V1 fallback (just in case)
    elif hasattr(cls, "__fields__"):
        for name, field in cls.__fields__.items():
            type_hint = format_type_hint(field.outer_type_)
            required = field.required
            default = str(field.default) if not required else None
            fields.append(
                FieldDoc(
                    name=name,
                    type_hint=type_hint,
                    required=required,
                    default=default,
                    description=field.field_info.description,
                )
            )

    return fields


def scan_module(module_name: str) -> Optional[ModuleDoc]:
    """Introspect a single module."""
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        print(f"Warning: Could not import {module_name}: {e}", file=sys.stderr)
        return None

    # Skip if not part of the package (e.g. external libs)
    if not hasattr(module, "__file__") or module.__file__ is None:
        return None

    classes = []
    functions = []

    for name, obj in inspect.getmembers(module):
        if name.startswith("_"):
            continue

        # Only include objects defined in this module
        if hasattr(obj, "__module__") and obj.__module__ != module_name:
            continue

        if inspect.isclass(obj):
            methods = []
            for method_name, method in inspect.getmembers(obj):
                if method_name.startswith("_") and method_name != "__init__":
                    continue
                if inspect.isfunction(method) or inspect.ismethod(method):
                    # Check if method is defined in this class (not inherited)
                    # Note: This is a strict check, might exclude overrides if not careful.
                    # But for "simple", checking __qualname__ is usually good.
                    if method.__qualname__.startswith(obj.__name__ + "."):
                        try:
                            sig = str(inspect.signature(method))
                        except ValueError:
                            sig = "(...)"

                        try:
                            source_line = inspect.getsourcelines(method)[1]
                        except (OSError, TypeError):
                            source_line = 0

                        methods.append(
                            FunctionDoc(
                                name=method_name,
                                signature=sig,
                                summary=get_summary(method),
                                parameters=get_parameters(method),
                                source_line=source_line,
                            )
                        )

            # Sort methods by source line
            methods.sort(key=lambda x: x.source_line)

            # Extract Pydantic fields if applicable
            fields = []
            if hasattr(obj, "model_fields") or hasattr(obj, "__fields__"):
                fields = get_pydantic_fields(obj)

            try:
                source_line = inspect.getsourcelines(obj)[1]
            except (OSError, TypeError):
                source_line = 0

            classes.append(
                ClassDoc(
                    name=name,
                    summary=get_summary(obj),
                    docstring=get_docstring(obj),
                    methods=methods,
                    fields=fields,
                    source_line=source_line,
                )
            )

        elif inspect.isfunction(obj):
            try:
                sig = str(inspect.signature(obj))
            except ValueError:
                sig = "(...)"

            try:
                source_line = inspect.getsourcelines(obj)[1]
            except (OSError, TypeError):
                source_line = 0

            functions.append(
                FunctionDoc(
                    name=name,
                    signature=sig,
                    summary=get_summary(obj),
                    parameters=get_parameters(obj),
                    source_line=source_line,
                )
            )

    # Sort by source line
    classes.sort(key=lambda x: x.source_line)
    functions.sort(key=lambda x: x.source_line)

    return ModuleDoc(
        name=module_name,
        path=str(module.__file__),
        summary=get_summary(module),
        classes=classes,
        functions=functions,
    )


def scan_package(package_name: str) -> List[ModuleDoc]:
    """Recursively scan a package."""
    package = importlib.import_module(package_name)
    if not hasattr(package, "__path__"):
        return []

    docs = []

    # Scan top-level package
    pkg_doc = scan_module(package_name)
    if pkg_doc:
        docs.append(pkg_doc)

    # Walk subpackages
    for _, name, is_pkg in pkgutil.walk_packages(package.__path__, package_name + "."):
        doc = scan_module(name)
        if doc:
            docs.append(doc)

    return docs


# --- Rendering ---


def render_markdown(modules: List[ModuleDoc]) -> str:
    """Render documentation to Markdown."""
    lines = ["# Odibi API Reference", "", "*Auto-generated by `odibi.introspect`*", ""]

    # Sort modules alphabetically
    modules.sort(key=lambda x: x.name)

    for module in modules:
        # Skip empty modules
        if not module.classes and not module.functions:
            continue

        lines.append(f"## Module `{module.name}`")
        if module.summary:
            lines.append(f"{module.summary}\n")

        if module.functions:
            lines.append("### Functions")
            for func in module.functions:
                lines.append(f"#### `{func.name}`")
                lines.append(f"```python\ndef {func.name}{func.signature}\n```")
                if func.summary:
                    lines.append(f"> {func.summary}\n")
                lines.append("")

        if module.classes:
            lines.append("### Classes")
            for cls in module.classes:
                lines.append(f"#### `class {cls.name}`")

                # Render full docstring if available (includes examples)
                if cls.docstring:
                    lines.append(f"{cls.docstring}\n")
                elif cls.summary:
                    lines.append(f"{cls.summary}\n")

                # Render Fields Table for Pydantic Models
                if cls.fields:
                    lines.append("| Field | Type | Required | Default | Description |")
                    lines.append("| --- | --- | --- | --- | --- |")
                    for field in cls.fields:
                        req = "Yes" if field.required else "No"
                        default = f"`{field.default}`" if field.default else "-"
                        desc = field.description or "-"
                        # Escape pipes in description
                        desc = desc.replace("|", "\\|").replace("\n", " ")
                        lines.append(
                            f"| **{field.name}** | `{field.type_hint}` | {req} | {default} | {desc} |"
                        )
                    lines.append("")

                for method in cls.methods:
                    lines.append(f"- **{method.name}**`{method.signature}`")
                    if method.summary:
                        lines.append(f"  - {method.summary}")
                lines.append("")

        lines.append("---\n")

    return "\n".join(lines)


def generate_docs(output_path: str = "docs/api.md"):
    """Run introspection and save to file."""
    print("Scanning odibi package...")
    modules = scan_package("odibi")

    print(f"Rendering documentation for {len(modules)} modules...")
    markdown = render_markdown(modules)

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    print(f"Documentation saved to {output_path}")
