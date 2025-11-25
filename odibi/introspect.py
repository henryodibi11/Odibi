"""Introspection tool for generating Configuration Manual."""

import inspect
import importlib
import sys
from pathlib import Path
from typing import List, Optional, Any, Dict, Type

from pydantic import BaseModel


# --- Data Models ---


class FieldDoc(BaseModel):
    name: str
    type_hint: str
    required: bool
    default: Optional[str] = None
    description: Optional[str] = None


class ModelDoc(BaseModel):
    name: str
    module: str
    summary: Optional[str]
    docstring: Optional[str]
    fields: List[FieldDoc]
    group: str  # "Core", "Connection", "Transformation", "Setting"


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

    # Handle Optional[T] -> T | None
    s = str(annotation).replace("typing.", "")
    s = s.replace("odibi.config.", "")
    s = s.replace("odibi.enums.", "")
    return s


def get_pydantic_fields(cls: Type[BaseModel]) -> List[FieldDoc]:
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
                if field.default is not None:
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
    # Pydantic V1 fallback
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


def scan_module_for_models(module_name: str, group_map: Dict[str, str]) -> List[ModelDoc]:
    """Scan a module for Pydantic models and assign groups."""
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        print(f"Warning: Could not import {module_name}: {e}", file=sys.stderr)
        return []

    models = []
    for name, obj in inspect.getmembers(module):
        # Only classes defined in this module (or re-exported explicitly if we wanted, but here we stick to source)
        if not inspect.isclass(obj):
            continue

        if hasattr(obj, "__module__") and obj.__module__ != module_name:
            continue

        # Check if Pydantic Model
        if issubclass(obj, BaseModel) and obj is not BaseModel:
            # Determine Group
            group = "Other"
            if name in group_map:
                group = group_map[name]
            elif module_name.startswith("odibi.transformers"):
                group = "Transformation"

            fields = get_pydantic_fields(obj)

            models.append(
                ModelDoc(
                    name=name,
                    module=module_name,
                    summary=get_summary(obj),
                    docstring=get_docstring(obj),
                    fields=fields,
                    group=group,
                )
            )

    return models


# --- Configuration ---

CONFIG_MODULES = [
    "odibi.config",
    "odibi.transformers.sql_core",
    "odibi.transformers.relational",
    "odibi.transformers.advanced",
    "odibi.transformers.merge_transformer",
]

# Explicit grouping for odibi.config classes
GROUP_MAPPING = {
    "ProjectConfig": "Core",
    "PipelineConfig": "Core",
    "NodeConfig": "Core",
    "ReadConfig": "Operation",
    "WriteConfig": "Operation",
    "TransformConfig": "Operation",
    "ValidationConfig": "Operation",
    "LocalConnectionConfig": "Connection",
    "AzureBlobConnectionConfig": "Connection",
    "DeltaConnectionConfig": "Connection",
    "SQLServerConnectionConfig": "Connection",
    "HttpConnectionConfig": "Connection",
    "StoryConfig": "Setting",
    "RetryConfig": "Setting",
    "LoggingConfig": "Setting",
    "PerformanceConfig": "Setting",
    "AlertConfig": "Setting",
}


def generate_docs(output_path: str = "docs/api.md"):
    """Run introspection and save to file."""
    print("Scanning configuration models...")

    all_models = []
    for mod in CONFIG_MODULES:
        all_models.extend(scan_module_for_models(mod, GROUP_MAPPING))

    # Organize by group
    grouped = {
        "Core": [],
        "Connection": [],
        "Operation": [],
        "Setting": [],
        "Transformation": [],
        "Other": [],
    }

    for m in all_models:
        if m.group in grouped:
            grouped[m.group].append(m)
        else:
            grouped["Other"].append(m)

    # Render
    lines = [
        "# Odibi Configuration Reference",
        "",
        "This manual details the YAML configuration schema for Odibi projects.",
        "*Auto-generated from Pydantic models.*",
        "",
    ]

    # Define Section Order
    sections = [
        ("Core", "Project Structure"),
        ("Connection", "Connections"),
        ("Operation", "Node Operations"),
        ("Setting", "Global Settings"),
        ("Transformation", "Transformation Reference"),
    ]

    for group_key, title in sections:
        models = grouped[group_key]
        if not models:
            continue

        lines.append(f"## {title}")
        lines.append("")

        # Sort models by name
        models.sort(key=lambda x: x.name)

        for model in models:
            lines.append(f"### `{model.name}`")

            if model.docstring:
                lines.append(f"{model.docstring}\n")

            if model.fields:
                lines.append("| Field | Type | Required | Default | Description |")
                lines.append("| --- | --- | --- | --- | --- |")
                for field in model.fields:
                    req = "Yes" if field.required else "No"
                    default = f"`{field.default}`" if field.default else "-"
                    desc = field.description or "-"
                    desc = desc.replace("|", "\\|").replace("\n", " ")

                    # Clean type hint for display
                    th = field.type_hint.replace("typing.", "").replace("odibi.config.", "")

                    lines.append(f"| **{field.name}** | `{th}` | {req} | {default} | {desc} |")
                lines.append("")

            lines.append("---\n")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Configuration Manual saved to {output_path}")
