"""Dynamic template generator from Pydantic models.

Single source of truth: Pydantic models + FunctionRegistry param models.
This module generates YAML templates, JSON schemas, and CLI output.
"""

from __future__ import annotations

import enum
import json
import re
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

try:
    from types import UnionType

    HAS_UNION_TYPE = True
except ImportError:
    UnionType = None
    HAS_UNION_TYPE = False

from pydantic import BaseModel
from pydantic.fields import FieldInfo


class TemplateGenerator:
    """Generate YAML templates from Pydantic models.

    Extracts:
    - Field descriptions from Field(description=...)
    - Constraints (ge/le) as "(min-max)"
    - Enum values as "option1 | option2 | option3"
    - Required vs optional fields
    - Class docstrings as section headers
    """

    def __init__(self):
        self._indent = "  "

    def _get_field_info(self, model: Type[BaseModel], field_name: str) -> FieldInfo:
        """Get FieldInfo for a field."""
        return model.model_fields[field_name]

    def _get_type_annotation(self, model: Type[BaseModel], field_name: str) -> Any:
        """Get raw type annotation for a field."""
        hints = getattr(model, "__annotations__", {})
        return hints.get(field_name)

    def _unwrap_annotated(self, type_hint: Any) -> Any:
        """Unwrap Annotated[T, ...] to get T."""
        origin = get_origin(type_hint)
        if origin is Annotated:
            args = get_args(type_hint)
            return args[0] if args else type_hint
        return type_hint

    def _is_optional(self, type_hint: Any) -> bool:
        """Check if type is Optional[T] (Union[T, None])."""
        origin = get_origin(type_hint)
        if origin is Union:
            args = get_args(type_hint)
            return type(None) in args
        if HAS_UNION_TYPE and isinstance(type_hint, UnionType):
            args = get_args(type_hint)
            return type(None) in args
        return False

    def _get_inner_type(self, type_hint: Any) -> Any:
        """Get inner type from Optional[T] or List[T]."""
        origin = get_origin(type_hint)
        if origin is Union:
            args = get_args(type_hint)
            non_none = [a for a in args if a is not type(None)]
            return non_none[0] if len(non_none) == 1 else type_hint
        if origin is list:
            args = get_args(type_hint)
            return args[0] if args else Any
        return type_hint

    def _format_constraint(self, field_info: FieldInfo) -> str:
        """Format ge/le constraints as (min-max)."""
        metadata = getattr(field_info, "metadata", []) or []

        ge_val = getattr(field_info, "ge", None)
        le_val = getattr(field_info, "le", None)
        gt_val = getattr(field_info, "gt", None)
        lt_val = getattr(field_info, "lt", None)

        for meta in metadata:
            if hasattr(meta, "ge"):
                ge_val = meta.ge
            if hasattr(meta, "le"):
                le_val = meta.le
            if hasattr(meta, "gt"):
                gt_val = meta.gt
            if hasattr(meta, "lt"):
                lt_val = meta.lt

        if ge_val is not None or gt_val is not None or le_val is not None or lt_val is not None:
            min_val = ge_val if ge_val is not None else gt_val
            max_val = le_val if le_val is not None else lt_val
            min_str = str(min_val) if min_val is not None else ""
            max_str = str(max_val) if max_val is not None else ""
            return f"({min_str}-{max_str})"
        return ""

    def _format_enum_values(self, type_hint: Any) -> str:
        """Format enum values as 'val1 | val2 | val3'."""
        inner_type = self._unwrap_annotated(type_hint)
        inner_type = self._get_inner_type(inner_type)

        if isinstance(inner_type, type) and issubclass(inner_type, enum.Enum):
            values = [e.value for e in inner_type]
            return " | ".join(str(v) for v in values)
        return ""

    def _get_default_value(self, field_info: FieldInfo, type_hint: Any) -> Any:
        """Get a sensible default value for YAML output."""
        from pydantic_core import PydanticUndefined

        default = field_info.default
        if default is not None and default is not ... and default is not PydanticUndefined:
            if isinstance(default, enum.Enum):
                return default.value
            if callable(default):
                return None
            return default

        if field_info.default_factory is not None:
            try:
                val = field_info.default_factory()
                if isinstance(val, BaseModel):
                    return None
                return val
            except Exception:
                return None

        return None

    def _format_yaml_value(self, value: Any, indent_level: int = 0) -> str:
        """Format a value for YAML output."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, str):
            if value and (value.startswith("$") or ":" in value or "{" in value):
                return f'"{value}"'
            return value if value else '""'
        if isinstance(value, list):
            if not value:
                return "[]"
            lines = []
            for item in value:
                lines.append(f"- {self._format_yaml_value(item)}")
            return "\n" + "\n".join(self._indent * (indent_level + 1) + line for line in lines)
        if isinstance(value, dict):
            if not value:
                return "{}"
            lines = []
            for k, v in value.items():
                lines.append(f"{k}: {self._format_yaml_value(v)}")
            return "\n" + "\n".join(self._indent * (indent_level + 1) + line for line in lines)
        if isinstance(value, enum.Enum):
            return value.value
        return str(value)

    def _is_list_of_unions(self, type_hint: Any) -> bool:
        """Check if type is List[Union[Model1, Model2, ...]]."""
        inner = self._unwrap_annotated(type_hint)
        origin = get_origin(inner)
        if origin is not list:
            return False
        args = get_args(inner)
        if not args:
            return False
        inner_type = self._unwrap_annotated(args[0])
        return self._is_discriminated_union(inner_type)

    def _get_list_union_models(self, type_hint: Any) -> List[Type[BaseModel]]:
        """Get models from List[Union[Model1, Model2, ...]]."""
        inner = self._unwrap_annotated(type_hint)
        args = get_args(inner)
        if not args:
            return []
        inner_type = self._unwrap_annotated(args[0])
        return self._get_nested_models(inner_type)

    def _generate_list_union_template(
        self,
        field_key: str,
        models: List[Type[BaseModel]],
        indent_level: int,
        show_comments: bool,
    ) -> str:
        """Generate YAML showing all variants for a List[Union[...]] field."""
        lines = []
        indent = self._indent * indent_level
        discriminator = self._get_discriminator_field(models)

        variant_names = []
        for m in models:
            if discriminator and discriminator in m.model_fields:
                field_info = m.model_fields[discriminator]
                default = self._get_default_value(field_info, None)
                if default:
                    variant_names.append(str(default))
                else:
                    variant_names.append(m.__name__)
            else:
                variant_names.append(m.__name__)

        lines.append(f"{indent}{field_key}:  # List of: {' | '.join(variant_names)}")

        for i, model in enumerate(models):
            variant_name = variant_names[i]
            lines.append(f"{indent}  # --- {variant_name} ---")

            field_strs = []
            for fname, finfo in model.model_fields.items():
                type_hint = self._get_type_annotation(model, fname)
                default = self._get_default_value(finfo, type_hint)
                yaml_val = self._format_yaml_value(default, indent_level + 1)
                if "\n" not in yaml_val:
                    field_strs.append(f"{fname}: {yaml_val}")

            lines.append(f"{indent}  #   {{ {', '.join(field_strs[:4])} }}")

        lines.append(f"{indent}  # --- Example entry: ---")
        lines.append(f"{indent}  # - type: {variant_names[0]}")
        first_model = models[0]
        for fname, finfo in first_model.model_fields.items():
            if fname == "type":
                continue
            if finfo.is_required():
                lines.append(f"{indent}  #   {fname}: <value>")

        return "\n".join(lines)

    def _is_nested_model(self, type_hint: Any) -> bool:
        """Check if type is a nested Pydantic model."""
        inner = self._unwrap_annotated(type_hint)
        inner = self._get_inner_type(inner)

        if isinstance(inner, type) and issubclass(inner, BaseModel):
            return True

        origin = get_origin(inner)
        if origin is Union:
            args = get_args(inner)
            non_none = [a for a in args if a is not type(None)]
            return any(isinstance(a, type) and issubclass(a, BaseModel) for a in non_none)
        return False

    def _get_nested_models(self, type_hint: Any) -> List[Type[BaseModel]]:
        """Get list of nested Pydantic models from a type hint."""
        inner = self._unwrap_annotated(type_hint)
        inner = self._get_inner_type(inner)

        if isinstance(inner, type) and issubclass(inner, BaseModel):
            return [inner]

        origin = get_origin(inner)
        if origin is Union:
            args = get_args(inner)
            non_none = [a for a in args if a is not type(None)]
            return [a for a in non_none if isinstance(a, type) and issubclass(a, BaseModel)]
        return []

    def _is_discriminated_union(self, type_hint: Any) -> bool:
        """Check if type is a discriminated union (multiple model variants)."""
        nested = self._get_nested_models(type_hint)
        return len(nested) > 1

    def _get_discriminator_field(self, models: List[Type[BaseModel]]) -> Optional[str]:
        """Find the discriminator field (usually 'mode' or 'type')."""
        if not models:
            return None
        first_model = models[0]
        for field_name in ["mode", "type", "kind", "variant"]:
            if field_name in first_model.model_fields:
                return field_name
        return None

    def _generate_union_variants(
        self,
        field_key: str,
        models: List[Type[BaseModel]],
        indent_level: int,
        show_comments: bool,
    ) -> str:
        """Generate YAML showing all variants of a discriminated union."""
        lines = []
        indent = self._indent * indent_level
        discriminator = self._get_discriminator_field(models)

        variant_names = []
        for m in models:
            if discriminator and discriminator in m.model_fields:
                field_info = m.model_fields[discriminator]
                default = self._get_default_value(field_info, None)
                if default:
                    variant_names.append(str(default))
                else:
                    variant_names.append(m.__name__)
            else:
                variant_names.append(m.__name__)

        lines.append(f"{indent}{field_key}:  # OPTIONS: {' | '.join(variant_names)}")

        for i, model in enumerate(models):
            variant_name = variant_names[i]
            lines.append(f"{indent}  # --- Option: {variant_name} ---")

            for fname, finfo in model.model_fields.items():
                type_hint = self._get_type_annotation(model, fname)
                default = self._get_default_value(finfo, type_hint)
                yaml_val = self._format_yaml_value(default, indent_level + 1)

                req_marker = "REQUIRED" if finfo.is_required() else "optional"
                desc = ""
                if show_comments and finfo.description:
                    desc = f" | {finfo.description[:40]}"

                if "\n" not in yaml_val:
                    lines.append(f"{indent}  # {fname}: {yaml_val}  # {req_marker}{desc}")

        lines.append(f"{indent}  # --- Your choice: ---")
        first_model = models[0]
        for fname, finfo in first_model.model_fields.items():
            type_hint = self._get_type_annotation(first_model, fname)
            default = self._get_default_value(finfo, type_hint)
            yaml_val = self._format_yaml_value(default, indent_level + 1)
            if "\n" not in yaml_val:
                lines.append(f"{indent}  {fname}: {yaml_val}")

        return "\n".join(lines)

    def generate_template(
        self,
        model: Type[BaseModel],
        indent_level: int = 0,
        show_optional: bool = True,
        show_comments: bool = True,
    ) -> str:
        """Generate a YAML template from a Pydantic model.

        Args:
            model: Pydantic model class
            indent_level: Current indentation level
            show_optional: Include optional fields
            show_comments: Include description comments

        Returns:
            YAML template string
        """
        lines: List[str] = []
        indent = self._indent * indent_level

        docstring = model.__doc__
        if docstring and show_comments and indent_level == 0:
            first_line = docstring.strip().split("\n")[0]
            lines.append(f"# {first_line}")
            lines.append("")

        for field_name, field_info in model.model_fields.items():
            type_hint = self._get_type_annotation(model, field_name)
            if type_hint is None:
                continue

            inner_type = self._unwrap_annotated(type_hint)
            is_required = field_info.is_required()

            if not show_optional and not is_required:
                continue

            comment_parts = []
            if show_comments:
                if is_required:
                    comment_parts.append("REQUIRED")
                else:
                    comment_parts.append("optional")

                constraint = self._format_constraint(field_info)
                if constraint:
                    comment_parts.append(constraint)

                enum_vals = self._format_enum_values(inner_type)
                if enum_vals:
                    comment_parts.append(enum_vals)

                if field_info.description:
                    desc = field_info.description.split("\n")[0][:60]
                    comment_parts.append(desc)

            field_key = field_info.alias if field_info.alias else field_name

            if self._is_list_of_unions(type_hint) and show_comments:
                list_models = self._get_list_union_models(type_hint)
                if list_models:
                    list_yaml = self._generate_list_union_template(
                        field_key,
                        list_models,
                        indent_level,
                        show_comments,
                    )
                    lines.append(list_yaml)
            elif self._is_nested_model(inner_type):
                nested_models = self._get_nested_models(inner_type)
                if nested_models:
                    if self._is_discriminated_union(inner_type) and show_comments:
                        union_yaml = self._generate_union_variants(
                            field_key,
                            nested_models,
                            indent_level,
                            show_comments,
                        )
                        lines.append(union_yaml)
                    else:
                        first_model = nested_models[0]
                        if show_comments and comment_parts:
                            lines.append(f"{indent}{field_key}:  # {' | '.join(comment_parts[:2])}")
                        else:
                            lines.append(f"{indent}{field_key}:")

                        nested_yaml = self.generate_template(
                            first_model,
                            indent_level=indent_level + 1,
                            show_optional=show_optional,
                            show_comments=show_comments,
                        )
                        lines.append(nested_yaml)
            else:
                default = self._get_default_value(field_info, inner_type)
                yaml_val = self._format_yaml_value(default, indent_level)

                if show_comments and comment_parts:
                    comment = "  # " + " | ".join(comment_parts)
                else:
                    comment = ""

                if "\n" in yaml_val:
                    lines.append(f"{indent}{field_key}:{yaml_val}{comment}")
                else:
                    lines.append(f"{indent}{field_key}: {yaml_val}{comment}")

        return "\n".join(lines)

    def generate_field_table(self, model: Type[BaseModel]) -> str:
        """Generate a markdown table of fields."""
        lines = [
            "| Field | Type | Required | Default | Description |",
            "|-------|------|----------|---------|-------------|",
        ]

        for field_name, field_info in model.model_fields.items():
            type_hint = self._get_type_annotation(model, field_name)
            if type_hint is None:
                continue

            type_str = self._format_type_for_display(type_hint)
            is_required = "Yes" if field_info.is_required() else "No"

            default = self._get_default_value(field_info, type_hint)
            default_str = str(default) if default is not None else "-"

            desc = field_info.description or "-"
            desc = desc.split("\n")[0][:50]

            lines.append(f"| {field_name} | {type_str} | {is_required} | {default_str} | {desc} |")

        return "\n".join(lines)

    def _format_type_for_display(self, type_hint: Any) -> str:
        """Format type hint for human-readable display."""
        inner = self._unwrap_annotated(type_hint)

        if isinstance(inner, type):
            if issubclass(inner, enum.Enum):
                return inner.__name__
            return inner.__name__

        origin = get_origin(inner)
        if origin is Union:
            args = get_args(inner)
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return f"Optional[{self._format_type_for_display(non_none[0])}]"
            return " | ".join(self._format_type_for_display(a) for a in non_none)

        if origin is list:
            args = get_args(inner)
            if args:
                return f"List[{self._format_type_for_display(args[0])}]"
            return "List"

        if origin is dict:
            args = get_args(inner)
            if len(args) >= 2:
                return f"Dict[{self._format_type_for_display(args[0])}, {self._format_type_for_display(args[1])}]"
            return "Dict"

        return str(inner)


_generator = TemplateGenerator()


def _get_connection_models() -> Dict[str, Type[BaseModel]]:
    """Get all connection config models."""
    from odibi.config import (
        AzureBlobConnectionConfig,
        DeltaConnectionConfig,
        HttpConnectionConfig,
        LocalConnectionConfig,
        SQLServerConnectionConfig,
    )

    return {
        "local": LocalConnectionConfig,
        "azure_blob": AzureBlobConnectionConfig,
        "azure_adls": AzureBlobConnectionConfig,
        "delta": DeltaConnectionConfig,
        "sql_server": SQLServerConnectionConfig,
        "azure_sql": SQLServerConnectionConfig,
        "http": HttpConnectionConfig,
    }


def _get_pattern_info() -> Dict[str, Dict[str, Any]]:
    """Get pattern information from pattern classes.

    Patterns use params dict (not Pydantic models), so we extract
    documentation from docstrings.
    """
    from odibi.patterns import _PATTERNS

    result = {}
    for name, pattern_cls in _PATTERNS.items():
        doc = pattern_cls.__doc__ or ""
        result[name] = {
            "class": pattern_cls,
            "docstring": doc,
            "name": name,
        }
    return result


def _get_node_config_models() -> Dict[str, Type[BaseModel]]:
    """Get node configuration models."""
    from odibi.config import (
        GateConfig,
        IncrementalConfig,
        NodeConfig,
        PipelineConfig,
        ProjectConfig,
        QuarantineConfig,
        ReadConfig,
        TransformConfig,
        ValidationConfig,
        WriteConfig,
    )

    return {
        "project": ProjectConfig,
        "pipeline": PipelineConfig,
        "node": NodeConfig,
        "read": ReadConfig,
        "write": WriteConfig,
        "transform": TransformConfig,
        "validation": ValidationConfig,
        "incremental": IncrementalConfig,
        "quarantine": QuarantineConfig,
        "gate": GateConfig,
    }


def _generate_pattern_template(pattern_name: str, pattern_info: Dict[str, Any]) -> str:
    """Generate YAML template for a pattern from its docstring.

    Patterns don't use Pydantic models for params, so we parse the
    Configuration Options section from the docstring.
    """

    docstring = pattern_info.get("docstring", "")
    lines = []

    first_line = docstring.strip().split("\n")[0] if docstring else pattern_name
    lines.append(f"# {first_line}")
    lines.append("")
    lines.append("pattern:")
    lines.append(f"  type: {pattern_name}")
    lines.append("  params:")

    params_section = re.search(
        r"Configuration Options.*?:\s*(.*?)(?:\n\n|\nExample|\nSupported|$)",
        docstring,
        re.DOTALL | re.IGNORECASE,
    )

    if params_section:
        param_lines = params_section.group(1).strip().split("\n")
        for line in param_lines:
            match = re.match(r"\s*-\s*\*\*(\w+)\*\*\s*\(([^)]+)\):\s*(.*)", line.strip())
            if match:
                param_name = match.group(1)
                param_type = match.group(2)
                param_desc = match.group(3).strip()
                is_required = "required" in param_type.lower()

                req_marker = "# REQUIRED" if is_required else "# optional"
                lines.append(f"    {param_name}: <{param_type}>  {req_marker} | {param_desc[:40]}")

    if len(lines) == 5:
        lines.append("    # See docstring for params - use: odibi explain " + pattern_name)

    return "\n".join(lines)


def show_template(
    template_type: str,
    show_optional: bool = True,
    show_comments: bool = True,
) -> str:
    """Generate and return a YAML template for a given type.

    Args:
        template_type: One of:
            - Connection types: local, azure_blob, azure_adls, delta, sql_server, azure_sql, http
            - Pattern types: dimension, fact, scd2, merge, aggregation, date_dimension
            - Node configs: node, read, write, transform, validation, etc.
        show_optional: Include optional fields (default True)
        show_comments: Include description comments (default True)

    Returns:
        YAML template string

    Example:
        >>> print(show_template("azure_blob"))
        # Azure Blob Storage / ADLS Gen2 connection.
        type: azure_blob
        account_name:   # REQUIRED
        container:      # REQUIRED
        auth:
          mode: aad_msi  # REQUIRED | account_key | sas | ...
    """
    template_type = template_type.lower().strip()

    connection_models = _get_connection_models()
    if template_type in connection_models:
        model = connection_models[template_type]
        return _generator.generate_template(
            model,
            show_optional=show_optional,
            show_comments=show_comments,
        )

    pattern_info = _get_pattern_info()
    if template_type in pattern_info:
        return _generate_pattern_template(template_type, pattern_info[template_type])

    node_models = _get_node_config_models()
    if template_type in node_models:
        model = node_models[template_type]
        return _generator.generate_template(
            model,
            show_optional=show_optional,
            show_comments=show_comments,
        )

    available = sorted(
        list(connection_models.keys()) + list(pattern_info.keys()) + list(node_models.keys())
    )
    raise ValueError(f"Unknown template type: '{template_type}'. Available: {', '.join(available)}")


def show_transformer(
    transformer_name: str,
    show_example: bool = True,
) -> str:
    """Generate YAML snippet and parameter documentation for a transformer.

    Args:
        transformer_name: Name of the registered transformer
        show_example: Include YAML example snippet

    Returns:
        Formatted documentation string

    Example:
        >>> print(show_transformer("fluid_properties"))
        # fluid_properties
        Calculate fluid properties from pressure and temperature.

        ## Parameters
        | Field | Type | Required | Default | Description |
        ...

        ## Example YAML
        ```yaml
        - operation: fluid_properties
          params:
            pressure_col: pressure
            temperature_col: temperature
        ```
    """
    from odibi.registry import FunctionRegistry
    from odibi.transformers import register_standard_library

    register_standard_library()

    if not FunctionRegistry.has_function(transformer_name):
        available = FunctionRegistry.list_functions()
        raise ValueError(
            f"Unknown transformer: '{transformer_name}'. "
            f"Available: {', '.join(sorted(available)[:10])}..."
        )

    func_info = FunctionRegistry.get_function_info(transformer_name)
    param_model = FunctionRegistry.get_param_model(transformer_name)

    lines = [f"# {transformer_name}"]

    if func_info.get("docstring"):
        doc = func_info["docstring"]
        first_para = doc.split("\n\n")[0].strip()
        lines.append(first_para)
        lines.append("")

    lines.append("## Parameters")

    if param_model and isinstance(param_model, type) and issubclass(param_model, BaseModel):
        lines.append(_generator.generate_field_table(param_model))
    else:
        lines.append("| Field | Type | Required | Default | Description |")
        lines.append("|-------|------|----------|---------|-------------|")
        for pname, pinfo in func_info.get("parameters", {}).items():
            if pname in ("current", "context"):
                continue
            is_req = "Yes" if pinfo.get("required") else "No"
            default = pinfo.get("default")
            default_str = str(default) if default is not None else "-"
            lines.append(f"| {pname} | - | {is_req} | {default_str} | - |")

    lines.append("")

    if show_example:
        lines.append("## Example YAML")
        lines.append("```yaml")
        lines.append(f"- operation: {transformer_name}")
        lines.append("  params:")

        if param_model and isinstance(param_model, type) and issubclass(param_model, BaseModel):
            for field_name, field_info in param_model.model_fields.items():
                if field_info.is_required():
                    lines.append(f"    {field_name}: <value>")
        else:
            for pname, pinfo in func_info.get("parameters", {}).items():
                if pname in ("current", "context"):
                    continue
                if pinfo.get("required"):
                    lines.append(f"    {pname}: <value>")

        lines.append("```")

    return "\n".join(lines)


def generate_json_schema(
    output_path: Optional[str] = None,
    include_transformers: bool = True,
) -> Dict[str, Any]:
    """Generate JSON Schema for VS Code autocomplete.

    Args:
        output_path: If provided, write schema to file
        include_transformers: Include transformer step definitions

    Returns:
        JSON Schema dict
    """
    from odibi.config import ProjectConfig

    schema = ProjectConfig.model_json_schema(by_alias=True)

    schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    schema["title"] = "Odibi Pipeline Configuration"
    schema["description"] = (
        "Configuration schema for Odibi data pipelines. "
        "Generated from Pydantic models - do not edit manually."
    )

    if include_transformers:
        try:
            from odibi.registry import FunctionRegistry
            from odibi.transformers import register_standard_library

            if not FunctionRegistry.list_functions():
                register_standard_library()

            transformer_names = FunctionRegistry.list_functions()

            if "$defs" not in schema:
                schema["$defs"] = {}

            schema["$defs"]["TransformOperation"] = {
                "type": "string",
                "enum": sorted(transformer_names),
                "description": f"Available transformers: {len(transformer_names)} registered",
            }
        except ImportError:
            pass

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)

    return schema


def list_templates() -> Dict[str, List[str]]:
    """List all available template types grouped by category.

    Returns:
        Dict with categories as keys and list of template names as values
    """
    return {
        "connections": list(_get_connection_models().keys()),
        "patterns": list(_get_pattern_info().keys()),
        "configs": list(_get_node_config_models().keys()),
    }
