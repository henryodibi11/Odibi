#!/usr/bin/env python3
"""
Generate JSON Schema for Odibi configuration files.
Injects dynamic schemas from registered transformers.
"""

import json
import os
import sys

# Ensure we can import odibi
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydantic.json_schema import models_json_schema

from odibi.config import ProjectConfig
from odibi.registry import FunctionRegistry

# Import other transformers as they are implemented to register them
# from odibi.transformers import ...


def generate_schema():
    print("Generating JSON Schema...")

    # Get base schema for ProjectConfig
    # We use a list of models to generate a shared definitions schema if needed,
    # but ProjectConfig includes NodeConfig, so it should be enough.
    _, top_schema = models_json_schema([(ProjectConfig, "validation")])
    schema = top_schema["$defs"]["ProjectConfig"]
    defs = top_schema["$defs"]

    # Resolve references in top_schema to be standalone if we want ProjectConfig as root
    # But standard Pydantic V2 output with $defs is fine for VS Code.
    # We need to structure it as a standard JSON Schema document.

    final_schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://raw.githubusercontent.com/henryodibi11/Odibi/main/docs/schemas/odibi.json",
        "title": "Odibi Project Configuration",
        "description": "Configuration schema for Odibi data engineering framework.",
        **schema,
        "$defs": defs,
    }

    # Dynamic Injection of Transformer Schemas
    # We want to modify NodeConfig schema in $defs
    if "NodeConfig" in final_schema["$defs"]:
        node_config = final_schema["$defs"]["NodeConfig"]

        # Find registered transformers with param models
        transformers = []
        for name in FunctionRegistry.list_functions():
            param_model = FunctionRegistry.get_param_model(name)
            if param_model:
                transformers.append((name, param_model))
                print(f"Found transformer with params: {name}")

        if transformers:
            # We need to add the param models to $defs
            # and create a conditional validation for 'params' field in NodeConfig

            # 1. Add Param Models to $defs
            # We can use models_json_schema to generate schemas for these models
            # and merge them into our definitions.
            param_models = [t[1] for t in transformers]
            _, param_defs = models_json_schema([(m, "validation") for m in param_models])

            if "$defs" in param_defs:
                final_schema["$defs"].update(param_defs["$defs"])

            # 2. Create 'allOf' conditional logic for NodeConfig
            # "allOf": [
            #   {
            #     "if": { "properties": { "transformer": { "const": "merge" } } },
            #     "then": { "properties": { "params": { "$ref": "#/$defs/MergeParams" } } }
            #   },
            #   ...
            # ]

            all_of = node_config.get("allOf", [])

            for name, model in transformers:
                model_name = model.__name__
                condition = {
                    "if": {"properties": {"transformer": {"const": name}}},
                    "then": {"properties": {"params": {"$ref": f"#/$defs/{model_name}"}}},
                }
                all_of.append(condition)

            node_config["allOf"] = all_of

            # Also update 'transformer' enum if we want strict validation
            # but it's Optional[str] in code. We could make it an enum in schema.
            # For now, just the params validation is the key requirement.

    # Output path
    output_dir = os.path.join(os.path.dirname(__file__), "..", "docs", "schemas")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "odibi.json")

    with open(output_path, "w") as f:
        json.dump(final_schema, f, indent=2)

    print(f"Schema generated at: {output_path}")


if __name__ == "__main__":
    generate_schema()
