"""Knowledge retrieval layer for the odibi MCP server.

Combines structured CLI data with semantic vector search.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Find odibi root
ODIBI_ROOT = Path(__file__).parent.parent
INDEX_DIR = ODIBI_ROOT / ".odibi" / "index"

# Add paths for imports
sys.path.insert(0, str(ODIBI_ROOT))
sys.path.insert(0, str(ODIBI_ROOT / "_archive"))


class OdibiKnowledge:
    """Unified knowledge retrieval for odibi."""

    # Priority docs for AI tools - most useful for understanding odibi
    PRIORITY_DOCS = [
        "docs/ODIBI_DEEP_CONTEXT.md",  # 2200+ lines of comprehensive context
        "docs/reference/yaml_schema.md",  # YAML config reference
        "docs/reference/cheatsheet.md",  # Quick reference
        "docs/troubleshooting.md",  # Common issues
        "docs/guides/best_practices.md",  # Best practices
        "docs/golden_path.md",  # Recommended workflow
        ".clinerules",  # AI rules
        "AGENTS.md",  # Agent instructions
    ]

    # Doc categories for targeted retrieval
    DOC_CATEGORIES = {
        "patterns": "docs/patterns/",
        "tutorials": "docs/tutorials/",
        "guides": "docs/guides/",
        "features": "docs/features/",
        "reference": "docs/reference/",
        "examples": "docs/examples/canonical/",
        "context": "docs/context/",  # Phase docs
    }

    def __init__(self, odibi_root: Path | None = None):
        self.odibi_root = odibi_root or ODIBI_ROOT
        self.index_dir = self.odibi_root / ".odibi" / "index"
        self._embedder = None
        self._vector_store = None
        self._initialized = False

    def _ensure_odibi_registered(self):
        """Ensure odibi transformers and connections are registered."""
        from odibi.connections.factory import register_builtins
        from odibi.transformers import register_standard_library

        # Always call register - it's idempotent
        register_standard_library()
        register_builtins()

    def _init_vector_store(self):
        """Lazily initialize vector store and embedder."""
        if self._initialized:
            return

        try:
            from agents.core.chroma_store import ChromaVectorStore
            from agents.core.embeddings import LocalEmbedder

            self._embedder = LocalEmbedder()
            self._vector_store = ChromaVectorStore(persist_dir=str(self.index_dir))
            self._initialized = True
            logger.info(f"Vector store initialized with {self._vector_store.count()} chunks")
        except ImportError as e:
            logger.warning(f"Vector store not available: {e}")
            self._initialized = True
        except OSError as e:
            # Handle Windows DLL issues with torch
            logger.warning(f"Vector store failed (likely PyTorch DLL issue): {e}")
            self._initialized = True

    # =========================================================================
    # Documentation Tools
    # =========================================================================

    def get_deep_context(self) -> str:
        """Get the full ODIBI_DEEP_CONTEXT.md - comprehensive framework docs."""
        path = self.odibi_root / "docs" / "ODIBI_DEEP_CONTEXT.md"
        if path.exists():
            return path.read_text(encoding="utf-8")
        return "ODIBI_DEEP_CONTEXT.md not found"

    def get_doc(self, doc_path: str) -> dict[str, Any]:
        """Get a specific documentation file by path.

        Args:
            doc_path: Relative path like "docs/patterns/scd2.md"
        """
        path = self.odibi_root / doc_path
        if not path.exists():
            # Try common prefixes
            for prefix in ["docs/", "docs/patterns/", "docs/guides/", "docs/features/"]:
                alt = self.odibi_root / prefix / doc_path
                if alt.exists():
                    path = alt
                    break

        if path.exists():
            return {
                "path": str(path.relative_to(self.odibi_root)),
                "content": path.read_text(encoding="utf-8"),
            }
        return {"error": f"Doc not found: {doc_path}"}

    def list_docs(self, category: str | None = None) -> list[dict[str, Any]]:
        """List available documentation files.

        Args:
            category: Optional filter (patterns, tutorials, guides, features, reference, examples)
        """
        result = []

        if category and category in self.DOC_CATEGORIES:
            base = self.odibi_root / self.DOC_CATEGORIES[category]
            if base.exists():
                for md in base.rglob("*.md"):
                    result.append(
                        {
                            "path": str(md.relative_to(self.odibi_root)),
                            "name": md.stem,
                            "category": category,
                        }
                    )
        else:
            # List priority docs
            for doc in self.PRIORITY_DOCS:
                path = self.odibi_root / doc
                if path.exists():
                    result.append(
                        {
                            "path": doc,
                            "name": Path(doc).stem,
                            "priority": True,
                        }
                    )

            # List categories
            if not category:
                result.append(
                    {
                        "categories": list(self.DOC_CATEGORIES.keys()),
                        "hint": "Use list_docs(category='patterns') to list docs in a category",
                    }
                )

        return result

    def search_docs(self, query: str) -> list[dict[str, Any]]:
        """Search documentation files for a keyword/phrase.

        Args:
            query: Search term
        """
        results = []
        query_lower = query.lower()

        docs_dir = self.odibi_root / "docs"
        for md in docs_dir.rglob("*.md"):
            try:
                content = md.read_text(encoding="utf-8")
                if query_lower in content.lower():
                    # Find matching lines
                    lines = content.split("\n")
                    matches = []
                    for i, line in enumerate(lines):
                        if query_lower in line.lower():
                            matches.append({"line": i + 1, "text": line.strip()[:100]})
                            if len(matches) >= 3:
                                break

                    results.append(
                        {
                            "path": str(md.relative_to(self.odibi_root)),
                            "matches": matches,
                        }
                    )
            except Exception:
                continue

        return results[:20]  # Limit results

    # =========================================================================
    # Structured Tools (Deterministic)
    # =========================================================================

    def list_transformers(self) -> list[dict[str, Any]]:
        """List all registered transformers with parameters."""
        self._ensure_odibi_registered()
        from odibi.registry import FunctionRegistry

        result = []
        for name in sorted(FunctionRegistry.list_functions()):
            info = FunctionRegistry.get_function_info(name)
            params = []
            for pname, pinfo in info.get("parameters", {}).items():
                if pname == "current":
                    continue
                params.append(
                    {
                        "name": pname,
                        "required": pinfo.get("required", False),
                        "default": self._serialize_value(pinfo.get("default")),
                    }
                )
            result.append(
                {
                    "name": name,
                    "parameters": params,
                    "docstring": self._first_line(info.get("docstring")),
                }
            )
        return result

    def list_patterns(self) -> list[dict[str, Any]]:
        """List all registered patterns."""
        from odibi.patterns import _PATTERNS
        import inspect

        result = []
        for name in sorted(_PATTERNS.keys()):
            pattern_cls = _PATTERNS[name]
            doc = inspect.getdoc(pattern_cls) or ""
            result.append(
                {
                    "name": name,
                    "class": pattern_cls.__name__,
                    "docstring": self._first_line(doc),
                }
            )
        return result

    def list_connections(self) -> list[dict[str, Any]]:
        """List all registered connection types."""
        self._ensure_odibi_registered()
        from odibi.plugins import _CONNECTION_FACTORIES

        connection_docs = {
            "local": "Local filesystem (CSV, Parquet, JSON, etc.)",
            "http": "HTTP/REST API endpoints",
            "azure_blob": "Azure Blob Storage",
            "azure_adls": "Azure Data Lake Storage Gen2",
            "delta": "Delta Lake tables",
            "sql_server": "Microsoft SQL Server",
            "azure_sql": "Azure SQL Database",
        }

        result = []
        for name in sorted(_CONNECTION_FACTORIES.keys()):
            result.append(
                {
                    "name": name,
                    "description": connection_docs.get(name, "No description"),
                }
            )
        return result

    def explain(self, name: str) -> dict[str, Any]:
        """Get detailed explanation for a transformer, pattern, or connection."""
        self._ensure_odibi_registered()
        import inspect

        from odibi.patterns import _PATTERNS
        from odibi.plugins import _CONNECTION_FACTORIES
        from odibi.registry import FunctionRegistry

        result = {"name": name, "found": False, "matches": []}

        # Check transformers
        if FunctionRegistry.has_function(name):
            info = FunctionRegistry.get_function_info(name)
            params = []
            for pname, pinfo in info.get("parameters", {}).items():
                if pname == "current":
                    continue
                params.append(
                    {
                        "name": pname,
                        "required": pinfo.get("required", False),
                        "default": self._serialize_value(pinfo.get("default")),
                    }
                )

            example_yaml = self._generate_transformer_yaml(name, params)

            result["found"] = True
            result["matches"].append(
                {
                    "type": "transformer",
                    "name": name,
                    "docstring": info.get("docstring"),
                    "parameters": params,
                    "example_yaml": example_yaml,
                }
            )

        # Check patterns
        if name in _PATTERNS:
            pattern_cls = _PATTERNS[name]
            doc = inspect.getdoc(pattern_cls) or ""
            result["found"] = True
            result["matches"].append(
                {
                    "type": "pattern",
                    "name": name,
                    "class": pattern_cls.__name__,
                    "docstring": doc,
                }
            )

        # Check connections
        if name in _CONNECTION_FACTORIES:
            connection_docs = {
                "local": "Local filesystem connection for reading/writing files.",
                "http": "HTTP/REST API connection for fetching data.",
                "azure_blob": "Azure Blob Storage connection.",
                "azure_adls": "Azure Data Lake Storage Gen2 connection.",
                "delta": "Delta Lake connection for reading/writing Delta tables.",
                "sql_server": "Microsoft SQL Server connection.",
                "azure_sql": "Azure SQL Database connection.",
            }
            result["found"] = True
            result["matches"].append(
                {
                    "type": "connection",
                    "name": name,
                    "description": connection_docs.get(name, "No documentation"),
                }
            )

        return result

    def get_transformer_signature(self) -> str:
        """Return the exact transformer function signature pattern."""
        return """# CRITICAL: Transformer Function Signature

```python
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.registry import FunctionRegistry

class MyParams(BaseModel):
    column: str = Field(..., description="Column name")
    value: str = Field(default="x", description="Default value")

def my_transformer(context: EngineContext, params: MyParams) -> EngineContext:
    sql = f"SELECT *, NULLIF({params.column}, '{params.value}') AS result FROM df"
    return context.sql(sql)

# Register it
FunctionRegistry.register(my_transformer, "my_transformer", MyParams)
```

IMPORTANT:
- First parameter is ALWAYS `context: EngineContext`
- Second parameter is ALWAYS `params: YourParamsModel` (a Pydantic BaseModel)
- Return type is ALWAYS `EngineContext`
- Use context.sql(sql_string) to execute SQL
- The input DataFrame is available as `df` in SQL

FOR MORE DETAILS, use these MCP tools:
- `get_doc("docs/guides/writing_transformations.md")` - Full transformer guide
- `get_doc("docs/features/transformers.md")` - Transformer system overview
- `list_transformers` - See all 52+ built-in transformers for reference
- `explain("<transformer_name>")` - Get details on any specific transformer
"""

    def get_yaml_structure(self) -> str:
        """Return the exact YAML structure for odibi pipelines."""
        return """# CRITICAL: Odibi YAML Pipeline Structure

```yaml
project: my_project          # Required: project name

connections:                 # Required: connection definitions
  my_data:
    type: local
    path: ./data

story:                       # Required: execution story output
  connection: my_data
  path: stories

system:                      # Required: system data (checksums, etc.)
  connection: my_data
  path: _system

pipelines:                   # Required: list of pipeline definitions
  - name: my_pipeline
    nodes:
      - name: source_node
        inputs:                        # Use 'inputs:' for reading data
          input_1:
            connection: my_data
            path: input.csv
            format: csv                # REQUIRED: csv, parquet, json, delta
        transform:
          steps:
            - function: add_column
              params:
                column_name: new_col
                value: "'default'"
        outputs:                       # Use 'outputs:' for writing data
          output_1:
            connection: my_data
            path: output.parquet
            format: parquet
```

IMPORTANT:
- All 5 top-level keys are required: project, connections, story, system, pipelines
- Node names must be alphanumeric + underscore only (no hyphens, dots, spaces)
- Transforms use `steps:` with `function:` and `params:` (NOT the short syntax)
- Always specify `format:` in inputs/outputs (csv, parquet, json, delta)
- Use `inputs:` and `outputs:` (not `source:` and `sink:`)

FOR MORE DETAILS, use these MCP tools:
- `get_doc("docs/reference/yaml_schema.md")` - Complete YAML reference
- `get_doc("docs/reference/configuration.md")` - All config options
- `list_transformers` - Available transform functions
- `explain("<name>")` - Details on any transformer/pattern/connection
"""

    # =========================================================================
    # Code Generation Tools
    # =========================================================================

    def generate_transformer(
        self, name: str, params: list[dict[str, str]], description: str = ""
    ) -> str:
        """Generate a complete custom transformer with boilerplate.

        Args:
            name: Transformer function name (snake_case)
            params: List of {"name": str, "type": str, "description": str, "required": bool}
            description: What the transformer does

        Returns:
            Complete Python code for the transformer
        """
        # Build param class fields
        param_lines = []
        for p in params:
            pname = p.get("name", "param")
            ptype = p.get("type", "str")
            pdesc = p.get("description", "")
            required = p.get("required", True)

            if required:
                param_lines.append(f'    {pname}: {ptype} = Field(..., description="{pdesc}")')
            else:
                default = '""' if ptype == "str" else "None"
                param_lines.append(
                    f'    {pname}: {ptype} = Field(default={default}, description="{pdesc}")'
                )

        params_class = f"{name.title().replace('_', '')}Params"
        param_block = "\n".join(param_lines) if param_lines else "    pass  # Add parameters here"

        return f'''"""Custom transformer: {name}

{description}
"""
from pydantic import BaseModel, Field
from odibi.context import EngineContext
from odibi.registry import FunctionRegistry


class {params_class}(BaseModel):
    """{description}"""
{param_block}


def {name}(context: EngineContext, params: {params_class}) -> EngineContext:
    """
    {description}

    Args:
        context: The engine context with input DataFrame available as 'df'
        params: Parameters for the transformation

    Returns:
        EngineContext with transformed data
    """
    sql = f"""
        SELECT
            *
            -- Add your transformation logic here
            -- Example: , UPPER({{{{params.column}}}}) as transformed_col
        FROM df
    """
    return context.sql(sql)


# Register the transformer
FunctionRegistry.register({name}, "{name}", {params_class})
'''

    def generate_pipeline_yaml(
        self,
        project_name: str,
        input_path: str,
        input_format: str,
        output_path: str,
        output_format: str,
        transforms: list[dict[str, Any]] | None = None,
    ) -> str:
        """Generate a complete pipeline YAML config.

        Args:
            project_name: Name of the project
            input_path: Path to input file
            input_format: Format (csv, parquet, json, delta)
            output_path: Path to output file
            output_format: Format (csv, parquet, json, delta)
            transforms: Optional list of {"function": str, "params": dict}

        Returns:
            Complete YAML pipeline configuration
        """
        transform_block = ""
        if transforms:
            steps = []
            for t in transforms:
                step = f"            - function: {t['function']}"
                if t.get("params"):
                    step += "\n              params:"
                    for k, v in t["params"].items():
                        if isinstance(v, str):
                            step += f'\n                {k}: "{v}"'
                        else:
                            step += f"\n                {k}: {v}"
                steps.append(step)
            transform_block = f"""
        transform:
          steps:
{chr(10).join(steps)}"""

        return f"""project: {project_name}

connections:
  local_data:
    type: local
    path: ./data

story:
  connection: local_data
  path: stories

system:
  connection: local_data
  path: _system

pipelines:
  - name: main_pipeline
    nodes:
      - name: process_data
        inputs:
          input_1:
            connection: local_data
            path: {input_path}
            format: {input_format}{transform_block}
        outputs:
          output_1:
            connection: local_data
            path: {output_path}
            format: {output_format}
"""

    def validate_yaml(self, yaml_content: str) -> dict[str, Any]:
        """Validate odibi pipeline YAML and return any errors.

        Args:
            yaml_content: YAML string to validate

        Returns:
            {"valid": bool, "errors": list, "warnings": list}
        """
        import yaml as pyyaml

        errors = []
        warnings = []

        try:
            config = pyyaml.safe_load(yaml_content)
        except pyyaml.YAMLError as e:
            return {"valid": False, "errors": [f"YAML parse error: {e}"], "warnings": []}

        if not isinstance(config, dict):
            return {"valid": False, "errors": ["Config must be a dictionary"], "warnings": []}

        # Check required top-level keys
        required_keys = ["project", "connections", "story", "system", "pipelines"]
        for key in required_keys:
            if key not in config:
                errors.append(f"Missing required key: '{key}'")

        # Check pipelines structure
        pipelines = config.get("pipelines", [])
        if not isinstance(pipelines, list):
            errors.append("'pipelines' must be a list")
        else:
            for i, pipeline in enumerate(pipelines):
                if not isinstance(pipeline, dict):
                    errors.append(f"Pipeline {i} must be a dictionary")
                    continue

                if "name" not in pipeline:
                    errors.append(f"Pipeline {i} missing 'name'")

                nodes = pipeline.get("nodes", [])
                for j, node in enumerate(nodes):
                    node_name = node.get("name", f"node_{j}")

                    # Check node name format
                    if node_name and not node_name.replace("_", "").isalnum():
                        errors.append(
                            f"Node '{node_name}': name must be alphanumeric + underscore only"
                        )

                    # Check for old syntax
                    if "source" in node:
                        errors.append(f"Node '{node_name}': use 'inputs:' instead of 'source:'")
                    if "sink" in node:
                        errors.append(f"Node '{node_name}': use 'outputs:' instead of 'sink:'")

                    # Check inputs have format
                    inputs = node.get("inputs", {})
                    for inp_name, inp_config in inputs.items():
                        if isinstance(inp_config, dict) and "format" not in inp_config:
                            errors.append(
                                f"Node '{node_name}' input '{inp_name}': missing required 'format'"
                            )

                    # Check outputs have format
                    outputs = node.get("outputs", {})
                    for out_name, out_config in outputs.items():
                        if isinstance(out_config, dict) and "format" not in out_config:
                            errors.append(
                                f"Node '{node_name}' output '{out_name}': missing required 'format'"
                            )

                    # Check transform syntax
                    transform = node.get("transform", {})
                    if isinstance(transform, list):
                        errors.append(
                            f"Node '{node_name}': transform must be object with 'steps:', not a list"
                        )
                    elif isinstance(transform, dict):
                        steps = transform.get("steps", [])
                        for k, step in enumerate(steps):
                            if "function" not in step:
                                errors.append(f"Node '{node_name}' step {k}: missing 'function'")

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
        }

    def diagnose_error(self, error_message: str) -> dict[str, Any]:
        """Diagnose a common odibi error and suggest fixes.

        Args:
            error_message: The error message from odibi

        Returns:
            {"diagnosis": str, "likely_cause": str, "fix": str, "docs": list}
        """
        error_lower = error_message.lower()

        diagnoses = [
            {
                "pattern": "format",
                "diagnosis": "Missing or invalid format specification",
                "likely_cause": "Input or output is missing the required 'format:' field",
                "fix": "Add format: csv, parquet, json, or delta to all inputs/outputs",
                "docs": ["docs/reference/yaml_schema.md"],
            },
            {
                "pattern": "source",
                "diagnosis": "Using deprecated 'source:' syntax",
                "likely_cause": "Old YAML syntax - 'source:' was renamed to 'inputs:'",
                "fix": "Replace 'source:' with 'inputs:' in your YAML",
                "docs": ["docs/reference/yaml_schema.md"],
            },
            {
                "pattern": "sink",
                "diagnosis": "Using deprecated 'sink:' syntax",
                "likely_cause": "Old YAML syntax - 'sink:' was renamed to 'outputs:'",
                "fix": "Replace 'sink:' with 'outputs:' in your YAML",
                "docs": ["docs/reference/yaml_schema.md"],
            },
            {
                "pattern": "node name",
                "diagnosis": "Invalid node name",
                "likely_cause": "Node names can only contain letters, numbers, and underscores",
                "fix": "Rename nodes to use only alphanumeric characters and underscores",
                "docs": ["docs/ODIBI_DEEP_CONTEXT.md"],
            },
            {
                "pattern": "enginecontext",
                "diagnosis": "Transformer signature issue",
                "likely_cause": "Custom transformer not following the required signature pattern",
                "fix": "Use: def my_func(context: EngineContext, params: MyParams) -> EngineContext",
                "docs": ["docs/guides/writing_transformations.md"],
            },
            {
                "pattern": "not registered",
                "diagnosis": "Transformer or pattern not found",
                "likely_cause": "The function is not registered with FunctionRegistry",
                "fix": "Call FunctionRegistry.register(func, 'name', ParamsClass) after defining",
                "docs": ["docs/features/transformers.md"],
            },
            {
                "pattern": "duckdb",
                "diagnosis": "DuckDB SQL syntax error",
                "likely_cause": "SQL syntax differs between DuckDB (Pandas) and Spark SQL",
                "fix": "Check DuckDB docs for syntax; use context.engine to detect runtime",
                "docs": ["docs/ODIBI_DEEP_CONTEXT.md"],
            },
            {
                "pattern": "temp view",
                "diagnosis": "Spark temp view issue",
                "likely_cause": "Node output not properly registered as temp view",
                "fix": "Ensure node names are valid identifiers for createOrReplaceTempView",
                "docs": ["docs/ODIBI_DEEP_CONTEXT.md"],
            },
        ]

        for d in diagnoses:
            if d["pattern"] in error_lower:
                return {
                    "diagnosis": d["diagnosis"],
                    "likely_cause": d["likely_cause"],
                    "fix": d["fix"],
                    "docs": d["docs"],
                }

        return {
            "diagnosis": "Unknown error",
            "likely_cause": "Could not automatically diagnose",
            "fix": "Check the full error traceback and consult docs",
            "docs": ["docs/troubleshooting.md", "docs/ODIBI_DEEP_CONTEXT.md"],
        }

    def get_example(self, pattern_name: str) -> dict[str, Any]:
        """Get a working example for a pattern or transformer.

        Dynamically loads examples from docs - patterns, tutorials, and examples.
        Falls back to hardcoded examples if docs not found.

        Args:
            pattern_name: Name of pattern (scd2, merge, dimension) or transformer

        Returns:
            {"name": str, "yaml": str, "python": str, "description": str, "source": str}
        """

        pattern_lower = pattern_name.lower().replace("-", "_").replace(" ", "_")

        # Search locations in priority order (exact matches first, then glob patterns)
        search_paths = [
            # Exact matches first
            (self.odibi_root / "docs" / "patterns" / f"{pattern_lower}.md", "pattern"),
            (self.odibi_root / "docs" / "features" / f"{pattern_lower}.md", "feature"),
            (self.odibi_root / "docs" / "guides" / f"{pattern_lower}.md", "guide"),
            (self.odibi_root / "docs" / "tutorials" / f"{pattern_lower}.md", "tutorial"),
            (self.odibi_root / "docs" / "reference" / f"{pattern_lower}.md", "reference"),
            (self.odibi_root / "docs" / "validation" / f"{pattern_lower}.md", "validation"),
            (self.odibi_root / "docs" / "semantics" / f"{pattern_lower}.md", "semantics"),
            (self.odibi_root / "docs" / "learning" / f"{pattern_lower}.md", "learning"),
            (self.odibi_root / "docs" / f"{pattern_lower}.md", "doc"),
            # Then glob patterns for partial matches
            (self.odibi_root / "docs" / "patterns" / f"*{pattern_lower}*.md", "pattern"),
            (self.odibi_root / "docs" / "features" / f"*{pattern_lower}*.md", "feature"),
            (self.odibi_root / "docs" / "guides" / f"*{pattern_lower}*.md", "guide"),
            (self.odibi_root / "docs" / "tutorials" / f"*{pattern_lower}*.md", "tutorial"),
            (
                self.odibi_root
                / "docs"
                / "tutorials"
                / "dimensional_modeling"
                / f"*{pattern_lower}*.md",
                "tutorial",
            ),
            (
                self.odibi_root / "docs" / "examples" / "canonical" / f"*{pattern_lower}*.md",
                "example",
            ),
            (self.odibi_root / "docs" / "reference" / f"*{pattern_lower}*.md", "reference"),
        ]

        # Try to find a matching doc
        for path_pattern, source_type in search_paths:
            # Handle glob patterns
            if "*" in str(path_pattern):
                parent = path_pattern.parent
                pattern = path_pattern.name
                if parent.exists():
                    import fnmatch

                    for f in parent.iterdir():
                        if fnmatch.fnmatch(f.name.lower(), pattern.lower()):
                            result = self._extract_example_from_doc(f, pattern_name, source_type)
                            if result:
                                return result
            elif path_pattern.exists():
                result = self._extract_example_from_doc(path_pattern, pattern_name, source_type)
                if result:
                    return result

        # Try transformer docs
        transformer_doc = self.odibi_root / "docs" / "features" / "transformers.md"
        if transformer_doc.exists():
            result = self._extract_transformer_example(transformer_doc, pattern_name)
            if result:
                return result

        # Fallback: search all pattern docs for the term
        patterns_dir = self.odibi_root / "docs" / "patterns"
        if patterns_dir.exists():
            for md_file in patterns_dir.glob("*.md"):
                content = md_file.read_text(encoding="utf-8")
                if pattern_lower in content.lower():
                    result = self._extract_example_from_doc(md_file, pattern_name, "pattern")
                    if result:
                        return result

        # List available examples
        available = self._list_available_examples()

        return {
            "error": f"No example found for '{pattern_name}'",
            "available": available,
            "hint": "Try one of the available examples, or use search_docs to find related content",
        }

    def _extract_example_from_doc(
        self, doc_path: Path, name: str, source_type: str
    ) -> dict[str, Any] | None:
        """Extract YAML examples and description from a markdown doc."""
        import re

        try:
            content = doc_path.read_text(encoding="utf-8")
        except Exception:
            return None

        # Get title/description from first heading and paragraph
        title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
        title = title_match.group(1) if title_match else name

        # Get first paragraph after title as description
        desc_match = re.search(r"^#.+\n\n(.+?)(?:\n\n|$)", content, re.MULTILINE | re.DOTALL)
        description = desc_match.group(1).strip()[:300] if desc_match else ""

        # Extract all YAML code blocks
        yaml_blocks = re.findall(r"```ya?ml\n(.*?)```", content, re.DOTALL)

        # Filter for meaningful YAML (has nodes, pipelines, or transform)
        meaningful_yaml = []
        for block in yaml_blocks:
            if any(
                kw in block
                for kw in ["nodes:", "pipelines:", "transform:", "pattern:", "function:"]
            ):
                meaningful_yaml.append(block.strip())

        # Extract Python code blocks
        python_blocks = re.findall(r"```python\n(.*?)```", content, re.DOTALL)
        meaningful_python = []
        for block in python_blocks:
            if any(kw in block for kw in ["EngineContext", "FunctionRegistry", "def ", "class "]):
                meaningful_python.append(block.strip())

        if not meaningful_yaml and not meaningful_python:
            return None

        return {
            "name": name,
            "title": title,
            "description": description,
            "yaml": meaningful_yaml[0] if meaningful_yaml else "",
            "yaml_examples": meaningful_yaml[:5],  # Up to 5 examples
            "python": meaningful_python[0] if meaningful_python else "",
            "python_examples": meaningful_python[:3],
            "source": str(doc_path.relative_to(self.odibi_root)),
            "source_type": source_type,
        }

    def _extract_transformer_example(
        self, doc_path: Path, transformer_name: str
    ) -> dict[str, Any] | None:
        """Extract a specific transformer example from transformers.md."""
        import re

        try:
            content = doc_path.read_text(encoding="utf-8")
        except Exception:
            return None

        # Look for section about this transformer
        pattern = rf"(?:^|\n)##\s+.*{re.escape(transformer_name)}.*\n(.*?)(?=\n##|\Z)"
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)

        if not match:
            return None

        section = match.group(1)

        # Extract YAML from section
        yaml_match = re.search(r"```ya?ml\n(.*?)```", section, re.DOTALL)
        yaml_content = yaml_match.group(1).strip() if yaml_match else ""

        # Get description
        desc_match = re.search(r"^(.+?)(?:\n\n|```)", section, re.DOTALL)
        description = desc_match.group(1).strip()[:200] if desc_match else ""

        if not yaml_content:
            return None

        return {
            "name": transformer_name,
            "title": transformer_name,
            "description": description,
            "yaml": yaml_content,
            "yaml_examples": [yaml_content],
            "python": "",
            "python_examples": [],
            "source": str(doc_path.relative_to(self.odibi_root)),
            "source_type": "transformer",
        }

    def _list_available_examples(self) -> list[str]:
        """List all available example names from docs."""
        available = set()
        skip_names = {"README", "anti_patterns", "THE_REFERENCE", "index", "ROADMAP"}

        # All doc folders to scan
        doc_folders = [
            "docs/patterns",
            "docs/features",
            "docs/guides",
            "docs/tutorials",
            "docs/reference",
            "docs/validation",
            "docs/semantics",
            "docs/context",
            "docs/examples/canonical",
            "docs/tutorials/dimensional_modeling",
            "docs/learning",
        ]

        for folder in doc_folders:
            folder_path = self.odibi_root / folder
            if folder_path.exists():
                for md in folder_path.glob("*.md"):
                    name = md.stem
                    # Clean up numbered prefixes
                    name = name.lstrip("0123456789_")
                    # Skip meta files
                    if name not in skip_names and not name.startswith("PHASE"):
                        available.add(name)

        # Top-level docs
        top_level = ["golden_path", "philosophy", "troubleshooting", "ODIBI_DEEP_CONTEXT"]
        for name in top_level:
            if (self.odibi_root / "docs" / f"{name}.md").exists():
                available.add(name)

        # Add all transformer names
        try:
            self._ensure_odibi_registered()
            from odibi.registry import FunctionRegistry

            for name in FunctionRegistry.list_functions():
                available.add(name)
        except Exception:
            pass

        return sorted(available)

    # =========================================================================
    # Decision Support Tools
    # =========================================================================

    def suggest_pattern(self, use_case: str) -> dict[str, Any]:
        """Suggest the best odibi pattern for a given use case.

        Args:
            use_case: Description of what the user wants to accomplish

        Returns:
            Recommended pattern with reasoning
        """
        use_case_lower = use_case.lower()

        patterns = {
            "scd2": {
                "name": "scd2",
                "description": "Slowly Changing Dimension Type 2 - tracks history with valid_from/valid_to dates",
                "use_when": [
                    "Need to track historical changes to dimension data",
                    "Want to know what a record looked like at any point in time",
                    "Auditing/compliance requires full change history",
                ],
                "keywords": [
                    "history",
                    "track changes",
                    "audit",
                    "historical",
                    "scd",
                    "slowly changing",
                    "versioning",
                    "temporal",
                ],
                "not_for": "Simple overwrites or append-only data",
            },
            "merge": {
                "name": "merge",
                "description": "Upsert pattern - insert new records, update existing ones based on key",
                "use_when": [
                    "Need to sync data from source to target",
                    "Want to update existing records and insert new ones",
                    "Don't need to track history of changes",
                ],
                "keywords": [
                    "upsert",
                    "sync",
                    "update",
                    "insert",
                    "merge",
                    "deduplicate",
                    "latest",
                ],
                "not_for": "Historical tracking (use scd2 instead)",
            },
            "dimension": {
                "name": "dimension",
                "description": "Simple dimension table load with surrogate key generation",
                "use_when": [
                    "Loading lookup/reference data",
                    "Need surrogate keys for star schema",
                    "Simple full refresh of dimension",
                ],
                "keywords": [
                    "dimension",
                    "lookup",
                    "reference",
                    "surrogate key",
                    "star schema",
                    "dim_",
                ],
                "not_for": "Tracking changes over time (use scd2)",
            },
            "fact": {
                "name": "fact",
                "description": "Fact table load with FK lookups to dimension tables",
                "use_when": [
                    "Loading transactional/event data",
                    "Need to resolve natural keys to surrogate keys",
                    "Building star schema fact tables",
                ],
                "keywords": [
                    "fact",
                    "transaction",
                    "event",
                    "measures",
                    "fk",
                    "foreign key",
                    "fact_",
                ],
                "not_for": "Dimension/reference data",
            },
            "aggregation": {
                "name": "aggregation",
                "description": "Pre-computed aggregations/summaries",
                "use_when": [
                    "Creating summary tables for reporting",
                    "Pre-computing KPIs or metrics",
                    "Reducing query complexity for BI tools",
                ],
                "keywords": [
                    "aggregate",
                    "summary",
                    "rollup",
                    "kpi",
                    "metrics",
                    "report",
                    "sum",
                    "count",
                    "avg",
                ],
                "not_for": "Raw data loading",
            },
            "date_dimension": {
                "name": "date_dimension",
                "description": "Generate a date dimension table with fiscal periods, holidays, etc.",
                "use_when": [
                    "Need a date/calendar dimension for star schema",
                    "Want fiscal year/quarter calculations",
                    "Need holiday flags or custom date attributes",
                ],
                "keywords": ["date", "calendar", "fiscal", "holiday", "dim_date", "time dimension"],
                "not_for": "Non-date dimensions",
            },
        }

        # Score each pattern based on keyword matches
        scores = {}
        for name, info in patterns.items():
            score = 0
            matched_keywords = []
            for kw in info["keywords"]:
                if kw in use_case_lower:
                    score += 1
                    matched_keywords.append(kw)
            scores[name] = {"score": score, "matched": matched_keywords}

        # Find best match
        best = max(scores.items(), key=lambda x: x[1]["score"])
        best_name = best[0]
        best_score = best[1]["score"]

        if best_score == 0:
            # No keywords matched - provide guidance
            return {
                "recommendation": None,
                "message": "Could not determine best pattern from description. Please provide more details.",
                "patterns_overview": [
                    {"name": p["name"], "use_when": p["use_when"][0]} for p in patterns.values()
                ],
                "tip": "Mention keywords like: history, sync, dimension, fact, aggregate, date",
            }

        recommended = patterns[best_name]
        alternatives = [
            {"name": name, "score": s["score"], "matched": s["matched"]}
            for name, s in sorted(scores.items(), key=lambda x: -x[1]["score"])
            if s["score"] > 0 and name != best_name
        ][:2]

        return {
            "recommendation": recommended["name"],
            "description": recommended["description"],
            "use_when": recommended["use_when"],
            "not_for": recommended["not_for"],
            "confidence": "high" if best_score >= 2 else "medium" if best_score == 1 else "low",
            "matched_keywords": best[1]["matched"],
            "alternatives": alternatives,
            "yaml_hint": f"pattern: {recommended['name']}",
        }

    def get_engine_differences(self) -> dict[str, Any]:
        """Get critical differences between Spark, Pandas, and Polars engines.

        Returns:
            Detailed comparison of engine behaviors
        """
        return {
            "summary": "odibi supports 3 engines with different SQL dialects and behaviors",
            "engines": {
                "spark": {
                    "sql_dialect": "Spark SQL (Hive-compatible)",
                    "temp_view_mechanism": "createOrReplaceTempView(node_name)",
                    "null_handling": "NULL propagates in expressions",
                    "case_sensitivity": "Case-insensitive by default",
                    "string_concat": "CONCAT(a, b) or a || b",
                    "date_functions": "date_add(), datediff(), current_date()",
                    "window_syntax": "ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)",
                    "type_casting": "CAST(x AS STRING), CAST(x AS INT)",
                    "boolean_literals": "TRUE, FALSE",
                    "array_access": "array[0] (0-indexed)",
                    "use_for": "Large-scale data, Databricks, cluster processing",
                },
                "pandas": {
                    "sql_dialect": "DuckDB SQL",
                    "temp_view_mechanism": "DuckDB registers DataFrames as views",
                    "null_handling": "NULL/None - similar to Spark",
                    "case_sensitivity": "Case-sensitive",
                    "string_concat": "a || b or CONCAT(a, b)",
                    "date_functions": "date_add(), date_diff(), current_date",
                    "window_syntax": "ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)",
                    "type_casting": "CAST(x AS VARCHAR), CAST(x AS INTEGER)",
                    "boolean_literals": "TRUE, FALSE, true, false",
                    "array_access": "array[1] (1-indexed in DuckDB!)",
                    "use_for": "Local development, small-medium data, testing",
                },
                "polars": {
                    "sql_dialect": "Polars SQL (DuckDB-like)",
                    "temp_view_mechanism": "SQLContext registers LazyFrames",
                    "null_handling": "null - similar semantics",
                    "case_sensitivity": "Case-sensitive",
                    "string_concat": "CONCAT(a, b) or a || b",
                    "date_functions": "Similar to DuckDB",
                    "window_syntax": "ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)",
                    "type_casting": "CAST(x AS VARCHAR)",
                    "boolean_literals": "TRUE, FALSE",
                    "array_access": "array[1] (1-indexed)",
                    "use_for": "Fast local processing, Rust performance",
                },
            },
            "critical_differences": [
                {
                    "issue": "Type names differ",
                    "spark": "STRING, INT, BIGINT, DOUBLE, BOOLEAN",
                    "pandas_duckdb": "VARCHAR, INTEGER, BIGINT, DOUBLE, BOOLEAN",
                    "solution": "Use odibi type mapping or test on both engines",
                },
                {
                    "issue": "Array indexing",
                    "spark": "0-indexed: array[0] is first element",
                    "pandas_duckdb": "1-indexed: array[1] is first element",
                    "solution": "Avoid raw array access; use UNNEST or transformers",
                },
                {
                    "issue": "ILIKE (case-insensitive LIKE)",
                    "spark": "Not supported - use LOWER(col) LIKE LOWER(pattern)",
                    "pandas_duckdb": "ILIKE supported natively",
                    "solution": "Use LOWER() for cross-engine compatibility",
                },
                {
                    "issue": "NULLIF behavior",
                    "spark": "NULLIF(a, b) returns NULL if a=b",
                    "pandas_duckdb": "Same behavior",
                    "solution": "Safe to use across engines",
                },
                {
                    "issue": "Date arithmetic",
                    "spark": "date_add(date, days), datediff(end, start)",
                    "pandas_duckdb": "date + INTERVAL '1 day', date_diff('day', start, end)",
                    "solution": "Use odibi date transformers for cross-engine safety",
                },
            ],
            "best_practices": [
                "Always test SQL on both Spark and Pandas/DuckDB",
                "Use uppercase type names (STRING works in Spark, VARCHAR in DuckDB)",
                "Avoid engine-specific functions in custom SQL",
                "Use odibi's built-in transformers - they handle engine differences",
                "For complex SQL, use query_codebase to find similar patterns",
            ],
            "node_name_rules": {
                "valid_chars": "alphanumeric and underscore only (a-z, A-Z, 0-9, _)",
                "reason": "Spark temp view names don't support hyphens, dots, or spaces",
                "examples": {
                    "valid": ["customer_dim", "fact_sales", "stg_orders"],
                    "invalid": ["customer-dim", "fact.sales", "stg orders"],
                },
            },
        }

    def get_validation_rules(self) -> dict[str, Any]:
        """Get all odibi validation rule types with examples.

        Returns:
            Complete validation rule reference
        """
        return {
            "overview": "odibi validation runs after transforms, before output. Failed rows go to quarantine.",
            "rule_types": {
                "not_null": {
                    "description": "Column must not contain NULL values",
                    "yaml": "- type: not_null\n  column: customer_id",
                    "sql_equivalent": "WHERE customer_id IS NOT NULL",
                },
                "unique": {
                    "description": "Column values must be unique (no duplicates)",
                    "yaml": "- type: unique\n  column: order_id",
                    "sql_equivalent": "GROUP BY order_id HAVING COUNT(*) = 1",
                },
                "range": {
                    "description": "Numeric value must be within min/max bounds",
                    "yaml": "- type: range\n  column: quantity\n  min: 0\n  max: 10000",
                    "sql_equivalent": "WHERE quantity >= 0 AND quantity <= 10000",
                },
                "regex": {
                    "description": "String must match a regular expression pattern",
                    "yaml": "- type: regex\n  column: email\n  pattern: '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$'",
                    "sql_equivalent": "WHERE email REGEXP pattern",
                },
                "in_list": {
                    "description": "Value must be one of an allowed set",
                    "yaml": "- type: in_list\n  column: status\n  values: [active, inactive, pending]",
                    "sql_equivalent": "WHERE status IN ('active', 'inactive', 'pending')",
                },
                "not_in_list": {
                    "description": "Value must NOT be in a forbidden set",
                    "yaml": "- type: not_in_list\n  column: status\n  values: [deleted, banned]",
                    "sql_equivalent": "WHERE status NOT IN ('deleted', 'banned')",
                },
                "custom_sql": {
                    "description": "Custom SQL expression that must evaluate to TRUE for valid rows",
                    "yaml": '- type: custom_sql\n  expression: "quantity * price > 0"',
                    "sql_equivalent": "WHERE quantity * price > 0",
                },
                "foreign_key": {
                    "description": "Value must exist in a reference table/column",
                    "yaml": "- type: foreign_key\n  column: customer_id\n  reference_node: dim_customer\n  reference_column: customer_key",
                    "note": "Reference node must be executed before this node",
                },
                "date_range": {
                    "description": "Date must be within a range",
                    "yaml": "- type: date_range\n  column: order_date\n  min: '2020-01-01'\n  max: '2030-12-31'",
                    "sql_equivalent": "WHERE order_date BETWEEN '2020-01-01' AND '2030-12-31'",
                },
                "length": {
                    "description": "String length must be within bounds",
                    "yaml": "- type: length\n  column: phone\n  min: 10\n  max: 15",
                    "sql_equivalent": "WHERE LENGTH(phone) BETWEEN 10 AND 15",
                },
            },
            "validation_config": {
                "yaml_structure": """validation:
  rules:
    - type: not_null
      column: id
    - type: unique
      column: id
    - type: range
      column: amount
      min: 0
  on_fail: quarantine  # or 'fail', 'warn'
  quarantine_path: _quarantine/""",
                "on_fail_options": {
                    "quarantine": "Move failed rows to quarantine table, continue with valid rows",
                    "fail": "Stop pipeline execution on any validation failure",
                    "warn": "Log warnings but continue with all rows",
                },
            },
            "quality_gates": {
                "description": "Set thresholds for acceptable data quality",
                "yaml": """quality_gate:
  min_row_count: 100
  max_null_percent:
    customer_id: 0
    email: 5
  max_duplicate_percent: 1""",
                "note": "Pipeline fails if quality gate thresholds are breached",
            },
            "best_practices": [
                "Always validate primary keys with not_null + unique",
                "Use foreign_key validation for referential integrity",
                "Set quality gates for production pipelines",
                "Use quarantine mode during development to see failures",
                "Check quarantine tables regularly in production",
            ],
        }

    # =========================================================================
    # Semantic Tools (RAG)
    # =========================================================================

    def query_codebase(self, question: str, k: int = 8) -> list[dict[str, Any]]:
        """Semantic search over the indexed codebase.

        Args:
            question: Natural language question about odibi
            k: Number of results to return

        Returns:
            List of relevant code chunks with scores
        """
        self._init_vector_store()

        if self._vector_store is None:
            return [{"error": "Vector store not available. Run reindex first."}]

        if self._vector_store.count() == 0:
            return [{"error": "Index is empty. Run reindex first."}]

        query_embedding = self._embedder.embed_query(question)
        results = self._vector_store.similarity_search(query_embedding, k=k)

        return [
            {
                "name": r.get("name", "unknown"),
                "type": r.get("chunk_type", "unknown"),
                "module": r.get("module_name", ""),
                "file": r.get("file_path", ""),
                "score": round(r.get("score", 0), 3),
                "content": r.get("content", "")[:1500],
                "docstring": r.get("docstring", ""),
            }
            for r in results
        ]

    def reindex(self, force: bool = False) -> dict[str, Any]:
        """Reindex the odibi codebase.

        Args:
            force: If True, delete existing index first

        Returns:
            Indexing statistics
        """
        try:
            from agents.pipelines.indexer import LocalIndexer
            from agents.core.chroma_store import ChromaVectorStore
            from agents.core.embeddings import LocalEmbedder

            embedder = LocalEmbedder()
            vector_store = ChromaVectorStore(persist_dir=str(self.index_dir))

            if force:
                vector_store.delete_all()

            indexer = LocalIndexer(
                odibi_root=str(self.odibi_root),
                embedder=embedder,
                vector_store=vector_store,
            )

            summary = indexer.run_indexing()

            # Reset cached instances
            self._embedder = embedder
            self._vector_store = vector_store
            self._initialized = True

            return summary

        except Exception as e:
            logger.exception("Reindex failed")
            return {"error": str(e)}

    def get_index_stats(self) -> dict[str, Any]:
        """Get statistics about the current index."""
        self._init_vector_store()

        if self._vector_store is None:
            return {"status": "unavailable", "count": 0}

        return {
            "status": "ready",
            "count": self._vector_store.count(),
            "index_dir": str(self.index_dir),
        }

    # =========================================================================
    # Helpers
    # =========================================================================

    def _first_line(self, text: str | None) -> str | None:
        if not text:
            return None
        lines = text.strip().split("\n")
        return lines[0].strip() if lines else None

    def _serialize_value(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool, list, dict)):
            return value
        return str(value)

    def _generate_transformer_yaml(self, name: str, params: list[dict]) -> str:
        lines = [
            "transform:",
            "  steps:",
            f"    - function: {name}",
            "      params:",
        ]
        for p in params:
            if p.get("required"):
                lines.append(f"        {p['name']}: <value>")
            elif p.get("default") is not None:
                lines.append(f"        # {p['name']}: {p['default']}")
        return "\n".join(lines)


# Singleton instance
_knowledge: OdibiKnowledge | None = None


def get_knowledge() -> OdibiKnowledge:
    """Get or create the knowledge instance."""
    global _knowledge
    if _knowledge is None:
        _knowledge = OdibiKnowledge()
    return _knowledge
