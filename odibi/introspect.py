"""Introspection tool for generating Configuration Manual."""

import importlib
import inspect
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, Union

try:
    from typing import Annotated, get_args, get_origin
except ImportError:
    from typing_extensions import Annotated, get_args, get_origin

# Python 3.10+ has types.UnionType for X | Y syntax
try:
    from types import UnionType

    HAS_UNION_TYPE = True
except ImportError:
    UnionType = None  # type: ignore
    HAS_UNION_TYPE = False

from pydantic import BaseModel

# Try to import registry/transformers to get function metadata
try:
    from odibi.registry import FunctionRegistry
    from odibi.transformers import register_standard_library

    # Ensure registry is populated
    register_standard_library()
    HAS_REGISTRY = True
except ImportError:
    HAS_REGISTRY = False
    print(
        "Warning: Could not import FunctionRegistry/transformers. Function details will be missing."
    )

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
    category: Optional[str] = None  # Sub-category for Transformations
    function_name: Optional[str] = None
    function_doc: Optional[str] = None
    used_in: List[str] = []


# --- Configuration ---

GROUP_MAPPING = {
    "ProjectConfig": "Core",
    "PipelineConfig": "Core",
    "NodeConfig": "Core",
    "ReadConfig": "Operation",
    "IncrementalConfig": "Operation",
    "WriteConfig": "Operation",
    "WriteMetadataConfig": "Operation",
    "StreamingWriteConfig": "Operation",
    "TriggerConfig": "Operation",
    "AutoOptimizeConfig": "Operation",
    "DeleteDetectionConfig": "Operation",
    "TransformConfig": "Operation",
    "TransformStep": "Operation",
    "ValidationConfig": "Operation",
    "PrivacyConfig": "Operation",
    "ColumnMetadata": "Core",
    "TimeTravelConfig": "Operation",
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
    "LineageConfig": "Setting",
    "StateConfig": "Setting",
    "SystemConfig": "Core",
    # Contract/Test types
    "NotNullTest": "Contract",
    "UniqueTest": "Contract",
    "AcceptedValuesTest": "Contract",
    "RowCountTest": "Contract",
    "CustomSQLTest": "Contract",
    "RangeTest": "Contract",
    "RegexMatchTest": "Contract",
    "VolumeDropTest": "Contract",
    "SchemaContract": "Contract",
    "DistributionContract": "Contract",
    "FreshnessContract": "Contract",
    # Quarantine & Quality Gates (Week 1)
    "QuarantineConfig": "Operation",
    "QuarantineColumnsConfig": "Operation",
    "GateConfig": "Operation",
    "GateThreshold": "Operation",
    "RowCountGate": "Operation",
    # Cross-Pipeline Dependencies
    "ReferenceResolutionError": "Core",
    # Semantic Layer
    "MetricDefinition": "Semantic",
    "DimensionDefinition": "Semantic",
    "MaterializationConfig": "Semantic",
    "SemanticLayerConfig": "Semantic",
    # FK Validation
    "RelationshipConfig": "Validation",
    "RelationshipRegistry": "Validation",
    # Patterns
    "DimensionPattern": "Pattern",
    "DateDimensionPattern": "Pattern",
    "FactPattern": "Pattern",
    "AggregationPattern": "Pattern",
    "AuditConfig": "Pattern",
    # SQL Server Merge (Phase 4)
    "SqlServerMergeOptions": "Operation",
    "SqlServerOverwriteOptions": "Operation",
    "SqlServerAuditColsConfig": "Operation",
    "SqlServerMergeValidationConfig": "Operation",
    "SqlServerSchemaEvolutionConfig": "Operation",
}

# Map modules to readable Categories
TRANSFORM_CATEGORY_MAP = {
    "odibi.transformers.sql_core": "Common Operations",
    "odibi.transformers.relational": "Relational Algebra",
    "odibi.transformers.advanced": "Advanced & Feature Engineering",
    "odibi.transformers.scd": "Warehousing Patterns",
    "odibi.transformers.validation": "Data Quality",
    "odibi.transformers.merge_transformer": "Warehousing Patterns",
    "odibi.transformers.delete_detection": "Data Engineering Patterns",
}

CUSTOM_ORDER = [
    # Core
    "ProjectConfig",
    "PipelineConfig",
    "NodeConfig",
    "ColumnMetadata",
    "SystemConfig",
    "StateConfig",
    "LineageConfig",
    # Operations (ETL flow)
    "ReadConfig",
    "IncrementalConfig",
    "TimeTravelConfig",
    "TransformConfig",
    "DeleteDetectionConfig",
    "ValidationConfig",
    "QuarantineConfig",
    "QuarantineColumnsConfig",
    "GateConfig",
    "GateThreshold",
    "RowCountGate",
    "WriteConfig",
    "WriteMetadataConfig",
    "StreamingWriteConfig",
    "TriggerConfig",
    "AutoOptimizeConfig",
    # Connections (Common first)
    "LocalConnectionConfig",
    "DeltaConnectionConfig",
    "AzureBlobConnectionConfig",
    "SQLServerConnectionConfig",
    "HttpConnectionConfig",
]

# Map type aliases to their documentation section anchors
TYPE_ALIAS_LINKS = {
    "TestConfig": "contracts-data-quality-gates",
    "ConnectionConfig": "connections",
}

# Known Type Aliases to simplify display
TYPE_ALIASES = {
    "ConnectionConfig": [
        "LocalConnectionConfig",
        "AzureBlobConnectionConfig",
        "DeltaConnectionConfig",
        "SQLServerConnectionConfig",
        "HttpConnectionConfig",
    ],
    "AzureBlobAuthConfig": [
        "AzureBlobKeyVaultAuth",
        "AzureBlobAccountKeyAuth",
        "AzureBlobSasAuth",
        "AzureBlobConnectionStringAuth",
        "AzureBlobMsiAuth",
    ],
    "SQLServerAuthConfig": [
        "SQLLoginAuth",
        "SQLAadPasswordAuth",
        "SQLMsiAuth",
        "SQLConnectionStringAuth",
    ],
    "HttpAuthConfig": ["HttpNoAuth", "HttpBasicAuth", "HttpBearerAuth", "HttpApiKeyAuth"],
    "TestConfig": [
        "NotNullTest",
        "UniqueTest",
        "AcceptedValuesTest",
        "RowCountTest",
        "CustomSQLTest",
        "RangeTest",
        "RegexMatchTest",
        "VolumeDropTest",
        "SchemaContract",
        "DistributionContract",
        "FreshnessContract",
    ],
}

SECTION_INTROS = {
    "Contract": """
### Contracts (Pre-Transform Checks)

Contracts are **fail-fast data quality checks** that run on input data **before** transformation.
They always halt execution on failure - use them to prevent bad data from entering the pipeline.

**Contracts vs Validation vs Quality Gates:**

| Feature | When it Runs | On Failure | Use Case |
|---------|--------------|------------|----------|
| **Contracts** | Before transform | Always fails | Input data quality (not-null, unique keys) |
| **Validation** | After transform | Configurable (fail/warn/quarantine) | Output data quality (ranges, formats) |
| **Quality Gates** | After validation | Configurable (abort/warn) | Pipeline-level thresholds (pass rate, row counts) |
| **Quarantine** | With validation | Routes bad rows | Capture invalid records for review |

**See Also:**
- [Validation Guide](../features/quality_gates.md) - Full validation configuration
- [Quarantine Guide](../features/quarantine.md) - Quarantine setup and review
- [Getting Started: Validation](../tutorials/getting_started.md#add-data-validation)

**Example:**
```yaml
- name: "process_orders"
  contracts:
    - type: not_null
      columns: [order_id, customer_id]
    - type: row_count
      min: 100
    - type: freshness
      column: created_at
      max_age: "24h"
  read:
    source: raw_orders
```
""",
    "Semantic": """
### Semantic Layer

The semantic layer provides a unified interface for defining and querying business metrics.
Define metrics once, query them by name across dimensions.

**Core Components:**
- **MetricDefinition**: Define aggregation expressions (SUM, COUNT, AVG)
- **DimensionDefinition**: Define grouping attributes with hierarchies
- **MaterializationConfig**: Pre-compute metrics at specific grain
- **SemanticQuery**: Execute queries like "revenue BY region, month"
- **Project**: Unified API that connects pipelines and semantic layer

**Unified Project API (Recommended):**
```python
from odibi import Project

project = Project.load("odibi.yaml")
result = project.query("revenue BY region")
print(result.df)
```

**YAML Configuration:**
```yaml
project: my_warehouse
engine: pandas

connections:
  gold:
    type: delta
    path: /mnt/data/gold

# Semantic layer at project level
semantic:
  metrics:
    - name: revenue
      expr: "SUM(total_amount)"
      source: gold.fact_orders    # connection.table notation
      filters:
        - "status = 'completed'"

  dimensions:
    - name: region
      source: gold.dim_customer
      column: region

materializations:
  - name: monthly_revenue
    metrics: [revenue]
    dimensions: [region, month]
    output: gold/agg_monthly_revenue
```

The `source: gold.fact_orders` notation resolves paths automatically from connections.
""",
    "Validation": """
### FK Validation

Declare and validate referential integrity between fact and dimension tables.

**Features:**
- Declare relationships in YAML
- Validate FK constraints on fact load
- Detect orphan records
- Generate lineage from relationships

**Example:**
```yaml
relationships:
  - name: orders_to_customers
    fact: fact_orders
    dimension: dim_customer
    fact_key: customer_sk
    dimension_key: customer_sk
    on_violation: error
```
""",
    "Pattern": """
### Data Patterns

Declarative patterns for common data warehouse building blocks. Patterns encapsulate
best practices for dimensional modeling, ensuring consistent implementation across
your data warehouse.

---

## DimensionPattern

Build complete dimension tables with surrogate keys and SCD (Slowly Changing Dimension) support.

**When to Use:**
- Building dimension tables from source systems (customers, products, locations)
- Need surrogate keys for star schema joins
- Need to track historical changes (SCD Type 2)

**Beginner Note:**
Dimensions are the "who, what, where, when" of your data warehouse.
A customer dimension has customer_id (natural key) and customer_sk (surrogate key).
Fact tables join to dimensions via surrogate keys.

**See Also:** [FactPattern](#factpattern), [DateDimensionPattern](#datedimensionpattern)

**Features:**
- Auto-generate integer surrogate keys (MAX(existing) + ROW_NUMBER)
- SCD Type 0 (static), 1 (overwrite), 2 (history tracking)
- Optional unknown member row (SK=0) for orphan FK handling
- Audit columns (load_timestamp, source_system)

**Params:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `natural_key` | str | Yes | Natural/business key column name |
| `surrogate_key` | str | Yes | Surrogate key column name to generate |
| `scd_type` | int | No | 0=static, 1=overwrite, 2=history (default: 1) |
| `track_cols` | list | SCD1/2 | Columns to track for change detection |
| `target` | str | SCD2 | Target table path to read existing history |
| `unknown_member` | bool | No | Insert row with SK=0 for orphan handling |
| `audit.load_timestamp` | bool | No | Add load_timestamp column |
| `audit.source_system` | str | No | Add source_system column with value |

**Supported Target Formats:**
- Spark: catalog.table, Delta paths, .parquet, .csv, .json, .orc
- Pandas: .parquet, .csv, .json, .xlsx, .feather, .pickle

**Example:**
```yaml
pattern:
  type: dimension
  params:
    natural_key: customer_id
    surrogate_key: customer_sk
    scd_type: 2
    track_cols: [name, email, address, city]
    target: warehouse.dim_customer
    unknown_member: true
    audit:
      load_timestamp: true
      source_system: "crm"
```

---

## DateDimensionPattern

Generate a complete date dimension table with pre-calculated attributes for BI/reporting.

**When to Use:**
- Every data warehouse needs a date dimension for time-based analytics
- Enable date filtering, grouping by week/month/quarter, fiscal year reporting

**Beginner Note:**
The date dimension is foundational for any BI/reporting system.
It lets you query "sales by month" or "orders in fiscal Q2" without complex date calculations.

**See Also:** [DimensionPattern](#dimensionpattern)

**Features:**
- Generates all dates in a range with rich attributes
- Calendar and fiscal year support
- ISO week numbering
- Weekend/month-end flags

**Params:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | str | Yes | Start date (YYYY-MM-DD) |
| `end_date` | str | Yes | End date (YYYY-MM-DD) |
| `date_key_format` | str | No | Format for date_sk (default: yyyyMMdd) |
| `fiscal_year_start_month` | int | No | Month fiscal year starts (1-12, default: 1) |
| `unknown_member` | bool | No | Add unknown date row with date_sk=0 |

**Generated Columns:**
`date_sk`, `full_date`, `day_of_week`, `day_of_week_num`, `day_of_month`,
`day_of_year`, `is_weekend`, `week_of_year`, `month`, `month_name`, `quarter`,
`quarter_name`, `year`, `fiscal_year`, `fiscal_quarter`, `is_month_start`,
`is_month_end`, `is_year_start`, `is_year_end`

**Example:**
```yaml
pattern:
  type: date_dimension
  params:
    start_date: "2020-01-01"
    end_date: "2030-12-31"
    fiscal_year_start_month: 7
    unknown_member: true
```

---

## FactPattern

Build fact tables with automatic surrogate key lookups from dimensions.

**When to Use:**
- Building fact tables from transactional data (orders, events, transactions)
- Need to look up surrogate keys from dimension tables
- Need to handle orphan records (missing dimension matches)

**Beginner Note:**
Facts are the "how much, how many" of your data warehouse.
An orders fact has measures (quantity, revenue) and dimension keys (customer_sk, product_sk).
The pattern automatically looks up SKs from dimensions.

**See Also:** [DimensionPattern](#dimensionpattern), [QuarantineConfig](#quarantineconfig)

**Features:**
- Automatic SK lookups from dimension tables (with SCD2 current-record filtering)
- Orphan handling: unknown (SK=0), reject (error), quarantine (route to table)
- Grain validation (detect duplicates)
- Calculated measures and column renaming
- Audit columns

**Params:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `grain` | list | No | Columns defining uniqueness (validates no duplicates) |
| `dimensions` | list | No | Dimension lookup configurations (see below) |
| `orphan_handling` | str | No | "unknown" \\| "reject" \\| "quarantine" (default: unknown) |
| `quarantine` | dict | quarantine | Quarantine config (see below) |
| `measures` | list | No | Measure definitions (passthrough, rename, or calculated) |
| `deduplicate` | bool | No | Remove duplicates before processing |
| `keys` | list | dedupe | Keys for deduplication |
| `audit.load_timestamp` | bool | No | Add load_timestamp column |
| `audit.source_system` | str | No | Add source_system column |

**Dimension Lookup Config:**
```yaml
dimensions:
  - source_column: customer_id      # Column in source fact
    dimension_table: dim_customer   # Dimension in context
    dimension_key: customer_id      # Natural key in dimension
    surrogate_key: customer_sk      # SK to retrieve
    scd2: true                      # Filter is_current=true
```

**Quarantine Config (for orphan_handling: quarantine):**
```yaml
quarantine:
  connection: silver                # Required: connection name
  path: fact_orders_orphans         # OR table: quarantine_table
  add_columns:
    _rejection_reason: true         # Add rejection reason
    _rejected_at: true              # Add rejection timestamp
    _source_dimension: true         # Add dimension name
```

**Example:**
```yaml
pattern:
  type: fact
  params:
    grain: [order_id]
    dimensions:
      - source_column: customer_id
        dimension_table: dim_customer
        dimension_key: customer_id
        surrogate_key: customer_sk
        scd2: true
      - source_column: product_id
        dimension_table: dim_product
        dimension_key: product_id
        surrogate_key: product_sk
    orphan_handling: unknown
    measures:
      - quantity
      - revenue: "quantity * unit_price"
    audit:
      load_timestamp: true
      source_system: "pos"
```

---

## AggregationPattern

Declarative aggregation with GROUP BY and optional incremental merge.

**When to Use:**
- Building summary/aggregate tables (daily sales, monthly metrics)
- Need incremental aggregation (update existing aggregates)
- Gold layer reporting tables

**Beginner Note:**
Aggregations summarize facts at a higher grain.
Example: daily_sales aggregates orders by date with SUM(revenue).

**See Also:** [FactPattern](#factpattern)

**Features:**
- Declare grain (GROUP BY columns)
- Define measures with SQL aggregation expressions
- Optional HAVING filter
- Audit columns

**Params:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `grain` | list | Yes | Columns to GROUP BY (defines uniqueness) |
| `measures` | list | Yes | Measure definitions with name and expr |
| `having` | str | No | HAVING clause for filtering aggregates |
| `incremental.timestamp_column` | str | No | Column to identify new data |
| `incremental.merge_strategy` | str | No | "replace", "sum", "min", or "max" |
| `audit.load_timestamp` | bool | No | Add load_timestamp column |
| `audit.source_system` | str | No | Add source_system column |

**Example:**
```yaml
pattern:
  type: aggregation
  params:
    grain: [date_sk, product_sk, region]
    measures:
      - name: total_revenue
        expr: "SUM(total_amount)"
      - name: order_count
        expr: "COUNT(*)"
      - name: avg_order_value
        expr: "AVG(total_amount)"
    having: "COUNT(*) > 0"
    audit:
      load_timestamp: true
```
""",
    "Transformation": """
### How to Use Transformers

You can use any transformer in two ways:

**1. As a Top-Level Transformer ("The App")**
Use this for major operations that define the node's purpose (e.g. Merge, SCD2).
```yaml
- name: "my_node"
  transformer: "<transformer_name>"
  params:
    <param_name>: <value>
```

**2. As a Step in a Chain ("The Script")**
Use this for smaller operations within a `transform` block (e.g. clean_text, filter).
```yaml
- name: "my_node"
  transform:
    steps:
      - function: "<transformer_name>"
         params:
           <param_name>: <value>
```

**Available Transformers:**
The models below describe the `params` required for each transformer.
""",
}

# --- Logic ---


def discover_modules(root_dir: str = "odibi") -> List[str]:
    """Recursively discover all Python modules in the package."""
    modules = []
    path = Path(root_dir)

    # Handle running from inside odibi/ vs root
    if not path.exists():
        # Try finding it in current directory
        if Path("odibi").exists():
            path = Path("odibi")
        else:
            # If we are inside the package already?
            # Assumption: Script is run from project root d:/odibi
            # so odibi/ should exist.
            print(f"Warning: Could not find root directory '{root_dir}'", file=sys.stderr)
            return []

    # Directories to skip (experimental/archived, or with optional deps like gradio)
    skip_dirs = {"agents", "ui", "_archive"}

    for file_path in path.rglob("*.py"):
        if "introspect.py" in str(file_path):  # Avoid self
            continue
        if "tests" in file_path.parts or "test_" in file_path.name:  # Skip test files
            continue
        if skip_dirs.intersection(file_path.parts):  # Skip experimental directories
            continue

        # Convert path to module notation
        # e.g. odibi\transformers\scd.py -> odibi.transformers.scd
        try:
            # If path is absolute or relative to cwd, we need to find the 'odibi' package root
            # simpler: assume we are at project root, so 'odibi/...' maps to 'odibi.'
            parts = list(file_path.parts)

            # Find where 'odibi' starts in the path parts
            if "odibi" in parts:
                start_idx = parts.index("odibi")
                rel_parts = parts[start_idx:]
                module_name = ".".join(rel_parts).replace(".py", "")

                # Fix __init__ (odibi.transformers.__init__ -> odibi.transformers)
                if module_name.endswith(".__init__"):
                    module_name = module_name[:-9]

                modules.append(module_name)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")

    return sorted(list(set(modules)))


def get_docstring(obj: Any) -> Optional[str]:
    doc = inspect.getdoc(obj)
    if doc is None:
        return None
    # Prevent inheriting Pydantic's internal docstring
    if doc == inspect.getdoc(BaseModel):
        return None
    return doc


def get_summary(obj: Any) -> Optional[str]:
    doc = get_docstring(obj)
    if not doc:
        return None
    return doc.split("\n")[0].strip()


def clean_type_str(s: str) -> str:
    """Clean up raw type string."""
    s = s.replace("typing.", "")
    s = s.replace("odibi.config.", "")
    s = s.replace("odibi.enums.", "")
    s = s.replace("pydantic.types.", "")
    s = s.replace("NoneType", "None")
    s = s.replace("False", "bool")  # Literal[False] often shows as False
    s = s.replace("True", "bool")
    return s


def format_type_hint(annotation: Any) -> str:
    """Robust type hint formatting."""
    if annotation is inspect.Parameter.empty:
        return "Any"

    # Handle Annotated (strip metadata)
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        if args:
            return format_type_hint(args[0])

    # Handle Union / Optional (including Python 3.10+ X | Y syntax)
    origin = get_origin(annotation)
    is_union = origin is Union or (HAS_UNION_TYPE and isinstance(annotation, UnionType))
    if is_union:
        args = get_args(annotation)
        # Check if it's Optional (Union[T, None])
        # Filter out NoneType
        non_none = [a for a in args if a is not type(None)]

        # Check if this Union matches a known Alias
        arg_names = set()
        for a in non_none:
            if hasattr(a, "__name__"):
                arg_names.add(a.__name__)
            else:
                arg_names.add(str(a))

        for alias, components in TYPE_ALIASES.items():
            if arg_names == set(components):
                return alias

        formatted_args = [format_type_hint(a) for a in non_none]
        if len(formatted_args) == 1:
            return f"Optional[{formatted_args[0]}]"
        return " | ".join(formatted_args)

    # Handle Literal
    s_annot = str(annotation)
    if "Literal" in s_annot and (
        "typing.Literal" in s_annot or "typing_extensions.Literal" in s_annot
    ):
        args = get_args(annotation)
        clean_args = []
        for a in args:
            if hasattr(a, "value"):  # Enum member
                clean_args.append(repr(a.value))
            else:
                clean_args.append(repr(a))
        return f"Literal[{', '.join(clean_args)}]"

    # Handle List/Dict
    if origin is list or origin is List:
        args = get_args(annotation)
        inner = format_type_hint(args[0]) if args else "Any"
        return f"List[{inner}]"

    if origin is dict or origin is Dict:
        args = get_args(annotation)
        k = format_type_hint(args[0]) if args else "Any"
        v = format_type_hint(args[1]) if args else "Any"
        return f"Dict[{k}, {v}]"

    # Handle Classes / Strings
    if isinstance(annotation, type):
        return clean_type_str(annotation.__name__)

    return clean_type_str(str(annotation))


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


def get_registry_info(model_cls: Type[BaseModel]) -> Dict[str, Optional[str]]:
    """Lookup function info from registry using the model class."""
    if not HAS_REGISTRY:
        return {}

    # Iterate registry to find matching model
    # FunctionRegistry._param_models: Dict[str, BaseModel]
    # Accessing protected member is necessary here
    for name, model in FunctionRegistry._param_models.items():
        if model is model_cls:
            # Found it!
            try:
                func_info = FunctionRegistry.get_function_info(name)
                return {
                    "function_name": name,
                    "function_doc": func_info.get("docstring"),
                }
            except ValueError:
                pass
    return {}


def scan_module_for_models(module_name: str, group_map: Dict[str, str]) -> List[ModelDoc]:
    """Scan a module for Pydantic models."""
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        print(f"Warning: Could not import {module_name}: {e}", file=sys.stderr)
        return []

    models = []
    for name, obj in inspect.getmembers(module):
        if not inspect.isclass(obj):
            continue

        # Allow including models from sub-modules if they are part of the target package
        # But generally prefer defining module
        if not hasattr(obj, "__module__"):
            continue

        # Filter out imported pydantic base
        if obj is BaseModel:
            continue

        if issubclass(obj, BaseModel):
            # Only document models that are either in the module or are explicitly desired
            # For odibi.config, we want everything defined there
            if obj.__module__ == module_name:
                # Determine Group and Category
                group = "Other"
                category = None

                if name in group_map:
                    group = group_map[name]
                elif module_name.startswith("odibi.transformers"):
                    group = "Transformation"
                    category = TRANSFORM_CATEGORY_MAP.get(module_name, "Other Transformers")

                # Get Registry Info
                reg_info = get_registry_info(obj)

                fields = get_pydantic_fields(obj)

                models.append(
                    ModelDoc(
                        name=name,
                        module=module_name,
                        summary=get_summary(obj),
                        docstring=get_docstring(obj),
                        fields=fields,
                        group=group,
                        category=category,
                        function_name=reg_info.get("function_name"),
                        function_doc=reg_info.get("function_doc"),
                    )
                )
    return models


def build_usage_map(models: List[ModelDoc]) -> Dict[str, Set[str]]:
    """Build a reverse index of where models are used."""
    usage = {m.name: set() for m in models}
    model_names = set(m.name for m in models)

    for m in models:
        for f in m.fields:
            # Naive check: if model name appears in type hint
            # This handles List[NodeConfig], Optional[ReadConfig], etc.
            for target in model_names:
                if target == m.name:
                    continue
                # Check for whole word match to avoid partials
                if re.search(r"\b" + re.escape(target) + r"\b", f.type_hint):
                    usage[target].add(m.name)

            # Expand type aliases - if field uses an alias, mark all components as used
            for alias, components in TYPE_ALIASES.items():
                if alias in f.type_hint:
                    for component in components:
                        if component in usage and component != m.name:
                            usage[component].add(m.name)

    return usage


def generate_docs(output_path: str = "docs/reference/yaml_schema.md"):
    """Run introspection and save to file."""
    print("Scanning configuration models...")

    modules = discover_modules()
    print(f"Discovered {len(modules)} modules.")

    all_models = []
    for mod in modules:
        all_models.extend(scan_module_for_models(mod, GROUP_MAPPING))

    # Build Reverse Index
    usage_map = build_usage_map(all_models)
    for m in all_models:
        if m.name in usage_map:
            m.used_in = sorted(list(usage_map[m.name]))

    # Organize by group
    grouped = {
        "Core": [],
        "Connection": [],
        "Operation": [],
        "Contract": [],
        "Setting": [],
        "Transformation": [],
        "Semantic": [],
        "Validation": [],
        "Pattern": [],
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
        ("Contract", "Contracts (Data Quality Gates)"),
        ("Setting", "Global Settings"),
        ("Transformation", "Transformation Reference"),
        ("Semantic", "Semantic Layer"),
        ("Validation", "FK Validation"),
        ("Pattern", "Data Patterns"),
    ]

    model_names = {m.name for m in all_models}

    for group_key, title in sections:
        models = grouped[group_key]
        if not models:
            continue

        lines.append(f"## {title}")
        lines.append("")

        if group_key in SECTION_INTROS:
            lines.append(SECTION_INTROS[group_key].strip())
            lines.append("")

            # Special handling for Transformation Grouping
            if group_key == "Transformation":
                lines.append("---")
                lines.append("")

                # Sort models by category, then name
                def transform_sort_key(m):
                    # Defined order of categories
                    cat_order = [
                        "Common Operations",
                        "Relational Algebra",
                        "Data Quality",
                        "Warehousing Patterns",
                        "Data Engineering Patterns",
                        "Advanced & Feature Engineering",
                        "Other Transformers",
                    ]
                    cat = m.category or "Other Transformers"
                    try:
                        cat_idx = cat_order.index(cat)
                    except ValueError:
                        cat_idx = 999

                    return (cat_idx, m.function_name or m.name)

                models.sort(key=transform_sort_key)

                current_category = None

                for model in models:
                    # Category Header
                    if model.category != current_category:
                        current_category = model.category
                        lines.append(f"### 📂 {current_category}")
                        lines.append("")

                    # Header with Function Name if available (preferred for transformers)
                    header_name = model.name
                    if model.function_name:
                        header_name = f"`{model.function_name}` ({model.name})"

                    lines.append(f"#### {header_name}")

                    # Function Docstring (Design/Impl details)
                    if model.function_doc:
                        lines.append(f"{model.function_doc}")
                        lines.append("")

                    # Model Docstring (Configuration details)
                    # If function doc is present, we might want to skip model doc if it's redundant,
                    # but usually model doc has the YAML examples which are critical.
                    if model.docstring:
                        if (
                            model.function_doc
                            and model.docstring.strip() == model.function_doc.strip()
                        ):
                            pass  # Skip duplicate
                        else:
                            lines.append(f"{model.docstring}\n")

                    lines.append("[Back to Catalog](#nodeconfig)")
                    lines.append("")

                    if model.fields:
                        lines.append("| Field | Type | Required | Default | Description |")
                        lines.append("| --- | --- | --- | --- | --- |")
                        for field in model.fields:
                            req = "Yes" if field.required else "No"
                            default = f"`{field.default}`" if field.default else "-"
                            desc = field.description or "-"
                            desc = desc.replace("|", "\\|").replace("\n", " ")

                            # Cross Linking
                            th_display = field.type_hint.replace("|", "\\|")
                            for target in sorted(list(model_names), key=len, reverse=True):
                                pattern = r"\b" + re.escape(target) + r"\b"
                                if re.search(pattern, th_display):
                                    th_display = re.sub(
                                        pattern, f"[{target}](#{target.lower()})", th_display
                                    )

                            # Link type aliases to their sections
                            for alias, anchor in TYPE_ALIAS_LINKS.items():
                                pattern = r"\b" + re.escape(alias) + r"\b"
                                if re.search(pattern, th_display):
                                    th_display = re.sub(
                                        pattern, f"[{alias}](#{anchor})", th_display
                                    )

                            # Auto-expand aliases in description
                            for alias, components in TYPE_ALIASES.items():
                                if alias in field.type_hint:
                                    links_list = []
                                    for c in components:
                                        if c in model_names:
                                            links_list.append(f"[{c}](#{c.lower()})")
                                        else:
                                            links_list.append(c)

                                    if links_list:
                                        prefix = (
                                            "<br>**Options:** " if desc != "-" else "**Options:** "
                                        )
                                        if desc == "-":
                                            desc = ""
                                        desc += f"{prefix}{', '.join(links_list)}"

                            lines.append(
                                f"| **{field.name}** | {th_display} | {req} | {default} | {desc} |"
                            )
                        lines.append("")

                    lines.append("---\n")

                # Skip standard processing for this group since we did custom rendering
                continue

            lines.append("---")
            lines.append("")

        # Sort models (Standard)
        def sort_key(m):
            try:
                return (0, CUSTOM_ORDER.index(m.name))
            except ValueError:
                return (1, m.name)

        models.sort(key=sort_key)

        for model in models:
            lines.append(f"### `{model.name}`")

            # Reverse Index
            if model.used_in:
                links = [f"[{u}](#{u.lower()})" for u in model.used_in]
                lines.append(f"> *Used in: {', '.join(links)}*")
                lines.append("")

            if model.docstring:
                lines.append(f"{model.docstring}\n")
                if model.group == "Transformation":
                    lines.append("[Back to Catalog](#nodeconfig)")
                    lines.append("")

            if model.fields:
                lines.append("| Field | Type | Required | Default | Description |")
                lines.append("| --- | --- | --- | --- | --- |")
                for field in model.fields:
                    req = "Yes" if field.required else "No"
                    default = f"`{field.default}`" if field.default else "-"
                    desc = field.description or "-"
                    desc = desc.replace("|", "\\|").replace("\n", " ")

                    # Cross Linking
                    th_display = field.type_hint.replace("|", "\\|")
                    # Find all model names in the type hint and link them
                    # Sort by length desc to replace longest first (avoid replacing substring)
                    for target in sorted(list(model_names), key=len, reverse=True):
                        pattern = r"\b" + re.escape(target) + r"\b"
                        if re.search(pattern, th_display):
                            th_display = re.sub(
                                pattern, f"[{target}](#{target.lower()})", th_display
                            )

                    # Link type aliases to their sections
                    for alias, anchor in TYPE_ALIAS_LINKS.items():
                        pattern = r"\b" + re.escape(alias) + r"\b"
                        if re.search(pattern, th_display):
                            th_display = re.sub(pattern, f"[{alias}](#{anchor})", th_display)

                    # Auto-expand aliases in description (to provide navigation)
                    for alias, components in TYPE_ALIASES.items():
                        if alias in field.type_hint:
                            links_list = []
                            for c in components:
                                if c in model_names:
                                    links_list.append(f"[{c}](#{c.lower()})")
                                else:
                                    links_list.append(c)

                            if links_list:
                                prefix = "<br>**Options:** " if desc != "-" else "**Options:** "
                                if desc == "-":
                                    desc = ""
                                desc += f"{prefix}{', '.join(links_list)}"

                    lines.append(
                        f"| **{field.name}** | {th_display} | {req} | {default} | {desc} |"
                    )
                lines.append("")

            lines.append("---\n")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Strip trailing whitespace from each line to match pre-commit hook behavior
    cleaned_lines = [line.rstrip() for line in lines]
    # Ensure file ends with newline (pre-commit end-of-file-fixer requirement)
    content = "\n".join(cleaned_lines)
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8")
    print(f"Configuration Manual saved to {output_path}")


if __name__ == "__main__":
    # Ensure current directory is in path for imports
    if str(Path.cwd()) not in sys.path:
        sys.path.insert(0, str(Path.cwd()))

    generate_docs()
