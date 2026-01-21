"""Demo script to show what generated documentation looks like."""

import shutil
from pathlib import Path

from odibi.config import DocsConfig, DocsIncludeConfig, DocsOutputConfig
from odibi.story.doc_generator import DocGenerator
from odibi.story.metadata import NodeExecutionMetadata, PipelineStoryMetadata

# Create realistic pipeline metadata
nodes = [
    NodeExecutionMetadata(
        node_name="load_erp_customers",
        operation="read_sql",
        status="success",
        duration=2.3,
        rows_in=0,
        rows_out=15420,
        schema_out=[
            "customer_id: int",
            "name: string",
            "email: string",
            "region: string",
            "created_at: timestamp",
        ],
        transformation_stack=["read_sql", "add_bronze_metadata"],
    ),
    NodeExecutionMetadata(
        node_name="cleanse_customers",
        operation="sql",
        status="success",
        duration=1.8,
        rows_in=15420,
        rows_out=15105,
        rows_change=-315,
        rows_change_pct=-2.0,
        schema_in=[
            "customer_id: int",
            "name: string",
            "email: string",
            "region: string",
            "created_at: timestamp",
        ],
        schema_out=[
            "customer_id: int",
            "name: string",
            "email: string",
            "region: string",
            "created_at: timestamp",
            "is_valid_email: bool",
        ],
        columns_added=["is_valid_email"],
        executed_sql=[
            """SELECT
    customer_id,
    TRIM(name) AS name,
    LOWER(email) AS email,
    region,
    created_at,
    CASE WHEN email LIKE '%@%.%' THEN true ELSE false END AS is_valid_email
FROM load_erp_customers
WHERE customer_id IS NOT NULL"""
        ],
        transformation_stack=["sql_transform", "filter_nulls"],
        validation_warnings=["315 rows filtered due to NULL customer_id"],
    ),
    NodeExecutionMetadata(
        node_name="dim_customer",
        operation="scd2",
        status="success",
        duration=4.2,
        rows_in=15105,
        rows_out=15105,
        rows_written=847,
        schema_in=[
            "customer_id: int",
            "name: string",
            "email: string",
            "region: string",
            "created_at: timestamp",
            "is_valid_email: bool",
        ],
        schema_out=[
            "customer_sk: bigint",
            "customer_id: int",
            "name: string",
            "email: string",
            "region: string",
            "is_valid_email: bool",
            "_valid_from: timestamp",
            "_valid_to: timestamp",
            "_is_current: bool",
        ],
        columns_added=["customer_sk", "_valid_from", "_valid_to", "_is_current"],
        columns_removed=["created_at"],
        description="Customer dimension with SCD Type 2 history tracking. Business key: customer_id.",
        historical_avg_duration=3.8,
        historical_avg_rows=14500.0,
    ),
]

metadata = PipelineStoryMetadata(
    pipeline_name="customer_dimension_pipeline",
    pipeline_layer="silver",
    started_at="2026-01-21T14:30:00",
    completed_at="2026-01-21T14:30:08",
    duration=8.3,
    total_nodes=3,
    completed_nodes=3,
    failed_nodes=0,
    skipped_nodes=0,
    project="Sales Analytics",
    business_unit="Commercial",
    nodes=nodes,
    git_info={"commit": "a1b2c3d4e5f6", "branch": "main", "author": "henry"},
)

# Generate docs
config = DocsConfig(
    enabled=True,
    output_path="example_docs/",
    outputs=DocsOutputConfig(),
    include=DocsIncludeConfig(schema_tables=True),
)

output_dir = Path("examples/generated_docs")
output_dir.mkdir(parents=True, exist_ok=True)

generator = DocGenerator(
    config=config,
    pipeline_name="customer_dimension_pipeline",
    workspace_root=str(output_dir.parent),
)

result = generator.generate(
    metadata,
    story_html_path="../stories/customer_dimension_pipeline/2026-01-21/run_14-30-00.html",
    run_memo_path=str(output_dir / "run_memo.md"),
)

print("Generated files:")
for name, path in result.items():
    if not name.startswith("node_card:"):
        print(f"  - {name}: {path}")

# Copy to examples for viewing
for name, path in result.items():
    src = Path(path)
    if src.exists():
        if "node_cards" in str(src):
            dest = output_dir / "node_cards" / src.name
            dest.parent.mkdir(parents=True, exist_ok=True)
        else:
            dest = output_dir / src.name
        shutil.copy(src, dest)

print(f"\nView generated docs in: {output_dir.absolute()}")
