import os

TEMPLATES = {
    "local-medallion": {
        "description": "Pandas + Local Parquet/Delta Medallion Architecture",
        "structure": {
            "data/landing": {},
            "data/raw": {},
            "data/silver": {},
            "data/gold": {},
            "stories": {},
        },
        "config": """
project: {project_name}
engine: pandas

# Performance Tuning (Added in v2.2)
performance:
  use_arrow: true

connections:
  local_lake:
    type: local
    base_path: ./data

story:
  connection: local_lake
  path: ../stories/

pipelines:
  - pipeline: ingestion
    description: Ingest data from landing to raw
    layer: bronze
    nodes:
      - name: ingest_customers
        description: Ingest customers CSV to Parquet
        read:
          connection: local_lake
          path: landing/customers.csv
          format: csv
        write:
          connection: local_lake
          path: raw/customers.parquet
          format: parquet
          mode: overwrite

  - pipeline: refinement
    description: Clean and upsert to Silver
    layer: silver
    nodes:
      - name: clean_customers
        description: Deduplicate and merge customers
        read:
          connection: local_lake
          path: raw/customers.parquet
          format: parquet
        transformer: merge
        params:
          target_connection: local_lake
          target_path: silver/customers.delta
          merge_keys: ["id"]
          strategy: upsert
          # Performance Optimization (Added in v2.2)
          # optimize_write: true
          # zorder_by: ["region"] # Only for Spark engine
""",
    },
    "azure-delta": {
        "description": "Spark + Azure Data Lake Storage + Delta Lake",
        "structure": {
            "stories": {},
        },
        "config": """
project: {project_name}
engine: spark

# Performance Tuning (Added in v2.2)
performance:
  use_arrow: true

connections:
  # Example ADLS connection (requires env vars)
  adls_primary:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}
    container: data
    auth:
      sas_token: ${AZURE_SAS_TOKEN}

  # Spark Delta Catalog
  delta_catalog:
    type: delta
    catalog: spark_catalog
    schema: default

story:
  connection: delta_catalog
  path: /stories/

pipelines:
  - pipeline: raw_ingestion
    description: Ingest raw data
    layer: bronze
    nodes:
      - name: json_to_delta
        read:
          connection: adls_primary
          path: landing/events/
          format: json
        write:
          connection: delta_catalog
          table: raw_events
          format: delta
          mode: append
          options:
            optimize_write: true  # Compaction
            # cluster_by: [event_type] # Liquid Clustering (if creating new table)

  - pipeline: aggregation
    description: Aggregate events
    layer: gold
    nodes:
      - name: daily_summary
        read:
          connection: delta_catalog
          table: raw_events
          format: delta
        transform:
          steps:
            - sql: |
                SELECT 
                  date(timestamp) as event_date, 
                  count(*) as event_count
                FROM table
                GROUP BY 1
        write:
          connection: delta_catalog
          table: daily_stats
          format: delta
          mode: overwrite
""",
    },
    "reference-lite": {
        "description": "Minimal example for quick testing",
        "structure": {
            "output": {},
            "stories": {},
        },
        "config": """
project: {project_name}
engine: pandas

connections:
  local:
    type: local
    base_path: .

story:
  connection: local
  path: stories/

pipelines:
  - pipeline: minimal
    nodes:
      - name: hello_world
        transform:
          steps:
            - sql: "SELECT 'Hello' as message, 1 as id"
        write:
          connection: local
          path: output/hello.csv
          format: csv
          mode: overwrite
""",
    },
}


def add_init_pipeline_parser(subparsers):
    """Add parser for init-pipeline command."""
    parser = subparsers.add_parser(
        "init-pipeline", help="Initialize a new pipeline project from a template"
    )
    parser.add_argument("name", help="Name of the project/directory")
    parser.add_argument(
        "--template",
        choices=TEMPLATES.keys(),
        default="local-medallion",
        help="Project template to use",
    )
    parser.add_argument(
        "--force", action="store_true", help="Overwrite existing directory if it exists"
    )


def init_pipeline_command(args):
    """Execute init-pipeline command."""
    project_name = args.name
    template_name = args.template
    template = TEMPLATES[template_name]

    print(f"Initializing project '{project_name}' using template '{template_name}'...")

    if os.path.exists(project_name) and not args.force:
        print(f"Error: Directory '{project_name}' already exists. Use --force to overwrite.")
        return 1

    if not os.path.exists(project_name):
        os.makedirs(project_name)

    # Create directory structure
    for path in template["structure"]:
        full_path = os.path.join(project_name, path)
        os.makedirs(full_path, exist_ok=True)

    # Create odibi.yaml
    config_content = template["config"].strip().format(project_name=project_name)
    with open(os.path.join(project_name, "odibi.yaml"), "w") as f:
        f.write(config_content)

    # Create README
    with open(os.path.join(project_name, "README.md"), "w") as f:
        f.write(f"# {project_name}\n\n")
        f.write(f"Generated with Odibi template: {template_name}\n")
        f.write(f"{template['description']}\n\n")
        f.write("## Usage\n\n")
        f.write("Run the pipeline:\n")
        f.write("```bash\n")
        f.write("odibi run odibi.yaml\n")
        f.write("```\n")

    # Create .gitignore
    with open(os.path.join(project_name, ".gitignore"), "w") as f:
        f.write("__pycache__/\n")
        f.write("*.pyc\n")
        f.write(".env\n")
        f.write("data/\n")
        f.write("stories/\n")
        f.write("output/\n")

    print(f"Project '{project_name}' created successfully!")
    print(f"\nNext steps:\n  cd {project_name}\n  odibi run odibi.yaml")
    return 0
