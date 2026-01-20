"""Scaffold a new Odibi project with the required directory structure and YAML templates.

This module provides a function `scaffold_odibi_project` that can be called from Python code.

Example:
    from scaffold_odibi_project import scaffold_odibi_project
    scaffold_odibi_project("OEE", "/Workspace/Repos/henry.odibi@ingredion.com/Dev Work/OEE")

Args:
    project_name (str): The name of the project (e.g., "OEE").
    path (str): The absolute path for the project root (Windows or Unix-style).

Raises:
    Exception: For any file or directory creation errors.
"""

from pathlib import Path

PROJECT_YAML_TEMPLATE = """\
project: {project_name}
engine: spark
version: "0.1.0"
owner: "Henry Odibi"
alerts:
  # Runs Alerts
  - type: teams
    url: "${{kv_data_runs}}"
    on_events:
      - on_start
      - on_success
    metadata:
      throttle_minutes: 1
      max_per_hour: 10
  # Quality Alerts
  - type: teams
    url: "${{kv_data_quality}}"
    on_events:
      - on_quarantine
      - on_threshold_breach
    metadata:
      throttle_minutes: 1
      max_per_hour: 10
      mention: "henry.odibi@ingredion.com"
      mention_on_failure: "henry.odibi@ingredion.com"
  # Critical Alerts
  - type: teams
    url: "${{kv_data_critical}}"
    on_events:
      - on_failure
      - on_gate_block
    metadata:
      throttle_minutes: 1
      max_per_hour: 10
      mention: "henry.odibi@ingredion.com"
      mention_on_failure: "henry.odibi@ingredion.com"
connections:
  goat_dm:
    type: azure_blob
    account_name: ${{PROD_AZURE_STORAGE_ACCOUNT}}
    container: digital-manufacturing
    validation_mode: eager
    auth:
      mode: direct_key
      account_key: ${{PROD_AZURE_STORAGE_KEY}}
  goat_datalake:
    type: azure_blob
    account_name: ${{QAT_AZURE_STORAGE_ACCOUNT}}
    container: datalake
    validation_mode: eager
    auth:
      mode: direct_key
      account_key: ${{QAT_AZURE_STORAGE_KEY}}
  opsvisdata:
    type: sql_server
    host: opsvisdbqat.database.windows.net
    database: opsvisdata
    port: 1433
    auth:
      mode: sql_login
      username: ${{opsvisdata_DB_USER}}
      password: ${{opsvisdata_DB_PASS}}
  nkcmfgproduction:
    type: sql_server
    host: na-operations.c7ddf5d4058b.database.windows.net
    database: nkcmfgproduction
    port: 1433
    auth:
      mode: sql_login
      username: ${{datapro_DB_USER}}
      password: ${{datapro_DB_PASS}}
  indy_production:
    type: sql_server
    host: na-operations.c7ddf5d4058b.database.windows.net
    database: indyProduction
    port: 1433
    auth:
      mode: sql_login
      username: ${{datapro_DB_USER}}
      password: ${{datapro_DB_PASS}}
  globaldigitalops:
    type: sql_server
    host: globaldigitalops.database.windows.net
    database: globaldigitalops
    port: 1433
    auth:
      mode: sql_login
      username: ${{globaldigitalops_DB_USER}}
      password: ${{globaldigitalops_DB_PASS}}
  goat_sql:
    type: sql_server
    host: globaldigitalops.database.windows.net
    database: goat_qat
    port: 1433
    auth:
      mode: sql_login
      username: ${{globaldigitalops_DB_USER}}
      password: ${{globaldigitalops_DB_PASS}}
system:
  connection: goat_datalake
  environment: qat
  sync_to:
    connection: goat_sql
    schema_name: odibi_system
  cost_per_compute_hour: 2
performance:
  use_arrow: true
  skip_null_profiling: false
  skip_catalog_writes: false
  skip_run_logging: false
  delta_table_properties:
    delta.autoOptimize.optimizeWrite: "true"
    delta.autoOptimize.autoCompact: "true"
    delta.minReaderVersion: '2'
    delta.minWriterVersion: '5'
    delta.columnMapping.mode: "name"
  spark_config:
    "spark.databricks.io.cache.enabled": "true"
    "spark.sql.shuffle.partitions": "200"
    "spark.sql.adaptive.enabled": "true"
    "spark.databricks.delta.optimizeWrite.enabled": "true"
retry:
  enabled: true
  max_attempts: 1
  backoff: exponential
logging:
  level: WARNING
  structured: True
story:
  connection: goat_datalake
  path: {project_name}/Stories/
  generate_lineage: True
  retention_days: 30
  retention_count: 100
  auto_generate: True
  max_sample_rows: 10
  async_generation: True
imports:
  - pipelines/bronze/bronze.yaml
  - pipelines/silver/silver.yaml
  - pipelines/gold/gold.yaml
  - semantic/semantic_{project_name}.yaml
vars:
  catalog: {project_name}_qat
environments:
  prod:
    connections:
      goat_sql:
        type: sql_server
        host: globaldigitalops.database.windows.net
        database: goat_prod
        port: 1433
        auth:
          mode: sql_login
          username: ${{globaldigitalops_DB_USER}}
          password: ${{globaldigitalops_DB_PASS}}
      goat_datalake:
        type: azure_blob
        account_name: ${{PROD_AZURE_STORAGE_ACCOUNT}}
        container: datalake
        validation_mode: eager
        auth:
          mode: direct_key
          account_key: ${{PROD_AZURE_STORAGE_KEY}}
    system:
      connection: goat_datalake
      environment: prod
      sync_to:
        connection: goat_sql
        schema_name: odibi_system
    vars:
      catalog: {project_name}_prod
"""

BRONZE_YAML_SAMPLE = """\
pipelines:
  - pipeline: bronze
    description: "Bronze: Read IPS S Curve from SharePoint Excel files"
    layer: bronze
    nodes: []
"""

SILVER_YAML_SAMPLE = """\
pipelines:
  - pipeline: silver
    description: "Silver: Transform and clean data for analytics"
    layer: silver
    nodes: []
"""

GOLD_YAML_SAMPLE = """\
pipelines:
  - pipeline: gold
    description: "Gold: Aggregate and prepare data for reporting"
    layer: gold
    nodes: []
"""

SEMANTIC_YAML_SAMPLE = """\
semantic:
  # Connection for creating views (must be SQL Server)
  connection: goat_sql

  # Optional: save generated SQL files here for documentation
  sql_output_path: "{project_name}/semantic/views/"

  # ---------------------------------------------------------------------------
  # COMPONENT METRICS (Additive - safe to SUM across any dimension)
  # ---------------------------------------------------------------------------
"""


def scaffold_odibi_project(project_name: str, path: str) -> None:
    """
    Scaffold a new Odibi project with the required directory structure and YAML templates.

    Args:
        project_name (str): The name of the project (e.g., "OEE").
        path (str): The absolute path for the project root (Windows or Unix-style).

    Raises:
        Exception: For any file or directory creation errors.
    """
    root_path = Path(path).resolve()

    pipelines_dir = root_path / "pipelines"
    bronze_dir = pipelines_dir / "bronze"
    silver_dir = pipelines_dir / "silver"
    gold_dir = pipelines_dir / "gold"
    semantic_dir = root_path / "semantic"

    try:
        # Create directories
        for d in [bronze_dir, silver_dir, gold_dir]:
            d.mkdir(parents=True, exist_ok=True)
        (bronze_dir / "sql").mkdir(parents=True, exist_ok=True)
        (silver_dir / "sql").mkdir(parents=True, exist_ok=True)
        (gold_dir / "sql").mkdir(parents=True, exist_ok=True)
        semantic_dir.mkdir(parents=True, exist_ok=True)

        # Create YAML files with sample content
        _create_yaml_file(bronze_dir / "bronze.yaml", BRONZE_YAML_SAMPLE)
        _create_yaml_file(silver_dir / "silver.yaml", SILVER_YAML_SAMPLE)
        _create_yaml_file(gold_dir / "gold.yaml", GOLD_YAML_SAMPLE)
        _create_yaml_file(
            semantic_dir / f"semantic_{project_name}.yaml",
            SEMANTIC_YAML_SAMPLE.format(project_name=project_name),
        )

        # Project YAML
        project_yaml_path = root_path / f"{project_name}.yaml"
        project_yaml_content = PROJECT_YAML_TEMPLATE.format(project_name=project_name)
        _create_yaml_file(project_yaml_path, project_yaml_content)
    except Exception as e:
        raise Exception(f"Failed to scaffold Odibi project: {e}") from e


def _create_yaml_file(path: Path, content: str = "") -> None:
    """Create a YAML file with optional content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
