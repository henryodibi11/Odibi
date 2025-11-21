import os


TEMPLATE = """# =============================================================================
# ODIBI CONFIGURATION TEMPLATE
# =============================================================================
# This file contains a comprehensive reference of all Odibi features.
# Uncomment the sections you need.
#
# Tips:
# - Use ${ENV_VAR} to reference environment variables (e.g., secrets).
# - Hover over keys in VS Code for documentation (if extension is configured).
# =============================================================================

project: my_new_project
version: "0.1.0"
description: "A data pipeline project"
owner: "Data Team"

# Execution Engine
# 'pandas': Good for local dev and small data (<10GB)
# 'spark':  Good for big data (Databricks, Synapse)
engine: pandas

# ==========================================
# Global Settings (Optional)
# ==========================================

# Retry Policy (for network/connection flakiness)
retry:
  enabled: true
  max_attempts: 3
  # Backoff Strategies:
  # - exponential: 1s, 2s, 4s, 8s... (Recommended for APIs)
  # - linear: 1s, 2s, 3s, 4s...
  # - constant: 1s, 1s, 1s... (Fast retries)
  backoff: exponential

# Logging Configuration
logging:
  level: INFO           # Options: DEBUG, INFO, WARNING, ERROR
  structured: false     # Set true for JSON logs (Splunk/Datadog)
  # metadata: {env: "prod", region: "us-east"}  # Extra fields for JSON logs

# Alerting (Webhooks)
# alerts:
#   - type: slack  # or 'teams', 'webhook'
#     url: ${SLACK_WEBHOOK_URL}
#     on_events: [on_failure]  # Options: on_start, on_success, on_failure
#     metadata: {channel: "#data-alerts"}

# ==========================================
# 1. Connections (Data Sources)
# ==========================================
connections:
  # --- Type: Local Filesystem ---
  local_data:
    type: local
    base_path: ./data

  # --- Type: Azure Data Lake Gen2 ---
  # my_datalake:
  #   type: azure_blob
  #   account_name: ${AZURE_STORAGE_ACCOUNT}
  #   container: raw-data
  #   # Validation Mode:
  #   # - lazy (default): Checks connection only when used. Faster startup.
  #   # - eager: Checks connection at startup. Fails fast if config is wrong.
  #   validation_mode: eager
  #
  #   # Auth Option 1: Default (Recommended)
  #   # Uses Environment Variables (AZURE_CLIENT_ID...) or Managed Identity
  #   # No extra config needed here.
  #
  #   # Auth Option 2: Explicit Access Key
  #   # auth:
  #   #   account_key: ${AZURE_STORAGE_KEY}
  #
  #   # Auth Option 3: SAS Token
  #   # auth:
  #   #   sas_token: ${AZURE_SAS_TOKEN}
  #
  #   # Auth Option 4: Secure Key Vault (Account Key)
  #   # auth:
  #   #   key_vault_name: my-keyvault
  #   #   secret_name: adls-account-key
  #
  #   # Auth Option 5: Secure Key Vault (SAS Token)
  #   # auth:
  #   #   auth_mode: sas_token
  #   #   key_vault_name: my-keyvault
  #   #   secret_name: adls-sas-token
  #
  #   # Note: 'account_name' is not sensitive (it's part of the URL).
  #   # You can hardcode it string or use an env var.

  # --- Type: Delta Lake (Spark/Databricks) ---
  # # Uses the Hive Metastore / Unity Catalog directly.
  # spark_catalog:
  #   type: delta
  #   catalog: hive_metastore  # Spark Only
  #   schema: default          # Spark Only

  # --- Type: Delta Lake (Pandas/Local) ---
  # # Uses direct file paths to read Delta tables.
  # local_delta:
  #   type: delta
  #   path: ./data/delta_tables  # Path-based for Pandas


  # --- Type: SQL Server / Azure SQL ---
  # # Host, port, and database are configuration, not secrets.
  # # Only username/password need protection (via Auth Option 2).
  # my_db:
  #   type: sql_server
  #   host: myserver.database.windows.net
  #   database: production_db
  #   port: 1433
  #   auth:
  #     username: ${DB_USER}
  #     password: ${DB_PASS}
  #
  #   # Auth Option 2: Key Vault (Secure Password)
  #   # auth:
  #   #   username: ${DB_USER}
  #   #   key_vault_name: my-keyvault
  #   #   secret_name: db-password

  # --- Type: HTTP (API) ---
  # public_api:
  #   type: http
  #   base_url: https://api.example.com/v1
  #   headers: {Authorization: "Bearer ${API_KEY}"}

# ==========================================
# 2. Pipelines (Logic)
# ==========================================
pipelines:
  - pipeline: main_etl
    description: "Example ETL pipeline"
    layer: silver  # Optional tag: bronze, silver, gold
    nodes:
      # ---------------------------------------------------------
      # Node 1: Read Data
      # ---------------------------------------------------------
      - name: read_input
        description: "Ingest raw data"
        
        # --- Scenario A: File-Based Read (CSV, Parquet, JSON) ---
        read:
          connection: local_data
          format: csv
          path: input.csv
          options: {header: true, sep: ","}

        # --- Scenario B: Table-Based Read (SQL, Delta Catalog) ---
        # read:
        #   connection: my_db  # or spark_catalog
        #   format: sql_server # or delta
        #   table: sales.transactions
        #   options: {fetch_size: 1000}
        
        # Performance: Cache result in memory if multiple nodes depend on it
        cache: false
        
        # Debugging: Override log level for this specific node
        # log_level: DEBUG

      # ---------------------------------------------------------
      # Node 2: Transform & Validate
      # ---------------------------------------------------------
      - name: clean_data
        depends_on: [read_input]
        
        transform:
          steps:
            # --- Option A: SQL (Most Common) ---
            - sql: "SELECT * FROM read_input WHERE amount > 0"

            # --- Option B: Built-in Operations (Engine Agnostic) ---
            # 1. Drop Duplicates
            # - operation: drop_duplicates
            #   params: {subset: ["id"], keep: "first"}
            
            # 2. Fill Nulls
            # - operation: fillna
            #   params: {value: 0, subset: ["amount"]}
            
            # 3. Drop Columns
            # - operation: drop
            #   params: {columns: ["temp_col"]}
            
            # 4. Rename Columns
            # - operation: rename
            #   params: {columns: {"old_name": "new_name"}}

            # --- Option C: Custom Python (Advanced) ---
            # 1. Create a file named 'transforms.py' in this folder.
            # 2. Add:
            #    from odibi import transform
            #    @transform
            #    def clean_currency(context, current, currency_code):
            #        return current
            # 3. Reference it here:
            # - function: clean_currency
            #   params: {currency_code: "USD"}
        
        # Data Quality Checks (Runs AFTER transform)
        validation:
          not_empty: true
          no_nulls: [id, transaction_date]
          
          # Schema Enforcement (Check column types)
          # schema:
          #   id: int
          #   amount: float
          #   name: string
          
          ranges:
            amount: {min: 0, max: 1000000}
          allowed_values:
            status: [completed, pending, failed]
        
        # Error Handling Strategy
        on_error: fail_fast  # Options: fail_fast (stop), fail_later (skip dependents), ignore

      # ---------------------------------------------------------
      # Node 3: Write & Protect
      # ---------------------------------------------------------
      - name: write_silver
        depends_on: [clean_data]
        
        # Privacy: Mask these columns in logs/stories (PII Protection)
        sensitive: [email, phone_number, ssn]
        # sensitive: true  # Masks ALL data samples
        
        # --- Scenario A: File-Based Write ---
        write:
          connection: local_data
          format: parquet
          path: silver/transactions.parquet
          # Modes: overwrite, append, error, ignore
          mode: overwrite

        # --- Scenario B: Table-Based Write (Delta/Spark) ---
        # write:
        #   connection: spark_catalog
        #   format: delta
        #   table: silver.transactions
        #   mode: append
        #   options:
        #     mergeSchema: true
        #     partitionBy: ["date", "region"]  # Physical partition layout
        #     overwriteSchema: false

        # --- Scenario C: Partitioned File Write ---
        # write:
        #   connection: my_datalake
        #   format: parquet
        #   path: silver/partitioned_data/
        #   mode: overwrite
        #   options:
        #     partitionBy: ["year", "month"]

# ==========================================
# 3. Data Story (Documentation/Audit)
# ==========================================
# Stories are Markdown reports generated after every run.
# They contain execution metadata, row counts, and data samples.
story:
  connection: local_data  # Where to save the report
  path: odibi_stories/
  max_sample_rows: 10     # Number of sample rows to capture
  auto_generate: true
  retention_days: 30      # Delete stories older than 30 days
  retention_count: 100    # Keep only the last 100 stories (deletes oldest first)

# ==========================================
# 4. Environments (Experimental/Roadmap)
# ==========================================
# Environments allow overriding settings for specific contexts (prod vs dev).
# Currently in active development (Phase 3).
# environments:
#   production:
#     logging: {level: WARNING}
#     retry: {max_attempts: 5}
"""


def create_command(args):
    """Create a new Odibi configuration file from template."""
    filename = args.name

    # Enforce extension convention if missing
    if not filename.endswith(".yaml") and not filename.endswith(".yml"):
        filename = f"{filename}.odibi.yaml"

    # Check if file exists to prevent accidental overwrite
    if os.path.exists(filename) and not args.force:
        print(f"❌ Error: File '{filename}' already exists.")
        print("   Use --force to overwrite it.")
        return 1

    try:
        with open(filename, "w") as f:
            f.write(TEMPLATE)
        print(f"✅ Created configuration file: {filename}")
        print(f"   Run: odibi run {filename}")
        return 0
    except Exception as e:
        print(f"❌ Failed to create file: {e}")
        return 1


def add_create_parser(subparsers):
    """Add create subcommand to argument parser."""
    parser = subparsers.add_parser("create", help="Create a new config file from template")
    parser.add_argument("name", help="Name of the config file (e.g. 'marketing')")
    parser.add_argument("--force", "-f", action="store_true", help="Overwrite existing file")
    return parser
