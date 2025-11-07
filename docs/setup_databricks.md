# Databricks Setup Guide

This guide explains how to use ODIBI with Databricks (Community Edition or Azure Databricks).

## Prerequisites

- Databricks account (Community Edition or Azure)
- Python 3.9+ installed locally
- ODIBI installed with Spark extras: `pip install "odibi[spark]"`

---

## Databricks Community Edition

**Best for:** Learning, prototyping, local development

### 1. Create a Free Account

Visit [community.cloud.databricks.com](https://community.cloud.databricks.com) and sign up.

### 2. Create a Cluster

1. Click **Compute** in the sidebar
2. Click **Create Cluster**
3. Configure:
   - **Cluster Name:** odibi-dev
   - **Runtime:** 13.3 LTS (includes Apache Spark 3.4)
   - **Node Type:** Single node (default for Community Edition)
4. Click **Create Cluster**
5. Wait 3-5 minutes for cluster to start

### 3. Upload ODIBI Code

**Option A: Upload as ZIP**
1. Package your project: `zip -r odibi_project.zip .`
2. In Databricks, go to **Workspace** → **Upload** → **Upload Files**
3. Extract in a notebook: `%sh unzip /FileStore/odibi_project.zip -d /tmp/odibi`

**Option B: Use Repos (Databricks 11.0+)**
1. Go to **Repos** in the sidebar
2. Click **Add Repo**
3. Paste your GitHub URL: `https://github.com/henryodibi11/Odibi`
4. Click **Create Repo**

### 4. Install ODIBI in Cluster

Create a notebook and run:

```python
# Install ODIBI with Spark extras
%pip install git+https://github.com/henryodibi11/Odibi.git

# Restart Python kernel to load new packages
dbutils.library.restartPython()
```

### 5. Run Your First Pipeline

```python
import yaml
from odibi.pipeline import Pipeline
from odibi.config import PipelineConfig
from odibi.connections import LocalDBFS

# Define config
config_yaml = """
project: Databricks Demo
engine: spark

connections:
  dbfs:
    type: local_dbfs
    root: /FileStore/odibi_data

pipelines:
  - name: bronze_to_silver
    nodes:
      - name: load_bronze
        read:
          connection: dbfs
          path: bronze/sales.csv
          format: csv
          options:
            header: true
        cache: true

      - name: clean_data
        depends_on: [load_bronze]
        transform:
          steps:
            - |
              SELECT
                transaction_id,
                customer_id,
                CAST(amount AS DOUBLE) as amount,
                CAST(timestamp AS TIMESTAMP) as timestamp
              FROM load_bronze
              WHERE amount > 0

      - name: write_silver
        depends_on: [clean_data]
        write:
          connection: dbfs
          path: silver/sales.parquet
          format: parquet
          mode: overwrite
"""

# Parse and run
config = yaml.safe_load(config_yaml)
pipeline_config = PipelineConfig(**config['pipelines'][0])
pipeline = Pipeline.from_config(pipeline_config, project_config=config)
result = pipeline.run()

print(f"✅ Pipeline completed: {result.status}")
```

---

## Azure Databricks

**Best for:** Production workloads, enterprise deployments

### 1. Create Azure Databricks Workspace

```bash
# Using Azure CLI
az databricks workspace create \
  --resource-group odibi-rg \
  --name odibi-workspace \
  --location eastus \
  --sku premium
```

Or use the Azure Portal:
1. Search for "Databricks"
2. Click **Create** → **Azure Databricks Service**
3. Fill in details and create

### 2. Create Cluster with Service Principal Auth

1. Go to **Compute** → **Create Cluster**
2. Configure:
   - **Runtime:** 13.3 LTS or higher
   - **Node Type:** Standard_DS3_v2 (or higher for production)
   - **Autoscaling:** Enable (min 2, max 8 workers)
   - **Terminate after:** 60 minutes of inactivity

3. **Add Spark Config** (for Azure ADLS access):
   ```
   spark.hadoop.fs.azure.account.auth.type OAuth
   spark.hadoop.fs.azure.account.oauth.provider.type org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
   spark.hadoop.fs.azure.account.oauth2.client.id <your-client-id>
   spark.hadoop.fs.azure.account.oauth2.client.secret <your-client-secret>
   spark.hadoop.fs.azure.account.oauth2.client.endpoint https://login.microsoftonline.com/<tenant-id>/oauth2/token
   ```

### 3. Install ODIBI via Cluster Library

1. Go to **Compute** → Select your cluster
2. Click **Libraries** tab
3. Click **Install New**
4. Select **PyPI**
5. Enter package: `git+https://github.com/henryodibi11/Odibi.git`
6. Click **Install**
7. Wait for status: **Installed**

### 4. Use Databricks Secrets for Credentials

```python
# Store secrets (one-time setup)
dbutils.secrets.put(scope="odibi-secrets", key="storage-account-key", value="<key>")

# Reference in ODIBI config
config_yaml = """
project: Production Pipeline
engine: spark

connections:
  adls:
    type: azure_adls
    account: mystorageaccount
    container: data
    path_prefix: bronze/
    auth_mode: managed_identity  # Uses cluster's managed identity
"""
```

---

## DBFS Path Resolution

ODIBI's `LocalDBFS` connection maps `dbfs:/` paths to local filesystem for development:

```python
from odibi.connections import LocalDBFS

conn = LocalDBFS(root="/FileStore/odibi_data")
path = conn.resolve("dbfs:/bronze/sales.csv")
print(path)  # /FileStore/odibi_data/bronze/sales.csv
```

In production with Spark, use `dbfs:/` paths directly:
```yaml
read:
  path: dbfs:/mnt/data/bronze/sales.parquet
  format: parquet
```

---

## Troubleshooting

### Import Error: `pyspark` not found

**Solution:** Install Spark extras:
```bash
%pip install "odibi[spark]"
dbutils.library.restartPython()
```

### ADLS Access Denied

**Check:**
1. Service Principal has **Storage Blob Data Contributor** role
2. Spark config contains correct `client.id`, `client.secret`, `tenant-id`
3. Storage account firewall allows Databricks IPs

### Cluster Startup Slow

**Normal:** First startup takes 3-5 minutes to provision VMs and download libraries. Subsequent starts are faster (~1 min).

---

## Best Practices

1. **Use Repos for Code**: Sync code via Git instead of uploading manually
2. **Store Secrets Safely**: Never hardcode credentials; use Databricks Secrets
3. **Enable Autoscaling**: Let Databricks scale workers based on workload
4. **Use Delta Lake**: For ACID transactions and time travel: `format: delta`
5. **Monitor Costs**: Set auto-termination to avoid idle cluster charges

---

## Next Steps

- See [setup_azure.md](setup_azure.md) for Azure ADLS authentication details
- Check [examples/example_spark.yaml](../examples/example_spark.yaml) for complete pipeline examples
- Review [PHASES.md](../PHASES.md) to see Spark feature roadmap (Phase 3)

---

**Questions?** Open an issue: https://github.com/henryodibi11/Odibi/issues
