# Azure Integration Setup Guide

This guide covers authenticating and connecting ODIBI to Azure services (ADLS Gen2, Azure SQL).

## Prerequisites

- Azure subscription
- Azure CLI installed: [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- ODIBI installed with Azure extras: `pip install "odibi[azure]"`

---

## Azure Data Lake Storage Gen2 (ADLS)

ODIBI's `AzureADLS` connection builds `abfss://` URIs for Spark and Azure Storage SDK.

### 1. Create Storage Account

```bash
# Login to Azure
az login

# Create resource group
az group create --name odibi-rg --location eastus

# Create storage account with hierarchical namespace (ADLS Gen2)
az storage account create \
  --name odibistorage \
  --resource-group odibi-rg \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Create container
az storage container create \
  --name data \
  --account-name odibistorage \
  --auth-mode login
```

### 2. Authentication Methods

#### Option A: Managed Identity (Recommended for Azure VMs/Databricks)

**Use when:** Running on Azure Databricks, Azure VMs, or Azure Functions

```yaml
connections:
  adls:
    type: azure_adls
    account: odibistorage
    container: data
    path_prefix: raw/
    auth_mode: managed_identity
```

**Setup:**
1. Enable managed identity on your Databricks cluster or VM
2. Grant **Storage Blob Data Contributor** role:
   ```bash
   az role assignment create \
     --role "Storage Blob Data Contributor" \
     --assignee-object-id <managed-identity-object-id> \
     --scope /subscriptions/<sub-id>/resourceGroups/odibi-rg/providers/Microsoft.Storage/storageAccounts/odibistorage
   ```

#### Option B: Azure CLI (Development/Local)

**Use when:** Running locally on your development machine

```yaml
connections:
  adls:
    type: azure_adls
    account: odibistorage
    container: data
    auth_mode: cli
```

**Setup:**
```bash
# Login with your Azure account
az login

# Grant yourself access (if not already granted)
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <your-email@domain.com> \
  --scope /subscriptions/<sub-id>/resourceGroups/odibi-rg/providers/Microsoft.Storage/storageAccounts/odibistorage
```

#### Option C: Service Principal (CI/CD Pipelines)

**Use when:** Running in GitHub Actions, Azure DevOps, or other automation

```yaml
connections:
  adls:
    type: azure_adls
    account: odibistorage
    container: data
    auth_mode: service_principal
    tenant_id: ${AZURE_TENANT_ID}
    client_id: ${AZURE_CLIENT_ID}
    client_secret: ${AZURE_CLIENT_SECRET}
```

**Setup:**
```bash
# Create service principal
az ad sp create-for-rbac \
  --name odibi-sp \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/<sub-id>/resourceGroups/odibi-rg/providers/Microsoft.Storage/storageAccounts/odibistorage

# Save output (tenant, appId, password) to environment variables
export AZURE_TENANT_ID="<tenant-id>"
export AZURE_CLIENT_ID="<app-id>"
export AZURE_CLIENT_SECRET="<password>"
```

**Environment file (`.env`):**
```bash
AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_SECRET=your-secret-here
```

Load in Python:
```python
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env file
```

#### Option D: Account Key via Azure Key Vault (Enterprise-Approved)

**Use when:** Managed Identity is not available but secrets are stored securely in Azure Key Vault

```yaml
connections:
  adls:
    type: azure_adls
    account: odibistorage
    container: data
    path_prefix: bronze/
    auth_mode: account_key
    key_vault_name: odibi-kv
    key_vault_secret: adls-storage-key
```

**Setup Steps:**

1. **Create Azure Key Vault:**
   ```bash
   az keyvault create \
     --name odibi-kv \
     --resource-group odibi-rg \
     --location eastus
   ```

2. **Store your storage account key securely:**
   ```bash
   # Get storage account key
   STORAGE_KEY=$(az storage account keys list \
     --resource-group odibi-rg \
     --account-name odibistorage \
     --query '[0].value' -o tsv)

   # Store in Key Vault
   az keyvault secret set \
     --vault-name odibi-kv \
     --name adls-storage-key \
     --value "$STORAGE_KEY"
   ```

3. **Assign Key Vault Secrets User role:**
   ```bash
   # For your user account (local development)
   az role assignment create \
     --role "Key Vault Secrets User" \
     --assignee <your-email@domain.com> \
     --scope /subscriptions/<sub-id>/resourceGroups/odibi-rg/providers/Microsoft.KeyVault/vaults/odibi-kv

   # For managed identity (production)
   az role assignment create \
     --role "Key Vault Secrets User" \
     --assignee-object-id <managed-identity-object-id> \
     --scope /subscriptions/<sub-id>/resourceGroups/odibi-rg/providers/Microsoft.KeyVault/vaults/odibi-kv
   ```

4. **In ODIBI, retrieve key dynamically:**
   ```python
   from azure.identity import DefaultAzureCredential
   from azure.keyvault.secrets import SecretClient

   # Authenticate using managed identity, CLI, or service principal
   credential = DefaultAzureCredential()
   client = SecretClient(
       vault_url="https://odibi-kv.vault.azure.net/",
       credential=credential
   )

   # Retrieve storage key securely
   storage_key = client.get_secret("adls-storage-key").value
   print("✅ Retrieved key securely from Key Vault")
   ```

**Note:** This pattern is **enterprise-approved** for production environments where Managed Identity is not yet available. Keys are never stored in code or configuration files.

---

## Azure SQL Database

ODIBI's `AzureSQL` connection generates ODBC connection strings for Pandas and Spark.

### 1. Create Azure SQL Database

```bash
# Create SQL server
az sql server create \
  --name odibi-sql-server \
  --resource-group odibi-rg \
  --location eastus \
  --admin-user odibiadmin \
  --admin-password '<Strong!Password123>'

# Create database
az sql db create \
  --resource-group odibi-rg \
  --server odibi-sql-server \
  --name odibi-db \
  --service-objective S0  # Standard tier

# Allow Azure services to access server
az sql server firewall-rule create \
  --resource-group odibi-rg \
  --server odibi-sql-server \
  --name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

# Allow your IP (for local development)
az sql server firewall-rule create \
  --resource-group odibi-rg \
  --server odibi-sql-server \
  --name AllowMyIP \
  --start-ip-address <your-public-ip> \
  --end-ip-address <your-public-ip>
```

### 2. Connection Configuration

#### Managed Identity (Recommended)

```yaml
connections:
  azure_sql:
    type: azure_sql
    server: odibi-sql-server.database.windows.net
    database: odibi-db
    # No username/password - uses managed identity
```

**Setup:**
1. Enable managed identity on your VM/Databricks cluster
2. Create SQL user for managed identity:
   ```sql
   CREATE USER [odibi-databricks] FROM EXTERNAL PROVIDER;
   ALTER ROLE db_datareader ADD MEMBER [odibi-databricks];
   ALTER ROLE db_datawriter ADD MEMBER [odibi-databricks];
   ```

#### SQL Authentication (Development)

```yaml
connections:
  azure_sql:
    type: azure_sql
    server: odibi-sql-server.database.windows.net
    database: odibi-db
    username: ${SQL_USER}
    password: ${SQL_PASSWORD}
```

**Environment file (`.env`):**
```bash
SQL_USER=odibiadmin
SQL_PASSWORD=<Strong!Password123>
```

### 3. Install ODBC Driver

**Windows:** Pre-installed on recent versions

**Linux (Ubuntu/Debian):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

**macOS:**
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

**Verify installation:**
```bash
odbcinst -q -d
# Should show: [ODBC Driver 18 for SQL Server]
```

---

## Permission Checklist

### For ADLS Gen2:
- [ ] Storage account created with **hierarchical namespace enabled**
- [ ] Container created (e.g., `data`)
- [ ] Identity (user/SP/managed identity) has **Storage Blob Data Contributor** role
- [ ] Firewall allows access (or set to allow all Azure services)

### For Azure SQL:
- [ ] SQL server firewall rule created for your IP/Azure services
- [ ] Database created with appropriate tier (S0 for dev, higher for prod)
- [ ] SQL user created (via managed identity or SQL auth)
- [ ] User has `db_datareader` and `db_datawriter` roles
- [ ] ODBC Driver 18 installed on client machine

### For Key Vault (if using Option D):
- [ ] Key Vault created in same resource group
- [ ] Storage account key stored as secret
- [ ] Identity has **Key Vault Secrets User** role
- [ ] Key Vault network settings allow access

---

## Path Resolution Examples

### ADLS URI Construction

```python
from odibi.connections import AzureADLS

conn = AzureADLS(
    account="odibistorage",
    container="data",
    path_prefix="bronze"
)

uri = conn.uri("sales/2024/transactions.parquet")
print(uri)
# Output: abfss://data@odibistorage.dfs.core.windows.net/bronze/sales/2024/transactions.parquet
```

### Azure SQL DSN

```python
from odibi.connections import AzureSQL

conn = AzureSQL(
    server="odibi-sql-server.database.windows.net",
    database="odibi-db"
)

dsn = conn.odbc_dsn()
print(dsn)
# Output: Driver={ODBC Driver 18 for SQL Server};Server=tcp:odibi-sql-server...;Authentication=ActiveDirectoryMsi;
```

---

## Security Best Practices

1. **Never commit secrets to Git**
   - Use `.env` files and add `.env` to `.gitignore`
   - Use environment variables in CI/CD pipelines
   - Use Azure Key Vault for production secrets

2. **Use Managed Identity when possible**
   - No credentials to rotate or leak
   - Automatic authentication in Azure environments
   - Simplifies deployment and configuration

3. **Least Privilege Access**
   - Grant only `Storage Blob Data Reader` if pipelines are read-only
   - Use separate service principals for dev/staging/prod
   - Regularly audit role assignments

4. **Rotate Secrets Regularly**
   - Rotate service principal secrets every 90 days
   - Use Azure Key Vault secret rotation policies
   - Monitor for expired credentials

5. **Enable Audit Logging**
   - Enable diagnostic settings on storage accounts
   - Track who accessed what data and when
   - Set up alerts for suspicious activity

---

## Troubleshooting

### Error: "This request is not authorized to perform this operation"

**Cause:** Missing **Storage Blob Data Contributor** role

**Fix:**
```bash
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee <user-or-sp-object-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>
```

### Error: "Cannot open server ... requested by the login"

**Cause:** Firewall blocking connection to Azure SQL

**Fix:**
```bash
# Get your public IP
curl ifconfig.me

# Add firewall rule
az sql server firewall-rule create \
  --resource-group odibi-rg \
  --server odibi-sql-server \
  --name AllowMyIP \
  --start-ip-address <your-ip> \
  --end-ip-address <your-ip>
```

### Error: "No module named 'azure.storage.blob'"

**Fix:**
```bash
pip install "odibi[azure]"
```

### Error: "Client does not have permission to perform action on Key Vault"

**Cause:** Missing **Key Vault Secrets User** role

**Fix:**
```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <user-or-managed-identity-object-id> \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name>
```

---

## Phase 1 Status

**Currently Available (Phase 1):**
- ✅ ADLS path/URI resolution
- ✅ Azure SQL DSN string generation
- ✅ Connection validation and configuration
- ✅ Key Vault integration pattern documented

**Coming in Phase 3:**
- ⏳ Actual read/write operations via `SparkEngine`
- ⏳ Integration with Pandas engine for Azure SQL
- ⏳ Support for Azure Blob Storage (non-ADLS)

---

## Next Steps

- See [setup_databricks.md](setup_databricks.md) for Databricks cluster setup
- Check [examples/example_spark.yaml](../examples/example_spark.yaml) for Azure pipeline examples
- Review [PHASES.md](../PHASES.md) for Azure feature roadmap

---

**Questions?** Open an issue: https://github.com/henryodibi11/Odibi/issues
