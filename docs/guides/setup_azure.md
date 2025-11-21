# Azure Integration Setup Guide (v2.1.0)

This guide covers authenticating and connecting Odibi to Azure services (ADLS Gen2, Azure SQL, Key Vault) using the latest **v2.1.0** standards.

**Key Features in v2.1.0:**
- **Auto-Auth**: Zero-config authentication using Managed Identity or Environment Variables (`DefaultAzureCredential`).
- **Universal Key Vault**: Retrieve ANY secret (Account Key, SAS Token, SQL Password) from Key Vault by referencing it in the config.

---

## Prerequisites

- Azure subscription
- Azure CLI installed: [Install Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- Odibi installed with Azure extras: `pip install "odibi[azure]"`

---

## 1. Azure Data Lake Storage Gen2 (ADLS)

Use the `azure_blob` connection type.

### Configuration Patterns

#### Option A: Auto-Auth (Recommended)
**Best for:** Production (Managed Identity) or Local Dev (Azure CLI login).
**How it works:** Odibi uses `DefaultAzureCredential` to find your identity automatically. No secrets in the config!

```yaml
connections:
  my_datalake:
    type: azure_blob
    account_name: "odibistorage"
    container: "data"
    # No 'auth' section needed!
    # Odibi will automatically try Managed Identity, CLI, or Env Vars.
```

**Setup:**
1. **Azure:** Grant your identity (User or Managed Identity) the **Storage Blob Data Contributor** role on the storage account.
2. **Local:** Run `az login`.
3. **Production:** Assign Managed Identity to the VM/Function/Databricks cluster.

#### Option B: Universal Key Vault (Account Key)
**Best for:** Scenarios where Managed Identity is not possible.
**How it works:** Store the Account Key in Key Vault, and tell Odibi where to find it.

```yaml
connections:
  my_datalake:
    type: azure_blob
    account_name: "odibistorage"
    container: "data"
    auth:
      key_vault_name: "my-keyvault"
      secret_name: "adls-account-key"  # The secret containing the Account Key
```

#### Option C: Universal Key Vault (SAS Token)
**Best for:** Restricted access with SAS tokens.

```yaml
connections:
  my_datalake:
    type: azure_blob
    account_name: "odibistorage"
    container: "data"
    auth:
      key_vault_name: "my-keyvault"
      secret_name: "adls-sas-token"  # The secret containing the SAS Token
```

---

## 2. Azure SQL Database

Use the `sql_server` connection type.

### Configuration Patterns

#### Option A: Auto-Auth (Managed Identity)
**Best for:** Production pipelines running in Azure.

```yaml
connections:
  my_sql_db:
    type: sql_server
    host: "odibi-sql.database.windows.net"
    database: "analytics_db"
    port: 1433
    auth: {}  # Empty auth dict signals "Use Default Driver Auth / Managed Identity"
```

**Setup:**
1. Enable Managed Identity on your compute resource.
2. In Azure SQL, create a user for the identity: `CREATE USER [my-identity] FROM EXTERNAL PROVIDER;`.
3. Grant permissions: `ALTER ROLE db_datareader ADD MEMBER [my-identity];`.

#### Option B: Universal Key Vault (SQL Password)
**Best for:** Legacy SQL Auth (Username/Password).

```yaml
connections:
  my_sql_db:
    type: sql_server
    host: "odibi-sql.database.windows.net"
    database: "analytics_db"
    port: 1433
    auth:
      username: "sqladmin"
      key_vault_name: "my-keyvault"
      secret_name: "sql-password"  # The secret containing the password
```

---

## 3. Key Vault Setup

If you use Key Vault references, Odibi needs to authenticate *to the Key Vault* first. It uses **Auto-Auth** for this too!

1. **Create Key Vault:**
   ```bash
   az keyvault create --name my-keyvault --resource-group my-rg
   ```

2. **Grant Access:**
   Grant your identity (User or Managed Identity) the **Key Vault Secrets User** role.
   ```bash
   az role assignment create \
     --role "Key Vault Secrets User" \
     --assignee <your-email-or-identity-id> \
     --scope /subscriptions/.../resourceGroups/my-rg/providers/Microsoft.KeyVault/vaults/my-keyvault
   ```

3. **Store Secrets:**
   ```bash
   az keyvault secret set --vault-name my-keyvault --name adls-account-key --value "your-key-here"
   ```

---

## Summary of Changes (v2.0 -> v2.1)

| Feature | v2.0 (Old) | v2.1 (New) |
| :--- | :--- | :--- |
| **Connection Type** | `azure_adls`, `azure_sql` | `azure_blob`, `sql_server` |
| **Auth Mode** | `auth_mode: key_vault` (top-level) | Removed. Use `auth: { key_vault_name: ... }` |
| **Managed Identity** | Explicit `auth_mode: managed_identity` | Implicit (Auto-Auth) via `DefaultAzureCredential` |
| **Key Vault** | Limited to specific auth modes | **Universal** (works for any secret in `auth` dict) |

For a complete configuration reference, see [docs/reference/configuration.md](../reference/configuration.md).
