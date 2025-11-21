# Local Development Guide (Phase 2A)

This guide explains how to set up ODIBI for local development with Azure ADLS.

## Overview

For local development, you have two options for Azure authentication:

1. **`key_vault` mode** (recommended if you have Azure CLI set up)
2. **`direct_key` mode** (simpler, uses storage account keys directly)

## Option 1: Key Vault Mode (Recommended)

**Prerequisites:**
- Azure CLI installed (`az login` authenticated)
- Your Azure account has access to the Key Vault

**Configuration:**

```yaml
# config.yaml
connections:
  bronze:
    type: azure_adls
    account: mystorageaccount
    container: bronze
    auth_mode: key_vault
    key_vault_name: company-keyvault
    secret_name: bronze-storage-key
```

**Setup:**

1. Authenticate with Azure CLI:
   ```bash
   az login
   ```

2. Run your pipeline:
   ```bash
   python run_pipeline.py --config config.yaml
   ```

ODIBI will automatically use your Azure CLI credentials to access Key Vault.

---

## Option 2: Direct Key Mode (Simpler)

**Best for:**
- Quick local testing
- When you don't have Azure CLI configured
- CI/CD pipelines using secrets

**Configuration:**

```yaml
# config.local.yaml
connections:
  bronze:
    type: azure_adls
    account: mystorageaccount
    container: bronze
    auth_mode: direct_key
    account_key: "${BRONZE_STORAGE_KEY}"  # From environment variable
```

**Setup:**

1. Create a `.env` file (add to `.gitignore`):
   ```env
   BRONZE_STORAGE_KEY=your-storage-account-key-here
   ```

2. Load environment variables and run:
   ```bash
   # Option 1: Export manually
   export BRONZE_STORAGE_KEY="your-key"
   python run_pipeline.py --config config.local.yaml

   # Option 2: Use dotenv library
   pip install python-dotenv
   # Then in your script:
   from dotenv import load_dotenv
   load_dotenv()
   ```

---

## Recommended Workflow

**For Production:**
- Use `config.yaml` with `auth_mode: key_vault`
- Commit this file to git (no secrets exposed)

**For Local Development:**
1. Copy production config: `cp config.yaml config.local.yaml`
2. Change `auth_mode` to `direct_key`
3. Add `account_key: "${ENV_VAR}"`
4. Create `.env` file with your keys
5. Add `config.local.yaml` and `.env` to `.gitignore`

**Example `.gitignore`:**
```
# Local development configs (contain secrets)
config.local.yaml
.env
*.local.yaml
```

---

## Multi-Account Example

When working with multiple storage accounts locally:

```yaml
# config.local.yaml
connections:
  bronze:
    type: azure_adls
    account: storageaccount1
    container: bronze
    auth_mode: direct_key
    account_key: "${BRONZE_KEY}"

  silver:
    type: azure_adls
    account: storageaccount2  # Different account
    container: silver
    auth_mode: direct_key
    account_key: "${SILVER_KEY}"
```

**`.env` file:**
```env
BRONZE_KEY=storage-account-1-key
SILVER_KEY=storage-account-2-key
```

---

## Getting Storage Account Keys

### Via Azure Portal:
1. Navigate to your Storage Account
2. Go to "Access keys"
3. Copy "key1" or "key2"

### Via Azure CLI:
```bash
az storage account keys list \
  --account-name mystorageaccount \
  --resource-group myresourcegroup \
  --query "[0].value" -o tsv
```

---

## Security Best Practices

✅ **DO:**
- Use `key_vault` mode in production
- Keep `.env` files in `.gitignore`
- Rotate storage keys regularly
- Use separate storage accounts for dev/prod

❌ **DON'T:**
- Commit storage account keys to git
- Use `direct_key` mode in production (it will warn you)
- Share `.env` files via email/chat

---

## Troubleshooting

### Issue: "key_vault mode requires 'key_vault_name' and 'secret_name'"
**Solution:** Make sure both fields are provided in your config.

### Issue: "direct_key mode requires 'account_key'"
**Solution:** Add `account_key` field or check your environment variable is set.

### Issue: "Access denied to vault 'company-kv'"
**Solutions:**
1. Run `az login` to authenticate
2. Ask admin to grant your account access to the Key Vault
3. Use `direct_key` mode as fallback

### Issue: Warning about using direct_key in production
**Solution:** This is expected. Set `ODIBI_ENV=production` only for production deployments. For local dev, don't set this variable.

---

## Supported File Formats

ODIBI's PandasEngine supports all these formats with ADLS connections:

| Format | Extension | Use Case | Notes |
|--------|-----------|----------|-------|
| **CSV** | `.csv` | Simple tabular data | Most common, human-readable |
| **Parquet** | `.parquet` | Data lake storage | Columnar, compressed, recommended |
| **JSON** | `.json` | Semi-structured data | JSON lines format |
| **Excel** | `.xlsx` | Business reports | Requires `openpyxl` |
| **Avro** | `.avro` | Event streaming | Binary, schema-included, requires `fastavro` |

**Example with different formats:**
```yaml
connections:
  bronze:
    type: azure_adls
    account: mystorageaccount
    container: bronze
    auth_mode: direct_key
    account_key: "${BRONZE_KEY}"

pipelines:
  - pipeline: format_examples
    nodes:
      # Read CSV
      - name: load_csv
        read:
          connection: bronze
          path: data/sales.csv
          format: csv
      
      # Write as Parquet
      - name: save_parquet
        depends_on: [load_csv]
        write:
          connection: bronze
          path: data/sales.parquet
          format: parquet
          
      # Write as Avro
      - name: save_avro
        depends_on: [load_csv]
        write:
          connection: bronze
          path: data/sales.avro
          format: avro
```

---

## Next Steps

- See [examples/templates/template_full_adls.yaml](../examples/templates/template_full_adls.yaml) for full ADLS pipeline example
- For Databricks setup, see [docs/databricks_setup.md](databricks_setup.md) (coming in Phase 2C)
