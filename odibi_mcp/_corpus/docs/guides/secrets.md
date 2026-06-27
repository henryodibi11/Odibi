# Managing Secrets

Odibi provides a unified way to handle secrets (API keys, database passwords, storage tokens) across local development and production environments. It supports `.env` files for local use and native **Azure Key Vault** integration for production.

## 1. Variable Substitution

You can reference environment variables in your `project.yaml` using the `${VAR_NAME}` syntax.

```yaml
connections:
  my_database:
    type: azure_sql
    host: ${DB_HOST}
    auth:
      username: ${DB_USER}
      password: ${DB_PASS}
```

## 2. Local Development (`.env`)

For local development, store your secrets in a `.env` file in your project root. Odibi automatically loads this file.

**Note:** Always add `.env` to your `.gitignore` to prevent committing secrets.

### CLI Commands

**Initialize a template:**
Generate a `.env.template` file based on the variables used in your config.
```bash
odibi secrets init project.yaml
```

**Validate your environment:**
Check if all required variables are set in your current environment.
```bash
odibi secrets validate project.yaml
```

## 3. Production (Azure Key Vault)

In production (e.g., Databricks, Azure Functions), relying on environment variables for everything can be insecure. Odibi supports fetching secrets directly from Azure Key Vault.

### Configuration

To use Key Vault, specify `key_vault_name` and `secret_name` in your connection config. Odibi will automatically fetch the secret securely using `DefaultAzureCredential` (Managed Identity / Service Principal).

```yaml
connections:
  adls_prod:
    type: azure_adls
    account: myprodstorage
    container: data
    # Instead of a hardcoded key or env var:
    key_vault_name: "my-key-vault"
    secret_name: "adls-prod-key"
```

### How it Works
1.  **Auth Detection**: If `key_vault_name` is present, Odibi attempts to authenticate with Azure using the environment's identity (e.g., the Databricks cluster's Managed Identity).
2.  **Parallel Fetching**: If multiple connections use Key Vault, Odibi fetches them in parallel during startup to minimize latency.
3.  **Caching**: Secrets are cached in memory for the duration of the run.

## Best Practices

1.  **Never Commit Secrets**: Do not put actual passwords in `project.yaml`. Use `${VAR}` placeholders.
2.  **Use `.env.template`**: Commit a template file with empty values so other developers know which variables they need to set.
3.  **Use Key Vault in Prod**: Avoid setting sensitive environment variables in cloud compute configurations if possible. Use Key Vault integration for rotation and auditing.
4.  **Redaction**: Odibi automatically attempts to redact known secret values from logs and generated stories.
