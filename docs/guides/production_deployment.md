# 🏭 Production Deployment

Moving from your laptop to production (e.g., Databricks, Azure Data Factory, Airflow) requires handling secrets, environments, and logging differently.

---

## 1. Secrets Management

**NEVER commit passwords to Git.**

Odibi supports environment variable substitution in `odibi.yaml`. Use the `${VAR_NAME}` syntax.

**Bad:**
```yaml
connections:
  db:
    password: "super_secret_password"  # ❌ Security Risk
```

**Good:**
```yaml
connections:
  db:
    password: "${DB_PASSWORD}"         # ✅ Safe
```

Then, set the environment variable `DB_PASSWORD` in your production environment (or `.env` file locally).

### Automatic Redaction
Odibi automatically detects values that look like secrets (keys, tokens, passwords) and replaces them with `[REDACTED]` in logs and Data Stories.

---

## 2. Data Privacy & PII

When processing personal data (GDPR/HIPAA), you must ensure that sensitive data does not leak into your logs or execution reports.

### Column-Level Redaction
If you want to see non-sensitive data in your reports but hide PII (Personally Identifiable Information), specify the columns list.

```yaml
nodes:
  - name: ingest_users
    read: ...
    # Only masks these columns in the HTML report
    sensitive: ["email", "ssn", "phone", "credit_card"]
```

### Full Node Redaction
For highly sensitive nodes (e.g., medical records, financial transactions), you can mask the entire sample.

```yaml
nodes:
  - name: process_health_records
    transform: ...
    # Replaces entire sample with "[REDACTED: Sensitive Data]"
    sensitive: true
```

*Note: This only affects the **Data Story** (logs/html). The actual data moving through the pipeline is **not** modified.*

---

## 3. Azure Integration

Odibi has native support for Azure resources.

### Authentication
We support **DefaultAzureCredential**. This means you don't need to manage keys manually.
1.  **Local:** It uses your Azure CLI login (`az login`).
2.  **Production:** It uses the Managed Identity of the VM/Pod.

```yaml
connections:
  data_lake:
    type: azure_adls
    account: mydatalake
    auth_mode: key_vault  # Fetches keys from Key Vault automatically
    key_vault: my-key-vault-name
```

---

## 3. Running on Databricks

Odibi runs natively on Databricks clusters.

1.  **Install:** Add `odibi[spark,azure]` to your cluster libraries.
2.  **Deploy:** Copy your project folder (YAML + SQL) to DBFS or git checkout.
3.  **Job:** Create a job that runs:
    ```bash
    odibi run odibi.yaml
    ```

*Tip: Use the "Spark" engine in your config to leverage the cluster's power.*

```yaml
project: My Big Data Project
engine: spark  # <--- Switches from Pandas to Spark
```

---

## 4. Monitoring & Logging

### Logging
Odibi logs structured JSON to stdout by default in production. This is easily ingested by Datadog, Splunk, or Azure Monitor.

```bash
# Force JSON logging
export ODIBI_LOG_FORMAT=json
odibi run odibi.yaml
```

### Data Stories as Artifacts
Configure Odibi to save Data Stories to a permanent location (like an S3 bucket or ADLS container) so you have a permanent audit trail.

```yaml
story:
  connection: data_lake  # Save reports to the cloud
  path: audit_reports/
```
