# Variable Substitution in YAML

Odibi supports powerful variable substitution in YAML configurations. This allows you to:

- Keep secrets out of your config files
- Reuse values across your configuration
- Generate dynamic dates at runtime
- Use the same YAML across dev/staging/prod environments

## Quick Reference

| Syntax | Purpose | Example |
|--------|---------|---------|
| `${VAR}` | Environment variable | `${API_TOKEN}` |
| `${env:VAR}` | Environment variable (explicit) | `${env:DB_PASSWORD}` |
| `${vars.name}` | Custom variable from `vars:` block | `${vars.env}` |
| `${date:expr}` | Dynamic date expression | `${date:today}`, `${date:-7d}` |
| `${date:expr:fmt}` | Date with custom format | `${date:today:%Y%m%d}` |

---

## Environment Variables

Use `${VAR_NAME}` to inject values from environment variables. This is the recommended way to handle secrets and environment-specific configuration.

### Basic Usage

```yaml
connections:
  my_database:
    type: sqlserver
    host: ${DB_HOST}
    database: ${DB_NAME}
    username: ${DB_USER}
    password: ${DB_PASSWORD}  # Never hardcode passwords!
```

Before running, set the environment variables:

```bash
# Linux/macOS
export DB_HOST=myserver.database.windows.net
export DB_USER=admin
export DB_PASSWORD=secret123

# Windows PowerShell
$env:DB_HOST = "myserver.database.windows.net"
$env:DB_USER = "admin"
$env:DB_PASSWORD = "secret123"
```

### Explicit `env:` Prefix

You can optionally use `${env:VAR}` for clarity:

```yaml
connections:
  storage:
    type: azure_blob
    account_name: ${env:AZURE_STORAGE_ACCOUNT}
    account_key: ${env:AZURE_STORAGE_KEY}
```

Both `${VAR}` and `${env:VAR}` work identically.

### Missing Variables

If an environment variable is not set, Odibi will raise an error:

```
ValueError: Missing environment variable: DB_PASSWORD
Tip: Check your .env file or environment setup.
```

This ensures you don't accidentally run with missing configuration.

---

## Custom Variables (`vars:` block)

Define reusable variables in your YAML and reference them with `${vars.name}`:

```yaml
vars:
  env: production
  region: us-east-1
  retention_days: 90

connections:
  bronze:
    type: azure_blob
    container: data-${vars.env}        # → data-production
    base_path: ${vars.region}/bronze   # → us-east-1/bronze

pipelines:
  - pipeline: cleanup
    nodes:
      - name: archive_old
        params:
          days: ${vars.retention_days}  # → 90
```

### When to Use `vars:`

- **Configuration values** that appear multiple times
- **Environment names** (dev, staging, prod)
- **Shared paths** or prefixes
- **Non-secret values** that vary by deployment

!!! tip "vars vs environment variables"
    Use `vars:` for non-sensitive, YAML-internal values.
    Use `${ENV_VAR}` for secrets and values that change per environment.

---

## Date Variables

Generate dynamic dates at runtime using `${date:expression}` syntax. Dates are resolved when the YAML is loaded.

### Named Date Expressions

| Expression | Description | Example Output |
|------------|-------------|----------------|
| `${date:now}` | Current datetime | `2024-01-15 14:30:45` |
| `${date:today}` | Today at midnight | `2024-01-15` |
| `${date:yesterday}` | Yesterday | `2024-01-14` |
| `${date:start_of_month}` | First day of month | `2024-01-01` |
| `${date:end_of_month}` | Last day of month | `2024-01-31` |
| `${date:start_of_year}` | First day of year | `2024-01-01` |

### Relative Date Expressions

Calculate dates relative to today:

| Expression | Description | Example (if today is 2024-01-15) |
|------------|-------------|----------------------------------|
| `${date:-7d}` | 7 days ago | `2024-01-08` |
| `${date:+30d}` | 30 days from now | `2024-02-14` |
| `${date:-1w}` | 1 week ago | `2024-01-08` |
| `${date:-1m}` | ~1 month ago | `2023-12-15` |
| `${date:-1y}` | 1 year ago | `2023-01-15` |

Supported units:
- `d` = days
- `w` = weeks
- `m` = months (approximate)
- `y` = years

### Custom Date Formats

Add a format string after a second colon:

```yaml
# Default format: YYYY-MM-DD
date_default: ${date:today}              # → 2024-01-15

# Compact format: YYYYMMDD
date_compact: ${date:today:%Y%m%d}       # → 20240115

# US format: MM/DD/YYYY
date_us: ${date:today:%m/%d/%Y}          # → 01/15/2024

# ISO with time
timestamp: ${date:now:%Y-%m-%dT%H:%M:%S} # → 2024-01-15T14:30:45
```

Format uses Python's [strftime](https://strftime.org/) syntax.

### Example: API with Date Filters

```yaml
pipelines:
  - pipeline: fetch_recent_data
    nodes:
      - name: api_data
        read:
          connection: my_api
          format: api
          path: /v1/records
          options:
            params:
              # Fetch data from last 7 days
              start_date: ${date:-7d}
              end_date: ${date:today}
            response:
              items_path: data
              add_fields:
                _fetched_at: ${date:now}
```

### Example: Date in File Paths

```yaml
write:
  connection: storage
  format: parquet
  # Creates: exports/2024/01/15/sales.parquet
  path: exports/${date:today:%Y}/${date:today:%m}/${date:today:%d}/sales.parquet
```

### Example: Report Description

```yaml
pipelines:
  - pipeline: weekly_report
    description: "Weekly data from ${date:-7d} to ${date:today}"
```

---

## Combining Variable Types

You can mix all variable types in a single configuration:

```yaml
vars:
  env: production
  base_path: data-warehouse

connections:
  storage:
    type: azure_blob
    account_name: ${AZURE_STORAGE_ACCOUNT}     # From environment
    account_key: ${AZURE_STORAGE_KEY}          # From environment
    container: ${vars.env}-data                # Custom var → production-data

pipelines:
  - pipeline: daily_export
    description: "Export run on ${date:today}"  # Dynamic date
    nodes:
      - name: export
        write:
          path: ${vars.base_path}/${date:today:%Y/%m/%d}/export.parquet
          # → data-warehouse/2024/01/15/export.parquet
```

---

## Processing Order

Variables are processed in this order:

1. **Environment variables** (`${VAR}`) - Processed first during YAML parsing
2. **Imports merged** - If you use `imports:`, they're loaded and merged
3. **Environment overrides** - `environments:` block applied if `env` is set
4. **Custom vars** (`${vars.xxx}`) - Resolved after all merges
5. **Date expressions** (`${date:xxx}`) - Resolved last

This means:
- You can use `${VAR}` inside imported files
- Custom vars can reference environment variables in their values
- Date expressions work anywhere in the final merged config

---

## API-Specific Date Shortcuts

When working with API connections, there are additional shortcut syntaxes available **only in API params and add_fields**:

### `$variable` Syntax (API only)

```yaml
options:
  params:
    start: $7_days_ago
    end: $today
  response:
    add_fields:
      _fetched_at: $now
```

| Variable | Description |
|----------|-------------|
| `$now` | Current datetime |
| `$today` | Today's date |
| `$yesterday` | Yesterday's date |
| `$7_days_ago` | 7 days ago |
| `$30_days_ago` | 30 days ago |
| `$today_compact` | Today as YYYYMMDD |
| `$yesterday_compact` | Yesterday as YYYYMMDD |
| `$7_days_ago_compact` | 7 days ago as YYYYMMDD |

### `{expression}` Syntax (API only)

```yaml
options:
  params:
    start: "{-7d}"
    end: "{today:%Y%m%d}"
```

| Expression | Example Output |
|------------|----------------|
| `{today}` | `2024-01-15` |
| `{now}` | `2024-01-15 14:30:45` |
| `{-7d}` | `2024-01-08` |
| `{+30d}` | `2024-02-14` |
| `{start_of_month}` | `2024-01-01` |
| `{today:%Y%m%d}` | `20240115` |

### Which Syntax to Use?

| Syntax | Where it works | Best for |
|--------|----------------|----------|
| `${date:xxx}` | **Anywhere in YAML** | Universal, recommended |
| `$variable` | API params/add_fields only | Quick shortcuts |
| `{expr}` | API params/add_fields only | Flexible expressions |

**Recommendation:** Use `${date:xxx}` for consistency across your entire configuration. Use the shortcuts in API configs when you want brevity.

---

## Injecting Custom Values

You can inject **any value** via environment variables, not just secrets:

```bash
# Set a custom date for testing
export REPORT_DATE=2024-01-01

# Set a custom filter
export DATA_FILTER=status=active
```

```yaml
pipelines:
  - pipeline: custom_report
    nodes:
      - name: filtered_data
        read:
          params:
            date: ${REPORT_DATE}       # → 2024-01-01
            filter: ${DATA_FILTER}     # → status=active
```

This is useful for:
- **Testing** with specific dates
- **Backfilling** historical data
- **Parameterized pipelines** driven by orchestrators

---

## Environment Overrides

Use the `environments:` block to override values per environment:

```yaml
vars:
  batch_size: 1000

connections:
  storage:
    type: azure_blob
    account_name: ${AZURE_ACCOUNT}
    container: data

environments:
  dev:
    vars:
      batch_size: 100
    connections:
      storage:
        container: data-dev
  
  prod:
    vars:
      batch_size: 10000
    connections:
      storage:
        container: data-prod
```

Run with environment:

```bash
python -m odibi run pipeline.yaml --env prod
```

---

## Troubleshooting

### "Missing environment variable: XXX"

The variable is not set in your environment.

```bash
# Check if it's set
echo $DB_PASSWORD  # Linux/macOS
echo %DB_PASSWORD%  # Windows CMD
$env:DB_PASSWORD   # Windows PowerShell
```

### Variable not substituted

Make sure you're using the correct syntax:
- `${VAR}` - Environment variable (requires var to be set)
- `${vars.name}` - Custom variable (requires `vars:` block)
- `${date:expr}` - Date expression (note the colon after `date`)

### Date is wrong

Date expressions use your **local system time**. Check:
- System clock is correct
- Timezone is set correctly for your use case

### `${date}` treated as environment variable

`${date}` (without colon) is treated as an environment variable lookup.
Use `${date:today}` for the date expression.

---

## Summary

```yaml
# Environment variables - for secrets and env-specific config
password: ${DB_PASSWORD}

# Custom vars - for reusable non-secret values  
vars:
  env: prod
container: ${vars.env}-data

# Date expressions - for dynamic dates anywhere in YAML
date_range: "${date:-7d} to ${date:today}"
compact_date: ${date:today:%Y%m%d}

# API shortcuts - only in API params/add_fields
params:
  start: $7_days_ago  # or {-7d}
```
