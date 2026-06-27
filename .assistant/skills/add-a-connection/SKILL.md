---
name: add-a-connection
description: "Use when defining an Odibi connection in YAML — the hardened type + auth{mode} discriminator model, one example per type (local, azure_blob, sql_server, http, delta), and the did-you-mean strictness that rejects the old top-level connection_string pattern."
requires: [odibi]
---

# add-a-connection Skill

Connections are named entries under `connections:` that tell Odibi where to read/write.
The connection model was **hardened**: each connection is `type:` + type-specific fields +
an **`auth:` block with a `mode:` discriminator**. Unknown keys are a **hard error with a
did-you-mean hint** (strict Pydantic models).

## When to Load This Skill

- Adding or editing a connection in pipeline YAML.
- A `validate_yaml` error points at a connection (unknown key / wrong auth shape).
- You're tempted to put `connection_string` at the connection top level — don't (see below).

## The Model: type + fields + auth{mode}

```yaml
connections:
  <name>:
    type: <local|azure_blob|sql_server|http|delta>
    # ... type-specific fields ...
    auth:                 # required for azure_blob / sql_server / http
      mode: <discriminator>
      # ... mode-specific fields ...
```

`auth.mode` is a discriminator — Odibi picks the auth schema from it. Put credentials
**inside `auth:`**, never as bare top-level keys. Secrets use `${ENV_VAR}` syntax.

## One Example Per Type

### local (no auth)
```yaml
local_data:
  type: local
  base_path: ./data        # default "./data"
```

### azure_blob (auth modes: account_key | sas | connection_string | key_vault | aad_msi)
```yaml
adls_bronze:
  type: azure_blob
  account_name: myaccount
  container: bronze
  auth:
    mode: key_vault              # production: pull key from Key Vault
    key_vault: kv-data
    secret: adls-account-key
# other modes:
#   mode: account_key   -> account_key: "${ADLS_ACCOUNT_KEY}"
#   mode: sas           -> sas_token: "${ADLS_SAS}"
#   mode: connection_string -> connection_string: "${ADLS_CONN}"
#   mode: aad_msi       -> client_id: "<optional user-assigned id>"   (default)
```

### sql_server (auth modes: sql_login | aad_password | aad_msi | connection_string)
```yaml
sql_dw:
  type: sql_server
  host: server.database.windows.net
  database: dw
  port: 1433                      # default 1433
  driver: "ODBC Driver 18 for SQL Server"   # default
  auth:
    mode: aad_msi                 # passwordless — preferred on Databricks/Azure (default)
# other modes:
#   mode: sql_login     -> username: dw_writer
#                          password: "${DW_PASSWORD}"
#   mode: aad_password  -> tenant_id / client_id / client_secret
#   mode: connection_string -> connection_string: "${DW_CONN}"
```

### http (auth modes: none | basic | bearer | api_key)
```yaml
api_source:
  type: http
  base_url: https://api.example.com
  headers:
    User-Agent: odibi-pipeline
  auth:
    mode: bearer
    token: "${API_TOKEN}"
# other modes:
#   mode: none
#   mode: basic    -> username / password
#   mode: api_key  -> header_name (default "Authorization"), value_template (default "Bearer {token}")
```

### delta (metastore-backed; no auth block)
```yaml
delta_silver:
  type: delta
  catalog: spark_catalog
  schema: silver_db               # YAML key is "schema"
  table: fact_sales               # optional default table
```

> For plain Delta files on a path, use a `local` (or `azure_blob`) connection and set
> `format: delta` on the node's `read:` / `write:` instead of the `delta` connection type.

## Strictness — What Gets Rejected

| Don't write | Use instead | Why |
|---|---|---|
| `connection_string:` at connection top level | `auth: { mode: connection_string, connection_string: ... }` | old pattern — top-level key now hard-errors |
| `auth_mode: sql` (flat) | `auth: { mode: sql_login, ... }` | auth is a nested block with a `mode:` discriminator |
| `username:` / `password:` at top level | inside `auth: { mode: sql_login, ... }` | credentials live under `auth` |
| `account_key:` at top level (azure_blob) | `auth: { mode: account_key, account_key: ... }` | — |
| `accountname`, `base_pth`, typos | fix the key | strict models suggest the nearest valid key |
| plaintext password | `"${ENV_VAR}"` | secrets must be env-var references |

## Workflow

1. `get_schema` for the connection type → exact fields + valid `auth.mode` values.
2. Write the block with `auth: { mode: ... }`.
3. `validate_yaml` → fix any did-you-mean error before running.
4. (Optional) `map_environment` / `profile_source` to confirm the connection reaches data.
