# ⚡ Odibi Cheatsheet

## 📄 `odibi.yaml` Anatomy

```yaml
version: 1
project: my_project

# 1. Define Connections (Sources/Sinks)
connections:
  raw_data:
    type: local
    path: ./data/raw
  analytics_db:
    type: azure_sql
    connection_string: ${DB_CONN_STR}

# 2. Define Pipeline Steps
pipeline:
  # Bronze: Ingest
  - name: ingest_users
    engine: pandas          # or 'spark'
    source: raw_data/users.csv
    sink: analytics_db/bronze_users

  # Silver: Transform
  - name: clean_users
    engine: spark
    source: analytics_db/bronze_users
    sink: analytics_db/silver_users
    sql: |
      SELECT
        trim(name) as name,
        cast(age as int) as age
      FROM ${source}
      WHERE age > 0
```

---

## 💻 Top CLI Commands

| Task | Command |
|------|---------|
| **Run safely** | `odibi run config.yaml --dry-run` |
| **Run fast** | `odibi run config.yaml --parallel 4` |
| **Debug setup** | `odibi doctor config.yaml` |
| **View report** | `odibi story view --latest` |
| **Start new** | `odibi generate-project -i ./data -o ./proj` |
| **Fuzz test** | `odibi stress config.yaml --runs 10` |

---

## 📂 Directory Structure

```text
my_project/
├── odibi.yaml          # The brain
├── .env                # Secrets (GitIgnored!)
├── data/               # Local data (GitIgnored!)
├── sql/                # Complex SQL files
│   ├── clean_users.sql
│   └── revenue_mart.sql
└── stories/            # Run reports
    ├── run_123.json
    └── run_124.json
```

---

## 🧩 SQL Template Variables

Odibi injects these variables into your SQL automatically:

| Variable | Description | Example |
|----------|-------------|---------|
| `${source}` | The table/file defined in `source`. | `SELECT * FROM ${source}` |
| `${sink}` | The table/file defined in `sink`. | `INSERT INTO ${sink} ...` |
| `${SELF}` | The current node's output name. | `CREATE VIEW ${SELF} AS ...` |
| `${params.x}` | Custom parameter passed in YAML. | `WHERE date > '${params.start_date}'` |
