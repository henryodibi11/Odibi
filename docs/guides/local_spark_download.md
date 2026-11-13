# Download remote Spark results to local Delta

Use `write.download` to run Spark transformations on Databricks and save a bounded
result as a **Delta table on the machine running Odibi**. This is useful for local
data exploration, repeatable fixtures and comparing declarative transformations
with reusable production-oriented PySpark functions. Odibi remains the workbench;
your production project does not have to depend on it.

## Prerequisites

- `engine: spark` and a working Spark session. For remote compute, configure the
  [Databricks connection](../setup_databricks_connect.md).
- A Spark client exposing public `DataFrame.toArrow()`. This exists in the
  inspected Databricks Connect 15.4.10 client; Apache Spark added it in 4.0.
  Check the actual client, not just the remote server's Spark version.
- Install the optional local writer:
  `python -m pip install 'odibi[polars]' 'deltalake>=0.25,<0.30'`.
  The download path requires delta-rs 0.25 or newer for the Polars Arrow-stream input;
  older Odibi-supported delta-rs versions remain usable by existing paths.
  Select Databricks Connect separately to match your runtime and Python. Do not
  install Odibi's `spark`, `all` or `dev` extras alongside Connect.
- Enough client memory for the complete downloaded result and its conversion.
  The row limit is not a byte limit or an out-of-memory guarantee.

## Configure the node's final write explicitly

These are fragments to add to an existing project, not a complete project file:

```yaml
engine: spark
connections:
  compute:
    type: databricks
    host: "${DATABRICKS_HOST}"
    client_id: "${DATABRICKS_CLIENT_ID}"
    client_secret: "${DATABRICKS_CLIENT_SECRET}"
    cluster_id: "${DATABRICKS_CLUSTER_ID}"
  warehouse:
    type: unity_catalog
    catalog: main
    schema: bronze
    create_schema: false
  local_output:
    type: local
    base_path: ./private_snapshots
```

Add a node to your pipeline:

```yaml
- name: orders_snapshot
  read:
    connection: warehouse
    format: delta
    table: main.bronze.orders
    options:
      filter: "order_date >= '2026-11-01'"
  write:
    connection: local_output
    format: delta
    path: orders
    mode: overwrite
    download:
      max_rows: 100000
```

The filter and any Spark transformations run remotely. At the write boundary,
Odibi collects at most `max_rows + 1` rows as Arrow. If the result exceeds the
bound, it fails **without modifying the destination**, rather than silently
truncating. Otherwise it converts to Polars and uses Odibi's existing Polars
Delta writer. The output above is `./private_snapshots/orders` on the client.
Relative paths resolve against the client process's working directory.

Without `download`, a Spark write keeps its existing behavior: the Spark runtime
resolves/writes the path. Merely using a `local` connection does not download rows.

## Supported output contract

- Batch Delta path writes to a `type: local` filesystem connection, not a URI.
- `overwrite` and `append`; append adds rows and does not deduplicate them.
- `partition_by`, `merge_schema`, `overwrite_schema` and `add_metadata` are supported.
  Schema flags are translated to the local Delta writer's options.
- Local Delta version, operation and downloaded row count are retained as write
  metadata. Spark-side Delta diff diagnostics are not run against the local path.
- Ordinary upstream Spark validation/transforms remain available. Subsequent nodes
  still receive the Spark DataFrame; this option does not switch the pipeline engine.

The initial implementation rejects arbitrary write `options`, table registration,
automatic maintenance, Z-ordering, table properties, streaming, skip-if-unchanged,
first-run queries, upsert/merge modes, node `schema_policy`, `materialized` and
`incremental` configurations. It is a snapshot handoff, not a cross-engine
incremental replication service. Polars/Delta can reject unsupported source types;
do not assume every Databricks-specific type has a local Delta equivalent.

System catalog, Story, patterns and custom functions have their own effects.
`write.download` only changes this node's final data write; it does not redirect
other storage operations or guarantee that an arbitrary project has no remote
writes. Keep employer data and project-specific code in approved work locations,
outside the public Odibi checkout.

## Continue locally

In a separate project configured with `engine: polars` (or `pandas`), use the
same local connection and read the snapshot:

```yaml
read:
  connection: local_output
  format: delta
  path: orders
```

No Databricks connection is needed for that local project. For a table too large
for client memory, filter/project it remotely first or write remotely to permitted
storage. This feature does not provide batched, resumable full-table exports.

## Discover this capability through MCP

Both repository and installed-wheel MCP documentation support:

```python
odibi("search_docs", query="write.download")
odibi("get_doc", doc_path="docs/guides/local_spark_download.md")
odibi("get_doc", doc_path="docs/setup_databricks_connect.md")
```

`list_docs` includes both pages as priority docs; `list_docs(category="guides")`
also lists this guide. The generated YAML schema documents `WriteConfig.download`
and its required positive integer `max_rows`.
