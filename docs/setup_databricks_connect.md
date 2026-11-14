# Local development with remote Databricks Spark

Odibi can create a remote Spark session using **Databricks Connect** and a
Databricks service principal's **OAuth machine-to-machine (M2M)** credentials.
Your Python orchestration runs locally; Spark DataFrame/SQL work runs on Databricks.
This is separate from ADLS service-principal authentication and from the
`unity_catalog` connection, which resolves table names but does not log in.

Use this connection to explore remote data declaratively and exercise custom
PySpark transformations from your IDE. Python imports/orchestration run on the
client; cluster-installed Python libraries are not automatically installed locally.
For bounded laptop-local Delta snapshots, see
[download remote Spark results](guides/local_spark_download.md).

## 1. Create a dedicated environment

Choose Python and Databricks Connect to match your compute. For example, for a
Databricks Runtime **15.4 LTS** cluster use **Python 3.11**:

```bash
python3.11 -m venv .venv-databricks
source .venv-databricks/bin/activate  # Windows: .venv-databricks\Scripts\activate
python -m pip install --upgrade pip
python -m pip install odibi 'databricks-connect==15.4.*'
python -m pip check
```

For a source checkout, replace `odibi` with `-e .`. Install test/lint tools
individually if needed. **Do not install `odibi[spark]`, `odibi[all]`, or
`odibi[dev]` in this environment**: these install `pyspark`/`delta-spark`, which
conflict with Databricks Connect's PySpark implementation. A fresh environment is
safer than repairing overlapping installations. No local JVM is required.

Other supported combinations include Python 3.12 with Connect `16.4.*` for a
16.4 cluster. For serverless, consult the current compatibility matrix; the
documented minimum patch releases include 15.4.10 and 16.4.1, and the Python
version must match the serverless environment. Do not blindly install the latest
Connect version. Runtime must be at least the Connect version; match the
major/minor line and Python minor version (especially for UDFs).

Odibi's current `pyarrow>=14,<17` constraint is compatible with the inspected
Connect 15.4.10 and 16.4.4 metadata. This is not a claim that arbitrary future
Connect versions work with that constraint. No unbounded Connect extra is provided:
select the version explicitly for your workspace.

## 2. Provision the service principal

- Enable Unity Catalog in the workspace. Use a supported dedicated/assigned or
  standard/shared access-mode cluster, or compatible serverless compute.
- Assign the service principal to the workspace and grant compute access
  (for clusters, `CAN ATTACH TO`; startup may require additional permissions).
- Create a **Databricks OAuth secret** for that service principal. Use its
  application ID and OAuth secret, **not** an Azure ADLS/Entra client secret.
- Grant only the Unity Catalog and storage permissions needed by your pipeline.
  Compute authentication does not grant data access.
- Export `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`, and
  `DATABRICKS_CLUSTER_ID` through your shell, IDE secret facility, or secret manager.
  Never commit their values. Existing `${VAR}` / `${env:VAR}` substitution works;
  Odibi does not add a new secret backend. Missing environment references fail
  during configuration loading.

## 3. Configure remote compute

Add this to an existing Odibi project YAML:

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
    schema: development
    create_schema: false
```

For serverless, **remove `cluster_id`** and set `serverless: true`. Exactly one
target is required. Explicit YAML targets override ambient SDK compute defaults.
Use a workspace `https://...` URL, not an account-console URL or SQL HTTP path.
This explicit-credentials mode uses OAuth M2M; SDK token refresh is automatic.

### Named CLI OAuth profile (interactive local development)

Alternatively, reuse an existing named `databricks auth login` profile:

```yaml
engine: spark
connections:
  compute:
    type: databricks
    profile: local-user-connect
```

The exact saved profile must contain its own `host`, `auth_type = databricks-cli`,
and either `cluster_id` or `serverless_compute_id = auto`. Odibi reads the file
selected by `DATABRICKS_CONFIG_FILE`, otherwise `~/.databrickscfg`. It does not
inherit fields from `DEFAULT` or silently switch to an ambient identity/target.
The SDK delegates OAuth acquisition/refresh to the Databricks CLI token cache.
This mode requires **databricks-sdk>=0.166.0** and **Databricks CLI>=0.207.1** on
PATH so token acquisition is bound to the selected profile, not only its host.
It supports saved CLI OAuth profiles, not arbitrary PAT/M2M profiles.

Do not combine `profile` with YAML `host`, `client_id` or `client_secret`.
Optionally set `cluster_id` or `serverless: true` in YAML to override the profile's
compute target; setting both is an error. Ambient `DATABRICKS_CONFIG_PROFILE`,
credentials and compute targets do not override explicit Odibi selection.
Odibi does not edit the environment or profile file. Authenticate that named
profile separately before starting a pipeline; session setup does not prompt
for login or fall back to service-principal credentials.

The Spark engine selects the single `databricks` connection automatically. Multiple
compute connections are rejected rather than guessing. An explicitly injected
`SparkEngine(spark_session=...)` still takes precedence. With no `databricks`
connection, existing active-session reuse (including notebooks) and local Spark
startup are unchanged. In notebooks install ordinary Odibi without Connect and
omit the compute connection to reuse the notebook session.

`compute` is **not** a node read/write, system-catalog or story storage connection.
Keep using Unity Catalog or storage connections for those resources. Remote Spark
cannot read arbitrary paths on your laptop; use data locations visible to compute
(for example UC tables/volumes). Story files written locally by the Python client
are distinct from data paths read remotely by Spark. ADLS configuration can be
restricted on shared/serverless compute; prefer UC storage credentials there.

### Keep metadata and stories on the laptop

With an explicit `databricks` compute connection, a non-URI `type: local`
system connection stores the system catalog and state on the Python client's
filesystem using Pandas/delta-rs, not remote Spark. Pipeline nodes still run
on Databricks. No remote metadata schema or download node is required:

```yaml
connections:
  data:
    type: local
    base_path: data
  # Keep the compute and warehouse connections from the example above.

system:
  connection: data
  path: _system_catalog

story:
  connection: data
  path: stories
```

This includes catalog bootstrap, run/node history, derived catalog summaries and
state/watermark rereads after restarting Odibi. Relative paths resolve on the
client; Windows drive paths are not sent to Databricks as Spark storage URIs.
The local catalog uses the `deltalake` dependency (included in base Odibi).
Keep its directory between runs when you want to retain history and watermarks.
Local metadata and stories do not copy the warehouse table to your laptop.

URI-backed storage (such as `dbfs:/...` or `abfss://...`) and Unity Catalog system
connections retain their remote Spark backend. Ordinary local Spark and notebook
session selection are unchanged. A failed Databricks session still fails setup;
explicit local metadata is not a silent compute fallback.

## 4. Verify your actual workspace

First run `python -m odibi doctor --format json` using the same Python
that will run the pipeline (or that interpreter's `odibi doctor --format json`).
Doctor reports **its own process**, including `sys.executable`, Python version,
Odibi import path/version and installed distribution versions. It reports Connect,
SDK, Arrow, Polars and delta-rs, detects standalone PySpark/delta-spark conflicts,
and checks known 15.4/16.4 Python pairings. It cannot certify arbitrary runtime
compatibility: consult the current matrix and run `python -m pip check` as well.
Doctor does not load YAML contents, credentials or profiles, initialize connections,
or open Spark/network sessions; `project_loaded: false` and empty `connections`
are intentional. A separate MCP worker's version is not pipeline execution
provenance. Existing long-lived Python workers must be restarted explicitly by
their operator to import an upgraded package; installing a wheel is not hot reload.

First use Databricks' environment diagnostic (configure the same standard
`DATABRICKS_*` variables, including `DATABRICKS_AUTH_TYPE=oauth-m2m`; for serverless,
unset `DATABRICKS_CLUSTER_ID` and set `DATABRICKS_SERVERLESS_COMPUTE_ID=auto`):

```bash
databricks-connect test
```

Then exercise Odibi's session integration:

```python
import os
from odibi.connections import DatabricksConnection
from odibi.engine.spark_engine import SparkEngine

compute = DatabricksConnection(
    host=os.environ["DATABRICKS_HOST"],
    client_id=os.environ["DATABRICKS_CLIENT_ID"],
    client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
    cluster_id=os.environ["DATABRICKS_CLUSTER_ID"],
    # For serverless: omit cluster_id and set serverless=True instead.
)
engine = SparkEngine(connections={"compute": compute})
assert engine.spark.range(7).count() == 7
print(engine.spark.sql("SELECT current_user(), current_catalog()").collect())
```

Creating/validating a connection object is local-only; session creation happens
when SparkEngine starts. A connection reuses its session across catalog/pipeline
engines. The caller owns session shutdown (stop it only after all users finish).
Session setup errors never fall back to local Spark or a local system catalog.
Errors deliberately omit raw SDK exception text because it can contain secrets.
Check permissions, network access, runtime/Python compatibility and credentials
without printing secrets or enabling unsafe SDK HTTP debug logging.

Odibi's explicit Databricks credentials do not select an ambient SDK profile:
even a missing `DATABRICKS_CONFIG_PROFILE` does not override this YAML connection.
Your shell environment and configuration file are not modified. In Odibi 3.16.0,
a missing ambient profile could still block setup; upgrade rather than changing
your global profile settings. Do not print credentials to diagnose setup. Python 3.14 is
not an appropriate replacement for the runtime-matched interpreter above: Odibi's
constrained PyArrow versions may lack wheels and fall back to an unsupported build.

## Unity Catalog tables, volumes and writes

Use `type: unity_catalog`, or `type: delta` with `catalog` and `schema`, for
addressing tables alongside the `databricks` compute connection. Batch and streaming
`read.table` resolve bare names through that connection: with catalog `main` and
schema `development`, `table: orders` reads `main.development.orders`.
Already-qualified two/three-part names are preserved, for example
`read: {connection: warehouse, format: delta, table: main.development.orders}`.
Local session tables, file paths and JDBC table reads keep their existing behavior.
Qualification does not grant permissions; an insufficient-permissions error alone
does not establish which Unity Catalog grant is missing.
For a volume, use `path: /Volumes/main/development/incoming/orders.parquet` with
`format: parquet`. These paths are read by remote Spark, not local Python `open()`.
Data access still requires the service principal's UC table/volume permissions.

Unity Catalog writes are supported through Spark `saveAsTable`; ADLS writes use
the configured remote storage path when compute access allows it. These writes
execute remotely. To save a result on the client instead, explicitly configure
[`write.download`](guides/local_spark_download.md). Spark/UC authentication does
not grant storage access and does not make an arbitrary pipeline read-only.

## AI and MCP discovery

The setup page is bundled in the installed MCP documentation corpus. Use
`odibi("search_docs", query="Databricks Connect")`,
`odibi("get_doc", doc_path="docs/setup_databricks_connect.md")`, or `list_docs`.
For the local snapshot capability search for `write.download` and retrieve
`docs/guides/local_spark_download.md`.

This feature covers **session setup**, not universal Spark Connect parity for
every Odibi transformer, Delta maintenance API or JVM-dependent operation. Check
Databricks Connect limitations before choosing a workflow.

## Verification boundary and official references

Automated Odibi tests cover typed YAML/env loading, lazy factory creation, session
selection/reuse, errors and unchanged local/notebook selection using mocks. Package
installation/import checks are local. **No real remote execution is claimed**
without a supplied workspace, compute target and credentials; run the checks above
before relying on a production workflow.

Official documentation checked for this implementation:

- [Compute configuration and SDK Config](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config)
- [Python/runtime compatibility and access modes](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/requirements)
- [OAuth M2M service principals](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)
- [Conflicting PySpark installations](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/troubleshooting)
- [Connect limitations](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/limitations)
