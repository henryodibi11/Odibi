# Local development with remote Databricks Spark

Odibi can create a remote Spark session using **Databricks Connect** and a
Databricks service principal's **OAuth machine-to-machine (M2M)** credentials.
Your Python orchestration runs locally; Spark DataFrame/SQL work runs on Databricks.
This is separate from ADLS service-principal authentication and from the
`unity_catalog` connection, which resolves table names but does not log in.

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
Only OAuth M2M is supported by this connection; SDK token refresh is automatic.

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

## 4. Verify your actual workspace

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
