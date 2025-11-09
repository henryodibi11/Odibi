# Phase 2 Design Decisions

**Status:** ✅ Design Refinement Complete  
**Last Updated:** 2025-11-08  
**Sessions:**
- Config Refactor + Azure Auth Strategy Planning
- Design Refinement - Critical Implementation Details (Testing, Delta, Performance, Session Management)

---

## Purpose

This document captures the architectural decisions made during Phase 2 planning. These decisions guide the implementation of Spark engine, Azure ADLS integration, and credential management.

**Key Principle:** These are **design decisions**, not implementation. Code will be written in Phase 2 execution based on this plan.

---

## Table of Contents

1. [Transform Pattern](#1-transform-pattern)
2. [Multi-Account Storage Strategy](#2-multi-account-storage-strategy)
3. [Authentication Strategy](#3-authentication-strategy)
4. [Databricks Integration](#4-databricks-integration)
5. [User Onboarding Tools](#5-user-onboarding-tools)
6. [Validation Strategy](#6-validation-strategy)
7. [Delta Lake Strategy](#7-delta-lake-strategy)
8. [Spark Session Management](#8-spark-session-management)
9. [Testing Strategy](#9-testing-strategy)
10. [Key Vault Performance](#10-key-vault-performance)
11. [Local Development Experience](#11-local-development-experience)
12. [Out of Scope](#12-out-of-scope-phase-2)
13. [Implementation Roadmap](#13-implementation-roadmap)
14. [Implementation Checklist](#14-implementation-checklist)

---

## 1. Transform Pattern

### Decision

**Transforms NEVER need connections** - they operate on data already in context.

### Rationale

- **Read** nodes bring data INTO context (requires connection for I/O)
- **Transform** nodes operate on data ALREADY in context (no I/O, pure computation)
- **Write** nodes send data OUT of context (requires connection for I/O)

### Works For

- ✅ Pandas (in-memory DataFrames)
- ✅ Spark (lazy execution DataFrames)
- ✅ Both development and production
- ✅ Testing (mock data in context, no connection needed)

### Example

```yaml
pipelines:
  - pipeline: bronze_to_silver
    nodes:
      # Read: Needs connection (I/O)
      - name: load_raw
        read:
          connection: bronze
          path: raw/sales.csv
      
      # Transform: No connection (operates on data in context)
      - name: clean_data
        depends_on: [load_raw]
        transform:
          steps:
            - "SELECT * FROM load_raw WHERE amount > 0"
      
      # Write: Needs connection (I/O)
      - name: save_clean
        depends_on: [clean_data]
        write:
          connection: silver
          path: cleaned/sales.parquet
```

### Testing Pattern

```python
def test_transform():
    # No connection needed
    context = PandasContext()
    context.register('raw_data', test_df)
    
    result = engine.execute_sql("SELECT * FROM raw_data WHERE ...", context)
    
    assert len(result) == expected_count
```

---

## 2. Multi-Account Storage Strategy

### Decision

Support multiple Azure storage accounts simultaneously with **different strategies** for Spark vs Pandas.

### Spark: Global Configuration

**Pattern:** Set all storage account configs upfront when SparkEngine initializes.

**Why it works:** Spark config keys are scoped by account name - no clashes.

```python
# Each account gets unique config key
spark.conf.set("fs.azure.account.key.account1.dfs.core.windows.net", "key1")
spark.conf.set("fs.azure.account.key.account2.dfs.core.windows.net", "key2")

# All reads automatically use correct config
df1 = spark.read.parquet("abfss://container@account1.../data.parquet")
df2 = spark.read.parquet("abfss://container@account2.../data.parquet")
```

**Implementation:**
```python
class SparkEngine:
    def __init__(self, connections):
        self.spark = SparkSession.builder.getOrCreate()
        self._configure_all_connections(connections)  # ← Set all at init
```

### Pandas: Per-Operation Injection

**Pattern:** Each read/write gets `storage_options` injected from its connection.

**Why it works:** Pandas has no global config - each operation is independent.

```python
# Each read gets its own storage_options
df1 = pd.read_parquet(
    "abfss://container@account1.../data.parquet",
    storage_options={"account_name": "account1", "account_key": "key1"}
)

df2 = pd.read_parquet(
    "abfss://container@account2.../data.parquet",
    storage_options={"account_name": "account2", "account_key": "key2"}
)
```

**Implementation:**
```python
class PandasEngine:
    def read(self, connection, path, format, options=None):
        options = self._merge_storage_options(connection, options)
        return pd.read_parquet(full_path, **options)
    
    def _merge_storage_options(self, connection, options):
        # Inject connection credentials, allow user override
        conn_opts = connection.pandas_storage_options()
        user_opts = options.get("storage_options", {})
        merged = {**conn_opts, **user_opts}
        return {**options, "storage_options": merged}
```

### Example Configuration

```yaml
connections:
  bronze:
    type: azure_adls
    account: account1
    container: bronze
    auth_mode: key_vault
    key_vault_name: company-kv
    secret_name: bronze-key
  
  silver:
    type: azure_adls
    account: account2  # Different storage account
    container: silver
    auth_mode: key_vault
    key_vault_name: company-kv
    secret_name: silver-key
  
  gold:
    type: azure_adls
    account: account3  # Third storage account
    container: gold
    auth_mode: key_vault
    key_vault_name: company-kv
    secret_name: gold-key
```

**All three accounts work simultaneously - no configuration conflicts.**

---

## 3. Authentication Strategy

### Decision

Support **two authentication modes** with Key Vault as the recommended default.

### Supported Modes

| Mode | Use Case | Priority | Security |
|------|----------|----------|----------|
| `key_vault` | Production (recommended) | Primary | ✅ High |
| `direct_key` | Local dev, testing | Fallback | 🟡 Medium |

### Mode: key_vault (Recommended)

**Configuration:**
```yaml
connections:
  storage:
    type: azure_adls
    account: mystorageaccount
    container: mycontainer
    auth_mode: key_vault  # ← Default
    key_vault_name: company-keyvault
    secret_name: storage-account-key
```

**How it works:**
1. Connection uses `DefaultAzureCredential` to authenticate to Key Vault
2. Fetches storage account key from specified secret
3. Caches key for duration of pipeline run
4. Uses key for all Spark/Pandas operations

**Benefits:**
- ✅ Secrets never in YAML files (safe to commit to git)
- ✅ Centralized secret management
- ✅ Automatic key rotation (change in Key Vault, no code changes)
- ✅ Audit trail (Key Vault logs who accessed what)
- ✅ Works in Databricks (workspace managed identity)

### Mode: direct_key (Fallback)

**Configuration:**
```yaml
connections:
  dev_storage:
    type: azure_adls
    account: devaccount
    container: dev
    auth_mode: direct_key
    account_key: "${STORAGE_KEY}"  # From environment variable
```

**How it works:**
1. User provides key directly (usually via environment variable)
2. Connection uses key as-is
3. No Key Vault access needed

**When to use:**
- Local development (Azure CLI auth to Key Vault not configured)
- CI/CD pipelines (using pipeline secrets)
- Quick prototyping/testing
- Environments without Key Vault access

**Warning behavior:**
- If `ODIBI_ENV=production`, warns user that `direct_key` is not recommended
- Validation ensures `account_key` is provided when using this mode

### Implementation

```python
class AzureADLS(BaseConnection):
    def __init__(
        self,
        account: str,
        container: str,
        path_prefix: str = "",
        auth_mode: str = "key_vault",  # ← Default to best practice
        key_vault_name: str = None,
        secret_name: str = None,
        account_key: str = None,
        validate: bool = True
    ):
        # ... store params ...
        self._cached_key = None
        
        if validate:
            self.validate()  # Fail fast
    
    def validate(self):
        """Validate config based on auth mode."""
        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                raise ValueError(
                    f"key_vault mode requires 'key_vault_name' and 'secret_name'"
                )
        elif self.auth_mode == "direct_key":
            if not self.account_key:
                raise ValueError(f"direct_key mode requires 'account_key'")
            
            # Warn in production
            if os.getenv("ODIBI_ENV") == "production":
                warnings.warn(
                    f"⚠️  Using direct_key in production not recommended. "
                    f"Use auth_mode: key_vault. Connection: {self.account}",
                    UserWarning
                )
        else:
            raise ValueError(
                f"Unsupported auth_mode: {self.auth_mode}. "
                f"Use 'key_vault' or 'direct_key'."
            )
    
    def get_storage_key(self) -> str:
        """Get storage account key (cached)."""
        if self._cached_key:
            return self._cached_key
        
        if self.auth_mode == "key_vault":
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
            
            credential = DefaultAzureCredential()
            kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
            client = SecretClient(vault_url=kv_uri, credential=credential)
            
            secret = client.get_secret(self.secret_name)
            self._cached_key = secret.value
            return self._cached_key
        
        elif self.auth_mode == "direct_key":
            return self.account_key
    
    def pandas_storage_options(self) -> dict:
        """Storage options for pandas."""
        return {
            "account_name": self.account,
            "account_key": self.get_storage_key()
        }
    
    def configure_spark(self, spark):
        """Configure Spark with storage key."""
        config_key = f"fs.azure.account.key.{self.account}.dfs.core.windows.net"
        spark.conf.set(config_key, self.get_storage_key())
```

### Key Design Principles

1. **Default to security:** `auth_mode: key_vault` is the default
2. **Validate early:** Check config on connection init, not first use
3. **Cache credentials:** Fetch once, reuse throughout pipeline
4. **Warn insecure patterns:** Alert when `direct_key` used in production
5. **Support flexibility:** Allow `direct_key` for legitimate dev/test use cases

---

## 4. Databricks Integration

### Discovery

**Azure Databricks workspace managed identity + DefaultAzureCredential WORKS!** ✅

This was validated during planning session.

### How It Works

1. **Databricks workspace** has system-assigned managed identity (auto-created by Azure)
2. **DefaultAzureCredential** (from `azure-identity` package) automatically detects and uses workspace identity
3. **Grant workspace identity** access to Key Vault via access policy
4. **ODIBI code works unchanged** in Databricks - no special handling needed

### Setup Requirements

**One-time per workspace:**

1. **Find workspace managed identity Object ID**
   - Portal: Databricks Workspace → Identity → System assigned → Object ID
   - CLI: `az databricks workspace show --query identity.principalId`
   - **Or use ODIBI setup notebook** (recommended - automates this)

2. **Grant Key Vault access**
   ```bash
   az keyvault set-policy \
     --name company-keyvault \
     --object-id <workspace-object-id> \
     --secret-permissions get list
   ```

3. **Install dependencies in cluster**
   ```python
   %pip install azure-identity azure-keyvault-secrets adlfs
   ```

### Permissions

| Permission | Required? | Purpose |
|------------|-----------|---------|
| `get` | ✅ Yes | Read secret values (ODIBI needs this) |
| `list` | 🟡 Recommended | List secrets (helpful for troubleshooting/validation) |

**Recommendation:** Grant both `get` and `list` for better developer experience.

### Security Notes

- Workspace managed identity is **Azure-managed** (no credentials to rotate)
- Access is **workspace-scoped** (all notebooks in workspace inherit it)
- Key Vault **logs all access** (audit trail for compliance)
- Works with **cross-subscription** Key Vaults (if access granted)

---

## 5. User Onboarding Tools

### Decision

Provide **automated setup tools** instead of manual documentation (CLI, portal screenshots).

### Tools to Build

#### 1. Setup Notebook (`setup/databricks_setup.ipynb`)

**Purpose:** Interactive, guided setup for Databricks + Key Vault integration.

**Features:**
- Auto-detect workspace managed identity (Object ID, App ID, Tenant ID)
- Display identity information clearly
- Generate exact Azure CLI command to grant Key Vault access
- Test Key Vault connection
- List available secrets (validate setup worked)
- Provide helpful error messages if setup incomplete

**User Experience:**
1. Upload notebook to Databricks workspace
2. Run cells sequentially
3. Copy/paste generated command
4. Verify with built-in tests
5. Done!

**Cell structure:**
```python
# Cell 1: Install dependencies
%pip install azure-identity azure-keyvault-secrets

# Cell 2: Get identity info
# Shows: Object ID, App ID, Tenant ID

# Cell 3: Generate CLI command
# Outputs exact `az keyvault set-policy` command to run

# Cell 4: Test connection
# Validates Key Vault access, lists secrets
```

#### 2. Utility Helper (`odibi/utils/setup_helpers.py`)

**Purpose:** Programmatic access to setup info for advanced users.

**Functions:**
```python
def get_databricks_identity_info() -> dict:
    """
    Get workspace managed identity information.
    
    Returns:
        dict: {
            "app_id": "...",
            "object_id": "...",
            "tenant_id": "..."
        }
    
    Raises:
        RuntimeError: If not running in Databricks or identity unavailable
    """

def print_keyvault_setup_instructions(key_vault_name: str):
    """
    Print complete setup instructions for given Key Vault.
    
    Args:
        key_vault_name: Name of your Key Vault
    
    Prints:
        - Workspace identity info
        - Exact Azure CLI command to run
        - Verification steps
    """
```

**Usage:**
```python
from odibi.utils import print_keyvault_setup_instructions

# One-liner for users
print_keyvault_setup_instructions("company-keyvault")
```

### What We're NOT Doing

❌ **Azure CLI documentation** - Users find terminal commands confusing  
❌ **Portal screenshots** - Get outdated quickly, hard to maintain  
❌ **Manual step-by-step guides** - Automated tools are better UX

### Documentation Strategy

**README.md:**
```markdown
## Databricks Setup (5 minutes)

### Quick Start

1. Upload `setup/databricks_setup.ipynb` to your workspace
2. Run the notebook - it shows you exactly what to do
3. Copy/paste the command it generates
4. Done! Start using ODIBI with Key Vault

### Alternative (Programmatic)

```python
from odibi.utils import print_keyvault_setup_instructions
print_keyvault_setup_instructions("your-keyvault-name")
```
```

**Focus:** Make setup **as easy as possible** while enforcing **best practices** (Key Vault).

---

## 6. Validation Strategy

### Decision

**Validate eagerly** (on connection init), not lazily (on first use).

### Rationale

**Eager validation (chosen):**
- ✅ **Fail fast** - Catch config errors before pipeline runs
- ✅ **Clear errors** - Validation context vs runtime context
- ✅ **Better UX** - Immediate feedback on config mistakes

**Lazy validation (rejected):**
- ❌ **Fail late** - Errors surface deep in pipeline execution
- ❌ **Confusing errors** - Runtime failures harder to debug
- ❌ **Wastes time** - Pipeline runs for minutes before failing

### Implementation

```python
class AzureADLS(BaseConnection):
    def __init__(self, ..., validate=True):
        # ... store params ...
        
        if validate:
            self.validate()  # ← Called immediately
    
    def validate(self):
        """Validate configuration (fail fast)."""
        # Check required fields based on auth_mode
        if self.auth_mode == "key_vault":
            if not self.key_vault_name or not self.secret_name:
                raise ValueError("...")
        # ... etc ...
```

### What Gets Validated

✅ **Config structure:**
- Required fields present
- Field types correct
- Auth mode valid

✅ **Logical consistency:**
- Auth mode has required fields
- Referenced connections exist (in PipelineConfig)

❌ **NOT validated:**
- Network connectivity to Key Vault (too slow, network-dependent)
- Storage account accessibility (requires credentials)
- Secret existence in Key Vault (could change between validation and use)

**Philosophy:** Validate what's **knowable from config**, not what requires **network I/O**.

### Testing Support

For testing, validation can be disabled:

```python
# Production: validate=True (default)
conn = AzureADLS(account="...", ..., validate=True)

# Testing: can skip validation
conn = AzureADLS(account="...", ..., validate=False)
```

---

## 7. Delta Lake Strategy

### Decision

**Delta Lake is the default file format** for all write operations. Enforce best practices while supporting legacy formats when needed.

### Why Delta as Default

Delta Lake provides critical features over Parquet/CSV:
- ✅ **ACID transactions** - No partial writes if job fails
- ✅ **Schema evolution** - Add columns without breaking readers
- ✅ **Time travel** - Audit trail, rollback, debugging
- ✅ **Better updates/deletes** - Don't rewrite entire dataset
- ✅ **Data quality** - Built-in validation and consistency
- ✅ **Same performance** - Still Parquet underneath

### Implementation Strategy

**Both Engines Support Delta:**

| Engine | Delta Library | Capabilities |
|--------|---------------|--------------|
| **Pandas** | `deltalake` (delta-rs) | Read, write, VACUUM, time travel, partitioning |
| **Spark** | `delta-spark` | Full Delta features (all Pandas features + OPTIMIZE, MERGE) |

### Configuration

```yaml
nodes:
  - name: write_output
    write:
      connection: silver
      path: sales.delta
      # format: delta  # ← Default, can be omitted
      mode: append
```

### Warn on Non-Delta Formats

```python
# Production environment
if format in ["parquet", "csv"] and os.getenv("ODIBI_ENV") == "production":
    warnings.warn(
        f"⚠️  Writing to {format} format. "
        f"Consider using format='delta' for ACID transactions, "
        f"schema evolution, and time travel.",
        UserWarning
    )
```

### Phase 2 Delta Features

**Core Operations (Both Engines):**
- ✅ Read/Write Delta tables
- ✅ Delta as default format
- ✅ Append/Overwrite modes
- ✅ Time travel (read specific versions via `versionAsOf`)
- ✅ VACUUM (cleanup old files, save storage costs)
- ✅ History (list all versions/commits)
- ✅ Restore (rollback to previous version)
- ✅ Schema evolution (automatic)
- ✅ Works with ADLS/S3/GCS

**Partitioning (Optional, Not Enforced):**
```yaml
# Optional partitioning for large datasets
write:
  path: sensors.delta
  partition_by: [year, month]  # ← Optional
```

**Warning on partitioning anti-patterns:**
```python
# Warn if too many partitions (> 1000)
# Warn if partitions too small (< 1000 rows per partition)
```

### Phase 3 Advanced Features (Deferred)

❌ **OPTIMIZE** - File compaction (Spark only, medium complexity)  
❌ **MERGE/UPSERT** - Update existing records (Spark only, complex)  
❌ **Z-ORDER** - Clustering optimization (Spark only, advanced)  
❌ **Streaming writes** - Structured Streaming to Delta (complex)

### Dependencies

```toml
[project.dependencies]
deltalake = ">=0.15.0"  # Delta for Pandas (core dependency)

[project.optional-dependencies]
spark = [
    "pyspark>=3.3.0",
    "delta-spark>=2.3.0",  # Delta for Spark
]
```

### Examples

**Read Delta (with time travel):**
```yaml
read:
  connection: bronze
  path: sales.delta
  format: delta
  options:
    versionAsOf: 5  # Read version 5
```

**Write Delta (with partitioning):**
```yaml
write:
  connection: silver
  path: sensors.delta
  format: delta
  partition_by: [year, month]
  mode: append
```

**Maintenance (VACUUM):**
```python
# Cleanup old files (save storage costs)
engine.vacuum_delta(
    connection="silver",
    path="sales.delta",
    retention_hours=168  # Keep last 7 days
)
```

---

## 8. Spark Session Management

### Decision

SparkEngine accepts **optional `spark_session` parameter** with intelligent fallback.

### Implementation

```python
class SparkEngine:
    def __init__(self, connections, spark_session=None):
        from pyspark.sql import SparkSession
        
        if spark_session:
            # User provided session - use it
            self.spark = spark_session
        else:
            # Get existing session or create new one
            self.spark = SparkSession.builder \
                .appName("ODIBI") \
                .getOrCreate()
        
        # Configure all ADLS storage accounts
        self._configure_all_connections(connections)
```

### How It Works

**Databricks (existing session):**
```python
# Databricks has active session
engine = SparkEngine(connections)
# Uses existing Databricks session via getOrCreate()
```

**Local development (auto-create):**
```python
# No active session
engine = SparkEngine(connections)
# Creates new local session via getOrCreate()
```

**Custom session (advanced):**
```python
# User creates session with custom config
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

engine = SparkEngine(connections, spark_session=spark)
# Uses user's custom session
```

### Rationale

**Simple approach:**
- `getOrCreate()` handles both Databricks and local scenarios
- No need to track session ownership
- No need for PySpark version detection
- Python handles cleanup automatically

**Can optimize later** if needed (session pooling, explicit stop, etc.).

---

## 9. Testing Strategy

### Decision

**Mock everything for automated tests**, use Databricks Community Edition for manual verification.

### Automated Testing (CI/CD)

**Test locally without cloud resources:**

| Test Layer | Approach | Tools |
|------------|----------|-------|
| **Spark logic** | Local Spark session (`master("local[1]")`) | pytest + pyspark |
| **Azure credentials** | Mock Azure SDK | pytest + unittest.mock |
| **Key Vault** | Mock SecretClient | pytest + monkeypatch |
| **Cloud I/O** | Skip in CI (test manually) | pytest.mark.skip |

### Example: Mock Key Vault

```python
@pytest.fixture
def mock_key_vault(monkeypatch):
    """Mock Azure Key Vault SecretClient."""
    from unittest.mock import MagicMock
    
    mock_secret = MagicMock()
    mock_secret.value = "test-storage-key-12345"
    
    mock_client = MagicMock()
    mock_client.get_secret.return_value = mock_secret
    
    monkeypatch.setattr(
        "azure.keyvault.secrets.SecretClient",
        lambda **kwargs: mock_client
    )
    return mock_client

def test_key_vault_auth_mode(mock_key_vault):
    """Test Key Vault authentication fetches and caches key."""
    conn = AzureADLS(
        account="testaccount",
        container="test",
        auth_mode="key_vault",
        key_vault_name="test-kv",
        secret_name="test-secret"
    )
    
    key1 = conn.get_storage_key()
    key2 = conn.get_storage_key()  # Should use cache
    
    assert key1 == "test-storage-key-12345"
    assert key2 == key1
    mock_key_vault.get_secret.assert_called_once()
```

### Example: Test Spark Config

```python
def test_spark_multi_account_config():
    """Test SparkEngine configures multiple storage accounts."""
    conn1 = AzureADLS(
        account="account1",
        container="c1",
        auth_mode="direct_key",
        account_key="fake-key-1",
        validate=False
    )
    conn2 = AzureADLS(
        account="account2",
        container="c2",
        auth_mode="direct_key",
        account_key="fake-key-2",
        validate=False
    )
    
    engine = SparkEngine(connections={"bronze": conn1, "silver": conn2})
    
    # Verify Spark config was set correctly
    key1 = engine.spark.conf.get(
        "fs.azure.account.key.account1.dfs.core.windows.net"
    )
    key2 = engine.spark.conf.get(
        "fs.azure.account.key.account2.dfs.core.windows.net"
    )
    
    assert key1 == "fake-key-1"
    assert key2 == "fake-key-2"
```

### Manual Testing (Databricks CE)

**When to test manually:**
- End-to-end pipeline runs
- Real Key Vault integration
- Real ADLS read/write
- Delta table operations on cloud storage

**Not for CI/CD** - too slow, requires Azure resources, network-dependent.

### What We're Testing

✅ **Our code's logic** (configuration, validation, caching)  
✅ **Integration patterns** (does our code call Azure SDK correctly?)  
❌ **Not testing Azure** (Key Vault works, Spark works - Microsoft's job)

---

## 10. Key Vault Performance

### Decision

**Parallel credential fetching** with proper error handling and timeouts.

### Performance Characteristics

**Key Vault latency:**
- Same region: 50-150ms per secret
- Cross-region: 150-500ms per secret

**Sequential vs Parallel:**
```
Sequential (5 connections): 5 × 100ms = 500ms
Parallel (5 connections):   ~150ms total
```

### Implementation

```python
import concurrent.futures

def _configure_all_connections(self, connections, timeout=30):
    """Fetch all Key Vault secrets in parallel."""
    adls_connections = [
        (name, conn) for name, conn in connections.items()
        if isinstance(conn, AzureADLS)
    ]
    
    if not adls_connections:
        return
    
    errors = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all fetch tasks
        future_to_conn = {
            executor.submit(conn.get_storage_key): (name, conn)
            for name, conn in adls_connections
        }
        
        # Wait for all with timeout
        done, not_done = concurrent.futures.wait(
            future_to_conn.keys(),
            timeout=timeout,
            return_when=concurrent.futures.ALL_COMPLETED
        )
        
        # Handle timeouts
        if not_done:
            for future in not_done:
                name, conn = future_to_conn[future]
                future.cancel()
                errors[name] = TimeoutError(
                    f"Key Vault fetch timed out after {timeout}s"
                )
        
        # Collect results and errors
        for future in done:
            name, conn = future_to_conn[future]
            try:
                future.result()
            except Exception as e:
                errors[name] = e
    
    # Fail fast if any errors
    if errors:
        error_details = "\n".join([
            f"  - {name}: {str(e)}"
            for name, e in errors.items()
        ])
        raise RuntimeError(
            f"Failed to fetch storage keys for {len(errors)} connection(s):\n"
            f"{error_details}"
        )
    
    # Configure Spark with cached keys
    for name, conn in adls_connections:
        conn.configure_spark(self.spark)
```

### Error Handling

**All errors reported clearly:**
```
RuntimeError: Failed to fetch storage keys for 2 connection(s):
  - bronze: Secret 'bronze-key' not found in vault 'company-kv'
  - gold: Access denied to vault 'company-kv' (403 Forbidden)
```

### Additional Optimizations

**1. Shared DefaultAzureCredential:**
```python
# Global credential (reused across connections)
_azure_credential = None

def get_azure_credential():
    global _azure_credential
    if not _azure_credential:
        from azure.identity import DefaultAzureCredential
        _azure_credential = DefaultAzureCredential()
    return _azure_credential
```

**2. Shared SecretClient per Key Vault:**
```python
# Cache clients by vault name
_secret_clients = {}

def get_secret_client(key_vault_name):
    if key_vault_name not in _secret_clients:
        credential = get_azure_credential()
        vault_url = f"https://{key_vault_name}.vault.azure.net"
        _secret_clients[key_vault_name] = SecretClient(
            vault_url=vault_url,
            credential=credential
        )
    return _secret_clients[key_vault_name]
```

### Expected Performance

| Connections | Startup Time |
|-------------|--------------|
| 1 | ~100ms |
| 5 | ~150ms (parallel) |
| 10 | ~200ms (parallel) |

**Good enough for Phase 2.** Can optimize further in Phase 3 if needed.

---

## 11. Local Development Experience

### Decision

**Explicit config files** for local vs production environments. No magic auto-detection.

### Pattern: Separate Config Files

```yaml
# config.prod.yaml (committed to git)
connections:
  bronze:
    type: azure_adls
    account: storageaccount
    container: bronze
    auth_mode: key_vault  # ← Explicit
    key_vault_name: company-kv
    secret_name: bronze-key
```

```yaml
# config.local.yaml (in .gitignore, developer creates)
connections:
  bronze:
    type: azure_adls
    account: storageaccount
    container: bronze
    auth_mode: direct_key  # ← Explicit
    account_key: "${BRONZE_KEY}"  # From .env file
```

### Developer Setup

**One-time setup:**
1. Copy `config.prod.yaml` to `config.local.yaml`
2. Change `auth_mode: key_vault` to `auth_mode: direct_key`
3. Add `account_key: "${BRONZE_KEY}"`
4. Create `.env` file with storage keys
5. Add `config.local.yaml` and `.env` to `.gitignore`

**Daily usage:**
```bash
# Local development
python run_pipeline.py --config config.local.yaml

# Production (Databricks)
python run_pipeline.py --config config.prod.yaml
```

### Rationale

**Why explicit configs:**
- ✅ No magic behavior (ODIBI_ENV auto-detection rejected)
- ✅ Clear intent (can see which auth mode from YAML)
- ✅ Simple to understand
- ✅ Safe (can't accidentally run prod config locally)

**Why direct_key for local dev:**
- ✅ Simpler setup than Azure CLI auth
- ✅ Works on any laptop
- ✅ No Azure permissions needed
- ✅ Faster (no Key Vault calls)

### .gitignore

```
# Local development configs (contain secrets)
config.local.yaml
.env
*.local.yaml
```

---

## 12. Out of Scope (Phase 2)

### Explicitly NOT Implementing

The following were considered but **excluded from Phase 2** to keep scope manageable:

#### Authentication Methods

❌ **SAS tokens** - Storage account keys via Key Vault are sufficient  
❌ **Service principal auth** - DefaultAzureCredential + managed identity is simpler  
❌ **Azure CLI credential passthrough** - DefaultAzureCredential handles this automatically  
❌ **Databricks secrets scope** - Key Vault integration is more direct  

**Rationale:** Start with account keys (simplest, most common). Add SAS/service principal in Phase 3+ if users request.

#### Advanced Features

❌ **Cross-tenant Key Vault** - Complex edge case, rarely needed  
❌ **Multiple Key Vaults per connection** - One Key Vault per project is sufficient  
❌ **Dynamic secret rotation mid-pipeline** - Re-run pipeline if keys rotate  
❌ **Connection pooling/reuse** - Premature optimization  

#### Other Cloud Providers

❌ **AWS credential management** - Phase 3+ (AWS S3 connection type)  
❌ **GCP credential management** - Phase 3+ (GCS connection type)  

**Rationale:** Focus on Azure (target environment). Other clouds are different **connection types**, not variations of Azure.

#### Documentation Formats

❌ **Portal screenshots** - Outdated quickly, hard to maintain  
❌ **Azure CLI manual docs** - Automated tools (setup notebook) are better  
❌ **Video tutorials** - Not maintainable in early stages  

**Rationale:** Automated tools (setup notebook + utility) provide better UX and stay current.

### Why These Exclusions Matter

**Focus principle:** Phase 2 should deliver **solid foundation** for Azure + Databricks, not every possible feature.

**Can add later:** All excluded items can be added in Phase 3+ if users request them.

**User escape hatch:** `direct_key` mode provides flexibility if Key Vault doesn't work for someone.

---

## 13. Implementation Roadmap

### Decision: 3-Phase Delivery (3 Weeks)

Phase 2 is divided into 3 mini-phases for incremental delivery and clear milestones.

**Phase 2A: Foundation (Week 1)**
- Goal: Get ADLS working with Key Vault auth
- Deliverable: Multi-account ADLS integration with Parquet/CSV support

**Phase 2B: Delta Lake (Week 2)**
- Goal: Add modern data engineering capabilities
- Deliverable: Production-ready Delta Lake support

**Phase 2C: Performance & Polish (Week 3)**
- Goal: Production-grade performance and documentation
- Deliverable: Enterprise-ready ODIBI

---

## 14. Implementation Checklist

### Phase 2A: Foundation (Week 1)

#### Code Changes

#### `odibi/connections/azure_adls.py`
- [ ] Add parameters: `auth_mode`, `key_vault_name`, `secret_name`, `account_key`
- [ ] Set `auth_mode="key_vault"` as default
- [ ] Add `validate()` method (eager validation)
- [ ] Add `get_storage_key()` method (Key Vault fetch + caching - sequential for 2A)
- [ ] Add `pandas_storage_options()` method (returns dict for fsspec)
- [ ] Add `configure_spark()` method (sets Spark config)
- [ ] Add warning when `direct_key` used in production (`ODIBI_ENV` check)

#### `odibi/engine/pandas_engine.py`
- [ ] Add `_merge_storage_options()` helper
- [ ] Update `read()` to support Parquet and CSV formats
- [ ] Update `write()` to support Parquet and CSV formats
- [ ] Call `_merge_storage_options()` before all reads/writes
- [ ] Skip `mkdir` for remote URIs (`abfss://`, `s3://`, etc.)

#### `odibi/engine/spark_engine.py`
- [ ] Add `spark_session` parameter to `__init__` (optional)
- [ ] Use `getOrCreate()` if no session provided
- [ ] Add `_configure_all_connections()` method (sequential for 2A)
- [ ] Call `_configure_all_connections()` in `__init__`
- [ ] Loop through connections, call `configure_spark()` on each ADLS connection
- [ ] Support Parquet and CSV read/write

#### Testing (Phase 2A)

- [ ] `tests/test_azure_adls_auth.py` (NEW):
  - Test validation for both auth modes
  - Test Key Vault fetch (mocked, sequential)
  - Test `pandas_storage_options()` returns correct dict
  - Test `configure_spark()` sets correct Spark config
  - Test caching behavior
  - Test production warning for `direct_key`

- [ ] `tests/test_spark_session.py` (NEW):
  - Test session parameter acceptance
  - Test getOrCreate() fallback
  - Test multi-account configuration

- [ ] Manual test in Databricks CE:
  - Basic pipeline with Key Vault
  - Multi-account Parquet read/write

#### Documentation (Phase 2A)

- [ ] `examples/template_full.yaml` - Basic ADLS example with Key Vault
- [ ] `README.md` - Quick start guide for ADLS + Key Vault
- [ ] `docs/LOCAL_DEVELOPMENT.md` (NEW) - config.local.yaml setup

#### Dependencies (Phase 2A)

- [ ] Add to `pyproject.toml`:
  ```toml
  [project.optional-dependencies]
  azure = [
      "azure-identity>=1.12.0",
      "azure-keyvault-secrets>=4.7.0",
      "adlfs>=2023.1.0",
  ]
  ```

---

### Phase 2B: Delta Lake (Week 2)

#### Code Changes

#### `odibi/engine/pandas_engine.py`
- [ ] Add Delta support: `read()` with `deltalake.DeltaTable`
- [ ] Add Delta support: `write()` with `write_deltalake()`
- [ ] Add `vacuum_delta()` method
- [ ] Add `get_delta_history()` method
- [ ] Add `restore_delta()` method
- [ ] Support `partition_by` parameter for Delta writes
- [ ] Support `filters` parameter for partition pruning
- [ ] Warn on partitioning anti-patterns (>1000 partitions, tiny partitions)
- [ ] Default format to `delta` in `write()` method
- [ ] Warn when using Parquet/CSV in production

#### `odibi/engine/spark_engine.py`
- [ ] Add Delta support: `read()`, `write()` with delta format
- [ ] Add `vacuum_delta()` method
- [ ] Add `get_delta_history()` method
- [ ] Add `restore_delta()` method
- [ ] Support `partition_by` parameter for Delta writes
- [ ] Support `filters` parameter for partition pruning
- [ ] Default format to `delta` in `write()` method

#### Testing (Phase 2B)

- [ ] `tests/test_delta_support.py` (NEW):
  - Test Delta read/write (both engines, mocked)
  - Test VACUUM operation
  - Test history and restore
  - Test partitioning support
  - Test partition filtering
  - Test warnings on partitioning anti-patterns
  - Test default format is Delta

- [ ] Manual test in Databricks CE:
  - Delta read/write/VACUUM on ADLS
  - Partitioned Delta tables
  - Time travel, history, restore

#### Documentation (Phase 2B)

- [ ] `docs/FILE_FORMATS.md` (NEW):
  - Explain Delta Lake vs Parquet vs CSV
  - When to use each format
  - Delta best practices
  - Partitioning guide (when to use, anti-patterns)
  - Examples of Delta operations (VACUUM, history, restore)

- [ ] Update `examples/template_full.yaml` - Delta examples with partitioning

#### Dependencies (Phase 2B)

- [ ] Add to `pyproject.toml`:
  ```toml
  [project.dependencies]
  deltalake = ">=0.15.0"  # Delta for Pandas
  
  [project.optional-dependencies]
  spark = [
      "pyspark>=3.3.0",
      "delta-spark>=2.3.0",
  ]
  ```

---

### Phase 2C: Performance & Polish (Week 3)

#### Code Changes

#### `odibi/connections/azure_adls.py`
- [ ] Implement shared `DefaultAzureCredential` (global singleton)
- [ ] Implement shared `SecretClient` per Key Vault (cache clients)

#### `odibi/engine/spark_engine.py`
- [ ] Refactor `_configure_all_connections()` to parallel fetching
- [ ] Implement timeout protection (30s default)
- [ ] Collect all errors and report clearly
- [ ] Add comprehensive error messages

#### `odibi/utils/setup_helpers.py` (NEW)
- [ ] Create file
- [ ] Add `get_databricks_identity_info()` function
- [ ] Add `print_keyvault_setup_instructions()` function
- [ ] Add docstrings with examples
- [ ] Handle errors gracefully (not in Databricks, identity unavailable, etc.)

#### Setup Tools (Phase 2C)

- [ ] `setup/databricks_setup.ipynb` (NEW):
  - Cell 1: Install dependencies
  - Cell 2: Get and display workspace identity info
  - Cell 3: Generate Azure CLI command with user's Key Vault name
  - Cell 4: Test Key Vault connection
  - Cell 5: List available secrets (validate setup)
  - Add markdown explanations between cells
  - Add troubleshooting section

#### Testing (Phase 2C)

- [ ] Add to `tests/test_azure_adls_auth.py`:
  - Test parallel Key Vault fetching
  - Test error handling (missing secrets, timeout)
  - Test shared credential/client caching

- [ ] Performance testing:
  - Measure startup time with 5 connections (target: <200ms)
  - Test timeout protection (simulate hung Key Vault)

- [ ] End-to-end test in Databricks CE:
  - Full pipeline with all features
  - Multi-account, Delta, partitioned
  - Setup notebook walkthrough

#### Documentation (Phase 2C)

- [ ] `docs/databricks_setup.md` (NEW):
  - Comprehensive setup guide
  - Explain why Key Vault (security, rotation, audit)
  - Document setup notebook usage
  - Document utility helper usage
  - Add troubleshooting section
  - Link to Azure Key Vault documentation

- [ ] `docs/CONFIGURATION_EXPLAINED.md`:
  - Add section on Azure authentication modes
  - Explain Key Vault vs direct_key trade-offs
  - Add examples with both modes
  - Link to databricks_setup.md

- [ ] Update `README.md`:
  - Databricks Setup section (5-minute setup)
  - Link to setup notebook
  - Key Vault configuration example
  - Note about direct_key fallback

- [ ] Create example pipelines:
  - `examples/example_multi_account.yaml`
  - `examples/example_delta_pipeline.yaml`
  - `examples/example_partitioned_delta.yaml`

---

## Success Criteria

**Phase 2 design is complete when:**

- ✅ All design decisions documented in this file
- ✅ Implementation roadmap created (3 phases)
- ✅ Dependencies identified
- ✅ User onboarding plan defined
- ✅ Testing strategy outlined
- ✅ Out-of-scope items explicitly listed
- ✅ Critical implementation details addressed (testing, Delta, performance, sessions)

**Phase 2A complete when:**
- ADLS connection working with Key Vault auth
- Multi-account support (Spark + Pandas)
- Parquet/CSV read/write functional
- Basic tests passing
- Can deploy basic pipelines to Databricks

**Phase 2B complete when:**
- Delta Lake support in both engines
- VACUUM, history, restore working
- Partitioning supported with warnings
- Delta-specific tests passing
- Can deploy production Delta pipelines

**Phase 2C complete when:**
- Parallel Key Vault fetching implemented
- Startup time <200ms for 5 connections
- Setup notebook tested in Databricks
- Comprehensive documentation complete
- Example pipelines work end-to-end
- All Phase 2 features production-ready

---

## Design Decisions Summary

**Core Decisions:**
1. ✅ **Transforms never need connections** - operate on data in context
2. ✅ **Multi-account support** - Spark global config, Pandas per-operation injection
3. ✅ **Key Vault auth default** - with direct_key fallback for local dev
4. ✅ **Delta Lake default format** - enforce best practices, support legacy formats
5. ✅ **Mock everything for tests** - Databricks CE for manual verification only
6. ✅ **Parallel Key Vault fetching** - 150ms vs 500ms for 5 connections
7. ✅ **Explicit configs** - no ODIBI_ENV magic, separate local/prod files
8. ✅ **Simple Spark session** - accept optional param, getOrCreate() fallback

**Key Features Added in Refinement:**
- Delta support in both engines (deltalake + delta-spark)
- VACUUM, history, restore operations
- Optional partitioning with anti-pattern warnings
- Parallel credential fetching with error handling
- Timeout protection for Key Vault calls
- Comprehensive test mocking strategy

---

## Implementation Timeline

**Week 1 (Phase 2A): Foundation**
- Day 1-2: Azure ADLS connection + Key Vault auth
- Day 3-4: Multi-account support + Spark session management
- Day 5: Testing + basic documentation
- **Deliverable:** Working ADLS integration

**Week 2 (Phase 2B): Delta Lake**
- Day 1-2: Delta read/write (both engines)
- Day 3: VACUUM, history, restore
- Day 4: Partitioning support
- Day 5: Testing + Delta documentation
- **Deliverable:** Production-ready Delta support

**Week 3 (Phase 2C): Performance & Polish**
- Day 1-2: Parallel Key Vault fetching + error handling
- Day 3: Setup notebook + utilities
- Day 4: Comprehensive documentation
- Day 5: End-to-end testing + examples
- **Deliverable:** Enterprise-ready ODIBI

**Total:** 3 weeks (15 days)

---

## Next Steps

1. **Week 1 Start:** Begin Phase 2A implementation
2. **Daily testing:** Run mocked tests locally
3. **Weekly milestones:** Test in Databricks CE at end of each phase
4. **Ship incrementally:** Each phase delivers value

---

**Status:** ✅ Design Refinement Complete - 3-Phase Roadmap Defined  
**Document Owner:** @henryodibi11  
**Last Review:** 2025-11-08  
**Implementation Start:** TBD
