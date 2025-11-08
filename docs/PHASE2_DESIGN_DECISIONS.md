# Phase 2 Design Decisions

**Status:** ✅ Planning Complete  
**Last Updated:** 2025-11-08  
**Session:** Config Refactor + Azure Auth Strategy Planning

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
7. [Out of Scope](#7-out-of-scope-phase-2)
8. [Implementation Checklist](#8-implementation-checklist)

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

## 7. Out of Scope (Phase 2)

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

## 8. Implementation Checklist

### Code Changes

#### `odibi/connections/azure_adls.py`
- [ ] Add parameters: `auth_mode`, `key_vault_name`, `secret_name`, `account_key`
- [ ] Set `auth_mode="key_vault"` as default
- [ ] Add `validate()` method (eager validation)
- [ ] Add `get_storage_key()` method (Key Vault fetch + caching)
- [ ] Add `pandas_storage_options()` method (returns dict for fsspec)
- [ ] Add `configure_spark()` method (sets Spark config)
- [ ] Add warning when `direct_key` used in production (`ODIBI_ENV` check)

#### `odibi/engine/pandas_engine.py`
- [ ] Add `_merge_storage_options()` helper
- [ ] Update `read()` to call `_merge_storage_options()` before pandas read
- [ ] Update `write()` to call `_merge_storage_options()` before pandas write
- [ ] Skip `mkdir` for remote URIs (`abfss://`, `s3://`, etc.)

#### `odibi/engine/spark_engine.py`
- [ ] Add `_configure_all_connections()` method
- [ ] Call `_configure_all_connections()` in `__init__`
- [ ] Loop through connections, call `configure_spark()` on each ADLS connection

#### `odibi/utils/setup_helpers.py` (NEW)
- [ ] Create file
- [ ] Add `get_databricks_identity_info()` function
- [ ] Add `print_keyvault_setup_instructions()` function
- [ ] Add docstrings with examples
- [ ] Handle errors gracefully (not in Databricks, identity unavailable, etc.)

### Setup Tools

#### `setup/databricks_setup.ipynb` (NEW)
- [ ] Create notebook
- [ ] Cell 1: Install dependencies (`%pip install azure-identity azure-keyvault-secrets`)
- [ ] Cell 2: Get and display workspace identity info
- [ ] Cell 3: Generate Azure CLI command with user's Key Vault name
- [ ] Cell 4: Test Key Vault connection
- [ ] Cell 5: List available secrets (validate setup)
- [ ] Add markdown explanations between cells
- [ ] Add troubleshooting section

### Documentation

#### `examples/template_full.yaml`
- [ ] Update connections section to show `key_vault` auth mode (primary example)
- [ ] Comment out `direct_key` example with note "For dev/test only"
- [ ] Add comments explaining Key Vault pattern

#### `README.md`
- [ ] Add "Databricks Setup" section (5-minute setup)
- [ ] Link to `setup/databricks_setup.ipynb`
- [ ] Show `key_vault` configuration example
- [ ] Add note about `direct_key` fallback for local dev

#### `docs/databricks_setup.md` (NEW)
- [ ] Create comprehensive setup guide
- [ ] Explain why Key Vault (security, rotation, audit)
- [ ] Document setup notebook usage
- [ ] Document utility helper usage
- [ ] Add troubleshooting section
- [ ] Link to Azure Key Vault documentation

#### `docs/CONFIGURATION_EXPLAINED.md`
- [ ] Add section on Azure authentication modes
- [ ] Explain Key Vault vs direct_key trade-offs
- [ ] Add examples with both modes
- [ ] Link to databricks_setup.md

### Testing

#### Unit Tests
- [ ] `tests/test_azure_adls_auth.py` (NEW):
  - Test validation for both auth modes
  - Test Key Vault fetch (mocked)
  - Test `pandas_storage_options()` returns correct dict
  - Test `configure_spark()` sets correct Spark config
  - Test caching behavior
  - Test production warning for `direct_key`

#### Integration Tests (Optional - requires Azure resources)
- [ ] `tests/integration/test_azure_auth.py`:
  - Test real Key Vault access (if test environment available)
  - Test multi-account Spark config
  - Test Pandas read/write with storage_options

### Dependencies

- [ ] Add to `pyproject.toml` under `[project.optional-dependencies]`:
  ```toml
  azure = [
      "azure-identity>=1.12.0",
      "azure-keyvault-secrets>=4.7.0",
      "adlfs>=2023.1.0",
  ]
  ```

### Examples

- [ ] Update `examples/example_spark.yaml`:
  - Use `key_vault` auth mode
  - Add comments explaining pattern
  - Show multi-account example

---

## Success Criteria

**Phase 2 design is complete when:**

- ✅ All design decisions documented in this file
- ✅ Implementation checklist created
- ✅ Dependencies identified
- ✅ User onboarding plan defined
- ✅ Testing strategy outlined
- ✅ Out-of-scope items explicitly listed

**Phase 2 implementation will be complete when:**

- All checklist items implemented
- All tests passing
- Setup notebook tested in real Databricks workspace
- Documentation complete and accurate
- Examples work end-to-end

---

## Next Steps

1. **Review this document** with stakeholders
2. **Prioritize checklist items** (what's MVP vs nice-to-have)
3. **Create implementation tasks** (GitHub issues)
4. **Begin Phase 2 execution**

---

**Status:** ✅ Planning Complete - Ready for Implementation  
**Document Owner:** @henryodibi11  
**Approvers:** [TBD]
