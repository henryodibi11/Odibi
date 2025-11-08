# Next Session: Phase 2 Implementation - Spark Engine & Azure ADLS

**Previous Session:** Phase 2 Design Planning Complete  
**Status:** ✅ Planning complete → Ready for implementation  
**Thread:** T-63b2eb6f-87f4-4630-830c-07f26db2aa71

---

## ✅ What Was Completed

### Config Refactor Documentation (Complete)
- ✅ Updated CHANGELOG.md with breaking changes & migration guide
- ✅ Fixed CONFIGURATION_EXPLAINED.md (removed dual-parsing, added story.connection confusion point)
- ✅ Updated walkthroughs 01 and 05 for v1.1
- ✅ Ran end-to-end pipeline test with example_local.yaml (both pipelines successful)
- ✅ All 86 tests passing, formatting/linting clean

### Phase 2 Design Planning (Complete)
- ✅ **Transform pattern decided**: No connections needed (operates on data in context)
- ✅ **Multi-account storage strategy**: Spark (global config) vs Pandas (per-operation injection)
- ✅ **Authentication modes**: `key_vault` (default) and `direct_key` (fallback)
- ✅ **Databricks integration validated**: Workspace managed identity + DefaultAzureCredential works
- ✅ **User onboarding plan**: Setup notebook + utility helpers (no manual docs)
- ✅ **Validation strategy**: Eager validation (fail fast on connection init)
- ✅ **Out of scope defined**: SAS tokens, service principal, cross-tenant KV, etc.
- ✅ **Complete design document created**: `docs/PHASE2_DESIGN_DECISIONS.md`
- ✅ **PHASES.md updated**: Phase 2 redefined with new scope and deliverables

---

## 🎯 Phase 2 Implementation Ready

**Design Document:** [docs/PHASE2_DESIGN_DECISIONS.md](docs/PHASE2_DESIGN_DECISIONS.md)

### Implementation Checklist

**See the design document for complete checklist.** Key areas:

#### Code Changes
1. **`odibi/connections/azure_adls.py`**
   - Add auth modes (`key_vault`, `direct_key`)
   - Add `get_storage_key()`, `pandas_storage_options()`, `configure_spark()`
   - Eager validation

2. **`odibi/engine/spark_engine.py`**
   - Implement `read()`, `write()`, `execute_sql()`
   - Configure all connections at init

3. **`odibi/engine/pandas_engine.py`**
   - Add `_merge_storage_options()`
   - Inject credentials into read/write

4. **`odibi/utils/setup_helpers.py`** (NEW)
   - `get_databricks_identity_info()`
   - `print_keyvault_setup_instructions()`

#### Setup Tools
5. **`setup/databricks_setup.ipynb`** (NEW)
   - Interactive Databricks + Key Vault setup
   - Auto-detect identity, generate commands, test connection

#### Documentation
6. **`docs/databricks_setup.md`** (NEW)
7. Update `README.md`, `examples/template_full.yaml`, `docs/CONFIGURATION_EXPLAINED.md`

#### Examples
8. **`examples/example_spark_databricks.yaml`** (NEW)
9. **`examples/example_azure_pandas.yaml`** (NEW)

#### Testing
10. Unit tests for auth modes, Key Vault mocking, Spark config
11. Integration tests (optional - requires Azure resources)

---

## 🔑 Key Design Decisions Summary

### 1. Transforms Don't Need Connections
- Read brings data INTO context → Transform operates on data IN context → Write sends data OUT
- Works for Pandas (in-memory) and Spark (lazy execution)

### 2. Multi-Account Storage
- **Spark**: Set all account configs globally at engine init (no clashes - keys scoped by account)
- **Pandas**: Inject `storage_options` per-operation from connection

### 3. Two Auth Modes
- **`key_vault`** (default): Fetch keys from Key Vault using DefaultAzureCredential
- **`direct_key`** (fallback): User provides key directly (env var, testing)

### 4. Databricks Integration
- Workspace managed identity + DefaultAzureCredential works! ✅
- One-time setup: Grant workspace identity access to Key Vault
- Setup notebook automates the process

### 5. User Onboarding
- **Primary**: Setup notebook (interactive, automated)
- **Secondary**: Utility helpers (programmatic)
- **No**: Azure CLI docs, portal screenshots (automated tools better)

### 6. Validation
- **Eager** (on connection init) not lazy (on first use)
- Fail fast with clear error messages

---

## 📋 Suggested Implementation Order

### Phase 1: Core Authentication (Week 1)
1. Implement `AzureADLS` with both auth modes
2. Add validation and warning logic
3. Add Key Vault integration with caching
4. Unit tests for auth modes

### Phase 2: Pandas Integration (Week 1)
1. Implement `pandas_storage_options()` in `AzureADLS`
2. Add `_merge_storage_options()` in `PandasEngine`
3. Update `read()` and `write()` to inject credentials
4. Skip mkdir for remote URIs
5. Integration tests with real/mocked ADLS

### Phase 3: Spark Engine (Week 2-3)
1. Implement `_configure_all_connections()` in `SparkEngine`
2. Implement `configure_spark()` in `AzureADLS`
3. Implement `SparkEngine.read()` for Parquet, CSV, Delta
4. Implement `SparkEngine.write()` for Parquet, CSV, Delta
5. Implement `SparkEngine.execute_sql()` with temp views
6. Integration tests with local Spark session

### Phase 4: User Onboarding (Week 3)
1. Create `setup/databricks_setup.ipynb`
2. Create `odibi/utils/setup_helpers.py`
3. Test in real Databricks workspace
4. Create `docs/databricks_setup.md`

### Phase 5: Documentation & Examples (Week 4)
1. Update `README.md` with Databricks quick start
2. Create `examples/example_spark_databricks.yaml`
3. Create `examples/example_azure_pandas.yaml`
4. Update `examples/template_full.yaml`
5. Update `docs/CONFIGURATION_EXPLAINED.md`

### Phase 6: QA & Polish (Week 4)
1. Run full test suite
2. Test in real Databricks environment
3. Validate multi-account scenarios
4. Check formatting, linting
5. Update CHANGELOG.md
6. Tag release (v1.2.0-alpha.1)

---

## 🚀 Prompt for Next Session

Copy and paste this:

```
Start Phase 2 implementation - Spark Engine & Azure ADLS authentication.

Design is complete (see docs/PHASE2_DESIGN_DECISIONS.md).

Begin with:
1. Implement azure_adls.py with key_vault and direct_key auth modes
2. Add validation, Key Vault integration, caching
3. Add pandas_storage_options() and configure_spark() methods

Follow the implementation checklist in docs/PHASE2_DESIGN_DECISIONS.md.

Let's start with AzureADLS connection class - implement the full authentication pattern.
```

---

## 📊 Progress Tracking

**Phase 1:** ✅ Complete (Scaffolding + Config Refactor)  
**Phase 2 Planning:** ✅ Complete  
**Phase 2 Implementation:** ⏳ Ready to start  

**Estimated Time:** 4 weeks for full Phase 2 implementation

---

**Last Updated:** 2025-11-08  
**Status:** Phase 2 design complete, ready for implementation  
**Next Focus:** Implement AzureADLS authentication with Key Vault
