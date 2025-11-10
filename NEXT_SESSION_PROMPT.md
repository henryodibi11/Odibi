# Phase 2A Implementation - Start Here

**Context:** Phase 1 complete (config refactor, 86 tests passing). Phase 2 design refinement complete - all 5 critical topics addressed (testing strategy, Delta Lake, Key Vault performance, local dev, Spark session management).

**Previous Thread:** T-e294a4cb-50a0-4e6e-b513-4a9766796155 (Design Refinement - Critical Implementation Details)

---

## 📋 Phase 2 Roadmap (3 Weeks)

**Week 1 (Phase 2A):** Foundation - ADLS + Key Vault  
**Week 2 (Phase 2B):** Delta Lake support  
**Week 3 (Phase 2C):** Performance & Polish  

**Current Focus:** Phase 2A (Week 1)

---

## 🎯 Phase 2A Goal

Implement Azure ADLS connection with Key Vault authentication. Users can build multi-account pipelines with Parquet/CSV.

**Success Criteria:**
- [ ] Can read CSV from one account, write Parquet to another
- [ ] Key Vault auth works in Databricks
- [ ] Direct_key works locally
- [ ] Mocked tests passing

---

## 📚 Key Documents

**Design Decisions:**
- `Odibi/docs/PHASE2_DESIGN_DECISIONS.md` - Complete design (sections 1-14)
  - Section 7: Delta Lake Strategy
  - Section 8: Spark Session Management
  - Section 9: Testing Strategy
  - Section 10: Key Vault Performance
  - Section 11: Local Development Experience
  - Section 14: Phase 2A Checklist

**Roadmap:**
- `Odibi/docs/PHASE2_ROADMAP.md` - 3-week visual plan

---

## 🔨 What to Build (Phase 2A)

### 1. `odibi/connections/azure_adls.py` (NEW)
```python
class AzureADLS(BaseConnection):
    def __init__(
        self,
        account: str,
        container: str,
        path_prefix: str = "",
        auth_mode: str = "key_vault",  # Default
        key_vault_name: str = None,
        secret_name: str = None,
        account_key: str = None,
        validate: bool = True
    ):
        # Implement Key Vault auth (sequential for 2A)
        # Add caching
        # Eager validation
```

**Methods:**
- `validate()` - Check config based on auth_mode
- `get_storage_key()` - Fetch from Key Vault or use direct_key (with caching)
- `pandas_storage_options()` - Return dict for fsspec
- `configure_spark()` - Set Spark config for this account

### 2. Update `odibi/engine/pandas_engine.py`
- Add `_merge_storage_options()` helper
- Update `read()` to support Parquet and CSV with ADLS
- Update `write()` to support Parquet and CSV with ADLS
- Skip `mkdir` for remote URIs (`abfss://`, `s3://`)

### 3. Update `odibi/engine/spark_engine.py`
- Add `spark_session` parameter to `__init__` (optional)
- Use `getOrCreate()` if no session provided
- Add `_configure_all_connections()` method (sequential for 2A)
- Support Parquet and CSV read/write

### 4. Testing (Phase 2A)
```python
# tests/test_azure_adls_auth.py (NEW)
@pytest.fixture
def mock_key_vault(monkeypatch):
    """Mock Azure Key Vault SecretClient."""
    # Mock get_secret() to return test keys

def test_key_vault_auth_mode(mock_key_vault):
    """Test Key Vault authentication fetches and caches key."""
    # Test caching behavior

def test_validation():
    """Test validation for both auth modes."""
    # Test missing fields raise errors
```

### 5. Basic Documentation
- `examples/template_full.yaml` - Basic ADLS example
- `README.md` - Quick start guide
- `docs/LOCAL_DEVELOPMENT.md` (NEW) - config.local.yaml setup

---

## 🧪 Testing Strategy (Important!)

**Automated Tests (NO cloud resources needed):**
- Mock `azure.keyvault.secrets.SecretClient`
- Use local Spark session (`master("local[*]")`)
- Mock all Azure SDK calls
- Fast, deterministic, no network dependency

**Manual Tests (Databricks CE):**
- End of week: Test real Key Vault integration
- Verify multi-account config works
- Only for validation, not CI/CD

---

## 🔑 Critical Design Decisions (Already Made)

### 1. Testing Strategy
- ✅ Mock everything for automated tests
- ✅ Databricks CE for manual verification only
- ✅ No cloud dependency in CI/CD

### 2. Delta Lake Strategy
- ⏸️ Deferred to Phase 2B
- Phase 2A: Parquet/CSV only

### 3. Key Vault Performance
- ✅ Sequential fetching in 2A (simple)
- ⏸️ Parallel fetching in 2C (optimization)

### 4. Local Development
- ✅ Explicit config files (config.local.yaml vs config.prod.yaml)
- ✅ No ODIBI_ENV magic

### 5. Spark Session Management
- ✅ Accept optional `spark_session` parameter
- ✅ Fallback: `SparkSession.builder.getOrCreate()`
- ✅ Simple, no overengineering

---

## 🚀 Start With

**Step 1:** Implement `AzureADLS` connection class
```python
# odibi/connections/azure_adls.py
# Follow Phase 2A checklist in PHASE2_DESIGN_DECISIONS.md section 14
```

**Step 2:** Add mocked tests
```python
# tests/test_azure_adls_auth.py
# Mock Azure SDK, test both auth modes
```

**Step 3:** Update engines
```python
# PandasEngine: _merge_storage_options()
# SparkEngine: spark_session param, _configure_all_connections()
```

---

## 📝 Prompt for Next Session

```
Continue Phase 2A implementation - Azure ADLS + Key Vault authentication.

Design refinement complete (all 5 critical topics addressed in docs/PHASE2_DESIGN_DECISIONS.md).

Follow Phase 2A checklist in section 14.

Start with: Implement AzureADLS connection class in odibi/connections/azure_adls.py.

Reference PHASE2_DESIGN_DECISIONS.md for all implementation details - sections 7-11 have thorough design decisions.
```

---

**Status:** ✅ Phase 2B Complete - Ready for Phase 2C (Performance & Polish)  
**Timeline:** Week 3 of 3 (Phase 2C: Performance Optimization)  
**Last Updated:** 2025-11-09

## Phase 2B Completion Summary

**Completed (Week 2):**
- ✅ Delta Lake read/write (PandasEngine with `deltalake` package)
- ✅ Delta Lake read/write (SparkEngine with `delta-spark` package)
- ✅ VACUUM, history, restore operations for both engines
- ✅ Partitioning support with anti-pattern warnings
- ✅ 12 comprehensive Delta tests (all passing)
- ✅ All 96 tests passing (non-Azure)
- ✅ Production YAML pipeline walkthrough (15 parts)
- ✅ Interactive Delta features walkthrough (12 parts)
- ✅ Complete Delta Lake guide documentation
- ✅ Azure ADLS + Delta Lake integration working end-to-end

**Success Criteria Met:**
- ✅ Can write Delta tables from both engines
- ✅ VACUUM works and reduces storage
- ✅ Time travel reads old versions (tested: v0 with 855 rows vs current 1,710 rows)
- ✅ Partitioning works with warnings on anti-patterns
- ✅ Delta-specific tests passing
- ✅ Production pipeline with Key Vault authentication working

## Next: Phase 2C - Performance & Polish

**Focus:** Production-grade performance and comprehensive tooling

**Key Tasks:**
1. Parallel Key Vault fetching (3x faster startup)
2. Enhanced error handling and timeout protection
3. Interactive Databricks setup notebook (`setup/databricks_setup.ipynb`)
4. Programmatic setup utilities (`odibi/utils/setup_helpers.py`)
5. Comprehensive documentation updates
6. Production example pipelines
7. Performance benchmarking and optimization
