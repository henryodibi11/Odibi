# Phase 2 Implementation Roadmap

**Timeline:** 3 weeks (15 days)  
**Status:** Ready to start  
**Last Updated:** 2025-11-08

---

## Visual Overview

```
Week 1: Foundation          Week 2: Delta Lake         Week 3: Performance & Polish
    (Phase 2A)                  (Phase 2B)                    (Phase 2C)
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ ADLS Connection     │    │ Delta Read/Write    │    │ Parallel Fetching   │
│ Key Vault Auth      │───▶│ VACUUM/History      │───▶│ Error Handling      │
│ Multi-Account       │    │ Partitioning        │    │ Setup Notebook      │
│ Spark Session       │    │ Time Travel         │    │ Documentation       │
│ Parquet/CSV I/O     │    │ Default Format      │    │ Example Pipelines   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
         ↓                           ↓                           ↓
   "It works!"              "It's modern!"            "It's production-ready!"
```

---

## Phase 2A: Foundation (Week 1)

### Goal
Get ADLS working with Key Vault auth - users can build basic pipelines.

### What Gets Built
- ✅ Azure ADLS connection class
- ✅ Key Vault authentication (sequential)
- ✅ Direct_key fallback for local dev
- ✅ Multi-account support (Spark + Pandas)
- ✅ Spark session management
- ✅ Parquet/CSV read/write

### Success Criteria
- [ ] Can read CSV from one account, write Parquet to another
- [ ] Key Vault auth works in Databricks
- [ ] Direct_key works locally
- [ ] Mocked tests passing

### Day-by-Day Plan
- **Day 1-2:** Azure ADLS connection + Key Vault auth
- **Day 3-4:** Multi-account + Spark session management
- **Day 5:** Testing + basic docs

### What You Can Do After 2A
```yaml
connections:
  bronze:
    type: azure_adls
    account: account1
    auth_mode: key_vault
    key_vault_name: company-kv
    secret_name: bronze-key
  
  silver:
    type: azure_adls
    account: account2
    auth_mode: key_vault
    secret_name: silver-key

pipelines:
  - pipeline: etl
    nodes:
      - name: read_data
        read:
          connection: bronze
          path: raw/sales.csv
          format: csv
      
      - name: write_data
        write:
          connection: silver
          path: clean/sales.parquet
          format: parquet
```

---

## Phase 2B: Delta Lake (Week 2)

### Goal
Add modern data engineering capabilities - Delta as default format.

### What Gets Built
- ✅ Delta read/write (both engines)
- ✅ VACUUM operation (save storage costs)
- ✅ Time travel, history, restore
- ✅ Partitioning support (optional)
- ✅ Delta as default format
- ✅ Warnings on anti-patterns

### Success Criteria
- [ ] Can write Delta tables from both engines
- [ ] VACUUM works and reduces storage
- [ ] Time travel reads old versions
- [ ] Partitioning works with warnings on bad patterns
- [ ] Delta-specific tests passing

### Day-by-Day Plan
- **Day 1-2:** Delta read/write (both engines)
- **Day 3:** VACUUM, history, restore
- **Day 4:** Partitioning support
- **Day 5:** Testing + Delta docs

### What You Can Do After 2B
```yaml
pipelines:
  - pipeline: etl
    nodes:
      - name: read_data
        read:
          connection: bronze
          path: raw/sales.csv
          format: csv
      
      - name: write_delta
        write:
          connection: silver
          path: clean/sales.delta  # ← Delta table!
          # format: delta (default)
          partition_by: [year, month]
          mode: append

# Maintenance
engine.vacuum_delta(
    connection="silver",
    path="clean/sales.delta",
    retention_hours=168  # Keep 7 days
)
```

---

## Phase 2C: Performance & Polish (Week 3)

### Goal
Production-grade performance and documentation.

### What Gets Built
- ✅ Parallel Key Vault fetching (3x faster)
- ✅ Proper error handling (all failures reported)
- ✅ Timeout protection (30s)
- ✅ Setup notebook for Databricks
- ✅ Comprehensive documentation
- ✅ Example pipelines

### Success Criteria
- [ ] Startup time <200ms for 5 connections
- [ ] Clear error messages when credentials fail
- [ ] Setup notebook guides users through Databricks setup
- [ ] Documentation complete (FILE_FORMATS, LOCAL_DEVELOPMENT, databricks_setup)
- [ ] Example pipelines work end-to-end

### Day-by-Day Plan
- **Day 1-2:** Parallel fetching + error handling
- **Day 3:** Setup notebook + utilities
- **Day 4:** Comprehensive documentation
- **Day 5:** End-to-end testing + examples

### Performance Improvements
```
Before 2C:
- 5 connections = 500ms startup (sequential)
- Missing secret = generic error
- No Databricks setup guide

After 2C:
- 5 connections = 150ms startup (parallel)
- Missing secret = "Connection 'bronze': Secret 'wrong-name' not found in vault 'company-kv'"
- Setup notebook guides users in 5 minutes
```

---

## Dependencies by Phase

### Phase 2A
```toml
[project.optional-dependencies]
azure = [
    "azure-identity>=1.12.0",
    "azure-keyvault-secrets>=4.7.0",
    "adlfs>=2023.1.0",
]
```

### Phase 2B
```toml
[project.dependencies]
deltalake = ">=0.15.0"  # Delta for Pandas

[project.optional-dependencies]
spark = [
    "pyspark>=3.3.0",
    "delta-spark>=2.3.0",
]
```

### Phase 2C
- No new dependencies (polish only)

---

## Testing Strategy by Phase

### Phase 2A Testing
- Mock Key Vault (sequential)
- Mock Spark session
- Basic ADLS operations
- Manual test in Databricks CE

### Phase 2B Testing
- Mock Delta operations
- Test VACUUM, history, restore
- Test partitioning warnings
- Manual Delta test in Databricks CE

### Phase 2C Testing
- Mock parallel Key Vault fetching
- Test timeout protection
- Test error aggregation
- Performance benchmarking
- End-to-end Databricks walkthrough

---

## Deliverables Summary

| Phase | Deliverable | Can You... |
|-------|-------------|------------|
| **2A** | Working ADLS integration | Build basic pipelines with Key Vault |
| **2B** | Production Delta support | Use modern data engineering patterns |
| **2C** | Enterprise-ready ODIBI | Deploy fast, reliable, documented pipelines |

---

## Risk Mitigation

**What if Phase 2A takes longer than 1 week?**
- Defer direct_key mode to Phase 2C (focus on Key Vault first)
- Simplify validation (basic checks only)
- Reduce test coverage (core functionality only)

**What if Delta integration is complex?**
- Phase 2B can extend to 1.5 weeks if needed
- Defer partitioning to Phase 2C
- Ship basic Delta first, add VACUUM/history later

**What if documentation takes too long?**
- Phase 2C can be 2 weeks if comprehensive docs needed
- Or ship 2A+2B, defer 2C documentation to Phase 3

**Flexibility:** Each phase delivers value independently.

---

## Go/No-Go Criteria

**Before starting Phase 2A:**
- [ ] Phase 1 complete (86 tests passing)
- [ ] This design doc reviewed and approved
- [ ] Development environment ready (Python, access to Azure test account)

**Before starting Phase 2B:**
- [ ] Phase 2A tests passing
- [ ] Basic ADLS working in Databricks
- [ ] No major blockers from 2A

**Before starting Phase 2C:**
- [ ] Phase 2B tests passing
- [ ] Delta working in Databricks
- [ ] Users can build Delta pipelines

---

**Ready to start Week 1 (Phase 2A)? All design decisions are documented, roadmap is clear, and success criteria are defined.**
