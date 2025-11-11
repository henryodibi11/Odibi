# Phase 2B Walkthrough - Getting Started

**Version:** v1.2.0-alpha.2-phase2b  
**Focus:** Delta Lake Support

---

## Quick Start

### 1. Install Dependencies

```bash
# Navigate to odibi directory
cd d:/odibi

# Install ODIBI with Delta support
pip install -e ".[pandas,azure]"

# Optional: Install Spark support
pip install -e ".[spark,azure]"
```

### 2. Open the Walkthrough

```bash
# Start Jupyter
jupyter notebook walkthroughs/phase2b_delta_lake.ipynb
```

---

## What's in the Walkthrough

The `phase2b_delta_lake.ipynb` notebook covers:

### Part 1-2: Basics
- ✅ Setup and sample data
- ✅ Write Delta tables
- ✅ Read Delta tables

### Part 3-4: Versioning
- ✅ Append mode (creates versions)
- ✅ Time travel (read specific versions)

### Part 5-7: Operations
- ✅ History tracking
- ✅ Creating test versions
- ✅ Restore to previous version

### Part 8: Maintenance
- ✅ VACUUM operations
- ✅ Dry run mode
- ✅ Clean old files

### Part 9: Advanced
- ✅ Partitioning with warnings
- ✅ Performance best practices

### Part 10: Cloud Integration
- ✅ Delta + ADLS
- ✅ Real cloud testing setup

### Part 11: Spark (Optional)
- ✅ SparkEngine Delta support
- ✅ Spark read/write operations

---

## Expected Outcomes

After completing the walkthrough, you'll be able to:

1. **Write Delta Tables**
   ```python
   engine.write(df, conn, format="delta", path="sales.delta", mode="append")
   ```

2. **Use Time Travel**
   ```python
   df_v5 = engine.read(conn, format="delta", path="sales.delta", options={"versionAsOf": 5})
   ```

3. **Track History**
   ```python
   history = engine.get_delta_history(conn, "sales.delta")
   ```

4. **Restore Tables**
   ```python
   engine.restore_delta(conn, "sales.delta", version=5)
   ```

5. **Run VACUUM**
   ```python
   result = engine.vacuum_delta(conn, "sales.delta", retention_hours=168)
   ```

6. **Use Delta with ADLS**
   ```python
   # Same API works with cloud storage!
   engine.write(df, adls_conn, format="delta", path="sales.delta")
   ```

---

## Testing with Real ADLS

### Step 1: Get Your Azure Credentials

You'll need:
- Storage account name
- Container name
- Storage account key (or Key Vault setup)

### Step 2: Update Part 10 of the Notebook

Replace placeholders:
```python
adls_conn = AzureADLS(
    account="YOUR_STORAGE_ACCOUNT",      # ← Your account
    container="YOUR_CONTAINER",           # ← Your container
    path_prefix="delta_test",
    auth_mode="direct_key",
    account_key="YOUR_ACCOUNT_KEY",       # ← Your key
    validate=True                         # ← Enable validation
)
```

### Step 3: Uncomment and Run Cloud Cells

The walkthrough has commented-out cells for cloud testing:
```python
# Uncomment when ready to test with real ADLS
# engine.write(df, adls_conn, format="delta", path="sales.delta")
```

---

## Troubleshooting

### Issue: Delta Lake Import Error

```
ImportError: No module named 'deltalake'
```

**Solution:**
```bash
pip install deltalake
# Or
pip install -e ".[pandas]"
```

### Issue: Spark Import Error

```
ImportError: No module named 'pyspark'
```

**Solution:**
```bash
pip install -e ".[spark]"
```

This is optional - you can skip Part 11 if you don't need Spark.

### Issue: ADLS Connection Failed

```
Error: Unable to connect to storage account
```

**Check:**
1. Account name is correct
2. Container exists
3. Account key is valid
4. Network can reach Azure (not behind firewall)

---

## Next Steps After Walkthrough

### 1. Build a Real Pipeline

Use the example:
```bash
cat examples/example_delta_pipeline.yaml
```

Update with your connections and run:
```python
from odibi.pipeline import Pipeline
manager = Pipeline.from_yaml("examples/example_delta_pipeline.yaml")
manager.run("production_delta_etl")
```

### 2. Read the Quick Reference

```bash
cat docs/DELTA_LAKE_GUIDE.md
```

This has all the common patterns and best practices.

### 3. Review Design Decisions

```bash
cat docs/PHASE2_DESIGN_DECISIONS.md
```

Section 7 explains Delta Lake strategy in detail.

### 4. Explore Phase 2A Integration

Delta works seamlessly with Phase 2A ADLS features:
```bash
jupyter notebook walkthroughs/phase2a_adls_test.ipynb
```

---

## Sample Workflow

Here's a typical workflow using the walkthrough:

### Day 1: Local Testing
```python
# Run Parts 1-9
# - Learn Delta basics
# - Test time travel
# - Practice restore operations
# - Run VACUUM
```

### Day 2: Cloud Integration
```python
# Set up ADLS connection (Part 10)
# - Get Azure credentials
# - Update connection config
# - Test write to ADLS
# - Test read from ADLS
```

### Day 3: Production Pipeline
```python
# Build real pipeline
# - Use example_delta_pipeline.yaml
# - Update with your data sources
# - Test end-to-end
# - Set up VACUUM schedule
```

---

## Files Created

The walkthrough creates these test files:
```
delta_test_data/
├── sales.delta/
│   ├── _delta_log/
│   │   ├── 00000000000000000000.json
│   │   ├── 00000000000000000001.json
│   │   └── ...
│   └── *.parquet
├── partitioned_sales.delta/
│   ├── month=1/*.parquet
│   ├── month=2/*.parquet
│   └── ...
└── spark_sales.delta/  (if Spark available)
```

**Cleanup:** Last cell in notebook has cleanup code (commented).

---

## Performance Expectations

### Local Testing
- Small datasets (< 1000 rows): < 1 second
- Medium datasets (1K-100K rows): 1-5 seconds
- Large datasets (> 100K rows): Consider Spark

### ADLS Testing
- First write: 2-5 seconds (includes auth)
- Subsequent writes: 1-3 seconds (credential cache)
- Reads: Similar to local (network dependent)

---

## Support

### Documentation
- `docs/DELTA_LAKE_GUIDE.md` - Quick reference
- `docs/PHASE2_DESIGN_DECISIONS.md` - Design docs
- `CHANGELOG.md` - What's new

### Examples
- `examples/example_delta_pipeline.yaml` - Full pipeline
- `examples/template_full.yaml` - All ODIBI features
- `examples/template_full_adls.yaml` - ADLS examples

### Tests
- `tests/test_delta_pandas.py` - Delta test suite
- Run with: `pytest tests/test_delta_pandas.py -v`

---

## Happy Delta Lake Engineering! 🚀

Phase 2B gives you production-ready data lakehouse capabilities:
- ✅ ACID transactions
- ✅ Time travel
- ✅ Schema evolution
- ✅ Data recovery
- ✅ Cost optimization (VACUUM)

Start with the walkthrough, then build your production pipelines!
