# Odibi MCP - Testing Checklist

## ✅ Setup Verification

- [x] MCP server configured in `.continue/config.yaml`
- [x] `ODIBI_CONFIG` points to `exploration.yaml`
- [x] `exploration.yaml` has valid connections
- [x] Python environment has `mcp` package installed
- [ ] MCP server starts without errors (check Continue dev tools)

## 🔍 Discovery Tools (Phase 1)

### `map_environment` - Connection Scouting
- [ ] **ADLS Storage:**
  ```
  map_environment('raw_adls')
  → Should show folders at root level (shallow scan)
  → structure[] contains FolderInfo with is_folder=true
  → Fast (< 3 seconds even on huge containers)
  ```

- [ ] **Drill deeper:**
  ```
  map_environment('raw_adls', path='manufacturing')
  → Shows subfolders or files
  → suggested_sources populated
  ```

- [ ] **Pattern filtering:**
  ```
  map_environment('raw_adls', pattern='*.csv')
  → Only shows CSV files
  ```

- [ ] **SQL Discovery:**
  ```
  map_environment('wwi_dw')
  → Shows schemas (Dimension, Fact, Integration)
  → structure[] contains SchemaInfo
  ```

- [ ] **SQL schema filter:**
  ```
  map_environment('wwi_dw', path='Fact')
  → Only Fact schema tables
  ```

- [ ] **SQL table pattern:**
  ```
  map_environment('wwi_dw', pattern='fact_*')
  → Only tables starting with fact_
  ```

### `profile_source` - Data Profiling

- [ ] **CSV file:**
  ```
  profile_source('raw_adls', 'path/to/file.csv')
  → schema[] with ColumnInfo
  → sample_rows[] with actual data
  → file_options with encoding/delimiter auto-detected
  → confidence > 0.8 if good
  → ready_for dict populated for next step
  ```

- [ ] **Parquet file:**
  ```
  profile_source('raw_adls', 'data.parquet')
  → schema inferred correctly
  → No encoding issues (binary format)
  ```

- [ ] **SQL table:**
  ```
  profile_source('wwi_dw', 'Fact.Sale')
  → schema[] from database
  → sample_rows from actual table
  → candidate_keys detected
  ```

- [ ] **Caching works:**
  ```
  profile_source('raw_adls', 'file.csv')  # First: 8 seconds
  profile_source('raw_adls', 'file.csv')  # Cached: < 100ms
  ```

- [ ] **Cache location:**
  ```
  Check: .odibi/profile_cache/*.json files exist
  ```

## 📚 Knowledge Tools

- [ ] **list_transformers:**
  ```
  list_transformers()
  → Returns 52+ transformers
  → Each has name, parameters, docstring
  ```

- [ ] **list_patterns:**
  ```
  list_patterns()
  → Returns 6 patterns (dimension, fact, scd2, merge, aggregation, date_dimension)
  ```

- [ ] **MCP Resources auto-loaded:**
  ```
  Ask Continue: "What's in the Quick Reference resource?"
  → Should have access without browsing
  ```

## 🛠️ Pipeline Builder (Phase 2)

- [ ] **apply_pattern_template:**
  ```
  apply_pattern_template(
    pattern='dimension',
    node_name='dim_customer',
    source_conn='wwi',
    source_table='Sales.Customer'
  )
  → Returns complete YAML
  ```

- [ ] **Session builder:**
  ```
  create_pipeline('my_pipeline')
  → Returns session_id

  add_node(session_id, 'bronze_orders')
  configure_read(session_id, 'bronze_orders', {...})
  render_pipeline_yaml(session_id)
  → Returns complete YAML
  ```

## 🧪 Validation

- [ ] **validate_pipeline:**
  ```
  validate_odibi_config(yaml_content)
  → Returns errors[] if invalid
  → Returns warnings[] if suspicious
  ```

## 🐛 Common Issues

### Issue: `structure: []` is empty
**Cause:** `catalog.folders` not being transformed  
**Fixed:** ✅ commit `af5d630`

### Issue: `'ProfileSourceResponse' object has no attribute 'model_dump'`
**Cause:** Dataclass vs Pydantic confusion  
**Fixed:** ✅ commit `cccb905` - uses `asdict()` now

### Issue: `map_environment` is slow (30+ seconds)
**Cause:** `recursive=True` scans entire container  
**Fixed:** ✅ commit `c822872` - shallow scan by default

### Issue: Profile cache not working
**Check:**
- `.odibi/profile_cache/` directory exists
- JSON files created after first profile
- use_cache=True (default)

## 📊 Performance Benchmarks

**map_environment (shallow scan):**
- Root level: < 2 seconds
- Subfolder: < 3 seconds
- SQL schemas: < 2 seconds

**profile_source (first time):**
- Local CSV: < 1 second
- ADLS CSV: 2-10 seconds
- SQL table: 3-15 seconds
- Large CSV with encoding detection: 10-30 seconds

**profile_source (cached):**
- Any source: < 100ms ⚡

## 🔄 End-to-End Workflow Test

```
1. map_environment('raw_adls')
   → See folders

2. map_environment('raw_adls', path='manufacturing')
   → See files

3. profile_source('raw_adls', 'manufacturing/sensors.csv')
   → Get schema + samples

4. apply_pattern_template(
     pattern='dimension',
     node_name='dim_sensors',
     source_conn='raw_adls',
     source_path='manufacturing/sensors.csv'
   )
   → Get complete YAML

5. validate_odibi_config(yaml)
   → Confirm valid

6. Save YAML to file
   → Ready to run with `python -m odibi run pipeline.yaml`
```

## ✅ Success Criteria

- [ ] All discovery tools return non-empty results
- [ ] Profiling works for CSV, Parquet, SQL
- [ ] Cache speeds up repeated profiles
- [ ] Pattern templates generate valid YAML
- [ ] Validation catches errors
- [ ] No attribute errors or exceptions
- [ ] Fast response times (< 5s for most operations)

## 🚨 Debug Commands

**Check MCP server logs:**
```
# In Continue dev tools console
# Look for errors during tool execution
```

**Test connection manually:**
```python
from odibi_mcp.context import MCPProjectContext

ctx = MCPProjectContext.from_odibi_yaml('exploration.yaml')
ctx.initialize_connections()
print(ctx.connections.keys())  # Should show: raw_adls, wwi, wwi_dw, etc.
```

**Clear profile cache:**
```python
from odibi_mcp.tools.profile_cache import clear_profile_cache
clear_profile_cache()  # Clears all
clear_profile_cache(older_than_hours=24)  # Only old entries
```

**Test discovery directly:**
```python
from odibi.connections import AzureADLSConnection

conn = AzureADLSConnection(
    account='globaldigitalopsteamprod',
    container='digital-manufacturing',
    # ... auth
)

catalog = conn.discover_catalog(recursive=False, limit=50)
print(catalog['total_datasets'])
print(catalog['folders'])
```
