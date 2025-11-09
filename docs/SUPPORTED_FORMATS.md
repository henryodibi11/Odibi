# Supported File Formats

ODIBI's PandasEngine supports multiple file formats with both local and cloud storage (ADLS, S3, etc.).

## Format Support Matrix

| Format | Read | Write | ADLS | S3 | Local | Dependencies |
|--------|------|-------|------|-------|-------|--------------|
| **CSV** | ✅ | ✅ | ✅ | ✅ | ✅ | pandas (built-in) |
| **Parquet** | ✅ | ✅ | ✅ | ✅ | ✅ | pyarrow or fastparquet |
| **JSON** | ✅ | ✅ | ✅ | ✅ | ✅ | pandas (built-in) |
| **Excel** | ✅ | ✅ | ✅ | ✅ | ✅ | openpyxl |
| **Avro** | ✅ | ✅ | ✅ | ✅ | ✅ | fastavro |

## Format Details

### CSV (Comma-Separated Values)

**Use Case:** Simple tabular data, human-readable format

**Read Example:**
```yaml
- name: load_csv
  read:
    connection: bronze
    path: data/sales.csv
    format: csv
    options:
      sep: ","
      header: true
```

**Write Example:**
```yaml
- name: save_csv
  write:
    connection: bronze
    path: output/results.csv
    format: csv
    mode: overwrite
```

---

### Parquet (Apache Parquet)

**Use Case:** Data lake storage, recommended for production

**Benefits:**
- Columnar format (efficient for analytics)
- Built-in compression
- Type preservation
- Fast reads for column-based queries

**Read Example:**
```yaml
- name: load_parquet
  read:
    connection: bronze
    path: data/sales.parquet
    format: parquet
```

**Write Example:**
```yaml
- name: save_parquet
  write:
    connection: silver
    path: output/sales.parquet
    format: parquet
    options:
      compression: snappy  # or gzip, brotli
```

---

### JSON (JSON Lines)

**Use Case:** Semi-structured data, nested objects

**Format:** JSON lines (newline-delimited JSON objects)

**Read Example:**
```yaml
- name: load_json
  read:
    connection: bronze
    path: data/events.json
    format: json
```

**Write Example:**
```yaml
- name: save_json
  write:
    connection: bronze
    path: output/events.json
    format: json
    options:
      orient: records
```

---

### Excel (Microsoft Excel)

**Use Case:** Business reports, spreadsheets

**Supported:** `.xlsx` files

**Dependencies:** Requires `openpyxl`
```bash
pip install openpyxl
```

**Read Example:**
```yaml
- name: load_excel
  read:
    connection: bronze
    path: reports/sales.xlsx
    format: excel
    options:
      sheet_name: "Sheet1"
```

**Write Example:**
```yaml
- name: save_excel
  write:
    connection: bronze
    path: output/report.xlsx
    format: excel
```

---

### Avro (Apache Avro)

**Use Case:** Event streaming, schema evolution

**Benefits:**
- Binary format (compact)
- Schema included in file
- Supports schema evolution
- Efficient for serialization

**Dependencies:** Requires `fastavro`
```bash
pip install fastavro
```

**Read Example:**
```yaml
- name: load_avro
  read:
    connection: bronze
    path: events/stream.avro
    format: avro
```

**Write Example:**
```yaml
- name: save_avro
  write:
    connection: bronze
    path: output/events.avro
    format: avro
```

**Note:** Avro schema is automatically inferred from DataFrame dtypes.

---

## Cloud Storage Support

All formats work seamlessly with cloud storage:

**ADLS (Azure Data Lake Storage):**
```yaml
connections:
  bronze:
    type: azure_adls
    account: mystorageaccount
    container: bronze
    auth_mode: key_vault
    key_vault_name: my-vault
    secret_name: storage-key

pipelines:
  - pipeline: multi_format
    nodes:
      - name: read_csv_from_adls
        read:
          connection: bronze
          path: data/sales.csv
          format: csv
      
      - name: write_avro_to_adls
        depends_on: [read_csv_from_adls]
        write:
          connection: bronze
          path: output/sales.avro
          format: avro
```

All formats support:
- ✅ Multi-account connections
- ✅ Key Vault authentication
- ✅ Storage options pass-through
- ✅ Remote URI handling (`abfss://`, `s3://`)

---

## Best Practices

### For Data Lakes (Recommended)
1. **Use Parquet for production data**
   - Efficient storage
   - Fast analytics
   - Type preservation

2. **Use CSV for human-readable data**
   - Easy to inspect
   - Compatible with all tools
   - Good for small datasets

3. **Use Avro for event streams**
   - Schema evolution support
   - Compact binary format
   - Good for append-only logs

### Performance Tips

**Parquet:**
- Use `snappy` compression (good balance of speed/size)
- Enable column pruning (read only needed columns)
- Consider partitioning for large datasets (Phase 2B)

**CSV:**
- Use chunking for large files
- Specify dtypes explicitly to avoid inference

**Avro:**
- Best for write-once, read-many workloads
- Schema is embedded (no separate schema files needed)

---

## Coming Soon (Phase 2B)

**Delta Lake Format:**
- ACID transactions
- Time travel
- Schema evolution
- Optimized for data lakes

See [PHASE2_DESIGN_DECISIONS.md](PHASE2_DESIGN_DECISIONS.md) for Delta Lake roadmap.
