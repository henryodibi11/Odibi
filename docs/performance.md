# ODIBI Performance Guide

This guide provides performance benchmarks, memory management strategies, and best practices for choosing between Pandas and Spark engines in Odibi.

## 📊 Engine Benchmarks

We benchmarked core operations (Read, Write, Filter, Aggregation) on a standard dataset (100,000 rows) to compare overhead and throughput.

### Methodology
- **Dataset:** 100,000 rows, 4 columns (Integer, String, Float, Integer)
- **Environment:** Local execution (Windows/Linux)
- **Versions:** Pandas 2.0+, PySpark 3.4+

### Results (Lower is Better)

| Operation | Pandas Engine | Spark Engine | Notes |
|-----------|---------------|--------------|-------|
| **Read CSV** | ~0.03s | ~2-5s (overhead) | Spark has significant startup/planning overhead for small data. |
| **Read Parquet** | ~0.03s | ~0.5s (overhead) | Parquet is faster than CSV but Spark overhead remains. |
| **Filter** | ~0.003s | < 0.1s (lazy) | Spark is lazy; measuring execution requires an action (e.g., count). |
| **GroupBy + Agg** | ~0.003s | ~0.5s | Pandas is instant for in-memory data. |
| **Write Parquet** | ~0.02s | ~1-2s | Spark write involves coordination overhead. |

> **Key Takeaway:** For small to medium datasets (< 1GB), **Pandas is significantly faster** due to lack of distributed computing overhead. Spark shines when data exceeds memory or requires distributed processing.

---

## 🛡️ Memory Safety (Pandas Engine)

The Pandas engine operates in-memory. To handle datasets larger than RAM, Odibi supports **automatic chunking**.

### How it Works
When reading large files (CSV, JSON), you can specify `chunksize` in `options`. The engine will:
1. Read the file in chunks (iterators).
2. Process each chunk independently (if logic allows).
3. Write chunks sequentially to the output.

### Configuration
```yaml
nodes:
  - name: process_large_file
    engine: pandas
    read:
      connection: my_datalake
      format: csv
      path: "raw/large_data.csv"
      options:
        chunksize: 50000  # Process 50k rows at a time
    write:
      connection: my_datalake
      format: parquet
      path: "processed/data.parquet"
      mode: overwrite
```

### Supported Operations
- **Read/Write:** Fully supported. `read()` returns an iterator, `write()` consumes it.
- **SQL:** Supported (materializes iterator to DuckDB/SQLite). *Note: For true streaming SQL, future versions will leverage DuckDB streaming.*
- **Transformations:** Supported if custom logic handles iterators or if using row-wise operations.

---

## 🚀 Best Practices

### When to use Pandas Engine
- Data fits in memory (< 16GB typically).
- Low latency is required (start-up time matters).
- Complex, non-parallelizable Python logic (using `apply`).
- Local development and testing.

### When to use Spark Engine
- Data exceeds single-machine memory (> 100GB).
- Massive aggregations or joins across large datasets.
- Existing infrastructure is Databricks/Synapse.
- Logic can be expressed purely in SQL or DataFrame API (vectorized).

### Optimization Tips
1. **Use Parquet:** Always prefer Parquet over CSV for internal data movement. It preserves schema and is compressed.
2. **Filter Early:** Apply filters as early as possible in the pipeline to reduce data volume.
3. **Chunking:** Use `chunksize` for large CSV/JSON imports in Pandas to avoid OOM.
4. **Lazy Evaluation:** Spark is lazy; chain operations together rather than materializing intermediate results.
5. **Parallel Execution:** Enable parallel execution for independent nodes.
   ```python
   # CLI
   odibi run --parallel --max-workers 4
   
   # Python
   pipeline.run(parallel=True, max_workers=8)
   ```
6. **Thread Tuning:** Adjust `max_workers` based on CPU cores and I/O wait times. For I/O bound tasks (many reads/writes), higher worker count is beneficial.
