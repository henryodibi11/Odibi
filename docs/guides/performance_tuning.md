---
title: Performance Tuning Guide
roles: [sr-de]
tags: [guide:howto, topic:performance, topic:optimization, topic:spark, topic:delta]
prereqs: [spark_engine.md, decision_guide.md]
next: [production_deployment.md, ../features/engines.md]
related: [production_deployment.md, ../tutorials/spark_engine.md, ../reference/PARITY_TABLE.md]
time: 30m
---

# Performance Tuning Guide

Odibi v3.4.3 introduces a "High-Performance Core" designed to handle everything from local laptop development to petabyte-scale Spark jobs. This guide explains the optimizations available and how to use them.

## 🚀 Quick Start

For 90% of users, just add this to your `odibi.yaml`:

```yaml
performance:
  use_arrow: true  # Massive speedup for Pandas I/O
```

---

## 1. Pandas Engine Optimizations

The Pandas engine is designed for speed on a single machine.

### 🏹 Apache Arrow Backend (`use_arrow: true`)

**What it does:**
Replaces standard NumPy memory layout with **Apache Arrow**. Arrow is a columnar memory format that allows "Zero-Copy" data transfer.

**Why use it?**
-   **Speed:** Reading Parquet files becomes nearly instant because the data maps directly from disk to memory without conversion overhead.
-   **Memory:** Reduces RAM usage by ~50% for string-heavy datasets (no more Python objects for strings).

**Configuration:**
```yaml
# odibi.yaml
performance:
  use_arrow: true
```

### ⚡ Parallel File I/O (Multi-Threading)

**What it does:**
When reading multiple files (e.g., `path: data/sales_*.csv`), Odibi now uses a thread pool to read them in parallel instead of one by one.

**Why use it?**
Pandas is normally single-threaded. If you have 8 CPU cores, reading 50 CSV files sequentially wastes 7 of them. Parallel I/O saturates your CPU/Disk bandwidth for linear speedups.

**How to use:**
Automatic! Just use a glob pattern in your path:
```yaml
read:
  path: raw/data_*.csv  # <--- Parallel reading activates automatically
```

---

## 2. Spark Engine Optimizations

The Spark engine focuses on "Data Layout" optimizations—making sure downstream queries are fast.

### 💧 Liquid Clustering (`cluster_by`)

**What it does:**
Replaces traditional Hive-style partitioning (`year=2023/month=01`) with a flexible, dynamic clustering system. It physically groups related data together in the files.

**Why use it?**
-   **No "Small File" Problem:** Traditional partitioning creates too many tiny files if you pick the wrong column (e.g., `user_id`). Liquid handles this automatically.
-   **Skew Resistance:** Handles uneven data (e.g., 90% of users in US, 1% in JP) without performance cliffs.
-   **Query Speed:** Massive data skipping. Queries filtering by clustered columns skip 99% of the file scans.

**How to use:**
Add `cluster_by` to your write node. If the table doesn't exist, Odibi creates it with clustering enabled.

```yaml
- name: write_sales
  write:
    table: silver.sales
    mode: append
    options:
      cluster_by: [region, date]  # <--- Enables Liquid Clustering
      optimize_write: true        # <--- Keeps clustering healthy
```

### 🧹 Auto-Optimization (`optimize_write`)

**What it does:**
Runs the Delta Lake `OPTIMIZE` command immediately after a write/merge operation.

**Why use it?**
Streaming and frequent batch jobs create "small files" (fragmentation) which kill read performance. This option compacts them into larger, efficient files (Bin-packing) and enforces clustering (Z-Order/Liquid).

**Configuration:**
```yaml
# In a standard Write node
options:
  optimize_write: true

# In a Merge Transformer
params:
  optimize_write: true
```

### 🎯 Smart Caching (Auto-Cache)

**What it does:**
Automatically caches Spark DataFrames for nodes with high downstream fanout (3+ dependencies by default).

**Why use it?**
- **Prevents ADLS re-reads:** If `dim_calendar` is used by 10 fact tables, it's read from ADLS once and cached in memory.
- **Cross-pipeline optimization:** When Gold references Silver nodes via `inputs`, it uses the cached temp view instead of re-reading from Delta.
- **Zero config for common case:** Smart defaults with explicit override available.

**How it works:**

```yaml
pipeline:
  name: silver
  auto_cache_threshold: 3  # Auto-cache nodes with 3+ dependencies (default)
  nodes:
    - name: dim_calendar
      # Used by 10 downstream nodes → automatically cached ✅

    - name: dim_product  
      # Used by 2 downstream nodes → not cached (below threshold)
      cache: true  # Manual override to cache it anyway

    - name: huge_dimension
      # Used by 5 downstream nodes → would be auto-cached...
      cache: false  # ...but explicit override prevents OOM ✅
```

**Disable auto-caching entirely:**
```yaml
pipeline:
  auto_cache_threshold: null  # All caching must be explicit
```

**When NOT to cache:**
- Tables larger than available cluster memory
- Nodes with no downstream dependencies
- Linear pipelines with no fanout

**Performance impact:**
- Typical Silver pipeline: Saves 5-10 minutes by eliminating redundant ADLS reads
- Cross-pipeline dependencies: Gold reads from Silver's cache (seconds vs minutes)

### 🌊 Streaming Support

**What it does:**
Allows you to switch from Batch (`read`/`write`) to Streaming (`readStream`/`writeStream`) with a single flag.

**Why use it?**
For real-time latency or processing infinite datasets (Kafka, Auto-Loader) without managing state manually.

**How to use:**
```yaml
- name: read_stream
  read:
    streaming: true  # <--- Activates Spark Structured Streaming
    format: cloudFiles
    path: raw_landing/
```

---

## 3. Polars Engine Optimizations

The Polars engine is a lightweight, fast alternative to Pandas with native Rust performance.

### 🦺 Lazy Execution

**What it does:**
Polars uses a lazy execution model where queries are not executed until you call `.collect()`. This allows the query optimizer to reorder, combine, and skip operations.

**Why use it?**
-   **Query Optimization:** Predicate pushdown, projection pruning, and filter hoisting happen automatically.
-   **Memory Efficiency:** Only columns you need are loaded into memory.
-   **Performance:** Often 5-10x faster than Pandas for analytical workloads.

**How to use:**
Set your engine to Polars and Odibi handles lazy evaluation automatically:
```yaml
engine: polars
```

### 📂 Scan Methods (Streaming Large Files)

**What it does:**
Uses `scan_csv`, `scan_parquet`, and `scan_ndjson` to read files lazily without loading them entirely into memory.

**Why use it?**
Process files larger than RAM by streaming them in chunks.

**Automatic:** Odibi's Polars engine uses scan methods by default for supported formats.

### ⚡ Native Parallelism

**What it does:**
Polars uses all available CPU cores automatically—no configuration needed.

**Why use it?**
Unlike Pandas (single-threaded), Polars parallelizes operations like groupby, join, and filter across all cores.

---

## Summary Cheat Sheet

| Optimization | Engine | Use Case | Impact |
| :--- | :--- | :--- | :--- |
| `use_arrow: true` | Pandas | Local processing, large Parquet files | **High** (Speed + Memory) |
| Parallel I/O | Pandas | Reading split CSV/JSON files | **High** (Linear I/O speedup) |
| `auto_cache_threshold: 3` | Spark | High-fanout nodes (dims, expensive transforms) | **Very High** (5-10 min savings) |
| `cluster_by` | Spark | High-cardinality filters, skewed data | **High** (Read performance) |
| `optimize_write` | Spark | Frequent writes, streaming, "small files" | **High** (Prevents degradation) |
| `streaming: true` | Spark | Real-time ingestion | **Architectural** |
| Lazy Execution | Polars | Analytical workloads, large datasets | **High** (Speed + Memory) |
| Scan Methods | Polars | Files larger than RAM | **High** (Streaming) |
| Native Parallelism | Polars | Multi-core utilization | **High** (Automatic) |
