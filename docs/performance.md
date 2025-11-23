# Performance Tuning Guide

Odibi v2.2 introduces a "High-Performance Core" designed to handle everything from local laptop development to petabyte-scale Spark jobs. This guide explains the optimizations available and how to use them.

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

## 3. Core Framework Optimizations

### ⚙️ C-Compilation (mypyc)

**What it does:**
Compiles the core Odibi orchestration logic (`graph.py`, `pipeline.py`) into C extensions.

**Why use it?**
Reduces the overhead of the framework itself. While your data processing happens in Spark/Pandas, the logic deciding *what* to run runs 2-10x faster.

**How to enable:**
Install `mypy` in your environment **before** installing Odibi.
```bash
pip install mypy
pip install odibi
```
The setup script detects `mypy` and automatically compiles the extensions.

---

## Summary Cheat Sheet

| Optimization | Engine | Use Case | Impact |
| :--- | :--- | :--- | :--- |
| `use_arrow: true` | Pandas | Local processing, large Parquet files | **High** (Speed + Memory) |
| Parallel I/O | Pandas | Reading split CSV/JSON files | **High** (Linear I/O speedup) |
| `cluster_by` | Spark | High-cardinality filters, skewed data | **High** (Read performance) |
| `optimize_write` | Spark | Frequent writes, streaming, "small files" | **High** (Prevents degradation) |
| `streaming: true` | Spark | Real-time ingestion | **Architectural** |
| `mypyc` Compile | Core | Heavy orchestration loops | **Medium** (Framework overhead) |
