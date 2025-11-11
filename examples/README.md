# ODIBI Examples & Templates

**Complete guide to ODIBI configuration files and examples**

---

## 📚 Quick Navigation

| File | Purpose | Best For |
|------|---------|----------|
| **[QUICK_START_GUIDE.md](QUICK_START_GUIDE.md)** | 5-minute quick reference | First-time users |
| **[MASTER_TEMPLATE.yaml](MASTER_TEMPLATE.yaml)** | Complete feature reference | Understanding all capabilities |
| **[example_local.yaml](example_local.yaml)** | Simple local pipeline | Learning & development |
| **[template_full.yaml](template_full.yaml)** | Full local configuration | Production local pipelines |
| **[template_full_adls.yaml](template_full_adls.yaml)** | Azure multi-account setup | Azure cloud deployments |
| **[example_delta_pipeline.yaml](example_delta_pipeline.yaml)** | Delta Lake examples | Time travel & ACID transactions |
| **[example_spark.yaml](example_spark.yaml)** | Spark engine setup | Large-scale data processing |

---

## 🚀 Getting Started

### Step 1: Install ODIBI

```bash
# Basic installation
pip install odibi

# With all features
pip install "odibi[all]"
```

### Step 2: Pick Your Starting Point

**Never used ODIBI before?**
→ Start with [QUICK_START_GUIDE.md](QUICK_START_GUIDE.md)

**Want a working example?**
→ Try [example_local.yaml](example_local.yaml)

**Need to understand all options?**
→ Read [MASTER_TEMPLATE.yaml](MASTER_TEMPLATE.yaml)

**Setting up Azure?**
→ Use [template_full_adls.yaml](template_full_adls.yaml)

**Using Delta Lake?**
→ Check [example_delta_pipeline.yaml](example_delta_pipeline.yaml)

### Step 3: Run Your First Pipeline

```bash
# Validate your config
odibi validate example_local.yaml

# Run the pipeline
odibi run example_local.yaml
```

---

## 📖 Templates Explained

### 🟢 QUICK_START_GUIDE.md

**5-minute quick reference guide**

- Installation instructions
- Common patterns (read/transform/write)
- File format examples
- Connection setup
- Delta Lake basics
- Troubleshooting tips

**Best for:** Getting up to speed quickly

---

### 🔵 MASTER_TEMPLATE.yaml

**Comprehensive reference showing ALL features**

Includes:
- ✅ All connection types (local, ADLS, Azure SQL)
- ✅ All authentication modes (Key Vault, managed identity, service principal)
- ✅ All file formats (CSV, Parquet, JSON, Excel, Avro, Delta)
- ✅ Delta Lake with time travel
- ✅ Multi-source joins
- ✅ Python transforms with Context API
- ✅ Multi-step SQL transformations
- ✅ Bronze → Silver → Gold patterns

**Best for:** Reference when building complex pipelines

---

### 🟡 example_local.yaml

**Simple local pipeline - perfect starting point**

```yaml
# Load CSV → Clean with SQL → Save as Parquet
project: Local Pandas Example
engine: pandas

connections:
  local:
    type: local
    base_path: ./data

pipelines:
  - pipeline: bronze_to_silver
    nodes:
      - name: load_raw_sales
        read:
          connection: local
          path: bronze/sales.csv
          format: csv
        cache: true
      
      - name: clean_sales
        depends_on: [load_raw_sales]
        transform:
          steps:
            - "SELECT * FROM load_raw_sales WHERE amount > 0"
      
      - name: save_silver
        depends_on: [clean_sales]
        write:
          connection: local
          path: silver/sales.parquet
          format: parquet
          mode: overwrite
```

**Best for:** Learning ODIBI basics

---

### 🟠 template_full.yaml

**Complete local configuration reference**

Includes:
- Project metadata
- Local + Azure connections (with examples)
- Story generation config
- Retry and logging settings
- Bronze → Silver → Gold pipelines
- Delta Lake with time travel
- Azure SQL integration
- Python transforms with Context API
- All supported formats

**Best for:** Production-ready local pipelines

**Updated Features:**
- ✅ Delta Lake examples with time travel
- ✅ Azure SQL connection examples
- ✅ Context API usage in Python transforms
- ✅ Schema evolution options

---

### 🔴 template_full_adls.yaml

**Azure multi-account ADLS setup**

Includes:
- Multi-account ADLS connections
- Key Vault authentication
- Bronze → Silver multi-account pattern
- Delta Lake on ADLS
- All authentication modes documented
- Format support matrix

**Best for:** Azure cloud deployments

**Recent Fixes:**
- ✅ Fixed SQL chaining bug (clean_temp → clean_sales)
- ✅ Added Delta Lake examples
- ✅ Added authentication mode reference
- ✅ Added story configuration

---

### 🟣 example_delta_pipeline.yaml

**Delta Lake deep dive**

Includes:
- Local Delta testing
- Time travel examples
- Version comparison
- Partitioned Delta tables
- Production Delta on ADLS
- Maintenance operations (VACUUM, RESTORE)

**Best for:** Learning Delta Lake capabilities

---

### ⚫ example_spark.yaml

**Spark engine configuration**

Includes:
- Spark engine setup
- ADLS with managed identity
- Azure SQL integration
- Multi-source ETL
- Window functions
- Partitioned writes
- Delta Lake on Spark

**Best for:** Large-scale data processing on Databricks

---

## 🎯 Common Use Cases

### Use Case 1: Local Development

**Start with:** [example_local.yaml](example_local.yaml)

```bash
odibi run example_local.yaml
```

### Use Case 2: Azure Production

**Start with:** [template_full_adls.yaml](template_full_adls.yaml)

1. Update storage account names
2. Configure Key Vault
3. Update paths
4. Run: `odibi run template_full_adls.yaml`

### Use Case 3: Delta Lake ACID Transactions

**Start with:** [example_delta_pipeline.yaml](example_delta_pipeline.yaml)

Benefits:
- ✅ ACID transactions (no partial writes)
- ✅ Time travel (read any version)
- ✅ Schema evolution (add columns safely)
- ✅ VACUUM old files

### Use Case 4: Multi-Source Integration

**Start with:** [MASTER_TEMPLATE.yaml](MASTER_TEMPLATE.yaml) - Pipeline 4

Load from:
- ADLS (transactions)
- Azure SQL (dimensions)
- Join and write to Delta

### Use Case 5: Custom Python Logic

**Start with:** [MASTER_TEMPLATE.yaml](MASTER_TEMPLATE.yaml) - Pipeline 3

Define transforms:
```python
from odibi import transform

@transform
def my_logic(context, param: float):
    df = context.get('node_name')
    # Your logic here
    return df
```

---

## 🔧 Configuration Checklist

Before running a pipeline, ensure:

- [ ] `project` name is set
- [ ] `engine` is specified ('pandas' or 'spark')
- [ ] At least one `connection` is defined
- [ ] `story` connection and path are configured
- [ ] At least one `pipeline` with nodes is defined
- [ ] Each node has read, transform, or write (or combination)
- [ ] `depends_on` references valid node names
- [ ] Connection names in nodes match defined connections

---

## 📚 Additional Resources

### Documentation
- [../docs/CONFIGURATION_EXPLAINED.md](../docs/CONFIGURATION_EXPLAINED.md) - Complete config guide
- [../docs/DELTA_LAKE_GUIDE.md](../docs/DELTA_LAKE_GUIDE.md) - Delta Lake reference
- [../docs/setup_azure.md](../docs/setup_azure.md) - Azure authentication
- [../docs/setup_databricks.md](../docs/setup_databricks.md) - Databricks setup

### Walkthroughs
- [../walkthroughs/01_local_pipeline_pandas.ipynb](../walkthroughs/01_local_pipeline_pandas.ipynb) - Interactive tutorial
- [../walkthroughs/phase2b_delta_lake.ipynb](../walkthroughs/phase2b_delta_lake.ipynb) - Delta Lake features
- [../walkthroughs/phase2b_production_pipeline.ipynb](../walkthroughs/phase2b_production_pipeline.ipynb) - Production patterns

### Getting Started
- [getting_started/](getting_started/) - Complete tutorial with sample data

---

## 🆘 Getting Help

**Found a bug or have a question?**
- GitHub Issues: https://github.com/henryodibi11/Odibi/issues
- Email: henryodibi@outlook.com

**Want to contribute?**
- See [../CONTRIBUTING.md](../CONTRIBUTING.md)

---

## 📝 Template Summary

| Template | Lines | Features | Complexity |
|----------|-------|----------|------------|
| QUICK_START_GUIDE.md | 400+ | Patterns & examples | ⭐ Easy |
| example_local.yaml | 122 | Basic ETL | ⭐ Easy |
| template_full.yaml | 400+ | All local features | ⭐⭐ Medium |
| template_full_adls.yaml | 178 | Azure multi-account | ⭐⭐ Medium |
| example_delta_pipeline.yaml | 213 | Delta Lake | ⭐⭐ Medium |
| example_spark.yaml | 177 | Spark engine | ⭐⭐⭐ Advanced |
| MASTER_TEMPLATE.yaml | 700+ | Everything! | ⭐⭐⭐ Advanced |

---

**Happy pipeline building! 🚀**

For the most up-to-date information, see the main [README.md](../README.md)
