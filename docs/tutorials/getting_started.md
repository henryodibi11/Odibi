# 🏁 Getting Started with Odibi

This tutorial will guide you through creating your first data pipeline. By the end, you will have a running project that reads data, cleans it, and generates an audit report ("Data Story").

**Prerequisites:**
*   Python 3.9 or higher installed.
*   Basic familiarity with terminal/command line.

---

## 1. Installation

First, install Odibi. We recommend creating a virtual environment to keep your system clean.

```bash
# 1. Create a virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 2. Install Odibi
pip install odibi
```

*Note: If you plan to use Spark or Azure later, you can install `pip install "odibi[spark,azure]"`, but for this tutorial, the base package is enough.*

---

## 2. Create Sample Data

Odibi shines when working with messy real-world data. Let's create some "bad" data to clean.

Create a folder named `raw_data` and a file inside it named `customers.csv`:

**raw_data/customers.csv**
```csv
id, name,           email,              joined_at
1,  Alice,          alice@example.com,  2023-01-01
2,  Bob,            bob@example.com,    2023-02-15
3,  Charlie,        NULL,               2023-03-10
4,  Dave,           dave@example.com,   invalid-date
```
*(Notice the extra spaces, the NULL value, and the invalid date string.)*

---

## 3. Generate Your Project

Instead of writing configuration files from scratch, use the **Odibi Initializer**. It creates a project skeleton with best practices baked in.

Run this command in your terminal:

```bash
odibi init-pipeline my_first_project --template local-medallion
```

This creates a new folder `my_first_project` with a standard structure:
*   **`odibi.yaml`**: The pipeline configuration.
*   **`data/`**: Folders for your data layers (landing, raw, silver, etc.).
*   **`README.md`**: Instructions for your project.

Move your sample data into the landing zone:
```bash
# On Windows (PowerShell)
mv raw_data/customers.csv my_first_project/data/landing/
# On Mac/Linux
mv raw_data/customers.csv my_first_project/data/landing/
```

> **Note:** You can also generate a project *from existing data* using `odibi generate-project`, but `init-pipeline` is the recommended way to start fresh.

---

## 4. Explore the Project

Navigate into your new project:

```bash
cd my_first_project
```

You will see a file structure like this:

*   **`odibi.yaml`**: The brain of your project. It defines the pipeline.
*   **`sql/`**: Contains SQL transformation files.
*   **`data/`**: (Created automatically) Where data will be stored.

Open `odibi.yaml` in your text editor. You will see two "nodes" (steps):
1.  **Ingestion Node:** Reads the `customers.csv` from `landing/`.
2.  **Refinement Node:** Merges the data into `silver/`.

Since we used the template, the config is already set up to look for `landing/customers.csv`.

---

## 5. Run the Pipeline

Now, execute the pipeline:

```bash
odibi run odibi.yaml
```

Odibi will:
1.  Read `customers.csv` from `landing/`.
2.  Convert it to Parquet in `raw/`.
3.  Merge it into a Delta/Parquet table in `silver/`.
4.  Generate a "Data Story".

---

## 6. View the Data Story

Data engineering is often invisible. Odibi makes it visible. Every run generates a report.

List the generated stories:

```bash
odibi story list
```

You will see output like:
```text
📚 Stories in .odibi/stories:
================================================================================
  📄 main_documentation.html
     Modified: 2025-11-21 14:30:00
     Size: 15.2KB
     Path: .odibi/stories/main_documentation.html
```

Open the HTML file in your browser to view the report:
- **Windows:** `start .odibi/stories/main_documentation.html`
- **Mac:** `open .odibi/stories/main_documentation.html`
- **Linux:** `xdg-open .odibi/stories/main_documentation.html`

**What to look for in the report:**
*   **Row Counts:** Did we lose any rows?
*   **Schema:** Did the column types change?
*   **Execution Time:** How long did it take?

---

## 7. Add Data Validation

Data pipelines are only as good as their data quality. Let's add validation tests to catch bad data before it corrupts your warehouse.

### Inline Validation in YAML

Add validation tests directly to your node:

```yaml
nodes:
  - name: customers
    read:
      connection: landing
      format: csv
      path: customers.csv
    validation:
      tests:
        - type: not_null
          columns: [id, name]
        - type: unique
          columns: [id]
        - type: row_count
          min: 1
      on_failure: warn  # or "error" to stop the pipeline
    write:
      connection: raw
      format: parquet
      path: customers
```

### Using Contracts for Input Validation

Contracts validate data **before** processing:

```yaml
nodes:
  - name: validate_orders
    contracts:
      - type: not_null
        columns: [order_id, customer_id, amount]
      - type: freshness
        column: created_at
        max_age: "24h"
    read:
      connection: landing
      path: orders.csv
    write:
      connection: raw
      path: orders
```

If contracts fail, the pipeline stops immediately with clear error messages.

### Running Validation

Run the pipeline and watch for validation warnings:

```bash
odibi run odibi.yaml
```

Validation results appear in both the console output and the Data Story.

---

## 8. Building Dimensions (SCD2)

Once you're comfortable with basic pipelines, you can build proper dimensional models. Here's a quick example of a Slowly Changing Dimension Type 2:

```yaml
nodes:
  - name: dim_customer
    read:
      connection: bronze
      table: raw_customers
    pattern:
      type: dimension
      params:
        natural_key: customer_id        # Business key
        surrogate_key: customer_sk      # Generated integer key
        scd_type: 2                     # Track history
        track_cols: [name, email, city]
        target: silver.dim_customer     # Read existing for merge
        unknown_member: true            # Add SK=0 for orphans
    write:
      connection: silver
      table: dim_customer
```

**What this does:**
- Generates integer surrogate keys (`customer_sk`)
- Tracks changes to `name`, `email`, `city` over time
- Maintains `is_current`, `valid_from`, `valid_to` columns
- Creates an "unknown" row (SK=0) for handling orphan fact records

For a complete dimensional modeling tutorial, see [Dimensional Modeling](dimensional_modeling/).

---

## 9. What's Next?

You have successfully built a data pipeline with data validation!

*   **[Incremental Loading](../patterns/incremental_stateful.md):** Learn how to efficiently process only new data using State Tracking ("Auto-Pilot").
*   **[Write Custom Transformations](../guides/writing_transformations.md):** Learn how to add Python logic (like advanced validation) to your pipeline.
*   **[Data Validation Guide](../validation/README.md):** Deep dive into all validation options.
*   **[Spark Engine Tutorial](spark_engine.md):** Scale up with Apache Spark.
*   **[Azure Connections](azure_connections.md):** Connect to Azure Blob, ADLS, and SQL.
*   **[Master the CLI](../guides/cli_master_guide.md):** Learn about `odibi stress` and `odibi doctor`.
