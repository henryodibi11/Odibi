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

## 7. What's Next?

You have successfully built a data pipeline!

*   **[Write Custom Transformations](../guides/writing_transformations.md):** Learn how to add Python logic (like advanced validation) to your pipeline.
*   **[Master the CLI](../guides/cli_master_guide.md):** Learn about `odibi stress` and `odibi doctor`.
