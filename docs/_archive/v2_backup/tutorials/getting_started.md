# Getting Started with Odibi

In this tutorial, you will go from "raw data" to a "production-grade pipeline" in under 10 minutes.

You will learn how to:
1.  **Scaffold** a project automatically using `odibi generate-project`.
2.  **Run** a data pipeline locally.
3.  **Inspect** the results using Data Stories.
4.  **Stress Test** your logic against random data.

---

## 1. Prerequisites

You need Python 3.9+ installed.

```bash
pip install odibi
```

---

## 2. Create Sample Data

Let's simulate a messy dataset. Create a folder named `raw_data` and add a file `users.csv`:

```bash
mkdir raw_data
```

**raw_data/users.csv**:
```csv
id, name,       email,           signup_date
1,  Alice,      alice@test.com,  2023-01-01
2,  Bob,        bob@test.com,    2023-02-15
3,  Charlie,    null,            2023-03-10
4,  Dave,       dave@test.com,   invalid-date
```
*Notice the mess: extra spaces, nulls, and invalid dates.*

---

## 3. Generate Your Project

Instead of writing configuration by hand, let Odibi analyze your data and build the project for you.

```bash
odibi generate-project --input ./raw_data --output ./my_first_project
```

Odibi will:
1.  Scan `users.csv`.
2.  Detect columns and types.
3.  Generate an `odibi.yaml` with a **Bronze** (Ingest) and **Silver** (Clean) layer.
4.  Write SQL transformation files to clean the column names.

Enter your new project:
```bash
cd my_first_project
```

---

## 4. Run the Pipeline

Execute the pipeline to process the data.

```bash
odibi run odibi.yaml
```

You will see terminal output showing the execution flow:
```text
✅ [Bronze] load_users_csv ... 4 rows
✅ [Silver] clean_users ... 4 rows
✨ Pipeline finished in 1.2s
```

---

## 5. View the Story

Odibi doesn't just run code; it tells a story. Every run generates a visual report.

```bash
odibi story view --latest
```

This opens an HTML report in your browser. Look for:
*   **Row Counts:** Did we lose any rows?
*   **Schema:** What columns do we have?
*   **Sample Data:** Does the data look clean?

---

## 6. Add a Business Logic Node

Let's add a "Gold" layer to count signups by month.

Open `odibi.yaml` and add this to the `pipeline` section:

```yaml
  - name: gold_monthly_signups
    depends_on: [clean_users]
    transform:
      operation: sql
      query: |
        SELECT
          strftime(strptime(signup_date, '%Y-%m-%d'), '%Y-%m') as month,
          count(*) as total_signups
        FROM ${source}
        WHERE signup_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    write:
      connection: local
      path: gold/monthly_signups.parquet
```

Run it again:
```bash
odibi run odibi.yaml
```

Check the story. You should see a new node `gold_monthly_signups` with the aggregated results.

---

## 7. Stress Test (The "Chaos Monkey")

How robust is your SQL? Let's find out by throwing random junk data at it.

```bash
odibi stress odibi.yaml --runs 10
```

Odibi will generate 10 random variations of `users.csv` (random strings, negative numbers, huge files) and run your pipeline against them.

If it crashes (e.g., `strptime` fails on bad dates), Odibi will tell you exactly which input caused it.

---

## Next Steps

You have built a working data pipeline with zero boilerplate.

*   **[Master CLI Guide](../guides/cli_master_guide.md):** Learn about `doctor`, `dry-run`, and more.
*   **[Configuration Reference](../reference/configuration.md):** See all available options.
*   **[Deployment](../guides/production_deployment.md):** Move this pipeline to the cloud.
