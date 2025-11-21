# 📘 Writing Custom Transformations

While SQL covers 90% of data engineering tasks, sometimes you need the full power of Python. Odibi allows you to write custom transformations that integrate seamlessly into your pipelines.

---

## 1. The Basics

A custom transformation is just a Python function decorated with `@transformation`.

Create a file named `my_transforms.py` in your project folder:

```python
# my_transforms.py
from odibi.transformations import transformation

@transformation("filter_high_value", category="filtering")
def filter_high_value(df, threshold=100):
    """
    Keep only rows where 'value' is greater than threshold.
    """
    # Standard Pandas logic
    return df[df['value'] > threshold]
```

**Key Rules:**
1.  **Decorate it:** You must use `@transformation` to register it.
2.  **Docstrings:** You *must* include a docstring. Odibi uses this for documentation.
3.  **Return DataFrame:** The function must return a pandas DataFrame.

---

## 2. Using it in YAML

To use your new transformation, reference it by the name you gave in the decorator (`"filter_high_value"`).

**odibi.yaml:**
```yaml
nodes:
  - name: filter_data
    depends_on: [load_data]
    transform:
      steps:
        - operation: filter_high_value  # Matches the name in @transformation
          params:
            threshold: 500              # Passed as argument to function
```

**Important:** You must ensure `my_transforms.py` is imported before `odibi run` executes.
*   **Option A (Recommended):** Run as a module: `python -m odibi run odibi.yaml` (ensure your script is in python path).
*   **Option B (Simple):** Create a `run.py` script:

```python
# run.py
import my_transforms  # <--- Registers the functions
from odibi.pipeline import Pipeline

manager = Pipeline.from_yaml("odibi.yaml")
manager.run()
```

---

## 3. Adding "Data Stories" (Explanations)

Odibi generates reports ("Stories") for every run. By default, it just shows the function name. You can provide a rich, human-readable explanation using the `.explain` decorator.

Update `my_transforms.py`:

```python
from odibi.transformations import transformation
from odibi.transformations.templates import purpose_detail_result

@transformation("filter_high_value", category="filtering")
def filter_high_value(df, threshold=100):
    """Filter high value transactions."""
    return df[df['value'] > threshold]

@filter_high_value.explain
def explain_filter(threshold=100, **context):
    """
    Generates the text that appears in the Story report.
    'context' contains info like project name, node name, etc.
    """
    return purpose_detail_result(
        purpose=f"Filter transactions above ${threshold}",
        details=[
            f"Threshold: ${threshold}",
            "Removes low-value noise from dataset"
        ],
        result="High-value dataset ready for analysis"
    )
```

Now, your HTML report will show a beautiful summary instead of just code!

---

## 4. Advanced: Using Context

Sometimes you need metadata about the pipeline (e.g., "Which plant is this running for?"). Odibi passes a `context` object if your function asks for it.

```python
@transformation("enrich_with_plant_info")
def enrich(df, **context):
    # Access project config variables
    plant_name = context.get('plant', 'Unknown Plant')

    df['plant_name'] = plant_name
    return df
```

---

## 5. Best Practices

1.  **Pure Functions:** Your transformation should not have side effects (like writing to a file). Return a DataFrame and let Odibi handle the I/O in the `write` section of the node.
2.  **Type Hints:** Use Python type hints. Odibi doesn't enforce them strictly yet, but it helps tooling.
3.  **Error Handling:** If data is invalid, raise a specific error (e.g., `ValueError`). Odibi will catch it and mark the node as "Failed" in the story.

---

## Summary

1.  Import `from odibi.transformations import transformation`.
2.  Decorate your function with `@transformation("name")`.
3.  (Optional) Add `@func.explain` for better reports.
4.  Import your module before running the pipeline.
