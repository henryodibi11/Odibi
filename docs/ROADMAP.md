# Odibi Phase 3 Plan: The "Rich Docs" Strategy

We are pivoting from a separate Cookbook to an **Embedded Knowledge** strategy. The goal is to make `docs/api.md` a single, high-value artifact that serves as both Reference and Guide.

We will achieve this by embedding "Gold Mine" recipes directly into the Python code docstrings.

## 1. The Strategy: "Code as Documentation"

We will rewrite the Pydantic model docstrings in `odibi/config.py` and `odibi/transformers/*.py` to include:
1.  **The Business Problem**: Why do I need this?
2.  **The Recipe**: A copy-pasteable YAML block.
3.  **The "Why"**: Brief explanation of the mechanics.

## 2. Targeted Upgrades

We will enhance the following models with extensive examples:

### A. `SCD2Params` (in `odibi/transformers/scd.py`)
*   **Theme**: "The Time Machine"
*   **Content**: Explain how to track history, handle effective dates, and close open records.
*   **Recipe**: A full YAML snippet showing keys, tracking columns, and flags.

### B. `WriteConfig` (in `odibi/config.py`)
*   **Theme**: "Big Data Performance"
*   **Content**: Explain when to use `partition_by` (filtering) vs `zorder_by` (skipping).
*   **Recipe**: A "Lakehouse Optimized" configuration.

### C. `ValidationConfig` (in `odibi/config.py`)
*   **Theme**: "The Indestructible Pipeline"
*   **Content**: Explain blocking vs. warning severity.
*   **Recipe**: A "Quality Gate" configuration preventing bad data from reaching Gold.

### D. `MergeParams` (in `odibi/transformers/merge_transformer.py`)
*   **Theme**: "GDPR & Compliance"
*   **Content**: Explain `delete_match` strategy.
*   **Recipe**: A "Right to be Forgotten" pipeline snippet.

## 3. Execution Steps

1.  [x] **Edit `odibi/transformers/scd.py`**: Replace the placeholder docstring with the full "Time Machine" guide.
2.  [x] **Edit `odibi/config.py`**: update `WriteConfig`, `ValidationConfig`, and `NodeConfig` with rich examples.
3.  [x] **Run `python odibi/introspect.py`**: Generate the new `api.md`.
4.  [x] **Review**: Confirm `api.md` looks like a "Gold Mine" without needing a separate cookbook.

## 4. Extended Improvements (Completed)

Based on feedback, we went further to improve usability:

*   **Smart Node Scenarios**: Added concrete examples for "Standard ETL", "Heavy Lifter", and "Tagged Runner" in `NodeConfig`.
*   **Universal Reader**: Added "Time Traveler" and "Streaming" recipes to `ReadConfig`.
*   **Transformer Catalog**: Added a searchable list of available transformers directly in `NodeConfig` docs.
*   **Concept Clarity**: Explicitly explained "Transformer (App) vs. Transform Steps (Script)" and "Chaining Operations".
*   **Navigation**: Added "Back to Catalog" links for better UX.
*   **"Kitchen Sink" Scenario**: Demonstrated `read` -> `transformer` -> `transform` -> `write` in a single node to prove composability.
