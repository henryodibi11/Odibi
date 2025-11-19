# Odibi Comprehensive Demo

This project demonstrates a complete Data Engineering workflow using Odibi.

## What's Included?

1.  **Project Config (`project.yaml`)**: 
    - Defines connections (Input `data/`, Output `output/`).
    - Configures the pipeline structure.
    - Sets up "Story" generation for observability.

2.  **Custom Transforms (`transforms.py`)**:
    - Shows how to write Python logic using the `@transform` decorator.
    - Example: Calculating bonuses and generating emails.

3.  **Mixed Transformations**:
    - Combines Python logic with SQL (DuckDB) for joins and aggregations.

## How to Run

1.  Open your terminal in this folder:
    ```bash
    cd examples/getting_started/comprehensive_demo
    ```

2.  Run the Python script:
    ```bash
    python run_demo.py
    ```

3.  Check the `output/` folder for results.

4.  Open the `output/stories/` folder to see the generated HTML report of the run!
