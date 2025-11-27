# Odibi Pandas Validation Suite

This directory contains focused tests to validate the Odibi Pandas Engine and Pipeline execution logic.

## Contents

*   `run_checks.py`: Unit-style checks for `PandasEngine` methods. It specifically verifies that methods correctly handle `LazyDataset` inputs by materializing them before processing.
*   `run_pipeline.py`: An integration test that executes a complete pipeline defined in `pipeline_config.yaml`. It verifies:
    *   Config parsing
    *   Dependency wiring
    *   Transformation execution
    *   Privacy (PII redaction)
    *   Validation rules
    *   Output file generation
*   `pipeline_config.yaml`: The configuration used by `run_pipeline.py`.

## How to Run

Ensure you are in the project root (where `odibi` package is importable) and have the environment activated.

### 1. Run Static Engine Checks
```bash
python tests/pandas_validation/run_checks.py
```
**Expected Output:** All checks should pass with "PASS".

### 2. Run Pipeline Integration Test
```bash
python tests/pandas_validation/run_pipeline.py
```
**Expected Output:**
*   Pipeline runs successfully.
*   Output DataFrame is printed (showing redacted columns).
*   "PASS: Runtime validation successful" message.

## Troubleshooting

If `run_pipeline.py` fails with a `UnicodeEncodeError` (logging emoji issue on Windows), the test logic might still have succeeded. Check the file outputs in `tests/pandas_validation/data/`.
