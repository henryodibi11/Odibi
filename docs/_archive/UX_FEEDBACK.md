# 📝 UX Feedback & Frustration Tracker

As we build the "Real World" showcase, we will document every friction point, error message ambiguity, and "missing feature" we encounter. This will drive the Roadmap for v2.1.

| ID | Category | Frustration / Issue | Workaround Used | Proposed Fix |
|----|----------|---------------------|-----------------|--------------|
| 01 | **Connectors** | No built-in HTTP/API connector. | Implemented `type: http` connector. | Add `type: http` source with pagination support. |
| 02 | **JSON Ingestion** | `read.format: json` failed on complex nested structure (`{"rates": {...}}`). | Pre-processed JSON to CSV using external script. | Add JSONPath support to `read` (e.g., `json_path: $.rates.*`) or custom parsers. |
| 03 | **Error Messages** | "Passing a bool to header is invalid" was cryptic for `read_csv`. | Sanitized options in `pandas_engine`. | Odibi should sanitize/validate format-specific options or document defaults better. |
| 04 | **Logging** | Windows Console crashed on Emoji (❌) output. | Patched `logging.py` to remove emoji on non-UTF8. | Detect encoding or fallback to ASCII on Windows. |
| 05 | **Transforms** | Cannot inject custom python functions directly in YAML. | Implemented auto-loader for `transforms.py`. | **Auto-Discovery:** CLI should automatically import `*.py` files in the project root so `@transform` decorators register themselves. |
| 06 | **Documentation** | README referenced non-existent `odibi_store.yaml` and didn't explain how to run split configs. | Updated README with sequential instructions. | Add `includes` directive to YAML or update docs to show sequential execution. |
| 07 | **File Ingestion** | `read.path` with glob (`*.parquet`) failed on Windows (`Invalid argument`). | Patched `pandas_engine.py` to expand globs. | Engine must expand `glob` patterns before calling `pd.read_parquet`. |
| 08 | **Story UI** | When `sensitive: true` (full table), the redaction message is hidden in Story table because columns don't match. | Patched `story/generator.py` to show message if schema mismatches. | Story generator should handle `sensitive: true` by printing the message directly instead of trying to render a table with original schema. |
| 09 | **Extensibility** | Cannot add custom **Format** readers (e.g. NetCDF) cleanly. `pandas_engine` formats are hardcoded. | Added `Engine.register_format` and updated `pandas_engine`. | Add `register_reader(format, func)` to Engine so plugins can extend `read`. |
