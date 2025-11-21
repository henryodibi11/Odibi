# 📝 UX Feedback & Frustration Tracker

As we build the "Real World" showcase, we will document every friction point, error message ambiguity, and "missing feature" we encounter. This will drive the Roadmap for v2.1.

| ID | Category | Frustration / Issue | Workaround Used | Proposed Fix |
|----|----------|---------------------|-----------------|--------------|
| 01 | **Connectors** | No built-in HTTP/API connector. | Wrote custom Python script node (then pre-processing script). | Add `type: http` source with pagination support. |
| 02 | **JSON Ingestion** | `read.format: json` failed on complex nested structure (`{"rates": {...}}`). | Pre-processed JSON to CSV using external script. | Add JSONPath support to `read` (e.g., `json_path: $.rates.*`) or custom parsers. |
| 03 | **Error Messages** | "Passing a bool to header is invalid" was cryptic for `read_csv`. | Removed `options: {header: true}`. | Odibi should sanitize/validate format-specific options or document defaults better. |
| 04 | **Logging** | Windows Console crashed on Emoji (❌) output. | None (just ignored stack trace). | Detect encoding or fallback to ASCII on Windows. |
| 05 | **Transforms** | Cannot inject custom python functions directly in YAML. | Used SQL instead. | **Auto-Discovery:** CLI should automatically import `*.py` files in the project root so `@transform` decorators register themselves. |
