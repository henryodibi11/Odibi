"""Create GitHub Issues for Copilot Coding Agent to fix testing gaps."""
import subprocess
import time

REPO = "henryodibi11/Odibi"

ISSUES = [
    {
        "title": "Add unit tests for transformers/advanced.py",
        "body": (
            "## Task\n"
            "Add comprehensive unit tests for `odibi/transformers/advanced.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/transformers/test_advanced.py`\n"
            "- Look at existing transformer tests in `tests/unit/transformers/` for patterns\n"
            "- Test each public function/class in advanced.py\n"
            "- Test edge cases: empty DataFrames, null values, invalid inputs\n"
            "- Use pandas DataFrames for testing (no Spark needed)\n\n"
            "## Verify\n"
            "```\npytest tests/unit/transformers/test_advanced.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for transformers/relational.py",
        "body": (
            "## Task\n"
            "Add comprehensive unit tests for `odibi/transformers/relational.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/transformers/test_relational.py`\n"
            "- Look at existing transformer tests in `tests/unit/transformers/` for patterns\n"
            "- Test each public function/class\n"
            "- Test edge cases: empty DataFrames, null foreign keys, missing references\n"
            "- Use pandas DataFrames for testing\n\n"
            "## Verify\n"
            "```\npytest tests/unit/transformers/test_relational.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for transformers/sql_core.py",
        "body": (
            "## Task\n"
            "Add comprehensive unit tests for `odibi/transformers/sql_core.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/transformers/test_sql_core.py`\n"
            "- Look at existing transformer tests in `tests/unit/transformers/` for patterns\n"
            "- Test SQL template rendering, parameter substitution, edge cases\n"
            "- Test with invalid SQL, empty inputs, special characters\n\n"
            "## Verify\n"
            "```\npytest tests/unit/transformers/test_sql_core.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for transformers/validation.py",
        "body": (
            "## Task\n"
            "Add comprehensive unit tests for `odibi/transformers/validation.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/transformers/test_validation_transformer.py`\n"
            "- Test all validation transformer functions\n"
            "- Test passing/failing validations, edge cases, null handling\n"
            "- Use pandas DataFrames for testing\n\n"
            "## Verify\n"
            "```\npytest tests/unit/transformers/test_validation_transformer.py -v\n```"
        ),
    },
    {
        "title": "Expand unit tests for transformers/manufacturing.py",
        "body": (
            "## Task\n"
            "Expand unit tests for `odibi/transformers/manufacturing.py`.\n\n"
            "## Instructions\n"
            "- Existing test: `tests/unit/test_manufacturing.py` - check what's already covered\n"
            "- Add missing test coverage for untested functions\n"
            "- Test manufacturing-specific edge cases: OEE calculations, cycle times, yield rates\n"
            "- Use pandas DataFrames for testing\n\n"
            "## Verify\n"
            "```\npytest tests/unit/test_manufacturing.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for connections/azure_adls.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/connections/azure_adls.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/connections/test_azure_adls.py`\n"
            "- Use mocks for Azure SDK calls (do NOT make real Azure connections)\n"
            "- Test connection initialization, path resolution, auth config validation\n"
            "- Test error handling for missing credentials, invalid paths\n"
            "- Look at `tests/unit/connections/test_api_fetcher.py` for patterns\n\n"
            "## Verify\n"
            "```\npytest tests/unit/connections/test_azure_adls.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for connections/azure_sql.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/connections/azure_sql.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/connections/test_azure_sql.py`\n"
            "- Use mocks for database connections (do NOT make real DB connections)\n"
            "- Test connection string building, auth validation, error handling\n"
            "- Look at `tests/unit/connections/test_api_fetcher.py` for patterns\n\n"
            "## Verify\n"
            "```\npytest tests/unit/connections/test_azure_sql.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for connections/local_dbfs.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/connections/local_dbfs.py`.\n\n"
            "## Instructions\n"
            "- Create test file: `tests/unit/connections/test_local_dbfs.py`\n"
            "- Test path resolution, DBFS path handling, read/write with tmp dirs\n"
            "- Test edge cases: missing files, invalid DBFS paths\n"
            "- Look at `tests/unit/test_local_dbfs.py` for existing coverage - extend what's missing\n\n"
            "## Verify\n"
            "```\npytest tests/unit/connections/test_local_dbfs.py -v\n```"
        ),
    },
    {
        "title": "Add unit tests for diagnostics/delta.py and diagnostics/diff.py",
        "body": (
            "## Task\n"
            "Add unit tests for `odibi/diagnostics/delta.py` and `odibi/diagnostics/diff.py`.\n\n"
            "## Instructions\n"
            "- Create: `tests/unit/test_diagnostics_delta.py` and `tests/unit/test_diagnostics_diff.py`\n"
            "- Test delta diagnostics: schema changes, row count changes, data drift detection\n"
            "- Test diff utilities: DataFrame comparison, column-level diffs\n"
            "- Use pandas DataFrames for testing\n"
            "- Look at `tests/unit/test_diagnostics_manager.py` for patterns\n\n"
            "## Verify\n"
            "```\npytest tests/unit/test_diagnostics_delta.py tests/unit/test_diagnostics_diff.py -v\n```"
        ),
    },
    {
        "title": "Expand unit tests for engine/polars_engine.py",
        "body": (
            "## Task\n"
            "Expand test coverage for `odibi/engine/polars_engine.py`.\n\n"
            "## Instructions\n"
            "- Existing tests: `tests/test_polars_engine.py` and `tests/unit/test_polars_context.py`\n"
            "- Check what methods/functions are NOT yet tested\n"
            "- Add tests for any uncovered engine methods\n"
            "- Ensure parity with pandas engine test coverage\n"
            "- Test edge cases: empty DataFrames, type mismatches, null handling\n\n"
            "## Verify\n"
            "```\npytest tests/test_polars_engine.py tests/unit/test_polars_context.py -v\n```"
        ),
    },
    {
        "title": "Expand unit tests for engine/pandas_engine.py - target 50%+ coverage",
        "body": (
            "## Task\n"
            "Expand test coverage for `odibi/engine/pandas_engine.py` (currently ~25%).\n\n"
            "## Instructions\n"
            "- Existing tests: `tests/test_pandas_engine_full_coverage.py`\n"
            "- Check what methods are NOT yet tested\n"
            "- Focus on: read/write operations, transform dispatching, merge logic, SCD2 handling\n"
            "- Test edge cases: empty DataFrames, missing columns, type coercion\n"
            "- Target: get coverage above 50%\n\n"
            "## Verify\n"
            "```\npytest tests/test_pandas_engine_full_coverage.py -v\n```"
        ),
    },
    {
        "title": "Expand unit tests for catalog.py and catalog_sync.py",
        "body": (
            "## Task\n"
            "Add dedicated unit tests for `odibi/catalog.py` and `odibi/catalog_sync.py`.\n\n"
            "## Instructions\n"
            "- Check existing: `tests/unit/test_catalog_integration.py` and "
            "`test_catalog_path_consistency.py`\n"
            "- Add missing coverage for catalog registration, lookup, sync operations\n"
            "- Test edge cases: duplicate entries, missing catalogs, sync conflicts\n"
            "- Use mocks for any file system or external calls\n\n"
            "## Verify\n"
            "```\npytest tests/unit/test_catalog_integration.py tests/unit/test_catalog_path_consistency.py -v\n```"
        ),
    },
]


def main():
    print(f"Creating {len(ISSUES)} GitHub Issues for Copilot...\n")

    for i, issue in enumerate(ISSUES, 1):
        print(f"[{i}/{len(ISSUES)}] {issue['title']}")
        result = subprocess.run(
            [
                "gh",
                "issue",
                "create",
                "--repo",
                REPO,
                "--title",
                issue["title"],
                "--body",
                issue["body"],
                "--label",
                "testing,copilot",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print(f"  -> {result.stdout.strip()}")
        else:
            print(f"  -> ERROR: {result.stderr.strip()}")
        time.sleep(2)

    print(f"\nDone! Check https://github.com/{REPO}/issues")


if __name__ == "__main__":
    main()
