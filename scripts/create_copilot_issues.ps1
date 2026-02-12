# Create GitHub Issues for Copilot Coding Agent to fix testing gaps
# Run: .\scripts\create_copilot_issues.ps1
# Prerequisites: gh auth login

$repo = "henryodibi11/Odibi"

$issues = @(
    @{
        title = "Add unit tests for transformers/advanced.py"
        body = @"
## Task
Add comprehensive unit tests for `odibi/transformers/advanced.py`.

## Instructions
- Create test file: `tests/unit/transformers/test_advanced.py`
- Look at existing transformer tests in `tests/unit/transformers/` for patterns
- Test each public function/class in advanced.py
- Test edge cases: empty DataFrames, null values, invalid inputs
- Use pandas DataFrames for testing (no Spark needed)

## Verify
``````
pytest tests/unit/transformers/test_advanced.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for transformers/relational.py"
        body = @"
## Task
Add comprehensive unit tests for `odibi/transformers/relational.py`.

## Instructions
- Create test file: `tests/unit/transformers/test_relational.py`
- Look at existing transformer tests in `tests/unit/transformers/` for patterns
- Test each public function/class
- Test edge cases: empty DataFrames, null foreign keys, missing references
- Use pandas DataFrames for testing

## Verify
``````
pytest tests/unit/transformers/test_relational.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for transformers/sql_core.py"
        body = @"
## Task
Add comprehensive unit tests for `odibi/transformers/sql_core.py`.

## Instructions
- Create test file: `tests/unit/transformers/test_sql_core.py`
- Look at existing transformer tests in `tests/unit/transformers/` for patterns
- Test SQL template rendering, parameter substitution, edge cases
- Test with invalid SQL, empty inputs, special characters

## Verify
``````
pytest tests/unit/transformers/test_sql_core.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for transformers/validation.py"
        body = @"
## Task
Add comprehensive unit tests for `odibi/transformers/validation.py`.

## Instructions
- Create test file: `tests/unit/transformers/test_validation_transformer.py`
- Test all validation transformer functions
- Test passing/failing validations, edge cases, null handling
- Use pandas DataFrames for testing

## Verify
``````
pytest tests/unit/transformers/test_validation_transformer.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for transformers/manufacturing.py"
        body = @"
## Task
Expand unit tests for `odibi/transformers/manufacturing.py`.

## Instructions
- Existing test: `tests/unit/test_manufacturing.py` - check what's already covered
- Add missing test coverage for untested functions
- Test manufacturing-specific edge cases: OEE calculations, cycle times, yield rates
- Use pandas DataFrames for testing

## Verify
``````
pytest tests/unit/test_manufacturing.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for connections/azure_adls.py"
        body = @"
## Task
Add unit tests for `odibi/connections/azure_adls.py`.

## Instructions
- Create test file: `tests/unit/connections/test_azure_adls.py`
- Use mocks for Azure SDK calls (do NOT make real Azure connections)
- Test connection initialization, path resolution, auth config validation
- Test error handling for missing credentials, invalid paths
- Look at `tests/unit/connections/test_api_fetcher.py` for patterns

## Verify
``````
pytest tests/unit/connections/test_azure_adls.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for connections/azure_sql.py"
        body = @"
## Task
Add unit tests for `odibi/connections/azure_sql.py`.

## Instructions
- Create test file: `tests/unit/connections/test_azure_sql.py`
- Use mocks for database connections (do NOT make real DB connections)
- Test connection string building, auth validation, error handling
- Look at `tests/unit/connections/test_api_fetcher.py` for patterns

## Verify
``````
pytest tests/unit/connections/test_azure_sql.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for connections/local_dbfs.py"
        body = @"
## Task
Add unit tests for `odibi/connections/local_dbfs.py`.

## Instructions
- Create test file: `tests/unit/connections/test_local_dbfs.py`
- Test path resolution, DBFS path handling, read/write operations with tmp dirs
- Test edge cases: missing files, invalid DBFS paths
- Look at `tests/unit/test_local_dbfs.py` and `tests/unit/test_local_connection.py` for existing coverage - extend what's missing

## Verify
``````
pytest tests/unit/connections/test_local_dbfs.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for diagnostics/delta.py and diagnostics/diff.py"
        body = @"
## Task
Add unit tests for `odibi/diagnostics/delta.py` and `odibi/diagnostics/diff.py`.

## Instructions
- Create test file: `tests/unit/test_diagnostics_delta.py` and `tests/unit/test_diagnostics_diff.py`
- Test delta diagnostics: schema changes, row count changes, data drift detection
- Test diff utilities: DataFrame comparison, column-level diffs
- Use pandas DataFrames for testing
- Look at `tests/unit/test_diagnostics_manager.py` for patterns

## Verify
``````
pytest tests/unit/test_diagnostics_delta.py tests/unit/test_diagnostics_diff.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for engine/polars_engine.py - expand coverage"
        body = @"
## Task
Expand test coverage for `odibi/engine/polars_engine.py`.

## Instructions
- Existing tests: `tests/test_polars_engine.py` and `tests/unit/test_polars_context.py`
- Check what methods/functions are NOT yet tested
- Add tests for any uncovered engine methods
- Ensure parity with pandas engine test coverage
- Test edge cases: empty DataFrames, type mismatches, null handling

## Verify
``````
pytest tests/test_polars_engine.py tests/unit/test_polars_context.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for pandas_engine.py - expand coverage to 50%+"
        body = @"
## Task
Expand test coverage for `odibi/engine/pandas_engine.py` (currently ~25%).

## Instructions
- Existing tests: `tests/test_pandas_engine_full_coverage.py`
- Check what methods are NOT yet tested
- Focus on: read/write operations, transform dispatching, merge logic, SCD2 handling
- Test edge cases: empty DataFrames, missing columns, type coercion
- Target: get coverage above 50%

## Verify
``````
pytest tests/test_pandas_engine_full_coverage.py -v
``````
"@
        labels = "testing,copilot"
    },
    @{
        title = "Add unit tests for catalog.py and catalog_sync.py"
        body = @"
## Task
Add dedicated unit tests for `odibi/catalog.py` and `odibi/catalog_sync.py`.

## Instructions
- Check existing: `tests/unit/test_catalog_integration.py` and `test_catalog_path_consistency.py`
- Add missing coverage for catalog registration, lookup, sync operations
- Test edge cases: duplicate entries, missing catalogs, sync conflicts
- Use mocks for any file system or external calls

## Verify
``````
pytest tests/unit/test_catalog_integration.py tests/unit/test_catalog_path_consistency.py -v
``````
"@
        labels = "testing,copilot"
    }
)

Write-Host "Creating $($issues.Count) GitHub Issues for Copilot..." -ForegroundColor Cyan
Write-Host ""

# First, create labels if they don't exist
try { gh label create "copilot" --repo $repo --color "1d76db" --description "Assigned to Copilot coding agent" 2>$null } catch {}
try { gh label create "testing" --repo $repo --color "0e8a16" --description "Testing improvements" 2>$null } catch {}

foreach ($issue in $issues) {
    Write-Host "Creating: $($issue.title)" -ForegroundColor Yellow
    $result = gh issue create --repo $repo --title $issue.title --body $issue.body --label $issue.labels --assignee "copilot-swe-agent[bot]" 2>&1
    Write-Host "  -> $result" -ForegroundColor Green
    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "Done! Check https://github.com/$repo/issues" -ForegroundColor Cyan
