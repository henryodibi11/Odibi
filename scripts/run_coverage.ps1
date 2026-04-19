# run_coverage.ps1 — Run test coverage in batches to avoid hangs.
#
# Usage:   .\scripts\run_coverage.ps1
# Output:  Combined coverage report printed to stdout + .coverage file
#
# Why batches? Some test files hang when combined in a single process due to
# ThreadPoolExecutor + coverage tracing interactions, Rich logging singleton
# pollution, or mock pyspark sys.modules contamination.

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

# Clean previous coverage data
if (Test-Path ".coverage") { Remove-Item ".coverage" }
if (Test-Path ".coverage.*") { Remove-Item ".coverage.*" }

Write-Host "=== Batch 1: Main unit test suite (excluding known hang sources) ===" -ForegroundColor Cyan
python -m coverage run --source=odibi -m pytest tests/unit/ -q --tb=no `
    --ignore=tests/unit/engine/test_pandas_engine_core.py `
    --ignore=tests/unit/connections/test_azure_adls_coverage.py `
    --ignore=tests/unit/test_simulation.py `
    --ignore=tests/unit/test_simulation_random_walk.py `
    --ignore=tests/unit/test_simulation_fixes.py `
    --ignore=tests/unit/test_simulation_daily_profile.py `
    --ignore=tests/unit/test_simulation_coverage_gaps.py `
    --ignore=tests/unit/test_stateful_simulation.py `
    --ignore=tests/unit/tools/test_adf_profiler_coverage.py `
    -k "not test_failure_logs_warning"

Write-Host ""
Write-Host "=== Batch 2: pandas_engine_core (isolated) ===" -ForegroundColor Cyan
python -m coverage run -a --source=odibi -m pytest tests/unit/engine/test_pandas_engine_core.py -q --tb=no

Write-Host ""
Write-Host "=== Batch 3: azure_adls_coverage (isolated) ===" -ForegroundColor Cyan
python -m coverage run -a --source=odibi -m pytest tests/unit/connections/test_azure_adls_coverage.py -q --tb=no

Write-Host ""
Write-Host "=== Batch 4: simulation tests (isolated) ===" -ForegroundColor Cyan
python -m coverage run -a --source=odibi -m pytest `
    tests/unit/test_simulation.py `
    tests/unit/test_simulation_random_walk.py `
    tests/unit/test_simulation_fixes.py `
    tests/unit/test_simulation_daily_profile.py `
    tests/unit/test_simulation_coverage_gaps.py `
    tests/unit/test_stateful_simulation.py `
    -q --tb=no

Write-Host ""
Write-Host "=== Batch 5: adf_profiler (isolated — uses ThreadPoolExecutor) ===" -ForegroundColor Cyan
python -m coverage run -a --source=odibi -m pytest tests/unit/tools/test_adf_profiler_coverage.py -q --tb=no

Write-Host ""
Write-Host "=== Coverage Report ===" -ForegroundColor Green
python -m coverage report --show-missing
