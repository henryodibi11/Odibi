# Odibi Test Coverage Plan

## Overview
This document tracks test coverage for the Odibi framework. Updated: 2026-01-17

## Current Status
- **Total Test Files**: 87 in `tests/unit/`
- **Coverage**: Most core modules now have tests

## Completed (Previously Identified Gaps)

### High Priority - ✅ ALL COMPLETE
- [x] `node.py` - test_node.py, test_node_executor.py
- [x] `pipeline.py` - test_pipeline.py, test_pipeline_results.py
- [x] `graph.py` - test_graph.py
- [x] `context.py` - test_context.py, test_context_management.py
- [x] `lineage.py` - test_lineage_tracking.py

### Medium Priority - ✅ ALL COMPLETE
- [x] `catalog_sync.py` - test_catalog_integration.py, test_catalog_path_consistency.py
- [x] `derived_updater.py` - test_derived_updater.py (36KB, comprehensive)
- [x] `registry.py` - test_registry.py
- [x] `references.py` - test_references.py
- [x] `config.py` - test_config_loader.py

### Low Priority - ✅ ALL COMPLETE
- [x] `plugins.py` - test_plugins_and_introspection.py
- [x] `project.py` - test_project.py

### Subdirectories - ✅ ALL HAVE TESTS
- [x] `diagnostics/` - test_diagnostics_manager.py
- [x] `orchestration/` - test_orchestration_airflow.py, test_orchestration_dagster.py
- [x] `state/` - test_state_manager.py
- [x] `utils/` - 6 test files (alerting, content_hash, duration, encoding, hashing, progress)
- [x] `validation/` - 5 test files in tests/unit/validation/
- [x] `semantics/` - 7 test files in tests/unit/semantics/

## Remaining Gaps

### Engine Parity
- [ ] Polars engine comprehensive tests (test_polars_context.py missing)
- [ ] Cross-engine comparison tests

### UI/Testing Utilities
- [ ] `ui/` - No tests (low priority, UI components)
- [ ] `testing/` - Test helpers (meta - testing the test utils)

### Edge Cases
- [ ] Error scenario coverage across modules
- [ ] Windows-specific path handling tests
- [ ] Unicode/encoding edge cases

## Test Commands
```bash
pytest tests/ -v                        # Run all tests
pytest tests/unit/test_X.py -v          # Run specific test
pytest --cov=odibi --cov-report=html    # Generate coverage report
```

## Notes
- Most gaps from original plan have been addressed
- Focus now on edge cases and engine parity
- Run `pytest --cov` to get actual coverage percentages
