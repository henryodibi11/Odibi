# Cline Task Queue

Work through these tasks ONE AT A TIME. After completing each:
1. Run verification (ruff check, ruff format, tests if applicable)
2. Mark the task with [x] when done
3. Move to the next task

---

## Phase 1: Documentation (Zero Risk)

- [x] **Task 1.1**: Add docstrings to `odibi/transformers/advanced.py` - all functions missing docstrings
- [x] **Task 1.2**: Add docstrings to `odibi/transformers/relational.py` - all functions missing docstrings  
- [x] **Task 1.3**: Add docstrings to `odibi/transformers/scd.py` - all functions missing docstrings
- [x] **Task 1.4**: Add docstrings to `odibi/transformers/merge_transformer.py` - all functions missing docstrings
- [x] **Task 1.5**: Add docstrings to `odibi/transformers/delete_detection.py` - all functions missing docstrings

## Phase 2: Linting/Formatting (Low Risk)

- [x] **Task 2.1**: Run `ruff check odibi/validation/ --fix` and fix any issues
- [x] **Task 2.2**: Run `ruff check odibi/patterns/ --fix` and fix any issues
- [x] **Task 2.3**: Run `ruff check odibi/connections/ --fix` and fix any issues
- [x] **Task 2.4**: Run `ruff check odibi/engine/ --fix` and fix any issues

## Phase 3: Test Coverage (Medium Risk)

- [x] **Task 3.1**: Add missing tests for `hash_columns` transformer (tests, lint, format: done 2026-01-18)
- [x] **Task 3.2**: Add edge case tests for `coalesce_columns` (empty list, all nulls) (tests, lint, format: done 2026-01-18)
- [x] **Task 3.3**: Add missing tests for `generate_surrogate_key` transformer (tests, lint, format: done 2026-01-18)
- [x] **Task 3.4**: Add missing tests for `regex_replace` transformer (tests, lint, format: done 2026-01-18)

## Phase 4: Error Message Improvements (Medium Risk)

- [x] **Task 4.1**: Improve error messages in `odibi/transformers/validation.py` (done - improved cross_check messages)
- [x] **Task 4.2**: N/A - `config.py` uses Pydantic, no custom errors to improve
- [x] **Task 4.3**: N/A - YAML errors handled by Pydantic/parser

## Phase 5: Type Hints (Low Risk)

- [x] **Task 5.1**: Add type hints to `odibi/utils/` functions missing them (done 2026-01-18)
- [x] **Task 5.2**: Add type hints to `odibi/story/` functions missing them (done 2026-01-18)

---

## Completed Tasks Log

| Task | Date | Commit |
|------|------|--------|
| sql_core.py docstrings | 2026-01-18 | af2db22 |
