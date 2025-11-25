# Agents Guide

This repository uses `pre-commit` for linting and formatting.

## Linting & Formatting

The following tools are used:
- `black` for code formatting
- `ruff` for linting and import sorting
- `pre-commit` hooks for general file quality

### Common Commands

- Run all checks (requires pre-commit installed):
  ```bash
  pre-commit run --all-files
  ```

- Run Ruff linting manually:
  ```bash
  ruff check .
  ```

- Run Ruff formatting/import sorting manually:
  ```bash
  ruff check --select I . --fix  # Sort imports
  ruff format .                  # Format code
  ```

- Run Black formatting manually:
  ```bash
  black .
  ```

## Testing

Run tests using `pytest`:
```bash
pytest
```
