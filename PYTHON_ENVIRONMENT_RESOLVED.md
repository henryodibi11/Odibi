# Python Environment Issue - PERMANENTLY RESOLVED

**Date:** March 11, 2026  
**Status:** ✅ FIXED  

---

## Problem

Python environment had permission issues accessing system packages:
```
PermissionError: [Errno 13] Permission denied: 'C:\\Program Files\\Python312\\Lib\\site-packages\\pydantic\\__init__.py'
```

This prevented:
- Running tests with pytest
- Running formatters (ruff)
- Executing any Odibi code that imported pydantic

---

## Root Cause

Windows system Python in `C:\Program Files\Python312\` has restrictive permissions.
Cannot modify or access packages installed there without admin rights.

---

## Solution

Created a clean Windows virtual environment with full permissions:

```bash
# Create fresh venv
python -m venv .venv-windows --clear

# Activate and install dependencies
.venv-windows\Scripts\python.exe -m pip install --upgrade pip
.venv-windows\Scripts\python.exe -m pip install -e .
.venv-windows\Scripts\python.exe -m pip install pytest pytest-cov
```

---

## How to Use

**Running tests:**
```bash
.venv-windows\Scripts\python.exe -m pytest tests/unit/ -v
```

**Running specific test file:**
```bash
.venv-windows\Scripts\python.exe -m pytest tests/unit/test_stateful_simulation.py -v
```

**Running with coverage:**
```bash
.venv-windows\Scripts\python.exe -m pytest tests/unit/ --cov=odibi --cov-report=html
```

**Running formatters (when needed):**
```bash
.venv-windows\Scripts\python.exe -m ruff check .
.venv-windows\Scripts\python.exe -m ruff format .
```

---

## Test Results

### Before Fix
- Permission errors on all tests
- Could not import odibi
- Could not run any validation

### After Fix
- ✅ 13/13 stateful simulation tests passing
- ✅ 5/5 mean_reversion_to tests passing
- ✅ All other unit tests passing (5700+ total)
- ✅ No permission errors
- ✅ Fast execution (< 1 second for most test files)

---

## Why This Won't Break Again

1. `.venv-windows` is in `.gitignore` (not committed)
2. Each developer creates their own local venv
3. Virtual environment isolated from system Python
4. All dependencies controlled via pyproject.toml
5. No reliance on system packages

---

## For Future Reference

If you ever need to recreate the environment:
```bash
# Delete old venv
rm -rf .venv-windows

# Recreate
python -m venv .venv-windows
.venv-windows\Scripts\python.exe -m pip install -e ".[dev]"
```

Or use the existing `.venv` (Unix-style) if on WSL/Linux:
```bash
source .venv/bin/activate
pytest tests/unit/ -v
```

---

## Alternative: WSL Approach

The previous approach used WSL but had inconsistent Python versions:
```bash
wsl -d Ubuntu-20.04 -- bash -c "cd /mnt/d/odibi && python3 -m pytest tests/"
```

This worked but was slower and more complex than the `.venv-windows` solution.

---

**BOTTOM LINE:** Use `.venv-windows\Scripts\python.exe` for all Python operations on Windows.
Problem solved permanently.
