# Verification Scripts

These scripts verify that canonical examples ran successfully. They're designed to be:
- **Automated**: Run in CI/CD pipelines
- **Clear**: Explicit pass/fail with descriptive errors
- **Educational**: Show what to check in your own pipelines

## Usage

```bash
# Run pipeline first
odibi run docs/examples/canonical/runnable/01_hello_world.yaml

# Then verify
python examples/verify/verify_01_hello_world.py

# Exit code 0 = success, non-zero = failure
echo $?  # Linux/Mac
echo %ERRORLEVEL%  # Windows
```

## Available Scripts

| Script | Example | What It Checks |
|--------|---------|----------------|
| `verify_01_hello_world.py` | Hello World | File exists, row count, schema |
| `verify_03_scd2_dimension.py` | SCD2 Dimension | SCD2 columns, surrogate keys, version history |
| `verify_04_fact_table.py` | Fact Table | FK lookups, grain, orphan handling |
| `verify_capstone_jr_de.py` | Jr DE Capstone | Complete validation pipeline |
| `verify_capstone_sr_de.py` | Sr DE Capstone | Full Bronze→Silver→Gold flow |

## Creating Your Own Verify Scripts

Use this template:

```python
#!/usr/bin/env python3
"""Verification script for [Your Pipeline]"""

import sys
from pathlib import Path

def verify():
    errors = []
    warnings = []
    
    # 1. Check files exist
    output = Path("data/output/my_table")
    if not output.exists():
        errors.append(f"❌ Output not found: {output}")
    
    # 2. Check data quality
    import pandas as pd
    df = pd.read_parquet(output)
    
    if len(df) == 0:
        errors.append("❌ No rows in output")
    
    # 3. Check schema
    required_cols = ['id', 'value']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f"❌ Missing columns: {missing}")
    
    # Summary
    if errors:
        print(f"❌ FAILED ({len(errors)} errors)")
        for e in errors:
            print(f"  {e}")
        return 1
    else:
        print("✅ ALL CHECKS PASSED")
        return 0

if __name__ == "__main__":
    sys.exit(verify())
```

## CI/CD Integration

### GitHub Actions

```yaml
- name: Run pipeline
  run: odibi run odibi.yaml

- name: Verify output
  run: python examples/verify/verify_my_pipeline.py
```

### Make target

```makefile
verify:
	python examples/verify/verify_01_hello_world.py
	python examples/verify/verify_03_scd2_dimension.py
	python examples/verify/verify_04_fact_table.py
```

## Exit Codes

- `0`: All checks passed
- `1`: One or more errors (hard failures)

Warnings don't cause non-zero exit (pipeline succeeded but with caveats).
