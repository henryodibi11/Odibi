# Example Data

This folder contains sample data for running the example pipelines.

## Contents

```
data/
├── raw/                    # Input data (committed)
│   ├── customers.csv       # For starter.odibi.yaml
│   └── sample_data.csv     # Generic sample
└── lake/                   # Output data (gitignored)
    ├── gold/               # Processed output
    ├── stories/            # Run reports
    └── _system_catalog/    # Metadata
```

## Running Examples

From the repository root:

```bash
# Run the starter example
odibi run examples/starter.odibi.yaml

# Run the simple template
odibi run examples/templates/simple_local.yaml
```
