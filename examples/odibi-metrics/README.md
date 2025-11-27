# Odibi Metrics (Dogfood Project)

This project tracks the development velocity of the Odibi framework itself. It serves as a "Dogfood" test case to ensure the framework is stable for real-world use.

## Setup

1. Install Odibi:
   ```bash
   pip install odibi
   ```

2. (Optional) Set GitHub Token to avoid rate limits:
   ```bash
   # Linux/Mac
   export GITHUB_TOKEN=your_token_here

   # Windows PowerShell
   $env:GITHUB_TOKEN="your_token_here"
   ```

## Running

```bash
odibi run odibi.yaml
```

## Viewing Results

After running, check the `stories/` folder for the HTML report, or look at the data:

```bash
# View the velocity CSV
cat data/gold/velocity.csv
```
