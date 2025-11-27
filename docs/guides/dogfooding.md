# 🐶 Dogfooding Guide

**"Eat your own dog food."**

This guide explains how we use Odibi to build Odibi, and how you can use the `odibi-metrics` project to validate your own environment.

---

## What is Dogfooding?

Dogfooding means **using your own product to do your actual job**.

Instead of testing Odibi with synthetic data (like "foo", "bar", "test_1"), we use it to track the development velocity of the Odibi framework itself. This forces us to encounter real-world problems—messy API data, rate limits, Unicode errors, schema drift—before our users do.

## The `odibi-metrics` Pipeline

We have included a reference implementation in `examples/odibi-metrics`. This is a real pipeline that:

1.  **Extracts:** Connects to the GitHub API to fetch Issues and PRs from `henryodibi11/Odibi`.
2.  **Transforms:** Cleans the data, handles timezones, and calculates weekly velocity (opened vs. closed tasks).
3.  **Loads:** Saves the results to a Gold layer (`velocity.csv`) and updates the System Catalog.

### How to Run It

```bash
# 1. Go to the example directory
cd examples/odibi-metrics

# 2. Run the pipeline
odibi run odibi.yaml
```

### What It Teaches You

If this pipeline fails, it means Odibi is not stable enough for production. We found (and fixed) the following issues using this exact pipeline:

*   ❌ **Unicode Errors:** Windows consoles crashing on emoji output.
*   ❌ **Schema Drift:** The System Catalog failing when new metadata fields were added.
*   ❌ **Timezone Bugs:** Pandas crashing when grouping TZ-aware dates.

## How to File Issues

When you find a bug while running Odibi (or the dogfood pipeline), you should track it using **GitHub Issues**.

1.  Go to the [Issues Tab](https://github.com/henryodibi11/Odibi/issues) on GitHub.
2.  Click **New Issue**.
3.  **Title:** Short summary (e.g., "Crash on Windows 11").
4.  **Body:** Paste the error log and your `odibi.yaml`.

**The Meta-Loop:**
Once you file the issue, the `odibi-metrics` pipeline will actually *download that issue* the next time it runs, adding it to your project statistics. You are using Odibi to measure how fast you are fixing Odibi.
