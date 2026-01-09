# Project Documentation Generator

Use this prompt template in the Odibi Agent UI to generate professional documentation for any odibi project.

---

## Prompt Template

Copy and paste this into the agent, replacing `{PROJECT_PATH}` with your project folder:

---

```
Generate comprehensive documentation for the odibi project at: {PROJECT_PATH}

## Your Task

Analyze all YAML configuration files, Python scripts, and existing docs in this project. Create professional-grade documentation that serves BOTH business users and technical users.

## Step 1: Discovery

First, explore the project:
1. Find and read the main `project.yaml` or `*_project.yaml` file
2. List all pipeline YAML files
3. Check for semantic layer configs (`semantic.yaml`, `metrics.yaml`, etc.)
4. Look for existing README, docs/, or documentation
5. Identify data sources (connections) and destinations

## Step 1.5: Data Profiling (Critical for Quality Docs)

**For each pipeline node's output table/view, run SQL to understand the actual data:**

```sql
-- Sample data (5 rows)
SELECT * FROM {table_name} LIMIT 5

-- Row count and date range
SELECT COUNT(*) as row_count,
       MIN(date_column) as earliest,
       MAX(date_column) as latest
FROM {table_name}

-- Column cardinality (understand what's in each column)
SELECT '{column}' as col, COUNT(DISTINCT {column}) as unique_values
FROM {table_name}
```

**Use this data to:**
- Write realistic example values in the data dictionary
- Understand what each column actually contains (not just the name)
- Identify primary keys, foreign keys, and relationships
- Spot data quality issues to document
- Describe date ranges and data freshness

**For semantic layer metrics, validate them:**
```sql
-- Run each metric to show example output
SELECT dimension_col, metric_value
FROM semantic_view
GROUP BY dimension_col
LIMIT 10
```

This makes documentation **grounded in reality**, not just config assumptions.

## Step 2: Create Documentation Structure

Generate documentation with these sections:

---

### README.md (Main Project Documentation)

#### 1. Executive Summary (Non-Technical)
- **What does this project do?** (2-3 sentences a business user can understand)
- **What business questions does it answer?**
- **Who are the intended users?**
- **What data sources does it use?** (plain English, no technical jargon)

#### 2. Quick Start
- Prerequisites (what needs to be installed/configured)
- How to run the pipelines (copy-paste commands)
- How to access the output data
- Common first-time issues and solutions

#### 3. Architecture Overview
- Create a **mermaid diagram** showing:
  - Data sources → Bronze → Silver → Gold → Semantic Layer
  - Show actual table/view names from the config
- Explain the medallion architecture in plain terms

#### 4. Data Sources
For each connection in the config:
| Source | Type | Description | Update Frequency |
|--------|------|-------------|------------------|
| (name) | (format) | (what data it contains) | (if known) |

#### 5. Pipeline Reference

For EACH pipeline, create a subsection:

##### Pipeline: {pipeline_name}
- **Purpose:** What business problem does this solve?
- **Layer:** Bronze/Silver/Gold
- **Schedule:** (if configured)
- **Input:** What it reads from
- **Output:** What it produces

**Nodes:**
| Node | Description | Key Transformations | Output |
|------|-------------|---------------------|--------|

**Key Transformations Explained:**
- For each complex transform (SCD2, merge, phase detection, etc.), explain in plain English what it does and why

#### 6. Semantic Layer (if present)
- **Available Metrics:** Table of all metrics with business definitions
- **Dimensions:** What you can slice/filter by
- **Example Queries:** 3-5 example SQL queries users can run
- **Dashboard Recommendations:** What visualizations would be useful

#### 7. Data Dictionary
For each final output table/view:
| Column | Type | Description | Example Values | Business Definition |
|--------|------|-------------|----------------|---------------------|

#### 8. Data Quality & Validation
- What validation rules are in place?
- What happens to bad data? (quarantine?)
- How to monitor data quality

#### 9. Troubleshooting Guide
- Common errors and solutions
- How to re-run failed pipelines
- Who to contact for help

#### 10. Technical Reference (For Developers)
- Full configuration reference
- How to add new pipelines
- Testing approach
- Deployment notes

---

## Step 3: Additional Docs (if complex project)

If the project has 5+ pipelines, also create:
- `docs/pipelines/` - One markdown file per pipeline with detailed specs
- `docs/data_dictionary.md` - Complete column-level documentation
- `docs/runbook.md` - Operational procedures

## Step 4: Output Format

1. Write the main README.md first
2. Ask if I want the additional docs
3. Use proper markdown formatting:
   - Tables for structured data
   - Mermaid diagrams for architecture
   - Code blocks for examples
   - Collapsible sections for lengthy content

## Style Guidelines

- **Be verbose** - More detail is better than less
- **Explain the "why"** - Not just what, but why it matters
- **Use business language first** - Then add technical details
- **Include examples** - Show don't just tell
- **Assume the reader is new** - Don't skip "obvious" things
- **Make it scannable** - Headers, bullets, tables over paragraphs
```

---

## Example Usage

**Simple project:**
```
Generate comprehensive documentation for the odibi project at: /Workspace/Users/me/sales_analytics
```

**With specific focus:**
```
Generate comprehensive documentation for the odibi project at: /dbfs/projects/inventory_tracking

Focus especially on:
- The semantic layer metrics (we have 20+ KPIs)
- Data quality rules (critical for compliance)
- The SCD2 transformations (complex logic)
```

**For multiple projects:**
```
Generate documentation for these related projects:
1. /projects/raw_ingestion (bronze layer)
2. /projects/data_warehouse (silver/gold)
3. /projects/analytics_layer (semantic)

Create a unified README that shows how they connect.
```

---

## Tips for Best Results

1. **Run in the project directory** - The agent works best when the project is the working directory

2. **Have clean YAML** - Well-commented YAML configs produce better docs

3. **Iterate** - Ask the agent to expand specific sections:
   - "Expand the data dictionary for the customer_dim table"
   - "Add more business context to the revenue metrics"
   - "Create a troubleshooting section for the API connection"

4. **Review and refine** - The first pass is a draft; ask for revisions:
   - "Make the executive summary less technical"
   - "Add SQL examples for the top 5 metrics"
   - "Include a diagram of the customer data flow"

---

## Customization Options

Add these to your prompt for specific needs:

**For compliance/audit:**
```
Include a data lineage section showing source-to-report traceability.
Add a section on data retention and PII handling.
```

**For onboarding:**
```
Add a "Day 1 Guide" section for new team members.
Include a glossary of all business terms used.
```

**For executives:**
```
Add a one-page executive summary at the top.
Include ROI/value metrics if available.
```

**For API consumers:**
```
Document all output tables as if they were API endpoints.
Include schema definitions in JSON format.
```
