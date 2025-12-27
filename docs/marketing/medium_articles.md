# Medium Article Outlines

Companion articles for the LinkedIn campaign. Each article expands on LinkedIn posts with full code, diagrams, and deep explanations.

---

## Phase 1 Articles (Weeks 1-2)

### Article 1: "How I Taught Myself Data Engineering While Working Night Shifts"

**Target length:** 1,500 words
**LinkedIn tie-in:** Post 1.1 (Origin Story)

**Outline:**
1. The setup: 2020 grad, pandemic, night shifts
2. The turning point: Being asked to do data work
3. The learning journey
   - YouTube, Udemy, LinkedIn Learning
   - What worked, what didn't
   - Time management while working full-time
4. The progression: Excel → Power BI → Azure → Python → Databricks
5. Key milestones and projects
6. Advice for others in similar situations
   - You don't need permission
   - Start with a real problem
   - 1000 hours is just commitment
7. Where I am now

**Code examples:** None (story-focused)

---

### Article 2: "The Bronze Layer Mistake That Cost Me 6 Months of Data"

**Target length:** 1,800 words
**LinkedIn tie-in:** Post 1.2 (Biggest Mistake)

**Outline:**
1. The story: What I did and why
2. What is the Bronze layer?
   - Medallion architecture diagram
   - Purpose: preservation, not transformation
3. The correct pattern
   - Full YAML example
   - Code walkthrough
4. What to add in Bronze (metadata only)
   - `_extracted_at`
   - `_source_file`
   - `_batch_id`
5. What NOT to do in Bronze
   - No filtering
   - No transformations
   - No deduplication
6. The Silver layer: Where cleaning belongs
7. Takeaway: Bronze = undo button

**Code examples:**
- Bad Bronze config
- Good Bronze config
- Metadata column additions

---

## Phase 2 Articles (Weeks 3-10)

### Article 3: "Setting Up a Bronze Layer with Delta Lake"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 3 posts

**Outline:**
1. Dataset introduction (Brazilian E-Commerce)
2. Project structure
3. Connections configuration
4. Bronze nodes for all 8 files
   - Full YAML for each
5. Adding extraction metadata
6. Running the pipeline
7. Verifying results
8. Next steps

**Code examples:**
- Full project YAML
- All Bronze node configs
- Verification queries

---

### Article 4: "Data Quality Contracts: Catching Problems Before Production"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 4-5 posts

**Outline:**
1. What are data contracts?
2. Types of contracts
   - not_null
   - accepted_values
   - row_count
   - uniqueness
   - custom SQL
3. Severity levels (error vs warn)
4. Full Silver layer example with contracts
5. What happens when contracts fail
6. Quarantine pattern for failed records
7. Best practices

**Code examples:**
- Contract configurations
- Quarantine setup
- Error handling

---

### Article 5: "Complete Silver Layer Configuration Guide"

**Target length:** 2,500 words
**LinkedIn tie-in:** Week 5 posts

**Outline:**
1. Purpose of Silver layer
2. Common transformations
   - Deduplication
   - Type casting
   - Null handling
   - Text cleaning
3. Full config for each table
   - orders
   - customers
   - products
   - etc.
4. Validation rules
5. Testing your Silver layer
6. Before/after data examples

**Code examples:**
- All Silver node configs
- Transform step examples
- Validation rules

---

### Article 6: "Facts vs Dimensions: A Practical Guide"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 6 posts

**Outline:**
1. The restaurant analogy
   - Facts = receipts
   - Dimensions = menus, customer lists
2. Identifying facts and dimensions in your data
3. Fact table characteristics
   - Keys (surrogate + degenerate)
   - Measures
   - Grain
4. Dimension table characteristics
   - Natural key
   - Surrogate key
   - Attributes
5. Star schema diagram
6. The e-commerce example
7. Common mistakes

**Code examples:**
- Fact table design
- Dimension table design
- Star schema SQL

---

### Article 7: "Building a Date Dimension from Scratch"

**Target length:** 1,500 words
**LinkedIn tie-in:** Week 7 posts

**Outline:**
1. Why every warehouse needs a date dimension
2. What columns to include
3. Generating with Odibi
4. Fiscal year handling
5. Unknown member row
6. Common queries using date dimension
7. Complete configuration

**Code examples:**
- Date dimension config
- Generated column list
- Query examples

---

### Article 8: "Fact Table Patterns: Lookups, Orphans, and Measures"

**Target length:** 2,200 words
**LinkedIn tie-in:** Week 8 posts

**Outline:**
1. Anatomy of a fact table
2. Surrogate key lookups
   - How it works
   - Configuration
3. Orphan handling strategies
   - unknown (default to 0)
   - reject (fail pipeline)
   - quarantine (route to table)
4. Defining measures
   - Passthrough
   - Calculated
   - Renamed
5. Grain validation
6. Complete fact table config
7. Testing your fact table

**Code examples:**
- Full fact pattern config
- Lookup configuration
- Quarantine setup

---

### Article 9: "From CSV to Star Schema: Complete Walkthrough"

**Target length:** 3,000 words
**LinkedIn tie-in:** Week 9 posts

**Outline:**
1. Starting point: 8 CSV files
2. The journey
   - Bronze: Raw landing
   - Silver: Cleaning
   - Gold: Dimensional model
3. Complete architecture diagram
4. Full configuration files
5. Running the full pipeline
6. Query examples
7. Results and metrics
8. What we built

**Code examples:**
- Complete project YAML
- All pipeline configs
- Query examples

---

### Article 10: "Introducing Odibi: Declarative Data Pipelines"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 10 posts

**Outline:**
1. Why I built Odibi
2. The problem with script-based pipelines
3. Declarative vs imperative
4. Core concepts
   - Nodes
   - Transformers
   - Patterns
   - Validation
5. Getting started
6. The e-commerce example
7. What's next
8. How to contribute

**Code examples:**
- Installation
- Basic usage
- Pattern examples

---

## Phase 3 Articles (Weeks 11-16)

### Article 11: "SCD2 Complete Guide: When and How to Track History"

**Target length:** 2,500 words
**LinkedIn tie-in:** Week 11 posts

**Outline:**
1. What is SCD2?
2. When to use it (and when not to)
3. How it works (with diagrams)
4. Configuration deep dive
5. Common gotchas
   - Deduplication
   - Volatile columns
   - Performance
6. Debugging SCD2 issues
7. Full example

---

### Article 12: "Dimension Table Patterns: Unknown Members, Keys, and Auditing"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 12 posts

---

### Article 13: "Fact Table Design: Grain, Measures, and Validation"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 13 posts

---

### Article 14: "Pre-Aggregation Strategies for Fast Dashboards"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 14 posts

---

### Article 15: "Data Quality Patterns: Contracts, Validation, and Quarantine"

**Target length:** 2,200 words
**LinkedIn tie-in:** Week 15 posts

---

### Article 16: "Incremental Loading: Watermarks, Merge, and Append"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 16 posts

---

## Phase 4 Articles (Weeks 17-22)

### Article 17: "Bronze Layer Anti-Patterns: 3 Mistakes That Will Haunt You"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 17 posts

---

### Article 18: "Silver Layer Best Practices: Centralize, Validate, Deduplicate"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 18 posts

---

### Article 19: "SCD2 Done Wrong: History Explosion and How to Prevent It"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 19 posts

---

### Article 20: "Performance Anti-Patterns: Why Your Pipeline Takes Hours"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 20 posts

---

### Article 21: "Configuration Patterns for Multi-Environment Pipelines"

**Target length:** 1,800 words
**LinkedIn tie-in:** Week 21 posts

---

### Article 22: "Debugging Data Pipelines: A Systematic Approach"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 22 posts

---

## Phase 5 Articles (Weeks 23-26)

### Article 23: "Building a Semantic Layer: Metrics Everyone Can Trust"

**Target length:** 2,200 words
**LinkedIn tie-in:** Week 23 posts

---

### Article 24: "Production Data Pipelines: Monitoring, Retry, and Testing"

**Target length:** 2,000 words
**LinkedIn tie-in:** Week 24 posts

---

### Article 25: "What I Learned Building an Open Source Data Framework"

**Target length:** 1,500 words
**LinkedIn tie-in:** Week 25 posts

---

### Article 26: "6 Months of Data Engineering Content: A Complete Index"

**Target length:** 1,000 words
**LinkedIn tie-in:** Week 26 posts

**This is a summary/index article linking to everything.**

---

## Publishing Schedule

| Week | Article | LinkedIn Posts |
|------|---------|----------------|
| 1 | Article 1 | 1.1, 1.2, 1.3 |
| 2 | Article 2 | 2.1, 2.2, 2.3 |
| 3 | Article 3 | 3.1, 3.2, 3.3 |
| 4 | Article 4 | 4.1, 4.2, 4.3 |
| 5 | Article 5 | 5.1, 5.2, 5.3 |
| ... | ... | ... |
| 26 | Article 26 | 26.1, 26.2, 26.3 |

## Medium Tips

1. **Publish on Tuesday-Thursday** for best reach
2. **Use code blocks** with syntax highlighting
3. **Add diagrams** (use Mermaid or draw.io)
4. **Include a "TL;DR"** at the top for skimmers
5. **Link to related articles** you've written
6. **End with a CTA** (follow, clap, comment)
7. **Cross-post** to LinkedIn after publishing
