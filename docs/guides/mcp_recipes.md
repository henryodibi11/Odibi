# Odibi MCP Recipes for AI Assistants

This guide teaches AI assistants how to combine odibi MCP tools to help developers build, debug, and maintain data pipelines.

---

## Recipe 1: "Help me build a new pipeline"

**When user says:** "I need to build a pipeline for...", "Create a pipeline that...", "Help me transform this data..."

**Steps:**

1. **Understand the data source**
   ```
   list_files(connection="<conn>", path="<path>", pattern="*")
   preview_source(connection="<conn>", path="<file>", max_rows=10)
   infer_schema(connection="<conn>", path="<file>")
   ```

2. **Suggest the right pattern**
   ```
   suggest_pattern(use_case="<user's description>")
   ```

3. **Get the correct YAML structure**
   ```
   get_yaml_structure()
   get_example(pattern_name="<suggested_pattern>")
   ```

4. **Find relevant transformers**
   ```
   list_transformers()
   explain(name="<transformer_name>")
   ```

5. **Generate and validate the YAML**
   ```
   validate_yaml(yaml_content="<generated_yaml>")
   ```

**Example conversation:**
> User: "I need to build a pipeline to track customer changes over time"
>
> AI: Let me help you build that. First, I'll check what pattern fits best...
> [calls suggest_pattern with use_case="track customer changes over time"]
>
> The SCD2 pattern is perfect for this. Let me get you an example...
> [calls get_example with pattern_name="scd2"]

---

## Recipe 2: "Why did my pipeline fail?"

**When user says:** "Pipeline failed", "Got an error", "Node is failing", "Debug this..."

**Steps:**

1. **Check the run status**
   ```
   story_read(pipeline="<pipeline_name>")
   ```

2. **Get details on the failed node**
   ```
   node_describe(pipeline="<pipeline>", node="<failed_node>")
   ```

3. **Examine the input data**
   ```
   node_sample_in(pipeline="<pipeline>", node="<node>", max_rows=10)
   ```

4. **Check for validation failures**
   ```
   node_failed_rows(pipeline="<pipeline>", node="<node>")
   ```

5. **Get diagnosis and fix suggestions**
   ```
   diagnose_error(error_message="<the_error>")
   ```

**Example conversation:**
> User: "My bronze pipeline failed with KeyError: 'customer_id'"
>
> AI: Let me investigate...
> [calls story_read to see which node failed]
> [calls node_sample_in to see the actual input data]
>
> I see the issue - the source column is named 'CustomerID' not 'customer_id'.
> You need to rename it in your transform steps.

---

## Recipe 3: "What data do I have available?"

**When user says:** "What files are in...", "Show me the data", "What's in this connection", "Explore the data"

**Steps:**

1. **List available files**
   ```
   list_files(connection="<conn>", path="<path>", pattern="*")
   ```

2. **For SQL connections, list tables**
   ```
   list_tables(connection="<sql_conn>", schema="dbo")
   describe_table(connection="<sql_conn>", table="<table>", schema="dbo")
   ```

3. **Preview the data**
   ```
   preview_source(connection="<conn>", path="<file>", max_rows=10)
   ```

4. **For Excel files, discover sheets first**
   ```
   list_sheets(connection="<conn>", path="<file>.xlsx")
   preview_source(connection="<conn>", path="<file>.xlsx", sheet="<sheet_name>", max_rows=10)
   ```

5. **Infer schema**
   ```
   infer_schema(connection="<conn>", path="<file>")
   ```

---

## Recipe 4: "How does X work in odibi?"

**When user says:** "How do I...", "What is...", "Explain...", "How does X work"

**Steps:**

1. **Check if it's a known feature**
   ```
   explain(name="<feature_name>")
   ```

2. **Search documentation**
   ```
   search_docs(query="<topic>")
   get_doc(doc_path="<relevant_doc>")
   ```

3. **For deep questions, search the codebase**
   ```
   query_codebase(question="<question>")
   ```

4. **For comprehensive understanding**
   ```
   get_deep_context()
   ```

**Example conversation:**
> User: "How does validation work in odibi?"
>
> AI: Let me explain...
> [calls explain with name="validation"]
> [calls get_validation_rules to list all rules]
>
> Odibi has 12 built-in validation rules including not_null, unique, range...

---

## Recipe 5: "Show me what this pipeline does"

**When user says:** "What does this pipeline do", "Explain this pipeline", "Show me the lineage"

**Steps:**

1. **List pipeline outputs**
   ```
   list_outputs(pipeline="<pipeline>")
   ```

2. **Get the lineage graph**
   ```
   lineage_graph(pipeline="<pipeline>", include_external=true)
   ```

3. **For specific nodes, get details**
   ```
   node_describe(pipeline="<pipeline>", node="<node>")
   output_schema(pipeline="<pipeline>", output_name="<node>")
   ```

4. **Check recent execution**
   ```
   story_read(pipeline="<pipeline>")
   pipeline_stats(pipeline="<pipeline>")
   ```

---

## Recipe 6: "Check my pipeline health"

**When user says:** "Is my pipeline healthy", "Any failures", "Pipeline status", "What's been failing"

**Steps:**

1. **Check overall failure summary**
   ```
   failure_summary(max_failures=20)
   ```

2. **Check specific pipeline stats**
   ```
   pipeline_stats(pipeline="<pipeline>")
   ```

3. **Check node-level stats**
   ```
   node_stats(pipeline="<pipeline>", node="<node>")
   ```

4. **Check for schema drift**
   ```
   schema_history(pipeline="<pipeline>", node="<node>")
   ```

---

## Recipe 7: "Write a custom transformer"

**When user says:** "I need a custom transformer", "Create a transformer that...", "Write a function to..."

**Steps:**

1. **Get the correct signature**
   ```
   get_transformer_signature()
   ```

2. **Check if similar transformer exists**
   ```
   list_transformers()
   explain(name="<similar_transformer>")
   ```

3. **Search for implementation examples**
   ```
   query_codebase(question="how to implement <transformer_type>")
   ```

4. **Generate the transformer**
   ```
   generate_transformer(name="<name>", params=[...], description="...")
   ```

---

## Recipe 8: "Compare two pipeline runs"

**When user says:** "What changed between runs", "Compare runs", "Why is output different"

**Steps:**

1. **Get recent runs**
   ```
   story_read(pipeline="<pipeline>")
   ```

2. **Compare the runs**
   ```
   story_diff(pipeline="<pipeline>", run_a="<run_id_1>", run_b="<run_id_2>")
   ```

3. **Check schema changes**
   ```
   schema_history(pipeline="<pipeline>", node="<node>")
   ```

4. **Sample data from both runs**
   ```
   node_sample(pipeline="<pipeline>", node="<node>", run_id="<run_1>")
   node_sample(pipeline="<pipeline>", node="<node>", run_id="<run_2>")
   ```

---

## Recipe 9: "Validate my YAML before running"

**When user says:** "Check my YAML", "Is this correct", "Validate this config"

**Steps:**

1. **Validate the YAML syntax**
   ```
   validate_yaml(yaml_content="<yaml>")
   ```

2. **Compare against correct structure**
   ```
   get_yaml_structure()
   ```

3. **Check transformer usage**
   ```
   explain(name="<transformer_used>")
   ```

---

## Recipe 10: "I'm new to odibi, help me get started"

**When user says:** "How do I start", "New to odibi", "Getting started", "Tutorial"

**Steps:**

1. **Provide comprehensive context**
   ```
   get_deep_context()
   ```

2. **Show available patterns**
   ```
   list_patterns()
   ```

3. **Show available connections**
   ```
   list_connections()
   ```

4. **Get the YAML structure**
   ```
   get_yaml_structure()
   ```

5. **Show a simple example**
   ```
   get_example(pattern_name="dimension")
   ```

---

## Tool Selection Quick Reference

| User Intent | Primary Tools |
|-------------|---------------|
| Build pipeline | `suggest_pattern`, `get_example`, `get_yaml_structure`, `list_transformers` |
| Debug failure | `story_read`, `node_describe`, `node_sample_in`, `node_failed_rows`, `diagnose_error` |
| Explore data | `list_files`, `preview_source`, `infer_schema`, `list_tables` |
| Learn odibi | `explain`, `search_docs`, `get_deep_context`, `query_codebase` |
| Check lineage | `lineage_graph`, `lineage_upstream`, `lineage_downstream`, `list_outputs` |
| Monitor health | `pipeline_stats`, `node_stats`, `failure_summary`, `schema_history` |
| Validate config | `validate_yaml`, `get_yaml_structure` |
| Write code | `generate_transformer`, `get_transformer_signature`, `query_codebase` |

---

---

## Context-First Workflows

These workflows teach AI to **gather full context before taking action**. This prevents hallucination and ensures suggestions are grounded in the actual project state.

---

### Workflow A: "Full Source Understanding" (Before Building Anything)

**Purpose:** Before generating any YAML or suggesting transformations, the AI MUST understand the source data completely.

**Mandatory Steps (in order):**

```
# Step 1: Discover what files/tables exist
list_files(connection="<conn>", path="<path>", pattern="*")
# or for SQL:
list_tables(connection="<sql_conn>", schema="dbo")

# Step 2: Preview the actual data (see real values)
preview_source(connection="<conn>", path="<file>", max_rows=20)

# Step 3: Infer the schema (understand types)
infer_schema(connection="<conn>", path="<file>")

# Step 4: For SQL tables, get full structure
describe_table(connection="<sql_conn>", table="<table>", schema="dbo")
```

**What AI should note:**
- Column names exactly as they appear (case-sensitive!)
- Data types (string dates vs actual dates, numeric vs string IDs)
- Null patterns - which columns have missing values?
- Cardinality hints - is this a lookup table or transactional data?
- Key candidates - what columns could be primary/business keys?

**Only AFTER completing all steps**, the AI may suggest patterns or generate YAML.

---

### Workflow B: "Pipeline Deep Dive" (Understanding Existing Pipelines)

**Purpose:** Before modifying, debugging, or extending a pipeline, understand it completely.

**Mandatory Steps:**

```
# Step 1: What outputs does this pipeline produce?
list_outputs(pipeline="<pipeline>")

# Step 2: What's the schema of each output?
output_schema(pipeline="<pipeline>", output_name="<output>")
# Repeat for each output

# Step 3: Understand the full lineage
lineage_graph(pipeline="<pipeline>", include_external=true)

# Step 4: For specific nodes of interest:
node_describe(pipeline="<pipeline>", node="<node>")

# Step 5: See actual data flowing through
node_sample_in(pipeline="<pipeline>", node="<node>", max_rows=10)
node_sample(pipeline="<pipeline>", node="<node>", max_rows=10)

# Step 6: Check execution history
story_read(pipeline="<pipeline>")
```

**What AI should understand before acting:**
- The flow of data through all nodes
- What transformations are applied at each step
- The expected input/output schemas
- Recent run success/failure history
- Any validation or quality rules in place

---

### Workflow C: "Framework Mastery Check" (Before Suggesting Solutions)

**Purpose:** Before suggesting a pattern, transformer, or approach, verify the AI understands the framework correctly.

**Steps:**

```
# Step 1: If suggesting a pattern, verify understanding
explain(name="<pattern_name>")
get_example(pattern_name="<pattern_name>")

# Step 2: If using transformers, verify each one
explain(name="<transformer_name>")
# Check parameters, expected inputs, edge cases

# Step 3: For complex questions, get full context
get_deep_context()

# Step 4: For implementation details, search docs
search_docs(query="<topic>")
get_doc(doc_path="<relevant_doc>")

# Step 5: For code-level understanding
query_codebase(question="how does <feature> work internally")
```

**AI should NEVER:**
- Guess transformer parameters - always use `explain` first
- Assume YAML structure - always reference `get_yaml_structure`
- Invent column names - always derive from actual `preview_source` or `infer_schema`

---

### Workflow D: "Complete Debug Investigation" (Before Suggesting Fixes)

**Purpose:** Before suggesting any fix, the AI must diagnose the root cause with evidence.

**Investigation Sequence:**

```
# Step 1: Get the error context
story_read(pipeline="<pipeline>")  # See which node failed

# Step 2: Understand what the node expects
node_describe(pipeline="<pipeline>", node="<failed_node>")

# Step 3: See what data actually arrived
node_sample_in(pipeline="<pipeline>", node="<failed_node>", max_rows=20)

# Step 4: Check for validation failures
node_failed_rows(pipeline="<pipeline>", node="<failed_node>")

# Step 5: Get AI diagnosis of the error
diagnose_error(error_message="<exact_error>")

# Step 6: Trace back to source if needed
lineage_upstream(pipeline="<pipeline>", node="<failed_node>")
# Then check those upstream nodes too
```

**Fix suggestions should include:**
- The exact root cause (with evidence from data samples)
- The specific line/config that needs to change
- A validated YAML snippet (run through `validate_yaml`)

---

### Workflow E: "Pre-Flight Check" (Before Running Any Pipeline)

**Purpose:** Validate everything before execution to prevent failures.

```
# Step 1: Validate the YAML config
validate_yaml(yaml_content="<full_yaml>")

# Step 2: Verify source data is accessible
list_files(connection="<source_conn>", path="<source_path>")
preview_source(connection="<source_conn>", path="<source_file>", max_rows=5)

# Step 3: For each transformer used, verify parameters
explain(name="<transformer_1>")
explain(name="<transformer_2>")
# ...for each transformer in the pipeline

# Step 4: Check the expected output schema
get_yaml_structure()  # Verify output config is valid

# Step 5: If extending existing pipeline, check compatibility
output_schema(pipeline="<existing>", output_name="<output>")
```

---

### Workflow F: "New Project Onboarding" (Complete Context Acquisition)

**Purpose:** When AI joins a project or user asks for help on an unfamiliar pipeline, gather ALL context first.

**Full Context Acquisition:**

```
# Step 1: Framework understanding
get_deep_context()

# Step 2: Available patterns and their purposes
list_patterns()

# Step 3: Available connections (where data lives)
list_connections()

# Step 4: Available transformers (what operations exist)
list_transformers()

# Step 5: For each pipeline in scope:
list_outputs(pipeline="<pipeline_1>")
lineage_graph(pipeline="<pipeline_1>")
story_read(pipeline="<pipeline_1>")

# Step 6: Explore actual source data
list_files(connection="<primary_source>", path="/")
preview_source(connection="<primary_source>", path="<key_file>")

# Step 7: Check documentation for project-specific patterns
search_docs(query="<project_name>")
```

**AI should build a mental model of:**
- Where source data comes from (connections, file patterns)
- What pipelines exist and their purposes
- The flow from sources → bronze → silver → gold
- Project-specific conventions or custom transformers
- Recent issues or failures to be aware of

---

## Anti-Patterns to Avoid

| ❌ Don't | ✅ Do Instead |
|----------|--------------|
| Guess column names | Use `preview_source` or `infer_schema` to see actual names |
| Assume transformer parameters | Use `explain` to verify exact parameter names and types |
| Generate YAML without validation | Always run `validate_yaml` before presenting to user |
| Suggest fixes without evidence | Use `node_sample_in` and `node_failed_rows` to see actual data |
| Skip lineage understanding | Use `lineage_graph` before modifying downstream nodes |
| Assume schema | Use `output_schema` to see actual column types |

---

## Context Checklist for AI

Before suggesting ANY pipeline changes, verify:

- [ ] I have seen the actual source data (`preview_source`)
- [ ] I know the exact column names and types (`infer_schema`)
- [ ] I understand the pattern being used (`explain`)
- [ ] I have validated my YAML (`validate_yaml`)
- [ ] I have checked lineage impact (`lineage_graph`)
- [ ] I have grounded my response in real data, not assumptions

---

## Best Practices for AI Assistants

1. **Always start with context** - Use `story_read` or `list_outputs` to understand the current state
2. **Show real data** - Use `preview_source` and `node_sample` to ground responses in actual data
3. **Validate before suggesting** - Use `validate_yaml` before presenting YAML to users
4. **Chain tools logically** - Debug flows: status → details → samples → diagnosis
5. **Use explain liberally** - Always clarify transformers/patterns before using them
6. **Prefer specific tools** - Use `explain` over `get_deep_context` for focused questions
7. **Never guess - always verify** - If unsure about column names, types, or parameters, use the tools to check
8. **Build complete mental models** - Use the Context-First Workflows before taking action

---

## Recipe 11: "Analyze Unknown Connection"

**When user says:** "What's in this connection?", "Explore [connection]", "Analyze [connection]", "Catalog [connection]"

**Two-Step Pattern:** Structure first, samples second.

**For Storage Connections:**

```
# Step 1: Discover structure (shallow, no samples by default)
discover_storage(connection="<conn>", path="")
# Returns: file names, formats, schemas - lightweight response

# Step 2: For interesting files, get samples
preview_source(connection="<conn>", path="<file>", max_rows=20)

# Step 3: For Excel files, discover sheets first
list_sheets(connection="<conn>", path="<file>.xlsx")
preview_source(connection="<conn>", path="<file>.xlsx", sheet="<sheet_name>")

# Optional: Deep scan if needed
discover_storage(connection="<conn>", path="", recursive=true, max_files=50)
```

**For Database Connections:**

```
# Step 0: List available schemas FIRST
list_schemas(connection="<sql_conn>")
# Returns: schema names with table counts - know what exists before diving in

# Step 1: Discover structure (shallow, no samples by default)
discover_database(connection="<sql_conn>", schema="dbo")
# Returns: table names, columns, row counts - lightweight response

# Step 2: For interesting tables, get samples
preview_source(connection="<sql_conn>", path="<table>", max_rows=20)

# Optional: Include samples if context budget allows
discover_database(connection="<sql_conn>", schema="dbo", sample_rows=3)
```

**AI should summarize:**
- File/table inventory (count by type/format)
- Key tables/files and their purposes
- Column patterns (potential keys, dimensions, facts)
- Data quality observations (nulls, duplicates)
- Suggested next steps (which tables to load first, recommended patterns)

---

## Recipe 12: "Compare Source vs Target Schema"

**When user says:** "Compare schemas", "Is source compatible with target?", "What's different between..."

**Steps:**

```
# Direct comparison with one call
compare_schemas(
    source_connection="<source_conn>",
    source_path="<source_path>",
    target_connection="<target_conn>",
    target_path="<target_path>",
    source_sheet="<optional_sheet>",  # For Excel
    target_sheet="<optional_sheet>"
)
```

**Response should include:**
- Compatibility status (breaking vs non-breaking)
- Added columns (in target, not in source)
- Removed columns (in source, missing in target - BREAKING)
- Type mismatches (BREAKING)
- Nullability changes
- Suggestions for handling differences

**Example conversation:**
> User: "Is my staging table compatible with the production target?"
>
> AI: Let me compare the schemas...
> [calls compare_schemas with source and target]
>
> The schemas have 2 breaking differences:
> - Column `customer_id` type changed: `string` → `int` (BREAKING)
> - Column `created_date` is missing in target (BREAKING)
>
> You'll need to either:
> 1. Add a type cast in your transform: `CAST(customer_id AS INT)`
> 2. Add the missing column to the target table

---

## Recipe 13: "Catalog a Database"

**When user says:** "Catalog [database]", "Document all tables in...", "Create data dictionary for..."

**Steps:**

```
# Step 0: List available schemas
list_schemas(connection="<sql_conn>")

# Step 1: Discover all tables in each schema
discover_database(connection="<sql_conn>", schema="dbo", max_tables=100, sample_rows=3)

# Step 2: For each important table, AI analyzes:
# - Column names and types
# - Sample values
# - Potential keys (columns ending in _id, unique values)
# - Table purpose (dim_, fact_, stg_, etc.)
# - Relationships (FK patterns)

# Step 3: Output structured catalog
```

**AI Output Format:**

```yaml
catalog:
  connection: <connection_name>
  schema: dbo
  tables:
    - name: dim_customer
      purpose: Customer master data (dimension table)
      row_count: 15000
      primary_key: customer_id
      columns:
        - name: customer_id
          type: int
          role: primary_key
        - name: customer_name
          type: varchar
          role: business_name
      relationships:
        - references: fact_orders.customer_id

    - name: fact_orders
      purpose: Sales transactions (fact table)
      row_count: 1500000
      primary_key: order_id
      foreign_keys:
        - customer_id → dim_customer.customer_id
        - product_id → dim_product.product_id
```

---

## Recipe 14: "Suggest Pipeline from Discovered Data"

**When user says:** "Build pipeline for what you found", "Create pipeline from discovery", "Load this as..."

**Prerequisites:** AI has already run discovery tools and understands the source data.

**Steps:**

```
# Step 1: Confirm understanding
preview_source(connection="<conn>", path="<file>")
infer_schema(connection="<conn>", path="<file>")

# Step 2: Recommend pattern based on data shape
suggest_pattern(use_case="<inferred from data>")

# Step 3: Get pattern example
get_example(pattern_name="<suggested_pattern>")

# Step 4: Find relevant transformers
list_transformers()
explain(name="<transformer_name>")

# Step 5: Generate YAML
generate_pipeline_yaml(...)  # Or manually construct

# Step 6: Validate before presenting
validate_yaml(yaml_content="<generated>")
```

**AI should propose:**
- Pattern choice with reasoning
- Input configuration (connection, path, format)
- Required transforms (type casts, renames, null handling)
- Output configuration
- Validation rules (if applicable)
