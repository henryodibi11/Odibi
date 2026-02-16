# Odibi MCP Recipes for AI Assistants

This guide teaches AI assistants how to combine odibi MCP tools to help developers build, debug, and maintain data pipelines.

---

## Recipe 1: "Help me build a new pipeline"

**When user says:** "I need to build a pipeline for...", "Create a pipeline that...", "Help me transform this data..."

**Steps:**

1. **Understand the data source**
   ```
   map_environment(connection="<conn>", path="<path>")
   profile_source(connection="<conn>", path="<file>")
   ```

2. **Check available patterns**
   ```
   list_patterns()
   explain(name="<pattern_name>")
   ```

3. **Find relevant transformers**
   ```
   list_transformers()
   explain(name="<transformer_name>")
   ```

4. **Write and validate the YAML**
   ```
   # Write YAML manually following docs/reference/yaml_schema.md
   validate_yaml(yaml_content="<generated_yaml>")
   ```

**Example conversation:**
> User: "I need to build a pipeline to track customer changes over time"
>
> AI: Let me help you build that. First, I'll check what pattern fits best...
> [calls list_patterns to see available options]
> [calls explain with name="scd2" to get details]
>
> The SCD2 pattern is perfect for this. See docs/reference/yaml_schema.md for the structure...

---

## Recipe 2: "Why did my pipeline fail?"

**When user says:** "Pipeline failed", "Got an error", "Node is failing", "Debug this..."

**Steps:**

1. **Check the run status**
   ```
   story_read(pipeline="<pipeline_name>")
   ```

2. **Check node output data**
   ```
   node_sample(pipeline="<pipeline>", node="<failed_node>", max_rows=10)
   ```

3. **Check for validation failures**
   ```
   node_failed_rows(pipeline="<pipeline>", node="<node>")
   ```

4. **Get diagnosis and fix suggestions**
   ```
   diagnose_error(error_message="<the_error>")
   ```

**Example conversation:**
> User: "My bronze pipeline failed with KeyError: 'customer_id'"
>
> AI: Let me investigate...
> [calls story_read to see which node failed]
> [calls node_sample to see output data]
>
> I see the issue - the source column is named 'CustomerID' not 'customer_id'.
> You need to rename it in your transform steps.

---

## Recipe 3: "What data do I have available?"

**When user says:** "What files are in...", "Show me the data", "What's in this connection", "Explore the data"

**Steps:**

1. **Scout the connection**
   ```
   map_environment(connection="<conn>", path="<path>")
   ```

2. **Profile specific files/tables**
   ```
   profile_source(connection="<conn>", path="<file>")
   ```

3. **For batch profiling**
   ```
   profile_folder(connection="<conn>", folder_path="<path>", pattern="*.csv")
   ```

---

## Recipe 4: "How does X work in odibi?"

**When user says:** "How do I...", "What is...", "Explain...", "How does X work"

**Steps:**

1. **Check if it's a known feature**
   ```
   explain(name="<feature_name>")
   ```

2. **List available options**
   ```
   list_transformers()
   list_patterns()
   list_connections()
   get_validation_rules()
   ```

3. **For deeper questions**
   ```
   # Use grep on the docs/ folder to search documentation
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

1. **Get the lineage graph**
   ```
   lineage_graph(pipeline="<pipeline>", include_external=true)
   ```

2. **Check recent execution**
   ```
   story_read(pipeline="<pipeline>")
   ```

3. **Sample node output**
   ```
   node_sample(pipeline="<pipeline>", node="<node>", max_rows=10)
   ```

---

## Recipe 6: "Check my pipeline health"

**When user says:** "Is my pipeline healthy", "Any failures", "Pipeline status", "What's been failing"

**Steps:**

1. **Check recent run status**
   ```
   story_read(pipeline="<pipeline>")
   ```

2. **Check for failed rows**
   ```
   node_failed_rows(pipeline="<pipeline>", node="<node>")
   ```

3. **View lineage for impact analysis**
   ```
   lineage_graph(pipeline="<pipeline>")
   ```

---

## Recipe 7: "Write a custom transformer"

**When user says:** "I need a custom transformer", "Create a transformer that...", "Write a function to..."

**Steps:**

1. **Check if similar transformer exists**
   ```
   list_transformers()
   explain(name="<similar_transformer>")
   ```

2. **Review existing transformer implementations**
   ```
   # Use grep on odibi/transformers/ folder to find examples
   ```

3. **Write transformer following the pattern**
   ```
   # See docs/reference/yaml_schema.md for transformer structure
   # Use @register_function decorator
   ```

---

## Recipe 8: "Compare two pipeline runs"

**When user says:** "What changed between runs", "Compare runs", "Why is output different"

**Steps:**

1. **Get recent runs**
   ```
   story_read(pipeline="<pipeline>")
   ```

2. **Sample data from runs**
   ```
   node_sample(pipeline="<pipeline>", node="<node>", max_rows=20)
   ```

3. **Check failed rows**
   ```
   node_failed_rows(pipeline="<pipeline>", node="<node>")
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
   # See docs/reference/yaml_schema.md for the complete schema
   ```

3. **Check transformer usage**
   ```
   explain(name="<transformer_used>")
   ```

---

## Recipe 10: "I'm new to odibi, help me get started"

**When user says:** "How do I start", "New to odibi", "Getting started", "Tutorial"

**Steps:**

1. **Bootstrap context**
   ```
   bootstrap_context()
   ```

2. **Show available patterns**
   ```
   list_patterns()
   ```

3. **Show available connections**
   ```
   list_connections()
   ```

4. **Review YAML structure**
   ```
   # See docs/reference/yaml_schema.md for complete schema
   ```

5. **Explain a pattern**
   ```
   explain(name="dimension")
   ```

---

## Tool Selection Quick Reference

| User Intent | Primary Tools |
|-------------|---------------|
| Build pipeline | `list_patterns`, `explain`, `list_transformers`, `validate_yaml` |
| Debug failure | `story_read`, `node_sample`, `node_failed_rows`, `diagnose_error` |
| Explore data | `map_environment`, `profile_source`, `profile_folder` |
| Learn odibi | `explain`, `list_transformers`, `list_patterns`, `get_validation_rules` |
| Check lineage | `lineage_graph` |
| Monitor health | `story_read`, `node_failed_rows` |
| Validate config | `validate_yaml` + docs/reference/yaml_schema.md |
| Write code | `list_transformers`, `explain` + grep odibi/transformers/ |

---

---

## Context-First Workflows

These workflows teach AI to **gather full context before taking action**. This prevents hallucination and ensures suggestions are grounded in the actual project state.

---

### Workflow A: "Full Source Understanding" (Before Building Anything)

**Purpose:** Before generating any YAML or suggesting transformations, the AI MUST understand the source data completely.

**Mandatory Steps (in order):**

```
# Step 1: Scout what files/tables exist
map_environment(connection="<conn>", path="<path>")

# Step 2: Profile the source (get schema, encoding, delimiter, sample data)
profile_source(connection="<conn>", path="<file>")

# Step 3: For batch profiling
profile_folder(connection="<conn>", folder_path="<path>", pattern="*.csv")
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
# Step 1: Understand the full lineage
lineage_graph(pipeline="<pipeline>", include_external=true)

# Step 2: Check execution history
story_read(pipeline="<pipeline>")

# Step 3: See actual data flowing through
node_sample(pipeline="<pipeline>", node="<node>", max_rows=10)

# Step 4: Check for failed rows
node_failed_rows(pipeline="<pipeline>", node="<node>")
```

**What AI should understand before acting:**
- The flow of data through all nodes
- What transformations are applied at each step
- Recent run success/failure history
- Any validation or quality rules in place

---

### Workflow C: "Framework Mastery Check" (Before Suggesting Solutions)

**Purpose:** Before suggesting a pattern, transformer, or approach, verify the AI understands the framework correctly.

**Steps:**

```
# Step 1: If suggesting a pattern, verify understanding
list_patterns()
explain(name="<pattern_name>")

# Step 2: If using transformers, verify each one
list_transformers()
explain(name="<transformer_name>")
# Check parameters, expected inputs, edge cases

# Step 3: For validation rules
get_validation_rules()

# Step 4: For deeper understanding
# Use grep on docs/ folder to search documentation
```

**AI should NEVER:**
- Guess transformer parameters - always use `explain` first
- Assume YAML structure - always reference docs/reference/yaml_schema.md
- Invent column names - always derive from actual `profile_source`

---

### Workflow D: "Complete Debug Investigation" (Before Suggesting Fixes)

**Purpose:** Before suggesting any fix, the AI must diagnose the root cause with evidence.

**Investigation Sequence:**

```
# Step 1: Get the error context
story_read(pipeline="<pipeline>")  # See which node failed

# Step 2: See what data came out
node_sample(pipeline="<pipeline>", node="<failed_node>", max_rows=20)

# Step 3: Check for validation failures
node_failed_rows(pipeline="<pipeline>", node="<failed_node>")

# Step 4: Get AI diagnosis of the error
diagnose_error(error_message="<exact_error>")

# Step 5: Trace lineage if needed
lineage_graph(pipeline="<pipeline>")
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
map_environment(connection="<source_conn>", path="<source_path>")
profile_source(connection="<source_conn>", path="<source_file>")

# Step 3: For each transformer used, verify parameters
explain(name="<transformer_1>")
explain(name="<transformer_2>")
# ...for each transformer in the pipeline

# Step 4: Reference YAML structure
# See docs/reference/yaml_schema.md for complete schema
```

---

### Workflow F: "New Project Onboarding" (Complete Context Acquisition)

**Purpose:** When AI joins a project or user asks for help on an unfamiliar pipeline, gather ALL context first.

**Full Context Acquisition:**

```
# Step 1: Bootstrap context (recommended first step)
bootstrap_context()

# Step 2: Available patterns and their purposes
list_patterns()

# Step 3: Available connections (where data lives)
list_connections()

# Step 4: Available transformers (what operations exist)
list_transformers()

# Step 5: For each pipeline in scope:
lineage_graph(pipeline="<pipeline_1>")
story_read(pipeline="<pipeline_1>")

# Step 6: Explore actual source data
map_environment(connection="<primary_source>", path="/")
profile_source(connection="<primary_source>", path="<key_file>")

# Step 7: For deeper questions
# Use grep on docs/ folder to search documentation
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
| Guess column names | Use `profile_source` to see actual names |
| Assume transformer parameters | Use `explain` to verify exact parameter names and types |
| Generate YAML without validation | Always run `validate_yaml` before presenting to user |
| Suggest fixes without evidence | Use `node_sample` and `node_failed_rows` to see actual data |
| Skip lineage understanding | Use `lineage_graph` before modifying downstream nodes |
| Assume YAML structure | Reference docs/reference/yaml_schema.md |

---

## Context Checklist for AI

Before suggesting ANY pipeline changes, verify:

- [ ] I have seen the actual source data (`profile_source`)
- [ ] I know the exact column names and types (`profile_source`)
- [ ] I understand the pattern being used (`explain`)
- [ ] I have validated my YAML (`validate_yaml`)
- [ ] I have checked lineage impact (`lineage_graph`)
- [ ] I have grounded my response in real data, not assumptions

---

## Best Practices for AI Assistants

1. **Always start with context** - Use `bootstrap_context` or `story_read` to understand the current state
2. **Show real data** - Use `profile_source` and `node_sample` to ground responses in actual data
3. **Validate before suggesting** - Use `validate_yaml` before presenting YAML to users
4. **Chain tools logically** - Debug flows: status → samples → diagnosis
5. **Use explain liberally** - Always clarify transformers/patterns before using them
6. **Reference yaml_schema.md** - For YAML structure, see docs/reference/yaml_schema.md
7. **Never guess - always verify** - If unsure about column names, types, or parameters, use the tools to check
8. **Build complete mental models** - Use the Context-First Workflows before taking action

---

## Recipe 11: "Analyze Unknown Connection"

**When user says:** "What's in this connection?", "Explore [connection]", "Analyze [connection]", "Catalog [connection]"

**Two-Step Pattern:** Structure first, samples second.

**For Any Connection:**

```
# Step 1: Scout the connection (files, tables, patterns)
map_environment(connection="<conn>", path="")
# Returns: structure overview, file patterns, table counts

# Step 2: Profile specific files/tables
profile_source(connection="<conn>", path="<file>")
# Returns: full schema, encoding, delimiter, sample data

# Step 3: For batch profiling
profile_folder(connection="<conn>", folder_path="<path>", pattern="*.csv")
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
# Profile both source and target
profile_source(connection="<source_conn>", path="<source_path>")
profile_source(connection="<target_conn>", path="<target_path>")

# Compare the schemas manually from profile results
```

**Response should include:**
- Compatibility status (breaking vs non-breaking)
- Added columns (in target, not in source)
- Removed columns (in source, missing in target - BREAKING)
- Type mismatches (BREAKING)
- Suggestions for handling differences

---

## Recipe 13: "Catalog a Database"

**When user says:** "Catalog [database]", "Document all tables in...", "Create data dictionary for..."

**Steps:**

```
# Step 1: Scout the connection
map_environment(connection="<sql_conn>")

# Step 2: Profile key tables
profile_source(connection="<sql_conn>", path="<table1>")
profile_source(connection="<sql_conn>", path="<table2>")

# Step 3: For batch profiling
profile_folder(connection="<sql_conn>", folder_path="<schema>", pattern="*")
```

**AI should analyze:**
- Column names and types
- Sample values
- Potential keys (columns ending in _id, unique values)
- Table purpose (dim_, fact_, stg_, etc.)
- Relationships (FK patterns)

---

## Recipe 14: "Suggest Pipeline from Discovered Data"

**When user says:** "Build pipeline for what you found", "Create pipeline from discovery", "Load this as..."

**Prerequisites:** AI has already run discovery tools and understands the source data.

**Steps:**

```
# Step 1: Confirm understanding with profile
profile_source(connection="<conn>", path="<file>")

# Step 2: Check available patterns
list_patterns()
explain(name="<suggested_pattern>")

# Step 3: Find relevant transformers
list_transformers()
explain(name="<transformer_name>")

# Step 4: Write YAML manually
# See docs/reference/yaml_schema.md for structure

# Step 5: Validate before presenting
validate_yaml(yaml_content="<generated>")
```

**AI should propose:**
- Pattern choice with reasoning
- Input configuration (connection, path, format)
- Required transforms (type casts, renames, null handling)
- Output configuration
- Validation rules (if applicable)
