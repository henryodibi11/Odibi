# Odibi Feature Specification: The "Orgasmic" Upgrade
2:
3: This document details the specification and implementation plan for the top 3 "Ease of Life" features. It is the primary reference for the implementation phase.
4:
5: ---
6:
7: ## 1. Validation (The "Quality Gate") 🛡️
8:
9: ### 💡 Concept
10: A declarative "Validation Block" on the Node level. Instead of writing manual SQL queries for common checks, users declare their intent. Odibi handles the execution, reporting, and alerting.
11:
12: ### 📝 User Story
13: > "As a Data Engineer, I want to ensure my 'Silver' tables never contain duplicate IDs or null timestamps, so that downstream reports are accurate. I want to define this in one line of config, not 20 lines of SQL."
14:
15: ### ⚙️ YAML Specification
16: ```yaml
17: - name: "dim_customers"
18:   depends_on: ["clean_customers"]
19:
20:   validation:
21:     mode: "fail"          # Options: fail (stop pipeline), warn (log only)
22:     on_fail: "alert"      # Options: alert (send slack), ignore
23:     tests:
24:       # Built-in Test Types
25:       - type: "not_null"
26:         columns: ["customer_id", "email"]
27:  
28:       - type: "unique"
29:         columns: ["customer_id"]
30:  
31:       - type: "accepted_values"
32:         column: "status"
33:         values: ["ACTIVE", "INACTIVE", "CHURNED"]
34:  
35:       - type: "row_count"
36:         min: 100
37:  
38:       # Custom Logic
39:       - type: "custom_sql"
40:         name: "age_sanity_check"
41:         condition: "age >= 18 AND age < 120"
42:         threshold: 0.01   # Allow 1% failure rate before breaking
43: ```
44:
45: ### 🏗️ Technical Implementation Plan
46: 1.  [x] **Pydantic Models (`odibi/config.py`)**:
47:     *   Update `ValidationConfig` to support the new structure (`mode`, `on_fail`, `tests` list).
48:     *   Create `TestConfig` polymorphic model (discriminator on `type`).
49: 2.  [x] **Validation Logic (`odibi/validation/engine.py`)**:
50:     *   Create a new `Validator` class.
51:     *   Implement methods for each test type (`check_not_null`, `check_unique`, etc.) that generate SQL/Pandas logic.
52:     *   Spark Optimization: Use `count(CASE WHEN ...)` to batch multiple checks into a single pass if possible.
53: 3.  [x] **Node Integration (`odibi/node.py`)**:
54:     *   Update `_execute_validation_phase` to use the new `Validator`.
55:     *   Handle `mode: fail` vs `warn` logic.
56: 4.  [x] **Reporting (`odibi/story/`)**:
57:     *   Capture validation results (pass/fail counts) in `NodeResult`.
58:     *   Render a "Data Quality" section in the Story HTML.
59:
60: ---
61:
62: ## 2. Incremental Loading (The "Auto-Pilot") ✨
63:
64: ### 💡 Concept
65: "Stateful" High-Water-Mark (HWM) management. Odibi tracks the last processed state (timestamp or ID) in a persistent backend and automatically filters input data.
66:
67: ### 📝 User Story
68: > "As a Data Engineer, I want to ingest only new orders from Postgres into Delta Lake. I don't want to manually query the target table or manage a state file. I just want to say 'keep it in sync'."
69:
70: ### ⚙️ YAML Specification
71: ```yaml
72: - name: "orders_incremental"
73:   read:
74:     connection: "postgres_prod"
75:     format: "sql"
76:     # The base query (Odibi appends the WHERE clause)
77:     query: "SELECT * FROM public.orders"
78:  
79:     incremental:
80:       mode: "stateful"                  # The new magic mode
81:       key_column: "updated_at"
82:       fallback_column: "created_at"     # If updated_at is null, check this
83:       watermark_lag: "2h"               # Safety buffer (overlaps window)
84:       state_key: "orders_prod_ingest"   # Unique ID for state tracking
85: ```
86:
87: ### 🏗️ Technical Implementation Plan
88: 1.  [x] **State Backend (`odibi/state.py`)**:
89:     *   *Done:* `DeltaStateBackend` is ready.
90: 2.  [x] **Config (`odibi/config.py`)**:
91:     *   Update `IncrementalConfig` to add `mode`, `state_key`, `watermark_lag`.
92: 3.  [x] **Read Logic (`odibi/node.py` - `_execute_read`)**:
93:     *   Inject `StateManager`.
94:     *   Retrieve last HWM from `state_manager.get_last_run_state(key)`.
95:     *   Construct `WHERE` clause: `column > last_hwm - lag`.
96:     *   **Critical:** After successful execution, update the State with the *new* `max(key_column)` from the fetched data.
97:
98: ---
99:
100: ## 3. Schema Management (The "Drift Handler") 🛡️
101:
102: ### 💡 Concept
103: Declarative handling of schema evolution. Instead of crashing on unknown columns, users define a policy (`enforce` vs `evolve`).
104:
105: ### 📝 User Story
106: > "As a Data Engineer, I work with a JSON API that frequently adds new fields. I want my 'Bronze' layer to automatically add these new columns to the table without crashing the pipeline."
107:
108: ### ⚙️ YAML Specification
109: ```yaml
110: - name: "web_events"
111:   depends_on: ["raw_json_logs"]
112:  
113:   schema:
114:     mode: "evolve"                    # Options: enforce, evolve
115:  
116:     # What to do when input has columns NOT in target
117:     on_new_columns: "add_nullable"    # ignore, fail, add_nullable
118:  
119:     # What to do when input is MISSING columns expected by target
120:     on_missing_columns: "fill_null"   # fail, fill_null
121:  
122:     # Explicit expectations (optional)
123:     expected:
124:       event_id: { type: "string", nullable: false }
125: ```
126:
127: ### 🏗️ Technical Implementation Plan
128: 1.  [x] **Config (`odibi/config.py`)**:
129:     *   Add `SchemaPolicyConfig` model.
130: 2.  [x] **Schema Logic (`odibi/engine/base.py` & subclasses)**:
131:     *   Add `harmonize_schema(df, target_schema, policy)` method.
132:     *   **Spark:** Use `df.withColumn` to add missing cols. Use `mergeSchema` option for Delta writes.
133:     *   **Pandas:** Use `reindex` or direct assignment.
134: 3.  [x] **Node Integration (`odibi/node.py`)**:
135:     *   Before Write Phase: Fetch target table schema (if exists).
136:     *   Compare `current_df.schema` vs `target_schema`.
137:     *   Apply harmonization logic based on policy.
138:
139: ---
140:
141: ## 🔮 Future Scope: Other "Ease of Life" Features
142:
143: These are prioritized for subsequent iterations.
144:
145: *   **"Auto-Optimize"**: `write.auto_optimize: true`. Automatically runs `OPTIMIZE` and `VACUUM` on Delta tables based on a "reporting" or "ingest" profile.
146: *   **Privacy Suite**: `privacy.anonymize: true`. Config-driven PII hashing/masking based on `ColumnMetadata`.
147: *   **New Transformers**:
148:     *   `normalize_json`: Deep flattening.
149:     *   `sessionize`: Window-based session creation.
150:     *   `geocode`: IP-to-Geo enrichment.
151:
152: ---
153:
154: ## ✅ Validation Strategy
155:
156: For every feature above, the definition of "Done" includes:
157: 1.  **Unit Tests**: Verify logic in isolation (e.g., `test_validation_engine.py`).
158: 2.  **Integration Tests**: End-to-end pipeline run with a local Delta/DuckDB setup.
159: 3.  **Story Check**: Verify that the feature's activity is correctly logged in the Odibi Story (HTML report).
160:
161: ---
162:
