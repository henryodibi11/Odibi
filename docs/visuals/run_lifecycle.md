# Odibi Run Lifecycle

> What happens when you run `odibi run odibi.yaml`? This diagram shows the complete execution flow.

---

## Complete Lifecycle Sequence

```mermaid
sequenceDiagram
    actor User
    participant CLI
    participant Parser
    participant Validator
    participant Planner
    participant Executor
    participant Engine
    participant Storage
    participant Quality
    participant Story
    participant Alerts

    User->>CLI: odibi run odibi.yaml
    
    Note over CLI,Parser: Phase 1: Configuration Loading
    CLI->>Parser: Load YAML
    Parser->>Parser: Substitute env vars ${VAR}
    Parser->>Parser: Resolve YAML anchors
    Parser->>Validator: ProjectConfig object
    
    Note over Validator: Phase 2: Validation
    Validator->>Validator: Check schema (Pydantic)
    Validator->>Validator: Validate connections
    Validator->>Validator: Check dependencies
    Validator->>Validator: Build DAG
    
    alt Validation Fails
        Validator-->>User: ❌ Error: Missing connection "raw_data"
    end
    
    Note over Planner,Executor: Phase 3: Planning
    Planner->>Planner: Topological sort
    Planner->>Planner: Determine parallel nodes
    Planner->>Executor: Execution plan
    
    Note over Executor,Storage: Phase 4: Execution
    loop For each node
        Executor->>Engine: Execute node
        
        alt Has Contracts
            Engine->>Quality: Check contracts
            Quality-->>Engine: Pass/Fail
            alt Contract Fails
                Quality-->>Executor: ❌ Stop (contract failed)
                Executor-->>Alerts: Send failure alert
                Alerts-->>User: 📧 Pipeline failed
            end
        end
        
        Engine->>Storage: Read data
        Storage-->>Engine: DataFrame
        
        Engine->>Engine: Apply transformations
        Engine->>Engine: Register in context
        
        alt Has Validation
            Engine->>Quality: Run validation tests
            Quality-->>Engine: Pass/Warn/Fail
            
            alt Validation Fails + mode: fail
                Quality-->>Executor: ❌ Stop
            end
            
            alt Validation Fails + on_fail: quarantine
                Quality->>Storage: Write bad records to quarantine
            end
        end
        
        Engine->>Storage: Write output
        Storage-->>Engine: Success
        
        Engine-->>Executor: Node complete
    end
    
    Note over Story: Phase 5: Observability
    Executor->>Story: Generate Data Story
    Story->>Story: Collect lineage
    Story->>Story: Profile data
    Story->>Story: Log validations
    Story->>Storage: Write HTML
    
    Note over Alerts: Phase 6: Notifications
    Executor->>Alerts: Pipeline complete
    Alerts-->>User: ✅ Success notification
    
    Executor-->>CLI: Success
    CLI-->>User: ✅ Pipeline completed in 2m 35s
    User->>CLI: odibi story last
    CLI-->>User: 📖 Opens Story in browser
```

---

## Phase Breakdown

### **Phase 1: Configuration Loading** (< 1 second)

```mermaid
graph TD
    A[odibi.yaml] --> B[YAML Parser]
    B --> C{Environment<br/>Variables?}
    C -->|Yes| D[Substitute ${VAR}]
    C -->|No| E[Continue]
    D --> E
    E --> F{YAML Anchors?}
    F -->|Yes| G[Resolve &refs]
    F -->|No| H[Pydantic Models]
    G --> H
    H --> I[ProjectConfig]
    
    style I fill:#06d6a0,stroke:#264653,color:#333
```

**What happens:**
- Read `odibi.yaml` from disk
- Replace `${DB_HOST}` with environment variable values
- Resolve YAML anchors (`&default_write`)
- Parse into Pydantic models (type-safe)

**Failures:**
- ❌ YAML syntax error
- ❌ Missing environment variable
- ❌ Invalid YAML structure

---

### **Phase 2: Validation** (< 1 second)

```mermaid
graph TD
    A[ProjectConfig] --> B{Schema Valid?}
    B -->|No| C[❌ Exit:<br/>Invalid config]
    B -->|Yes| D{Connections<br/>Defined?}
    D -->|No| E[❌ Exit:<br/>Connection not found]
    D -->|Yes| F{Circular<br/>Dependencies?}
    F -->|Yes| G[❌ Exit:<br/>Cyclic dependency]
    F -->|No| H[Build DAG]
    H --> I[✅ Ready to Execute]
    
    style C fill:#e63946,stroke:#264653,color:#fff
    style E fill:#e63946,stroke:#264653,color:#fff
    style G fill:#e63946,stroke:#264653,color:#fff
    style I fill:#06d6a0,stroke:#264653,color:#333
```

**Checks:**
- Pydantic schema validation (types, required fields)
- All `connection:` references exist in `connections:`
- All `depends_on:` references exist
- No circular dependencies (A → B → A)
- Transformer parameters valid

**Failures:**
- ❌ Missing required field
- ❌ Invalid transformer name
- ❌ Undefined connection
- ❌ Cycle detected in DAG

---

### **Phase 3: Planning** (< 1 second)

```mermaid
graph LR
    A[DAG] --> B[Topological Sort]
    B --> C{Can Run<br/>in Parallel?}
    C -->|Yes| D[Parallel Group 1:<br/>node_a, node_b]
    C -->|No| E[Sequential:<br/>node_c depends on node_a]
    D --> F[Execution Plan]
    E --> F
    
    style F fill:#06d6a0,stroke:#264653,color:#333
```

**Planning decisions:**
- Which nodes can run in parallel?
- What order to execute sequential nodes?
- How to manage context/state between nodes?

**Example:**
```
Execution Plan:
1. Parallel: [load_customers, load_products]
2. Sequential: [clean_customers] (depends on load_customers)
3. Parallel: [dim_customer, dim_product]
4. Sequential: [fact_sales] (depends on dim_customer, dim_product)
```

---

### **Phase 4: Execution** (Bulk of time)

```mermaid
graph TD
    A[Start Node] --> B{Has<br/>Contracts?}
    B -->|Yes| C[Check Contracts]
    B -->|No| D[Read Data]
    C -->|Fail| Z[❌ Stop Pipeline]
    C -->|Pass| D
    
    D --> E[Apply Transformations]
    E --> F[Register in Context]
    F --> G{Has<br/>Validation?}
    G -->|No| H[Write Output]
    G -->|Yes| I[Run Tests]
    
    I -->|Pass| H
    I -->|Warn| J[Log Warning]
    I -->|Fail + mode: fail| Z
    I -->|Fail + on_fail: quarantine| K[Split Data]
    
    J --> H
    K --> L[Write Good Records]
    K --> M[Write Bad Records to Quarantine]
    L --> N[✅ Node Complete]
    M --> N
    H --> N
    
    style Z fill:#e63946,stroke:#264653,color:#fff
    style N fill:#06d6a0,stroke:#264653,color:#333
```

**Per-Node Flow:**
1. **Contracts** (input validation) → Fail fast if bad data
2. **Read** → Load from connection
3. **Transform** → SQL, Python functions, patterns
4. **Context** → Register DataFrame for downstream nodes
5. **Validation** (output tests) → Check quality
6. **Write** → Save to target

---

### **Phase 5: Observability** (< 5 seconds)

```mermaid
graph TD
    A[Pipeline Complete] --> B[Collect Lineage]
    B --> C[Profile Data]
    C --> D[Log Validations]
    D --> E[Generate HTML]
    E --> F[Write Story]
    F --> G[Update State]
    G --> H[📖 Story Ready]
    
    style H fill:#06d6a0,stroke:#264653,color:#333
```

**Story Generation:**
- **Lineage:** DAG visualization (Mermaid)
- **Profile:** Row counts, schema, sample data
- **Validations:** Test results, warnings, errors
- **Execution Log:** Timing, errors, stack traces
- **Explanations:** Business logic from YAML

**State Tracking:**
- Update high-water marks (HWM) for incremental loading
- Record run timestamp
- Track node success/failure

---

### **Phase 6: Notifications** (< 1 second)

```mermaid
graph LR
    A{Pipeline<br/>Status?} -->|Success| B[✅ on_success alerts]
    A -->|Failure| C[❌ on_failure alerts]
    A -->|Start| D[🚀 on_start alerts]
    
    B --> E[Send Slack/Email]
    C --> E
    D --> E
    E --> F[User Notified]
    
    style F fill:#06d6a0,stroke:#264653,color:#333
```

---

## Error Handling

### **Fail Fast (Default)**

```
Error in node "clean_customers" → Stop entire pipeline
```

**Use when:** Critical data (financial, compliance)

---

### **Continue on Error**

```yaml
nodes:
  - name: optional_enrichment
    on_error: continue  # Log error, continue pipeline
```

**Use when:** Nice-to-have transformations

---

### **Retry with Backoff**

```yaml
retry:
  enabled: true
  max_attempts: 3
  backoff: exponential  # 1s, 2s, 4s
```

**Use when:** Network blips, transient errors

---

## Performance Optimization

### **Parallel Execution**

Nodes with no dependencies run in parallel automatically:

```mermaid
graph TB
    Start --> A[load_customers]
    Start --> B[load_products]
    Start --> C[load_orders]
    
    A --> D[End]
    B --> D
    C --> D
    
    style Start fill:#2a9d8f,stroke:#264653,color:#fff
    style D fill:#06d6a0,stroke:#264653,color:#333
```

All 3 nodes run **simultaneously**.

---

### **Caching**

```yaml
nodes:
  - name: dim_customer
    cache: true  # Reused by multiple fact tables
```

DataFrame stays in memory for downstream nodes.

---

### **Incremental Loading**

Only process new data:

```
First run:  1,000,000 rows (5 minutes)
Second run: 1,000 rows (2 seconds)
```

See [Incremental Decision Tree](incremental_decision_tree.md).

---

## CLI Options

```bash
# Standard run
odibi run odibi.yaml

# Dry-run (validate without writing)
odibi run odibi.yaml --dry-run

# Run specific node only
odibi run odibi.yaml --node clean_customers

# Set environment
odibi run odibi.yaml --env production

```

---

## Debugging Tools

### **Pre-flight Checks**

```bash
# Validate YAML schema
odibi validate odibi.yaml

# Check environment health
odibi doctor

# Visualize DAG
odibi graph odibi.yaml
```

### **Post-mortem**

```bash
# View last story
odibi story last

# Check state
odibi catalog state odibi.yaml

# Read execution logs
cat .odibi/logs/odibi_20250111_143000.log
```

---

## Related

- [How to Read a Data Story](../guides/how_to_read_a_story.md) - Interpret execution results
- [Troubleshooting Guide](../troubleshooting.md) - Common errors
- [CLI Master Guide](../guides/cli_master_guide.md) - All CLI commands

---

[← Back to Visuals](README.md) | [Architecture](odibi_architecture.md) | [SCD2 Timeline](scd2_timeline.md)
