# Odibi in One Picture

> The complete Odibi architecture from YAML to execution.

---

## System Architecture

```mermaid
graph TB
    subgraph "User Interface Layer"
        YAML[📄 odibi.yaml<br/>Declarative Config]
        CLI[⌨️ odibi CLI<br/>run, validate, doctor]
        PythonAPI[🐍 Python API<br/>Pipeline objects]
    end

    subgraph "Configuration & Validation Layer"
        YAML --> Parser[YAML Parser<br/>+ Env Vars]
        CLI --> Parser
        PythonAPI --> Config
        Parser --> Config[Pydantic Models<br/>ProjectConfig, PipelineConfig, NodeConfig]
        Config --> Validator[Schema Validator<br/>Dependencies, Types, Connections]
    end

    subgraph "Planning & Orchestration Layer"
        Validator --> DAG[Dependency Graph<br/>Topological Sort]
        DAG --> Planner[Execution Planner<br/>Parallel/Sequential]
        Planner --> Executor[Pipeline Executor<br/>Context Manager]
    end

    subgraph "Execution Engine Layer"
        Executor --> EngineRouter{Engine<br/>Router}
        EngineRouter -->|engine: pandas| PandasEngine[Pandas Engine<br/>DuckDB SQL]
        EngineRouter -->|engine: polars| PolarsEngine[Polars Engine<br/>LazyFrame]
        EngineRouter -->|engine: spark| SparkEngine[Spark Engine<br/>Catalyst SQL]
    end

    subgraph "Data Layer - Bronze/Silver/Gold"
        PandasEngine --> Bronze[(🥉 Bronze<br/>Raw, Immutable<br/>Append-Only)]
        PolarsEngine --> Bronze
        SparkEngine --> Bronze
        
        Bronze --> Silver[(🥈 Silver<br/>Cleaned<br/>Deduplicated<br/>SCD2)]
        
        Silver --> Gold[(🥇 Gold<br/>Facts<br/>Dimensions<br/>Aggregations)]
    end

    subgraph "Quality & Observability Layer"
        Bronze -.Contracts.-> QualityEngine[Quality Engine]
        Silver -.Validation.-> QualityEngine
        Gold -.Validation.-> QualityEngine
        
        QualityEngine --> Gates[Quality Gates<br/>Pass/Warn/Fail]
        Gates -->|Fail| Quarantine[Quarantine<br/>Bad Records]
        Gates -->|Pass| Continue[Continue]
        
        Executor --> Story[Data Story<br/>HTML Report]
        Executor --> State[System Catalog<br/>State Tracking]
        Executor --> Lineage[OpenLineage<br/>Metadata]
        Executor --> Alerts[Alerts<br/>Slack/Email]
    end

    subgraph "Connections Layer"
        PandasEngine --> Connections
        PolarsEngine --> Connections
        SparkEngine --> Connections
        
        Connections[Connection Manager]
        Connections --> Local[📁 Local Files]
        Connections --> Azure[☁️ Azure Blob/ADLS]
        Connections --> SQL[🗄️ SQL Server]
        Connections --> API[🌐 HTTP/REST APIs]
        Connections --> Delta[△ Delta Lake]
    end

    style YAML fill:#2a9d8f,stroke:#264653,color:#fff
    style Config fill:#e76f51,stroke:#264653,color:#fff
    style DAG fill:#f4a261,stroke:#264653,color:#333
    style PandasEngine fill:#457b9d,stroke:#264653,color:#fff
    style PolarsEngine fill:#457b9d,stroke:#264653,color:#fff
    style SparkEngine fill:#457b9d,stroke:#264653,color:#fff
    style Bronze fill:#cd7f32,stroke:#264653,color:#fff
    style Silver fill:#c0c0c0,stroke:#264653,color:#333
    style Gold fill:#ffd700,stroke:#264653,color:#333
    style Story fill:#06d6a0,stroke:#264653,color:#333
    style Gates fill:#e63946,stroke:#264653,color:#fff
```

---

## Data Flow Example

Here's how a typical pipeline execution flows:

```mermaid
sequenceDiagram
    participant User
    participant CLI
    participant Validator
    participant Executor
    participant Engine
    participant Storage
    participant Story

    User->>CLI: odibi run odibi.yaml
    CLI->>Validator: Parse & Validate YAML
    Validator->>Validator: Check dependencies<br/>Validate connections<br/>Type check params
    Validator->>Executor: Build execution plan
    
    Executor->>Engine: Execute node: load_raw
    Engine->>Storage: Read CSV from Bronze
    Storage-->>Engine: DataFrame
    Engine-->>Executor: Register "load_raw" in context
    
    Executor->>Engine: Execute node: clean_data<br/>(depends_on: load_raw)
    Engine->>Engine: Get "load_raw" from context
    Engine->>Engine: Apply SQL transform
    Engine->>Engine: Run validations
    Engine->>Storage: Write Parquet to Silver
    Engine-->>Executor: Register "clean_data"
    
    Executor->>Story: Generate Data Story
    Story->>Story: Collect lineage<br/>Profile data<br/>Log validations
    Story-->>User: Open HTML report
    
    Executor-->>CLI: Success ✓
    CLI-->>User: Pipeline completed
```

---

## Engine Parity Principle

The same YAML config runs on **all three engines** with identical results:

```mermaid
graph LR
    YAML[odibi.yaml<br/>Single Config]
    
    YAML -->|engine: pandas| Dev[💻 Dev Laptop<br/>Pandas<br/>< 1GB data]
    YAML -->|engine: polars| Local[🖥️ Local Workstation<br/>Polars<br/>1-10GB data]
    YAML -->|engine: spark| Prod[☁️ Databricks<br/>Spark<br/>> 10GB data]
    
    Dev --> Output[Same Output<br/>Same Row Counts<br/>Same Schema]
    Local --> Output
    Prod --> Output
    
    style YAML fill:#2a9d8f,stroke:#264653,color:#fff
    style Dev fill:#457b9d,stroke:#264653,color:#fff
    style Local fill:#457b9d,stroke:#264653,color:#fff
    style Prod fill:#457b9d,stroke:#264653,color:#fff
    style Output fill:#06d6a0,stroke:#264653,color:#333
```

---

## Quality Layer Detail

```mermaid
graph TB
    Read[Read Data] --> Contract{Contracts<br/>Pass?}
    Contract -->|Fail| Abort[❌ Stop Pipeline<br/>Log Error]
    Contract -->|Pass| Transform[Transform Data]
    
    Transform --> Validate{Validation<br/>Tests Pass?}
    
    Validate -->|All Pass| Write[✅ Write to Target]
    Validate -->|Some Fail<br/>mode: warn| WriteWarn[⚠️ Write + Log Warnings]
    Validate -->|Some Fail<br/>on_fail: quarantine| Split[Split Data]
    
    Split --> Good[✅ Good Records<br/>→ Target]
    Split --> Bad[❌ Bad Records<br/>→ Quarantine]
    
    Validate -->|Fail<br/>mode: fail| AbortValidation[❌ Stop Pipeline]
    
    style Contract fill:#f4a261,stroke:#264653,color:#333
    style Validate fill:#e76f51,stroke:#264653,color:#fff
    style Write fill:#06d6a0,stroke:#264653,color:#333
    style Abort fill:#e63946,stroke:#264653,color:#fff
    style AbortValidation fill:#e63946,stroke:#264653,color:#fff
```

---

## Key Takeaways

### 1. **Three Layers**
- **Configuration Layer**: YAML → Pydantic models → validation
- **Execution Layer**: DAG → Engine → Storage
- **Observability Layer**: Story + State + Lineage + Alerts

### 2. **Engine Abstraction**
One config, three engines. Develop locally (Pandas), deploy to prod (Spark).

### 3. **Quality First**
Contracts check **inputs**. Validations check **outputs**. Gates decide **what happens on failure**.

### 4. **Medallion Pattern**
- **Bronze**: Raw truth (immutable)
- **Silver**: Cleaned context (SCD2, deduplication)
- **Gold**: Business insights (facts, aggregations)

### 5. **Observable by Default**
Every run generates a Data Story. No extra work required.

---

## Related

- [The Definitive Guide](../guides/the_definitive_guide.md) - Deep dive into each layer
- [Philosophy](../philosophy.md) - Design principles
- [Engine Parity Table](../reference/PARITY_TABLE.md) - Feature comparison

---

[← Back to Journeys](../journeys/README.md)
