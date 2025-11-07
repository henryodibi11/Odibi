# ODIBI Framework - Improvements & Technical Debt

**Purpose:** Track design improvements, technical debt, and production-readiness enhancements.

**Status:** MVP Complete - These are nice-to-haves for production hardening.

---

## Critical (Code Quality Issues)

### 0a. TransformConfig Steps Type Not Validated
**File:** `odibi/config.py` line 120  
**Issue:** Transform steps use `Dict[str, Any]` instead of `TransformStep` model.

**Current:**
```python
class TransformConfig(BaseModel):
    steps: List[Union[str, Dict[str, Any]]]  # ← No validation!
```

**Problem:**
- Invalid dict keys not caught by Pydantic
- No type checking on step structure
- We defined `TransformStep` but don't use it!

**Fix:**
```python
class TransformConfig(BaseModel):
    steps: List[Union[str, TransformStep]]  # ← Use the model!
```

**Impact:** High - Better validation, catches config errors  
**Effort:** Low - Change one type annotation  
**Breaking Change:** No - stricter validation (good thing)

---

### 0b. Transform Functions Can't Access Current DataFrame
**File:** `odibi/node.py` line 245  
**Issue:** `current_df` parameter passed to `_execute_function_step` but never used.

**Current:**
```python
def _execute_function_step(self, function_name, params, current_df):
    # current_df is ignored!
    func = FunctionRegistry.get(function_name)
    result = func(self.context, **params)
    return result
```

**Problem:**
- Transform functions can't operate on the current DataFrame in the pipeline
- Must always fetch from context by name
- Breaks functional composition pattern

**Example that doesn't work:**
```yaml
transform:
  steps:
    - "SELECT * FROM load"  # Returns DataFrame
    - function: double_values  # Can't access that DataFrame!
```

**Fix:** Pass current_df as special parameter
```python
def _execute_function_step(self, function_name, params, current_df):
    func = FunctionRegistry.get(function_name)
    sig = inspect.signature(func)

    if 'current' in sig.parameters:
        result = func(self.context, current=current_df, **params)
    else:
        result = func(self.context, **params)

    return result

# Usage:
@transform
def double_values(context, current):  # ← Gets current df
    return current * 2
```

**Impact:** High - Enables functional composition  
**Effort:** Medium - Signature inspection + tests  
**Breaking Change:** No - backward compatible

---

### 0c. NodeResult Could Use Pydantic
**File:** `odibi/node.py` line 14  
**Issue:** `NodeResult` uses `@dataclass` instead of Pydantic `BaseModel`.

**Current:**
```python
from dataclasses import dataclass

@dataclass
class NodeResult:
    node_name: str
    success: bool
    # ...
```

**Enhancement:**
```python
from pydantic import BaseModel, Field

class NodeResult(BaseModel):
    node_name: str
    success: bool
    duration: float
    rows_processed: Optional[int] = None
    schema: Optional[List[str]] = None
    error: Optional[Exception] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
```

**Benefits:**
- Consistent with rest of codebase
- Validation on result creation
- Better serialization (`.model_dump()`)

**Impact:** Low - Code consistency  
**Effort:** Low  
**Breaking Change:** No - compatible API

---

## High Priority (Production Readiness)

### 1. Connection Validation - Input vs Output Paths
**File:** `odibi/connections/local.py`  
**Issue:** `validate()` always creates directories, even for input paths that should exist.

**Current Behavior:**
```python
def validate(self) -> None:
    # Always creates directory
    self.base_path.mkdir(parents=True, exist_ok=True)
```

**Problem:**
- Typos in input paths create wrong directories instead of failing
- Input paths should ERROR if they don't exist
- Output paths should auto-create

**Recommended Fix:**
```python
class LocalConnection(BaseConnection):
    def __init__(self, base_path: str = "./data", mode: str = "rw"):
        """
        Args:
            base_path: Base directory path
            mode: "r" (read-only), "w" (write-only), or "rw" (read-write)
        """
        self.base_path = Path(base_path)
        self.mode = mode

    def validate(self) -> None:
        """Validate connection based on mode."""
        if "r" in self.mode:
            # For reading, path MUST exist
            if not self.base_path.exists():
                raise ConnectionError(
                    f"Input path not found: {self.base_path}",
                    suggestions=[
                        "Check the path in your connection config",
                        "Ensure data files are in the correct location"
                    ]
                )

        if "w" in self.mode:
            # For writing, create if needed
            self.base_path.mkdir(parents=True, exist_ok=True)
```

**Impact:** Medium - Catches config errors earlier  
**Effort:** Low - ~20 lines of code  
**Breaking Change:** Yes - would need to update connection configs to specify mode

---

### 2. Pydantic Field Name Shadowing
**File:** `odibi/config.py` (lines 40, 156)  
**Issue:** Fields named `validate` shadow Pydantic's built-in `validate()` method.

**Current:**
```python
class BaseConnectionConfig(BaseModel):
    validate: str = "lazy"  # ← Shadows BaseModel.validate()

class NodeConfig(BaseModel):
    validate: Optional[ValidationConfig] = None  # ← Shadows BaseModel.validate()
```

**Problem:**
- Naming collision with Pydantic's `validate()` method
- Causes warnings
- Could confuse users trying to call `config.validate()`

**Recommended Fix:**
```python
class BaseConnectionConfig(BaseModel):
    validation_mode: str = "lazy"  # or validate_on_load

class NodeConfig(BaseModel):
    validation: Optional[ValidationConfig] = None  # or validation_rules
```

**Impact:** Low - Just warnings, functionality works  
**Effort:** Low - Rename 2 fields + update tests  
**Breaking Change:** Yes - YAML configs would need field name updates

---

## Medium Priority (Enhanced Features)

### 3. Better Error Messages for Missing Dependencies
**File:** `odibi/graph.py`  
**Current:** Shows which dependency is missing but not suggestions.

**Enhancement:**
```python
if missing_deps:
    errors = []
    for node, dep in missing_deps:
        errors.append(f"Node '{node}' depends on '{dep}' which doesn't exist")

        # Suggest similar names (fuzzy matching)
        similar = [
            name for name in self.nodes.keys()
            if difflib.SequenceMatcher(None, dep, name).ratio() > 0.6
        ]
        if similar:
            errors.append(f"  Did you mean: {', '.join(similar)}?")

    raise DependencyError(
        f"Missing dependencies found:\n  " + "\n  ".join(errors)
    )
```

**Impact:** Medium - Better UX  
**Effort:** Low - Add fuzzy matching  
**Breaking Change:** No

---

### 4. SQL Execution Requires DuckDB
**File:** `odibi/engine/pandas_engine.py` line 135  
**Issue:** SQL execution fails if DuckDB not installed, with unclear error.

**Current:**
```python
try:
    import duckdb
    # ... execute SQL
except ImportError:
    raise TransformError(
        "SQL execution requires 'duckdb' or 'pandasql'. "
        "Install with: pip install duckdb"
    )
```

**Enhancement:**
- Check at pipeline load time, not execution time
- Add to project config: `sql_engine: "duckdb"`  
- Fail fast with clear install instructions

**Impact:** Medium - Better UX  
**Effort:** Low  
**Breaking Change:** No

---

### 5. Connection Config in YAML vs Python Objects
**File:** `odibi/pipeline.py` line 17  
**Current:** Connections passed as dict, not validated.

**Issue:**
```python
connections = {
    "local": LocalConnection(base_path="./data")  # ← Python object
}
```

But project.yaml has:
```yaml
connections:
  local:
    type: local
    base_path: ./data  # ← YAML config
```

**Problem:** No automatic conversion from YAML → Connection objects.

**Enhancement:** Connection factory/builder
```python
class ConnectionFactory:
    @staticmethod
    def from_config(name: str, config: Dict) -> BaseConnection:
        conn_type = config.get("type")
        if conn_type == "local":
            return LocalConnection(**config)
        elif conn_type == "azure_blob":
            return AzureBlobConnection(**config)
        # ...
```

**Impact:** High - Makes framework truly config-driven  
**Effort:** Medium  
**Breaking Change:** No

---

## Medium Priority (Enhanced Features) - Continued

### 11. Missing Data Formats
**File:** `odibi/engine/pandas_engine.py`  
**Issue:** Avro format not supported, mentioned in docs but not implemented.

**Current:** Only CSV, Parquet, JSON, Excel  
**Needed:** Avro (fastavro library)

**Enhancement:**
```python
elif format == "avro":
    try:
        from fastavro import reader
        # ... read avro file
    except ImportError:
        raise ValueError("Avro format requires 'fastavro'. Install with: pip install fastavro")
```

**Impact:** Medium - Enables Avro workflows  
**Effort:** Low - ~50 lines of code  
**Breaking Change:** No

---

### 12. Unpivot Operation Missing
**File:** `odibi/engine/pandas_engine.py` line 160  
**Issue:** Only `pivot` operation implemented, `unpivot` (melt) missing.

**Current:**
```python
def execute_operation(self, operation, params, df):
    if operation == "pivot":
        return self._pivot(df, params)
    else:
        raise ValueError(f"Unsupported operation: {operation}")
```

**Enhancement:** Add unpivot using `pd.melt()`
```python
elif operation == "unpivot":
    return self._unpivot(df, params)

def _unpivot(self, df, params):
    return df.melt(
        id_vars=params.get("id_vars", []),
        value_vars=params.get("value_vars"),
        var_name=params.get("var_name", "variable"),
        value_name=params.get("value_name", "value")
    )
```

**Impact:** Medium - Common data transformation  
**Effort:** Low - ~15 lines  
**Breaking Change:** No

---

### 13. Table Parameter Confusing for File-Based Engines
**File:** `odibi/engine/pandas_engine.py` line 22  
**Issue:** `table` and `path` parameters overlap for file-based connections.

**Current:**
```python
if path:
    full_path = connection.get_path(path)
elif table:
    full_path = connection.get_path(table)  # Same as path!
```

**Problem:**
- For LocalConnection, table and path do the same thing
- Confusing which to use
- Table is meant for SQL/Delta connections

**Enhancement:** Better documentation + validation
```python
def read(self, connection, format, table=None, path=None, options=None):
    """
    Note:
        For file-based connections (LocalConnection), use 'path'.
        For database connections (SQLConnection, DeltaConnection), use 'table'.

        Currently, both work the same for file-based connections.
    """
    # Could add warning if both provided
    if table and path:
        warnings.warn("Both 'table' and 'path' provided. Using 'path'.")
```

**Impact:** Low - Documentation clarity  
**Effort:** Low  
**Breaking Change:** No

---

## Low Priority (Nice to Have)

### 6. Config File Path Not Tracked
**File:** `odibi/pipeline.py` line 128  
**Issue:** Config file path not passed to Node, so error messages can't show file location.

**Current:**
```python
node = Node(
    config=node_config,
    context=self.context,
    engine=self.engine,
    connections=self.connections,
    config_file=None,  # TODO: track config file
)
```

**Problem:**
- Error messages can't show which YAML file caused the error
- Less helpful for debugging multi-file projects

**Fix:**
```python
class Pipeline:
    def __init__(self, pipeline_config, engine="pandas", connections=None, config_file=None):
        self.config = pipeline_config
        self.config_file = config_file  # ← Store it
        # ...

    def run(self):
        # ...
        node = Node(
            config=node_config,
            config_file=self.config_file,  # ← Pass it
            # ...
        )
```

**Impact:** Medium - Better error messages  
**Effort:** Low  
**Breaking Change:** No - optional parameter

---

### 7. Parallel Execution Not Implemented
**File:** `odibi/pipeline.py` line 97  
**Current:** `parallel` parameter ignored.

```python
def run(self, parallel: bool = False) -> PipelineResults:
    # parallel parameter is ignored
```

**Enhancement:** Use execution layers from graph
```python
if parallel:
    for layer in self.graph.get_execution_layers():
        # Use ThreadPoolExecutor or multiprocessing
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self._run_node, name) for name in layer]
            # Wait for all to complete
```

**Impact:** Medium - Performance for large pipelines  
**Effort:** High - Thread safety, error handling  
**Breaking Change:** No

---

### 7. Caching Not Implemented
**File:** `odibi/node.py` line 95  
**Current:** `cache: true` in config doesn't actually cache.

```python
if self.config.cache:
    self._cached_result = result_df  # ← Stored but never used
```

**Enhancement:** Check cache before execution
```python
if self.config.cache and self._cached_result is not None:
    return self._use_cached_result()
```

**Impact:** Medium - Performance for repeated runs  
**Effort:** Low  
**Breaking Change:** No

---

### 8. HTML Story Generator Enhancement
**File:** `odibi/story.py` (basic markdown implemented)  
**Status:** Basic markdown story works, needs HTML + metadata integration  
**Reference:** `d:/projects/Energy Efficiency/metadata/Step Explanations/` pattern

**Current:** Basic markdown story with data samples

**Enhancement Needed:** Professional HTML stories with rich metadata

---

#### Design: Separate Metadata Files (Energy Efficiency Pattern)

**File Structure:**
```
project/
├── pipelines/
│   └── my_pipeline.yaml              # ← Logic only (clean)
├── metadata/
│   └── my_pipeline_metadata.py       # ← Rich explanations
└── stories/
    └── my_pipeline_20251106.html     # ← Generated HTML
```

**Reference Implementation:**  
See `d:/projects/Energy Efficiency/metadata/Step Explanations/NKC/nkc_germ_dryer_1.py` for the pattern.

---

#### Metadata File Structure:

```python
# metadata/sales_pipeline_metadata.py
from textwrap import dedent

pipeline_name = "sales_etl"
pipeline_description = "Process sales data with revenue and category analysis"

node_metadata = {
    "load_sales": {
        "purpose": "Load raw sales transactions from CSV source",

        "details": dedent("""
            - Reads from local filesystem: data/sales.csv
            - Expected columns: id, date, product, category, quantity, price, customer_id
            - Data frequency: Transaction-level (multiple per day)
            - No transformations applied (raw load)
        """),

        "expected_schema": [
            ("id", "int", "Unique transaction ID"),
            ("date", "string", "Transaction date (YYYY-MM-DD)"),
            ("product", "string", "Product name"),
            ("category", "string", "Product category"),
            ("quantity", "int", "Units sold"),
            ("price", "float", "Unit price (USD)"),
            ("customer_id", "int", "Customer identifier"),
        ],

        "result": "Raw sales dataset ready for transformation",
    },

    "add_revenue": {
        "purpose": "Calculate total revenue for each transaction",

        # For complex formulas, use markdown tables (like Energy Efficiency)
        "formulas": dedent("""
            | **Column** | **Formula** | **Description** |
            |------------|-------------|-----------------|
            | `revenue` | `quantity × price` | Standard revenue calculation |
        """),

        "details": dedent("""
            - Multiplies quantity by unit price for each transaction
            - Handles decimal precision for currency (2 decimal places)
            - Validates no negative quantities or prices
            - Rounds to nearest cent
        """),

        "added_columns": [
            ("revenue", "float", "Total revenue per transaction (USD)"),
        ],

        "data_quality": {
            "validations": [
                "No negative values in revenue column",
                "Revenue matches (quantity × price) validation",
                "No null values in calculated column"
            ],
        },

        "result": "Sales dataset enriched with revenue column for financial analysis",
    },
}

# Example: Complex formula table (like dryer calculations)
# For transformation steps with 25+ derived columns:
complex_calculation_example = {
    "calc_dryer_metrics": {
        "purpose": "Calculate gas dryer efficiency and thermodynamic metrics",

        # Large formula table (markdown preserved in dedent)
        "formulas": dedent("""
            | **Column** | **Formula** |
            |------------|-------------|
            | `Product Rate Dry Basis` | `Product Rate × (1 - Final Product Moisture)` |
            | `Heat of Vaporization` | `-0.000003 × POWER(Temp, 3) + 0.0016 × POWER(Temp, 2) - 0.8587 × Temp + 1107` |
            | `Sensible Heat Product` | `Product Rate Dry Basis × Specific Heat × (Exhaust Temp - Feed Temp)` |
            | `Supply Rate Wet Cake` | `Product Rate Dry Basis ÷ (1 - Supply Moisture Content)` |
            | `Water Initial` | `Supply Moisture Content × Supply Rate Wet Cake` |
            | `Evaporated Water` | `Supply Rate Wet Cake - Product Rate` |
            | `Required Dry Air Flow` | `Evaporated Water ÷ (Humidity Ratio Outlet - Humidity Ratio Inlet)` |
            | `Air Energy Demand` | `Required Dry Air Flow × Sensible Heat Air × (Temp Diff)` |
            | `Latent Heat` | `Evaporative Capacity × Heat of Vaporization` |
            | `Minimum Energy Demand` | `Cake Heating + Latent Heat` |
            | `Target Efficiency` | `(Min Energy ÷ (Min Energy + Air Energy)) × 100` |
            | `Dryer Efficiency` | `(KPI at Max × Target) ÷ KPI at Actual` |
            # ... 15+ more formulas
        """),

        "details": dedent("""
            - Uses IAPWS-97 standard for steam properties
            - Applies psychrometric equations for humidity calculations
            - All polynomial fits empirically derived for product type
            - Conversion factors applied: Product Rate (2200 ton/hr → lb/hr)
        """),

        "result": "Complete thermodynamic analysis with 27 derived KPI columns",
    }
}
```

---

#### HTML Generation:

**Story generator converts markdown tables → HTML tables:**

```python
class HTMLStoryGenerator:
    def _html_node_section(self, node_name, node_result, metadata, context):
        """Generate HTML for node with metadata."""

        # Convert markdown formula table to HTML
        formulas_html = self._markdown_table_to_html(
            metadata.get('formulas', '')
        )

        return f"""
        <div class="node-card">
            <div class="node-header">
                <h3>✅ Node: {node_name}</h3>
                <span class="duration">{node_result.duration:.4f}s</span>
            </div>

            <div class="node-body">
                <div class="purpose">
                    <strong>Purpose:</strong> {metadata.get('purpose', '')}
                </div>

                {formulas_html}

                <div class="details">
                    <h4>Details:</h4>
                    {self._markdown_to_html(metadata.get('details', ''))}
                </div>

                {self._html_added_columns(metadata.get('added_columns', []))}

                {self._html_data_comparison(node_name, context)}

                {self._html_data_quality(metadata.get('data_quality', {}))}

                <div class="result">
                    <strong>Result:</strong> {metadata.get('result', '')}
                </div>
            </div>
        </div>
        """

    def _markdown_table_to_html(self, markdown_table: str) -> str:
        """Convert markdown table to styled HTML table."""
        if not markdown_table:
            return ""

        lines = [line.strip() for line in markdown_table.strip().split('\n') if line.strip()]

        if len(lines) < 3:  # Need header, separator, at least one row
            return ""

        # Parse header
        header = [cell.strip() for cell in lines[0].split('|')[1:-1]]

        # Parse rows (skip separator line)
        rows = []
        for line in lines[2:]:
            cells = [cell.strip() for cell in line.split('|')[1:-1]]
            rows.append(cells)

        # Generate HTML
        html = ['<div class="formulas">']
        html.append('<h4>Formulas (Exact Column Names):</h4>')
        html.append('<table class="formula-table">')

        # Header
        html.append('<thead><tr>')
        for h in header:
            html.append(f'<th>{h}</th>')
        html.append('</tr></thead>')

        # Body
        html.append('<tbody>')
        for row in rows:
            html.append('<tr>')
            for i, cell in enumerate(row):
                # First column is code (column name)
                if i == 0:
                    html.append(f'<td><code>{cell}</code></td>')
                else:
                    html.append(f'<td>{cell}</td>')
            html.append('</tr>')
        html.append('</tbody>')

        html.append('</table>')
        html.append('</div>')

        return '\n'.join(html)
```

---

#### HTML Output Example:

```html
<div class="node-card">
    <h3>✅ Node: calc_dryer_metrics</h3>

    <div class="purpose">
        <strong>Purpose:</strong> Calculate gas dryer efficiency and thermodynamic metrics
    </div>

    <div class="formulas">
        <h4>Formulas (Exact Column Names):</h4>
        <table class="formula-table">
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Formula</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><code>Product Rate Dry Basis</code></td>
                    <td><code>Product Rate × (1 - Final Product Moisture)</code></td>
                </tr>
                <tr>
                    <td><code>Heat of Vaporization</code></td>
                    <td><code>-0.000003 × POWER(Temp, 3) + 0.0016 × POWER(Temp, 2) - 0.8587 × Temp + 1107</code></td>
                </tr>
                <!-- 25+ more rows rendered nicely! -->
            </tbody>
        </table>
    </div>

    <div class="details">
        <h4>Details:</h4>
        <ul>
            <li>Uses IAPWS-97 standard for steam properties</li>
            <li>Applies psychrometric equations for humidity</li>
            <li>All polynomial fits empirically derived</li>
        </ul>
    </div>

    <div class="result">
        <strong>Result:</strong> Complete thermodynamic analysis with 27 derived KPI columns
    </div>
</div>
```

**With CSS styling:**
- Alternating row colors for readability
- Fixed header on scroll (for 25+ row tables)
- Syntax highlighting for formulas
- Collapsible sections (expand/collapse big tables)

---

#### CSS for Formula Tables:

```css
.formula-table {
    width: 100%;
    border-collapse: collapse;
    font-family: 'Courier New', monospace;
    font-size: 0.9em;
    margin: 1em 0;
}

.formula-table thead {
    background-color: #2c3e50;
    color: white;
    position: sticky;
    top: 0;
}

.formula-table th {
    padding: 12px;
    text-align: left;
    font-weight: bold;
}

.formula-table tbody tr:nth-child(even) {
    background-color: #f8f9fa;
}

.formula-table tbody tr:hover {
    background-color: #e9ecef;
}

.formula-table td {
    padding: 10px 12px;
    border-bottom: 1px solid #dee2e6;
}

.formula-table code {
    background: #f4f4f4;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 0.95em;
}

/* Collapsible formula sections for large tables */
.formulas.collapsed tbody {
    display: none;
}

.formulas h4 {
    cursor: pointer;
    user-select: none;
}

.formulas h4:hover {
    color: #007bff;
}
```

---

#### JavaScript for Interactivity:

```javascript
// Collapse/expand large formula tables
function toggleFormulas(nodeId) {
    const section = document.getElementById(`formulas-${nodeId}`);
    section.classList.toggle('collapsed');

    const icon = document.getElementById(`icon-${nodeId}`);
    icon.textContent = section.classList.contains('collapsed') ? '▶' : '▼';
}

// Sort tables by column
function sortTable(tableId, columnIndex) {
    const table = document.getElementById(tableId);
    const rows = Array.from(table.querySelectorAll('tbody tr'));

    rows.sort((a, b) => {
        const aVal = a.cells[columnIndex].textContent;
        const bVal = b.cells[columnIndex].textContent;
        return aVal.localeCompare(bVal);
    });

    const tbody = table.querySelector('tbody');
    rows.forEach(row => tbody.appendChild(row));
}
```

---

#### Integration with Pipeline:

```python
# When running pipeline
from odibi.pipeline import Pipeline

pipeline = Pipeline(
    pipeline_config,
    connections=connections,
    generate_story=True,
    story_config={
        "format": "html",  # or "markdown"
        "metadata_file": "metadata/my_pipeline_metadata.py",
        "max_sample_rows": 5,
        "output_path": "./stories/"
    }
)

results = pipeline.run()

# Story generated at:
print(results.story_path)
# stories/my_pipeline_20251106.html
```

---

#### For Complex Pipelines (Like Dryer Calculations):

**metadata/dryer_pipeline_metadata.py:**
```python
from textwrap import dedent

node_metadata = {
    "calc_dryer_efficiency": {
        "purpose": "Calculate gas dryer thermal efficiency and energy metrics",

        # Large formula table (25+ rows) - matches Energy Efficiency pattern
        "formulas": dedent("""
            | **Column** | **Formula** |
            |------------|-------------|
            | `Product Rate Dry Basis` | `Product Rate × (1 - Final Product Moisture)` |
            | `Heat of Vaporization` | `-0.000003 × POWER(Dryer Exhaust Temperature, 3) + 0.0016 × POWER(Dryer Exhaust Temperature, 2) - 0.8587 × Dryer Exhaust Temperature + 1107` |
            | `Sensible Heat Product` | `Product Rate Dry Basis × Specific Heat Capacity Product × (Dryer Exhaust Temperature - Feed Temperature)` |
            | `Supply Rate Wet Cake` | `Product Rate Dry Basis ÷ (1 - Supply Moisture Content)` |
            | `Water Initial` | `Supply Moisture Content × Supply Rate Wet Cake` |
            | `Evaporated Water` | `Supply Rate Wet Cake - Product Rate` |
            | `Required Dry Air Flow` | `Evaporated Water ÷ (Humidity Ratio Outlet - Humidity Ratio Inlet)` |
            | `Air Energy Demand` | `Required Dry Air Flow × Sensible Heat Air × (Dryer Exhaust Temperature - Inlet Air Temperature Dry Bulb)` |
            | `Evaporative Capacity` | `Supply Rate Wet Cake - Product Rate` |
            | `Latent Heat` | `Evaporative Capacity × Heat of Vaporization` |
            | `Sensible Heat Water` | `Water Initial × Specific Heat Capacity Water × (Dryer Exhaust Temperature - Feed Temperature)` |
            | `Cake Heating` | `Sensible Heat Product + Sensible Heat Water` |
            | `Minimum Energy Demand` | `Cake Heating + Latent Heat` |
            | `Target Efficiency` | `(Minimum Energy Demand ÷ (Minimum Energy Demand + Air Energy Demand)) × 100` |
            | `Steam Latent Heat` | `(-0.00000000000003 × POWER(Dryer Steam Pressure, 5)) + (0.0000000002 × POWER(Dryer Steam Pressure, 4)) - (0.0000007 × POWER(Dryer Steam Pressure, 3)) + (0.0009 × POWER(Dryer Steam Pressure, 2)) - (0.7143 × Dryer Steam Pressure) + 951.59` |
            | `Max Energy Demand to Steam` | `(Minimum Energy Demand + Air Energy Demand) ÷ Steam Latent Heat` |
            | `KPI at Max Efficiency` | `Max Energy Demand to Steam ÷ Product Rate` |
            | `KPI at Actual Efficiency` | `Dryer Steam Flow ÷ Product Rate` |
            | `Dryer Efficiency` | `(KPI at Max Efficiency × Target Efficiency) ÷ KPI at Actual Efficiency` |
            | `Steam Demand at Max Efficiency` | `KPI at Max Efficiency × Product Rate` |
            | `Efficiency Loss` | `((Dryer Efficiency - Target Efficiency) ÷ Target Efficiency) × 100` |
            | `Energy Loss` | `(Dryer Steam Flow - Steam Demand at Max Efficiency) × Dryer_Steam_h` |
            | `Energy Consumption` | `Dryer Steam Flow × Dryer_Steam_h` |
            | `Actual Economy Net` | `Evaporated Water ÷ Dryer Steam Flow` |
            | `Target Efficiency Net` | `((Evaporated Water × Heat of Vaporization) ÷ ((Evaporated Water ÷ Target Economy Net) × Steam Latent Heat)) × 100` |
            | `Actual Efficiency Net` | `(Latent Heat ÷ (Steam Latent Heat × Dryer Steam Flow)) × 100` |
            | `Gap Efficiency Net` | `Target Efficiency Net - Actual Efficiency Net` |
            | `Energy Loss Net` | `((Actual Efficiency Net - Target Efficiency Net) ÷ Target Efficiency Net) × Energy Consumption` |
        """),

        "details": dedent("""
            - Uses IAPWS-97 standard for saturated steam enthalpy
            - Applies psychrometric equations for air-moisture calculations
            - All polynomial fits are empirically derived for product type
            - Conversion factors: Product Rate (2200 ton/hr → lb/hr), Moisture (% → fraction)
        """),

        "added_columns": [
            ("Product Rate Dry Basis", "float", "Dry basis product rate (lb/hr)"),
            ("Heat of Vaporization", "float", "Latent heat of water (BTU/lb)"),
            ("Dryer Efficiency", "float", "Thermal efficiency (%)"),
            ("Energy Loss", "float", "Energy loss (BTU/hr)"),
            # ... 23 more columns
        ],

        "result": "Complete thermodynamic analysis with efficiency metrics for performance benchmarking",
    }
}
```

**HTML renders this as:**
- Expandable section: "Show/Hide Formulas (27 formulas)" ▼
- When expanded: Beautiful sortable table with 27 rows
- Syntax-highlighted formulas
- Easy to scan for stakeholders

---

#### Benefits:

1. **Clean Separation:**
   - Pipeline YAML = logic only
   - Metadata Python = rich documentation
   - HTML Story = combined output

2. **Stakeholder-Friendly:**
   - Professional appearance
   - Sortable/filterable tables
   - Formula tables properly formatted
   - Before/after data comparisons
   - Collapsible sections (hide complexity)

3. **Matches Your Workflow:**
   - Same pattern as Energy Efficiency project
   - Python files with `dedent()`
   - Markdown tables preserved
   - Easy to maintain

4. **Self-Contained:**
   - Single HTML file
   - Embedded CSS/JavaScript
   - Double-click to open in browser
   - Share with stakeholders easily

---

**Impact:** High - Professional documentation for stakeholders  
**Effort:** Medium - ~400 lines (HTML generator + metadata loader + CSS/JS)  
**Breaking Change:** No - markdown stories still work, HTML is enhancement  

**Implementation Priority:** High (after basic markdown story works)

**Reference Files:**
- `d:/projects/Energy Efficiency/metadata/Step Explanations/NKC/nkc_germ_dryer_1.py`
- `d:/projects/Energy Efficiency/metadata/STEP_EXPLANATION_TEMPLATE.md`

---

### 9. Retry Logic Not Implemented
**File:** `odibi/node.py`  
**Current:** Nodes fail once, no retries.

**Config exists:**
```yaml
defaults:
  retry:
    enabled: true
    max_attempts: 3
    backoff: exponential
```

But not used.

**Enhancement:** Wrap node execution in retry loop
```python
for attempt in range(max_attempts):
    try:
        return self.execute()
    except Exception as e:
        if attempt < max_attempts - 1:
            sleep(backoff_time)
            continue
        raise
```

**Impact:** Medium - Resilience  
**Effort:** Medium  
**Breaking Change:** No

---

### 10. No CLI Tools
**Files:** Not created yet  
**Current:** Must use Python API.

**Enhancement:** Create CLI
```bash
odibi run project.yaml
odibi validate project.yaml
odibi run-node my_node --project project.yaml
odibi graph --pipeline my_pipeline --visualize
```

**Impact:** High - Usability  
**Effort:** Medium  
**Breaking Change:** No

---

## Questions for Design Decisions

### Q1: LocalConnection Mode
**Should connections know if they're for input or output?**

**Option A:** Connection-level mode
```yaml
connections:
  input_data:
    type: local
    base_path: ./input
    mode: read  # ← Validate exists

  output_data:
    type: local
    base_path: ./output
    mode: write  # ← Create if missing
```

**Option B:** Infer from usage
- If used in `read:`, validate exists
- If used in `write:`, create if missing

**Decision:** TBD

---

### Q2: Breaking Changes
**Should we fix field name shadowing (breaking change) before v1.0?**

Current: In MVP phase, can still break things  
Future: After v1.0, must maintain compatibility

**Decision:** TBD

---

## Tracking

**Last Updated:** 2025-11-05  
**Review Schedule:** Before each release  
**Owner:** Framework maintainers

---

## How to Use This File

1. **Add items** as you discover them during code review or usage
2. **Prioritize** based on user impact and effort
3. **Reference in issues** when creating GitHub issues
4. **Check off** when implemented
5. **Archive** completed items to CHANGELOG.md

---

## Template for New Items

```markdown
### N. Short Title
**File:** path/to/file.py line X  
**Issue:** What's wrong or missing

**Current Behavior:**
[code example]

**Problem:**
- Why it's an issue
- Impact on users

**Recommended Fix:**
[code example]

**Impact:** High/Medium/Low  
**Effort:** High/Medium/Low  
**Breaking Change:** Yes/No
```
