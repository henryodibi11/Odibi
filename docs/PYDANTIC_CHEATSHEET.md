# Pydantic Cheatsheet for ODIBI

**What is Pydantic?** A Python library that validates data using type hints and gives you clear error messages when data is wrong.

**Why we use it:** Our YAML configs get converted to Python objects. Pydantic ensures they're valid BEFORE execution.

---

## Basic Concepts

### 1. Models = Data Structures with Validation

Think of a Pydantic model like a strict dictionary with rules.

```python
from pydantic import BaseModel

# Define the structure
class Person(BaseModel):
    name: str          # Required field, must be string
    age: int           # Required field, must be integer
    email: str         # Required field, must be string

# Valid data
person = Person(name="Alice", age=30, email="alice@example.com")
print(person.name)  # "Alice"
print(person.age)   # 30

# Invalid data - Pydantic raises ValidationError
try:
    bad_person = Person(name="Bob", age="thirty")  # age should be int, not str
except ValidationError as e:
    print(e)  # Shows exactly what's wrong
```

**Output:**
```
1 validation error for Person
age
  Input should be a valid integer, unable to parse string as an integer
```

---

## 2. Field Types

### Required vs Optional

```python
from typing import Optional
from pydantic import BaseModel

class Config(BaseModel):
    # Required - MUST be provided
    name: str
    port: int
    
    # Optional - can be None
    description: Optional[str] = None
    timeout: Optional[int] = None
```

**Usage:**
```python
# Valid - only required fields
config1 = Config(name="server", port=8080)
print(config1.description)  # None

# Valid - with optional fields
config2 = Config(name="server", port=8080, description="Main server")
print(config2.description)  # "Main server"

# Invalid - missing required field
config3 = Config(name="server")  # ❌ Missing 'port'
```

---

### Default Values

```python
class ServerConfig(BaseModel):
    host: str = "localhost"      # Default: "localhost"
    port: int = 8080             # Default: 8080
    debug: bool = False          # Default: False
    max_connections: int = 100   # Default: 100
```

**Usage:**
```python
# Use all defaults
server1 = ServerConfig()
print(server1.host)  # "localhost"
print(server1.port)  # 8080

# Override some defaults
server2 = ServerConfig(port=9000, debug=True)
print(server2.port)  # 9000
print(server2.debug)  # True
print(server2.host)  # "localhost" (still default)
```

---

## 3. Field() - Advanced Configuration

`Field()` adds metadata and validation to fields.

```python
from pydantic import BaseModel, Field

class NodeConfig(BaseModel):
    name: str = Field(
        description="Unique node identifier"
    )
    
    retry_count: int = Field(
        default=3,
        ge=1,           # Greater than or equal to 1
        le=10,          # Less than or equal to 10
        description="Number of retry attempts"
    )
    
    cache: bool = Field(
        default=False,
        description="Cache node results"
    )
```

**Common Field parameters:**
- `default=value` - Default value
- `description="..."` - Human-readable description
- `ge=n` - Greater than or equal to n
- `le=n` - Less than or equal to n
- `gt=n` - Greater than n
- `lt=n` - Less than n
- `min_length=n` - Minimum string/list length
- `max_length=n` - Maximum string/list length
- `pattern="regex"` - String must match regex

**Example with validation:**
```python
class Person(BaseModel):
    name: str = Field(min_length=1, max_length=50)
    age: int = Field(ge=0, le=150)
    email: str = Field(pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")

# Valid
person = Person(name="Alice", age=30, email="alice@example.com")

# Invalid - age too high
person = Person(name="Bob", age=200, email="bob@example.com")
# ValidationError: age should be less than or equal to 150
```

---

## 4. Complex Types

### Lists

```python
from typing import List

class PipelineConfig(BaseModel):
    nodes: List[str]                    # List of strings
    tags: List[str] = []                # Optional list (default empty)
    priorities: List[int] = [1, 2, 3]  # List with default values
```

**Usage:**
```python
pipeline = PipelineConfig(
    nodes=["node1", "node2", "node3"]
)
print(pipeline.tags)  # []
```

---

### Dictionaries

```python
from typing import Dict, Any

class ProjectConfig(BaseModel):
    connections: Dict[str, Any]           # Dict with any values
    settings: Dict[str, str] = {}         # Dict with string values
    metadata: Dict[str, int] = {}         # Dict with int values
```

**Usage:**
```python
project = ProjectConfig(
    connections={
        "db1": {"type": "postgres", "host": "localhost"},
        "db2": {"type": "mysql", "port": 3306}
    }
)
print(project.connections["db1"]["type"])  # "postgres"
```

---

### Nested Models

```python
class ReadConfig(BaseModel):
    connection: str
    path: str

class NodeConfig(BaseModel):
    name: str
    read: ReadConfig  # Nested model

# Create nested structure
node = NodeConfig(
    name="load_data",
    read=ReadConfig(
        connection="local",
        path="data.csv"
    )
)

print(node.read.connection)  # "local"
```

Or use dict:
```python
node = NodeConfig(
    name="load_data",
    read={
        "connection": "local",
        "path": "data.csv"
    }
)
# Pydantic automatically converts dict to ReadConfig
```

---

### Optional Nested Models

```python
class NodeConfig(BaseModel):
    name: str
    read: Optional[ReadConfig] = None
    write: Optional[WriteConfig] = None

# Valid - no read or write
node1 = NodeConfig(name="transform_only")

# Valid - with read
node2 = NodeConfig(
    name="read_node",
    read=ReadConfig(connection="local", path="data.csv")
)
```

---

## 5. Enums - Limited Choices

```python
from enum import Enum

class EngineType(str, Enum):
    SPARK = "spark"
    PANDAS = "pandas"

class ProjectConfig(BaseModel):
    engine: EngineType = EngineType.PANDAS

# Valid
project = ProjectConfig(engine="pandas")
project = ProjectConfig(engine=EngineType.SPARK)

# Invalid
project = ProjectConfig(engine="dask")  # ❌ Not in enum
```

---

## 6. Validators - Custom Validation Logic

### Field Validator

Validates a single field.

```python
from pydantic import BaseModel, field_validator

class NodeConfig(BaseModel):
    name: str
    depends_on: List[str] = []
    
    @field_validator('name')
    @classmethod
    def name_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('name cannot be empty')
        return v.strip()  # Clean up whitespace

# Valid
node = NodeConfig(name="  my_node  ", depends_on=[])
print(node.name)  # "my_node" (whitespace removed)

# Invalid
node = NodeConfig(name="   ")  # ❌ name cannot be empty
```

---

### Model Validator

Validates across multiple fields.

```python
from pydantic import BaseModel, model_validator

class ReadConfig(BaseModel):
    connection: str
    table: Optional[str] = None
    path: Optional[str] = None
    
    @model_validator(mode='after')
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        if not self.table and not self.path:
            raise ValueError("Either 'table' or 'path' must be provided")
        return self

# Valid
config1 = ReadConfig(connection="db", table="users")
config2 = ReadConfig(connection="local", path="data.csv")

# Invalid
config3 = ReadConfig(connection="db")  # ❌ Need table OR path
```

**Modes:**
- `mode='before'` - Run before field validation
- `mode='after'` - Run after field validation (most common)

---

## 7. Union Types - Multiple Possible Types

```python
from typing import Union

class TransformStep(BaseModel):
    sql: Optional[str] = None
    function: Optional[str] = None
    
    @model_validator(mode='after')
    def check_exactly_one(self):
        if not any([self.sql, self.function]):
            raise ValueError("Must provide sql OR function")
        if self.sql and self.function:
            raise ValueError("Cannot provide both sql AND function")
        return self

# Valid
step1 = TransformStep(sql="SELECT * FROM table")
step2 = TransformStep(function="my_transform")

# Invalid
step3 = TransformStep()  # ❌ Need one
step4 = TransformStep(sql="...", function="...")  # ❌ Can't have both
```

---

## 8. Real ODIBI Examples

### Example 1: NodeConfig

```python
class NodeConfig(BaseModel):
    name: str = Field(description="Unique node name")
    description: Optional[str] = Field(default=None, description="Human-readable description")
    depends_on: List[str] = Field(default_factory=list, description="List of node dependencies")
    
    # Operations (at least one required)
    read: Optional[ReadConfig] = None
    transform: Optional[TransformConfig] = None
    write: Optional[WriteConfig] = None
    
    # Optional features
    cache: bool = Field(default=False, description="Cache result for reuse")
    
    @model_validator(mode='after')
    def check_at_least_one_operation(self):
        """Ensure at least one operation is defined."""
        if not any([self.read, self.transform, self.write]):
            raise ValueError(
                f"Node '{self.name}' must have at least one of: read, transform, write"
            )
        return self
```

**What this does:**
- `name` is required
- `description` is optional (defaults to None)
- `depends_on` is optional list (defaults to empty list)
- Must have at least one of: read, transform, write
- `cache` defaults to False

---

### Example 2: ReadConfig

```python
class ReadConfig(BaseModel):
    connection: str = Field(description="Connection name from project.yaml")
    format: str = Field(description="Data format (csv, parquet, delta, etc.)")
    table: Optional[str] = Field(default=None, description="Table name for SQL/Delta")
    path: Optional[str] = Field(default=None, description="Path for file-based sources")
    options: Dict[str, Any] = Field(default_factory=dict, description="Format-specific options")
    
    @model_validator(mode='after')
    def check_table_or_path(self):
        """Ensure either table or path is provided."""
        if not self.table and not self.path:
            raise ValueError("Either 'table' or 'path' must be provided for read config")
        return self
```

**What this does:**
- `connection` and `format` are required
- Must provide either `table` OR `path` (but not neither)
- `options` defaults to empty dict

---

## 9. Working with YAML

### Loading YAML → Pydantic

```python
import yaml
from pydantic import BaseModel

# YAML file content
yaml_content = """
name: load_data
description: Load CSV data
read:
  connection: local
  format: csv
  path: input.csv
cache: true
"""

# Parse YAML
data = yaml.safe_load(yaml_content)

# Validate with Pydantic
try:
    node = NodeConfig(**data)  # ** unpacks dict
    print(f"✅ Valid node: {node.name}")
except ValidationError as e:
    print(f"❌ Invalid config:")
    print(e)
```

---

## 10. Common Patterns in ODIBI

### Pattern 1: Required Field with Description

```python
name: str = Field(description="Node name")
```

### Pattern 2: Optional Field with Default

```python
cache: bool = Field(default=False, description="Cache results")
```

### Pattern 3: Optional Field (None by default)

```python
description: Optional[str] = Field(default=None, description="Human-readable description")
```

### Pattern 4: List with Default Empty

```python
depends_on: List[str] = Field(default_factory=list, description="Dependencies")
```

**Why `default_factory=list`?** 
- ✅ Each instance gets its own list
- ❌ `default=[]` would share the same list across instances (dangerous!)

### Pattern 5: Dict with Default Empty

```python
options: Dict[str, Any] = Field(default_factory=dict, description="Options")
```

---

## 11. Error Messages

### Example Error

```python
try:
    node = NodeConfig(
        name="",  # Empty name
        # Missing read/transform/write
    )
except ValidationError as e:
    print(e)
```

**Output:**
```
2 validation errors for NodeConfig
name
  Value error, name cannot be empty
NodeConfig
  Value error, Node '' must have at least one of: read, transform, write
```

**Understanding errors:**
- Shows which field failed
- Shows what went wrong
- Can have multiple errors at once

---

## 12. Accessing Fields

```python
node = NodeConfig(
    name="my_node",
    read=ReadConfig(connection="local", format="csv", path="data.csv")
)

# Access fields like normal attributes
print(node.name)              # "my_node"
print(node.read.connection)   # "local"
print(node.read.format)       # "csv"
print(node.cache)             # False (default)
print(node.depends_on)        # [] (default)

# Convert to dict
node_dict = node.model_dump()
print(node_dict)
# {
#   "name": "my_node",
#   "read": {"connection": "local", "format": "csv", "path": "data.csv", ...},
#   "cache": False,
#   ...
# }
```

---

## 13. Quick Reference

| Pattern | Example | Meaning |
|---------|---------|---------|
| `field: str` | `name: str` | Required string |
| `field: int` | `age: int` | Required integer |
| `field: bool` | `active: bool` | Required boolean |
| `field: Optional[str]` | `desc: Optional[str] = None` | Optional string (can be None) |
| `field: List[str]` | `tags: List[str]` | Required list of strings |
| `field: Dict[str, Any]` | `opts: Dict[str, Any]` | Required dictionary |
| `field: str = "default"` | `host: str = "localhost"` | String with default value |
| `field: List[str] = Field(default_factory=list)` | `deps: List[str] = Field(default_factory=list)` | Empty list by default |
| `field: str = Field(description="...")` | `name: str = Field(description="Node name")` | With description |
| `field: int = Field(ge=1, le=10)` | `retry: int = Field(ge=1, le=10)` | Integer between 1-10 |

---

## 14. Tips

1. **Always use `Optional[T]`** when field can be None
2. **Use `Field(default_factory=list)`** for lists, not `default=[]`
3. **Add descriptions** - helps with documentation and errors
4. **Use validators** for complex logic (either/or, dependencies)
5. **Use Enums** for limited choices (engine type, modes, etc.)
6. **Test with invalid data** - see what errors users will get

---

## 15. Debugging Validation Errors

```python
from pydantic import ValidationError

try:
    config = NodeConfig(**data)
except ValidationError as e:
    print("Validation failed!")
    
    # Get all errors
    for error in e.errors():
        print(f"Field: {error['loc']}")      # Which field
        print(f"Error: {error['msg']}")       # What's wrong
        print(f"Type: {error['type']}")       # Error type
        print(f"Input: {error['input']}")     # What was provided
        print("---")
```

---

## Summary

**Pydantic = Type hints + Validation + Clear errors**

In ODIBI, Pydantic:
1. Validates YAML configs before execution
2. Converts YAML → Python objects automatically
3. Gives clear error messages ("Missing field X", "Field Y must be integer")
4. Prevents runtime errors (catch config mistakes early)
5. Self-documents the config structure

**Next step:** Look at `odibi/config.py` with this cheatsheet open!
