# Skill 06 — Add a Connection

> **Layer:** Building
> **When:** Adding a new connection type to odibi (e.g., Snowflake, BigQuery, S3).
> **Prerequisite:** Complete Skill 01 and Skill 02.

---

## Overview

Connections are how odibi reads from and writes to external systems. Each connection type extends `BaseConnection` and registers itself in the factory.

**Key files:**
- `odibi/connections/base.py` — `BaseConnection` ABC
- `odibi/connections/local.py` — canonical exemplar (simplest full implementation)
- `odibi/connections/factory.py` — factory functions + registration
- `odibi/plugins.py` — `register_connection_factory()`

---

## Step-by-Step

### Step 1: Create the Connection Class

Create `odibi/connections/my_source.py`:

```python
"""My Source connection."""

from typing import Any, Dict, List, Optional

from odibi.connections.base import BaseConnection
from odibi.utils.logging_context import get_logging_context


class MySourceConnection(BaseConnection):
    """Connection to MySource."""

    def __init__(self, host: str, database: str, **kwargs):
        """Initialize connection.

        Args:
            host: Server hostname
            database: Database name
        """
        ctx = get_logging_context()
        ctx.log_connection(
            connection_type="my_source",
            connection_name="MySourceConnection",
            action="init",
            host=host,
            database=database,
        )
        self.host = host
        self.database = database
        # Store additional config
        self.port = kwargs.get("port", 5432)

    def get_path(self, relative_path: str) -> str:
        """Get fully qualified table reference.

        Args:
            relative_path: Table name or schema.table

        Returns:
            Fully qualified reference
        """
        if "." in relative_path:
            return relative_path
        return f"public.{relative_path}"

    def validate(self) -> None:
        """Validate connection is reachable.

        Raises:
            ConnectionError: If validation fails
        """
        ctx = get_logging_context()
        if not self.host:
            raise ConnectionError("MySource connection: 'host' is required")
        if not self.database:
            raise ConnectionError("MySource connection: 'database' is required")
        ctx.debug("MySource connection validated", host=self.host)
```

### Step 2: Implement Discovery API (Optional but Recommended)

Override the discovery methods from `BaseConnection`:

```python
    def discover_catalog(
        self,
        include_schema: bool = False,
        include_stats: bool = False,
        limit: int = 200,
        recursive: bool = True,
        path: str = "",
        pattern: str = "",
    ) -> Dict[str, Any]:
        """Discover available tables."""
        # Query information_schema or equivalent
        # Return CatalogSummary dict
        ...

    def get_schema(self, dataset: str) -> Dict[str, Any]:
        """Get column names and types for a table."""
        ...

    def profile(
        self, dataset: str, sample_rows: int = 1000, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Profile a table with statistics."""
        ...

    def preview(
        self, dataset: str, rows: int = 5, columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Preview sample rows."""
        ...

    def get_freshness(self, dataset: str, timestamp_column: Optional[str] = None) -> Dict[str, Any]:
        """Get data freshness info."""
        ...
```

### Step 3: Add Factory Function

Add to `odibi/connections/factory.py`:

```python
def create_my_source_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for MySourceConnection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="my_source", connection_name=name, action="create")

    from odibi.connections.my_source import MySourceConnection

    host = config.get("host")
    if not host:
        raise ValueError(
            f"Connection '{name}' missing 'host'. "
            f"Expected 'host' in config, got keys: {list(config.keys())}"
        )

    database = config.get("database")
    if not database:
        raise ValueError(
            f"Connection '{name}' missing 'database'. "
            f"Expected 'database' in config, got keys: {list(config.keys())}"
        )

    connection = MySourceConnection(host=host, database=database, **config)
    ctx.log_connection(
        connection_type="my_source", connection_name=name, action="created",
        host=host, database=database,
    )
    return connection
```

### Step 4: Register in Factory

In the `register_builtins()` function at the bottom of `factory.py`:

```python
def register_builtins():
    register_connection_factory("my_source", create_my_source_connection)
```

### Step 5: Write Tests

Create `tests/unit/connections/test_my_source_coverage.py`:

```python
import pytest
from unittest.mock import MagicMock, patch

from odibi.connections.my_source import MySourceConnection


class TestMySourceInit:
    def test_basic_init(self):
        conn = MySourceConnection(host="localhost", database="testdb")
        assert conn.host == "localhost"
        assert conn.database == "testdb"
        assert conn.port == 5432

    def test_custom_port(self):
        conn = MySourceConnection(host="localhost", database="testdb", port=3306)
        assert conn.port == 3306


class TestMySourceValidate:
    def test_missing_host(self):
        conn = MySourceConnection(host="", database="testdb")
        with pytest.raises(ConnectionError, match="host"):
            conn.validate()

    def test_missing_database(self):
        conn = MySourceConnection(host="localhost", database="")
        with pytest.raises(ConnectionError, match="database"):
            conn.validate()

    def test_valid_connection(self):
        conn = MySourceConnection(host="localhost", database="testdb")
        conn.validate()  # Should not raise


class TestMySourceGetPath:
    def test_simple_table(self):
        conn = MySourceConnection(host="localhost", database="testdb")
        assert conn.get_path("users") == "public.users"

    def test_qualified_table(self):
        conn = MySourceConnection(host="localhost", database="testdb")
        assert conn.get_path("myschema.users") == "myschema.users"
```

### Step 6: Verify

```bash
pytest tests/unit/connections/test_my_source_coverage.py -v

# Verify factory registration
python -c "from odibi.connections.factory import register_builtins; register_builtins(); print('OK')"
```

---

## Inherited Discovery API

`BaseConnection` provides these methods that raise `NotImplementedError` by default:

| Method | Purpose | Override When |
|--------|---------|---------------|
| `discover_catalog()` | List available datasets | Always recommended |
| `get_schema()` | Column names and types | Always recommended |
| `profile()` | Statistics (nulls, cardinality, min/max) | For AI-assisted discovery |
| `preview()` | Sample rows | For debugging |
| `get_freshness()` | Data freshness | For monitoring |

Also available for SQL connections:
| Method | Purpose |
|--------|---------|
| `quote_identifier()` | Dialect-specific quoting |
| `qualify_table()` | Build schema.table reference |
| `build_select_query()` | Build SELECT with WHERE/LIMIT |

---

## YAML Config for Users

After adding a connection, users configure it like this:

```yaml
connections:
  my_db:
    type: my_source
    host: server.example.com
    database: analytics
    port: 5432
```

---

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Not using deferred imports in factory | Import inside the factory function, not at module top |
| Missing validation error messages | Include field name and available keys in error text |
| Not logging connection events | Use `ctx.log_connection()` for init/create/validate |
| Forgetting `register_builtins()` registration | Connection won't be available from YAML |
| Storing secrets in plain text | Use `${ENV_VAR}` syntax — odibi expands environment variables |
| Not implementing `get_path()` | Base class requires it — used for path resolution |
