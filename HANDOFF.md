# ODIBI Open-Source Preparation - Handoff Document

**Thread:** T-e73a4321-4a01-4841-b252-a121b8ae1911  
**Original Date:** 2025-11-06  
**Last Updated:** 2025-11-07  
**Status:** ✅ Phase 1 Complete (100%) - Ready for Phase 2

---

## 🎯 Objective

Prepare ODIBI framework to become an official open-source project with:
- ✅ Open-source governance files (LICENSE, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, CODEOWNERS, CHANGELOG)
- ✅ Scaffolded Spark engine and Azure connections (Phase 1 - stubs only, no full implementation)
- ✅ CI/CD and testing infrastructure
- ✅ Documentation for community onboarding
- ✅ Comprehensive walkthroughs with explanations
- ✅ **Critical constraint**: All 78 existing Pandas tests MUST still pass

---

## ✅ Phase 1 Complete - All Work Finished

### Summary
Phase 1 scaffolding and walkthroughs are 100% complete. The project is production-ready and prepared for Phase 2 (CLI & Testing Utilities).

**Completed Phases:**
- ✅ Phase 1A-E: Governance, scaffolding, CI/CD, docs, examples
- ✅ Phase 1F: Walkthroughs with comprehensive explanations and troubleshooting

**Release Tags:**
- `v1.1.0-alpha.1-ci-setup` - Initial scaffolding
- `v1.1.0-alpha.2-walkthroughs` - Walkthroughs complete

---

## 📋 Detailed Completion Log

### 1. Analysis
- Reviewed existing framework structure
- Confirmed v1.0.0 Pandas MVP is stable (78 passing tests)
- Consulted Oracle for open-source best practices
- Created 18-item TODO checklist

### 2. Package Configuration
- **Fixed** `pyproject.toml`:
  - Flattened `[all]` extras (was self-referencing, now lists dependencies directly)
  - Commented out CLI entry point (not implemented yet)
- **Added** `odibi/py.typed` for type information distribution

### 3. Governance Files
- ✅ **Created** `CONTRIBUTING.md` - Complete contributor guide
- ✅ **Created** `CODE_OF_CONDUCT.md` - Contributor Covenant v2.1
- ✅ **Created** `SECURITY.md` - Vulnerability reporting process
- ✅ **Created** `CODEOWNERS` - Maintainer assignments
- ✅ **Created** `CHANGELOG.md` - Version history tracking
- ✅ **Confirmed** `LICENSE` - MIT License (Henry Odibi 2025)

### 4. Code Scaffolding
- ✅ **Created** `odibi/engine/spark_engine.py` - Spark engine with import guards
- ✅ **Created** `odibi/connections/azure_adls.py` - Azure Data Lake connector
- ✅ **Created** `odibi/connections/azure_sql.py` - Azure SQL connector
- ✅ **Created** `odibi/connections/local_dbfs.py` - Mock DBFS for testing
- ✅ **Created** `tests/test_extras_imports.py` - Import guard tests
- ✅ **All 78 Pandas tests** still passing

### 5. CI/CD Infrastructure
- ✅ **Created** `.github/workflows/ci.yml` - Multi-Python CI pipeline
- ✅ **Created** `.pre-commit-config.yaml` - Code quality hooks
- ✅ **Created** GitHub issue templates (bug_report, feature_request)
- ✅ **Created** Pull request template

### 6. Documentation
- ✅ **Created** `docs/setup_databricks.md` - Databricks setup guide
- ✅ **Created** `docs/setup_azure.md` - Azure connection guide
- ✅ **Updated** `README.md` - Badges, installation, extras, contributing links

### 7. Examples
- ✅ **Created** `examples/example_spark.yaml` - Spark pipeline template
- ✅ **Updated** `examples/example_local.yaml` - Simplified Pandas example

### 8. Walkthroughs (Phase 1F - November 2025)
- ✅ **Created** 6 comprehensive Jupyter notebooks:
  - `00_setup_environment.ipynb` - Setup and ODIBI mental model
  - `01_local_pipeline_pandas.ipynb` - Full pipeline with explanations
  - `02_cli_and_testing.ipynb` - Testing patterns and CLI preview
  - `03_spark_preview_stub.ipynb` - Spark architecture overview
  - `04_ci_cd_and_precommit.ipynb` - Code quality automation
  - `05_build_new_pipeline.ipynb` - Build from scratch tutorial
- ✅ **Added** concept explanations (Config vs Runtime, SQL-over-Pandas)
- ✅ **Added** troubleshooting sections with common errors
- ✅ **Added** debugging guidance for pipeline failures
- ✅ **Fixed** API issues (connection instantiation, YAML parsing, encoding)
- ✅ **Tested** all notebooks execute cell-by-cell successfully

### 9. Release Management
- ✅ **Tagged** `v1.1.0-alpha.1-ci-setup` - Initial scaffolding complete
- ✅ **Tagged** `v1.1.0-alpha.2-walkthroughs` - Walkthroughs complete
- ✅ **Updated** `CHANGELOG.md` with all Phase 1 changes

---

## 🎯 Next Phase: Phase 2 - CLI Tools + Testing Utilities

Phase 1 is complete. Ready to begin Phase 2 development.

See PHASES.md for Phase 2 specifications and deliverables.

#### 1. CODE_OF_CONDUCT.md
```markdown
Location: /d:/odibi/CODE_OF_CONDUCT.md
Content: Contributor Covenant v2.1
Reference: https://www.contributor-covenant.org/version/2/1/code_of_conduct/
```

#### 2. SECURITY.md
```markdown
Location: /d:/odibi/SECURITY.md
Content:
- Supported versions (v1.0.0+)
- Reporting process (email to henry.odibi@ingredion.com or GitHub Security Advisories)
- Response timeline (72 hours acknowledgment)
```

#### 3. CODEOWNERS
```
Location: /d:/odibi/CODEOWNERS
Content:
* @henryodibi11
/odibi/engine/ @henryodibi11
/docs/ @henryodibi11
```

#### 4. CHANGELOG.md
```markdown
Location: /d:/odibi/CHANGELOG.md
Format: Keep a Changelog (https://keepachangelog.com/)
Content:
## [Unreleased]

## [1.1.0-alpha.1] - TBD

### Added
- Scaffolded Spark engine with import guards (Phase 1)
- Scaffolded Azure connections: ADLS, SQL, local DBFS mock
- Open-source governance: CONTRIBUTING, CODE_OF_CONDUCT, SECURITY
- CI/CD with multi-Python testing (3.9-3.12)
- Pre-commit hooks for code quality

### Changed
- Flattened optional dependencies in pyproject.toml

## [1.0.0] - 2025-11-05

### Added
- Initial release: Pandas MVP with 78 passing tests
- Core framework: Context, Registry, Graph, Pipeline
- Local filesystem connection
- Pydantic-based configuration
- Complete examples and documentation
```

---

### **PHASE B: Scaffolding (Critical - 2 hours)**

#### 5. Spark Engine
**File:** `/d:/odibi/odibi/engine/spark_engine.py`

```python
"""Spark execution engine (Phase 1: Scaffolding only).

Status: Experimental - Basic introspection implemented.
Full read/write/SQL implementation planned for Phase 3.
See PHASES.md lines 171-173 for roadmap.
"""

from typing import Any, Dict, List, Tuple, Optional
from .base import Engine


class SparkEngine(Engine):
    """Spark execution engine with PySpark backend.

    Phase 1 (Current): Import guards + introspection methods only
    Phase 3 (Planned): read(), write(), execute_sql() implementation
    """

    name = "spark"

    def __init__(self, session=None, config: Optional[Dict[str, Any]] = None):
        """Initialize Spark engine with import guard.

        Args:
            session: Existing SparkSession (optional, creates new if None)
            config: Engine configuration (optional)

        Raises:
            ImportError: If pyspark not installed
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError(
                "Spark support requires 'pip install odibi[spark]'. "
                "See docs/setup_databricks.md for setup instructions."
            ) from e

        from pyspark.sql import SparkSession
        self.spark = session or SparkSession.builder.appName("odibi").getOrCreate()
        self.config = config or {}

    def get_schema(self, df) -> List[Tuple[str, str]]:
        """Get DataFrame schema as list of (name, type) tuples."""
        return [(f.name, f.dataType.simpleString()) for f in df.schema]

    def get_shape(self, df) -> Tuple[int, int]:
        """Get DataFrame shape as (rows, columns)."""
        return (df.count(), len(df.columns))

    def count_rows(self, df) -> int:
        """Count rows in DataFrame."""
        return df.count()

    # Phase 3 stubs
    def read(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.read() will be implemented in Phase 3. "
            "See PHASES.md line 171 for implementation plan."
        )

    def write(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.write() will be implemented in Phase 3. "
            "See PHASES.md line 172 for implementation plan."
        )

    def execute_sql(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_sql() will be implemented in Phase 3. "
            "See PHASES.md line 171 for implementation plan."
        )

    def execute_transform(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_transform() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )
```

**Update:** `/d:/odibi/odibi/engine/__init__.py`
```python
# Add at end:
# Lazy import for Spark (optional dependency)
def get_spark_engine():
    from .spark_engine import SparkEngine
    return SparkEngine
```

---

#### 6. Azure ADLS Connection
**File:** `/d:/odibi/odibi/connections/azure_adls.py`

```python
"""Azure Data Lake Storage Gen2 connection (Phase 1: Path resolution only)."""

import posixpath
from typing import Optional
from .base import BaseConnection


class AzureADLS(BaseConnection):
    """Azure Data Lake Storage Gen2 connection.

    Phase 1: Path/URI resolution only (no network I/O)
    Phase 3: Read/write implementation with azure-storage-blob
    """

    def __init__(
        self,
        account: str,
        container: str,
        path_prefix: str = "",
        auth_mode: str = "managed_identity",
        **kwargs
    ):
        """Initialize ADLS connection.

        Args:
            account: Storage account name (e.g., 'mystorageaccount')
            container: Container/filesystem name
            path_prefix: Optional prefix for all paths
            auth_mode: Authentication mode (managed_identity, cli, sas)
        """
        self.account = account
        self.container = container
        self.path_prefix = path_prefix.strip("/")
        self.auth_mode = auth_mode

    def uri(self, path: str) -> str:
        """Build abfss:// URI for given path.

        Args:
            path: Relative path within container

        Returns:
            Full abfss:// URI

        Example:
            >>> conn = AzureADLS(account="myaccount", container="data")
            >>> conn.uri("folder/file.csv")
            'abfss://data@myaccount.dfs.core.windows.net/folder/file.csv'
        """
        if self.path_prefix:
            full_path = posixpath.join(self.path_prefix, path.lstrip("/"))
        else:
            full_path = path.lstrip("/")

        return f"abfss://{self.container}@{self.account}.dfs.core.windows.net/{full_path}"
```

---

#### 7. Azure SQL Connection
**File:** `/d:/odibi/odibi/connections/azure_sql.py`

```python
"""Azure SQL Database connection (Phase 1: DSN builder only)."""

from typing import Optional
from .base import BaseConnection


class AzureSQL(BaseConnection):
    """Azure SQL Database connection.

    Phase 1: ODBC DSN string generation only
    Phase 3: Read/write via SQLAlchemy + pyodbc
    """

    def __init__(
        self,
        server: str,
        database: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ):
        """Initialize Azure SQL connection.

        Args:
            server: SQL server hostname (without 'tcp:' prefix)
            database: Database name
            driver: ODBC driver name
            username: SQL auth username (optional, uses Azure AD if None)
            password: SQL auth password (optional)
        """
        self.server = server
        self.database = database
        self.driver = driver
        self.username = username
        self.password = password

    def odbc_dsn(self) -> str:
        """Build ODBC connection string.

        Returns:
            ODBC DSN string

        Example:
            >>> conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
            >>> conn.odbc_dsn()
            'Driver={ODBC Driver 18 for SQL Server};Server=tcp:myserver...'
        """
        dsn = (
            f"Driver={{{self.driver}}};"
            f"Server=tcp:{self.server},1433;"
            f"Database={self.database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        if self.username and self.password:
            dsn += f"UID={self.username};PWD={self.password};"
        else:
            dsn += "Authentication=ActiveDirectoryMsi;"

        return dsn
```

---

#### 8. Local DBFS Mock
**File:** `/d:/odibi/odibi/connections/local_dbfs.py`

```python
"""Local DBFS mock for testing Databricks pipelines locally."""

import os
from pathlib import Path
from typing import Union
from .base import BaseConnection


class LocalDBFS(BaseConnection):
    """Mock DBFS connection for local development.

    Maps dbfs:/ paths to local filesystem for testing.
    Useful for developing Databricks pipelines locally.
    """

    def __init__(self, root: Union[str, Path] = ".dbfs"):
        """Initialize local DBFS mock.

        Args:
            root: Local directory to use as DBFS root (default: .dbfs)
        """
        self.root = Path(root).resolve()

    def resolve(self, path: str) -> str:
        """Resolve dbfs:/ path to local filesystem path.

        Args:
            path: DBFS path (e.g., 'dbfs:/FileStore/data.csv')

        Returns:
            Absolute local filesystem path

        Example:
            >>> conn = LocalDBFS(root="/tmp/dbfs")
            >>> conn.resolve("dbfs:/FileStore/data.csv")
            '/tmp/dbfs/FileStore/data.csv'
        """
        # Remove dbfs:/ prefix
        clean_path = path.replace("dbfs:/", "").lstrip("/")

        # Join with root
        local_path = self.root / clean_path

        return str(local_path)

    def ensure_dir(self, path: str) -> None:
        """Create parent directories for given path.

        Args:
            path: DBFS path
        """
        local_path = Path(self.resolve(path))
        local_path.parent.mkdir(parents=True, exist_ok=True)
```

**Update:** `/d:/odibi/odibi/connections/__init__.py`
```python
# Add at end:
from .azure_adls import AzureADLS
from .azure_sql import AzureSQL
from .local_dbfs import LocalDBFS

__all__ = ["BaseConnection", "LocalConnection", "AzureADLS", "AzureSQL", "LocalDBFS"]
```

---

### **PHASE C: Testing (Critical - 1 hour)**

#### 9. Extras Import Tests
**File:** `/d:/odibi/tests/test_extras_imports.py`

```python
"""Test optional dependency imports and guards."""

import pytest


@pytest.mark.extras
def test_spark_engine_import_without_pyspark(monkeypatch):
    """SparkEngine should raise helpful error when pyspark missing."""
    # Hide pyspark
    monkeypatch.setitem(__import__('sys').modules, 'pyspark', None)
    monkeypatch.setitem(__import__('sys').modules, 'pyspark.sql', None)

    from odibi.engine.spark_engine import SparkEngine

    with pytest.raises(ImportError, match="pip install odibi\\[spark\\]"):
        SparkEngine()


@pytest.mark.extras
@pytest.mark.skipif(
    not pytest.importorskip("pyspark", minversion="3.4"),
    reason="pyspark not installed"
)
def test_spark_engine_import_with_pyspark():
    """SparkEngine should initialize when pyspark is available."""
    from odibi.engine.spark_engine import SparkEngine

    engine = SparkEngine()
    assert engine.name == "spark"
    assert hasattr(engine, 'get_schema')
    assert hasattr(engine, 'get_shape')


@pytest.mark.extras
def test_spark_engine_methods_not_implemented():
    """SparkEngine stubs should raise NotImplementedError with helpful messages."""
    pytest.importorskip("pyspark")
    from odibi.engine.spark_engine import SparkEngine

    engine = SparkEngine()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.read()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.write()

    with pytest.raises(NotImplementedError, match="Phase 3"):
        engine.execute_sql()
```

---

#### 10. Connection Path Tests
**File:** `/d:/odibi/tests/test_connections_paths.py`

```python
"""Test connection path resolution (no network I/O)."""

import pytest
from odibi.connections.azure_adls import AzureADLS
from odibi.connections.azure_sql import AzureSQL
from odibi.connections.local_dbfs import LocalDBFS


def test_azure_adls_uri_basic():
    """Test ADLS URI generation."""
    conn = AzureADLS(account="myaccount", container="data")
    uri = conn.uri("folder/file.csv")

    assert uri == "abfss://data@myaccount.dfs.core.windows.net/folder/file.csv"


def test_azure_adls_uri_with_prefix():
    """Test ADLS URI with path prefix."""
    conn = AzureADLS(account="myaccount", container="data", path_prefix="raw/")
    uri = conn.uri("file.csv")

    assert uri == "abfss://data@myaccount.dfs.core.windows.net/raw/file.csv"


def test_azure_sql_dsn_with_managed_identity():
    """Test Azure SQL DSN with managed identity."""
    conn = AzureSQL(server="myserver.database.windows.net", database="mydb")
    dsn = conn.odbc_dsn()

    assert "Driver={ODBC Driver 18 for SQL Server}" in dsn
    assert "Server=tcp:myserver.database.windows.net,1433" in dsn
    assert "Database=mydb" in dsn
    assert "Authentication=ActiveDirectoryMsi" in dsn


def test_azure_sql_dsn_with_credentials():
    """Test Azure SQL DSN with SQL authentication."""
    conn = AzureSQL(
        server="myserver.database.windows.net",
        database="mydb",
        username="admin",
        password="secret"
    )
    dsn = conn.odbc_dsn()

    assert "UID=admin" in dsn
    assert "PWD=secret" in dsn
    assert "Authentication" not in dsn


def test_local_dbfs_resolve():
    """Test local DBFS path resolution."""
    conn = LocalDBFS(root="/tmp/test-dbfs")
    path = conn.resolve("dbfs:/FileStore/data.csv")

    assert path == "/tmp/test-dbfs/FileStore/data.csv"


def test_local_dbfs_resolve_no_prefix():
    """Test DBFS resolution handles missing dbfs:/ prefix."""
    conn = LocalDBFS(root="/tmp/test-dbfs")
    path = conn.resolve("FileStore/data.csv")

    assert path == "/tmp/test-dbfs/FileStore/data.csv"
```

**Update:** `/d:/odibi/pytest.ini`
```ini
# Add to existing markers section:
markers =
    extras: tests requiring optional dependencies (pyspark, azure)
    slow: slow tests (deselect with '-m "not slow"')
```

---

### **PHASE D: CI/CD (Critical - 30 min)**

#### 11. GitHub Actions CI
**File:** `/d:/odibi/.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test-base:
    name: Test Base (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.9", "3.10", "3.11", "3.12"]

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python ${{ matrix.python-version }}
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -e .[dev]

      - name: Run linting
        run: |
          black --check odibi/ tests/
          ruff check odibi/ tests/

      - name: Run base tests (Pandas only)
        run: |
          pytest -v --cov=odibi --cov-report=xml --cov-report=term-missing

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        if: matrix.python-version == '3.11'
        with:
          file: ./coverage.xml
          fail_ci_if_error: false

  test-extras:
    name: Test Extras (Spark/Azure)
    runs-on: ubuntu-latest
    continue-on-error: true

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install with extras
        run: |
          python -m pip install --upgrade pip
          pip install -e .[dev,spark,azure]

      - name: Run extras tests
        run: |
          pytest -v -k "extras or connections_paths"
```

---

#### 12. Pre-commit Config
**File:** `/d:/odibi/.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 24.1.1
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.15
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]

  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: end-of-file-fixer
      - id: trailing-whitespace
      - id: check-yaml
      - id: check-added-large-files
        args: ['--maxkb=1000']
```

---

### **PHASE E: Documentation (1 hour)**

#### 13. Databricks Setup Guide
**File:** `/d:/odibi/docs/setup_databricks.md`

```markdown
# Databricks & Spark Setup

Guide for using ODIBI with Databricks and Spark.

## Installation

### On Databricks Cluster

```python
# Install from PyPI (when published)
%pip install odibi[spark]

# Or install from GitHub
%pip install git+https://github.com/henryodibi11/Odibi.git#egg=odibi[spark]
```

### Local Development

```bash
# Install with Spark support
pip install odibi[spark]
```

## Local Testing with Mock DBFS

Use `LocalDBFS` to test Databricks pipelines locally:

```yaml
# project.yaml
project: Local Databricks Test
engine: pandas  # Use pandas locally, spark on Databricks

connections:
  dbfs:
    type: local_dbfs
    root: ./.dbfs  # Local directory mimicking DBFS
```

```yaml
# pipelines/test.yaml
pipeline: local_test

nodes:
  - name: load_data
    read:
      connection: dbfs
      path: dbfs:/FileStore/input.csv  # Maps to ./.dbfs/FileStore/input.csv
      format: csv
```

## Cluster Configuration

**Recommended settings:**
- Databricks Runtime: 13.3 LTS or higher
- Spark version: 3.4+
- Python: 3.10 or 3.11

## Using Spark Engine

**Status:** Phase 1 - Basic introspection only. Full implementation in Phase 3.

```yaml
# project.yaml
project: Spark Pipeline
engine: spark  # Requires pyspark installed

connections:
  adls:
    type: azure_adls
    account: myaccount
    container: data
```

**Currently Available:**
- Schema inspection
- Row counting
- Shape queries

**Coming in Phase 3:**
- Read/write operations
- SQL execution
- Full transform support

## Authentication

Databricks clusters have Azure credentials pre-configured. No additional auth needed for ADLS when using managed identity.

## See Also

- [Azure Setup Guide](setup_azure.md)
- [PHASES.md](../PHASES.md) - Roadmap for Spark features
```

---

#### 14. Azure Setup Guide
**File:** `/d:/odibi/docs/setup_azure.md`

```markdown
# Azure Integration Setup

Guide for using ODIBI with Azure services.

## Installation

```bash
pip install odibi[azure]
```

## Azure Data Lake Storage Gen2 (ADLS)

### Configuration

```yaml
connections:
  adls:
    type: azure_adls
    account: mystorageaccount
    container: data
    path_prefix: raw/  # Optional
    auth_mode: managed_identity  # or cli, sas
```

### Authentication Modes

**1. Managed Identity (Recommended for Databricks/VMs)**
```yaml
auth_mode: managed_identity
```
No credentials needed - uses Azure Managed Identity.

**2. Azure CLI**
```yaml
auth_mode: cli
```
Uses credentials from `az login`.

**3. SAS Token**
```yaml
auth_mode: sas
sas_token: ${AZURE_SAS_TOKEN}  # From environment variable
```

### Permissions Required

**For read operations:**
- Storage Blob Data Reader

**For write operations:**
- Storage Blob Data Contributor

### Path Resolution

ADLS connection builds `abfss://` URIs:

```python
# Config:
# account: myaccount
# container: data
# path_prefix: raw/

# Path: "folder/file.csv"
# Resolves to: abfss://data@myaccount.dfs.core.windows.net/raw/folder/file.csv
```

## Azure SQL Database

### Configuration

```yaml
connections:
  azure_sql:
    type: azure_sql
    server: myserver.database.windows.net
    database: mydb
    driver: "ODBC Driver 18 for SQL Server"  # Optional
```

### Authentication

**Managed Identity (Recommended):**
```yaml
# No username/password - uses Azure AD identity
server: myserver.database.windows.net
database: mydb
```

**SQL Authentication:**
```yaml
server: myserver.database.windows.net
database: mydb
username: ${SQL_USER}
password: ${SQL_PASSWORD}
```

### Required Drivers

**Windows:**
- ODBC Driver 18 for SQL Server (pre-installed on recent Windows)

**Linux/macOS:**
```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18

# macOS
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

## Phase 1 Status

**Currently Available:**
- Path/URI resolution
- Connection validation
- DSN string generation

**Coming in Phase 3:**
- Actual read/write operations
- Integration with Spark/Pandas engines

## Security Best Practices

1. **Never commit credentials** - Use environment variables
2. **Use Managed Identity** when running on Azure
3. **Rotate SAS tokens** regularly
4. **Use least privilege** - grant minimum required permissions

## Troubleshooting

**Import error when using Azure connections:**
```bash
pip install odibi[azure]
```

**ODBC driver errors:**
- Verify driver installed: `odbcinst -q -d`
- Check driver name matches config

**Authentication failures:**
- Verify Azure AD permissions
- Test `az login` works
- Check managed identity assignment

## See Also

- [Databricks Setup](setup_databricks.md)
- [PHASES.md](../PHASES.md) - Azure feature roadmap
```

---

#### 15. Example Spark YAML
**File:** `/d:/odibi/examples/example_spark.yaml`

```yaml
# Example Spark pipeline configuration
# Phase 1: Template only - full execution in Phase 3

project: Spark Example Pipeline
engine: spark  # Requires: pip install odibi[spark]

connections:
  adls:
    type: azure_adls
    account: mystorageaccount
    container: data
    path_prefix: raw/
    auth_mode: managed_identity

  dbfs:
    type: local_dbfs
    root: ./.dbfs  # For local testing

pipelines:
  - name: spark_etl
    nodes:
      - name: load_sales
        # Phase 3: Will read from ADLS
        read:
          connection: adls
          path: sales/2024/*.parquet
          format: parquet
        cache: true

      - name: aggregate
        depends_on: [load_sales]
        # Phase 3: Will execute Spark SQL
        transform:
          steps:
            - |
              SELECT
                region,
                SUM(amount) as total_sales,
                COUNT(*) as transaction_count
              FROM load_sales
              GROUP BY region

      - name: write_results
        depends_on: [aggregate]
        # Phase 3: Will write to ADLS
        write:
          connection: adls
          path: aggregated/sales_by_region.parquet
          format: parquet
          mode: overwrite
```

---

#### 16. GitHub Issue Templates

**File:** `/d:/odibi/.github/ISSUE_TEMPLATE/bug_report.md`
```markdown
---
name: Bug Report
about: Report a bug in ODIBI
title: "[BUG] "
labels: bug
assignees: ''
---

## Bug Description
A clear description of the bug.

## Environment
- **ODIBI version:** (run `pip show odibi`)
- **Python version:** (run `python --version`)
- **OS:** Windows/Linux/macOS
- **Installed extras:** [spark/azure/none]

## Reproduction Steps
1. Create config...
2. Run command...
3. See error...

## Expected Behavior
What should happen.

## Actual Behavior
What actually happens.

## Error Message
```
Paste full error traceback here
```

## Minimal Config
```yaml
# Smallest config that reproduces the issue
```

## Additional Context
Any other relevant information.
```

**File:** `/d:/odibi/.github/ISSUE_TEMPLATE/feature_request.md`
```markdown
---
name: Feature Request
about: Suggest a feature for ODIBI
title: "[FEATURE] "
labels: enhancement
assignees: ''
---

## Problem
What problem does this solve?

## Proposed Solution
How should it work?

## Alternatives Considered
Other approaches you've thought about.

## Phase Alignment
Which phase does this fit into? (see PHASES.md)

## Additional Context
Mockups, examples, references.
```

**File:** `/d:/odibi/.github/PULL_REQUEST_TEMPLATE.md`
```markdown
## What
Brief description of changes.

## Why
Problem being solved or feature being added.

## How
Implementation approach.

## Testing
- [ ] All existing tests pass
- [ ] New tests added for new functionality
- [ ] Code formatted with Black
- [ ] Linting passes (Ruff)
- [ ] Documentation updated

## Phase
Which phase deliverable does this address? (see PHASES.md)

## Checklist
- [ ] CHANGELOG.md updated
- [ ] No decrease in test coverage
- [ ] Breaking changes documented (if any)
```

---

#### 17. Update README

**Changes needed in** `/d:/odibi/README.md`:

1. **Add badges after title:**
```markdown
# ODIBI Framework

[![CI Status](https://github.com/henryodibi11/Odibi/workflows/CI/badge.svg)](https://github.com/henryodibi11/Odibi/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
```

2. **Update Installation section** (after line 27):
```markdown
### Installation

```bash
# Basic installation (Pandas engine)
pip install odibi

# With Spark support
pip install odibi[spark]

# With Azure support
pip install odibi[azure]

# With all extras
pip install odibi[all]

# Development installation
git clone https://github.com/henryodibi11/Odibi.git
cd Odibi
pip install -e .[dev]
```
```

3. **Replace "Contributing" section** (around line 227):
```markdown
## Contributing

**ODIBI is open source and welcomes contributions!**

See [CONTRIBUTING.md](CONTRIBUTING.md) for:
- Development setup
- Coding standards
- Testing requirements
- Pull request process

Check [PHASES.md](PHASES.md) for current roadmap and available work.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.
```

4. **Update Roadmap section** (around line 220):
```markdown
### Roadmap

See [PHASES.md](PHASES.md) for detailed roadmap.

**Current:** Phase 1 - Spark & Azure scaffolding (v1.1.0-alpha.1)  
**Next:** Phase 2 - CLI tools & testing utilities (v1.2.0)

- ✅ v1.0.0: Pandas MVP (78 tests passing)
- 🚧 v1.1.0: Spark engine + Azure connections (scaffolding)
- ⏳ v1.2.0: CLI tools, testing utilities
- ⏳ v1.3.0: Stories, advanced connectors
- ⏳ v2.0.0: Community ecosystem, plugin system
```

---

## ✅ Success Criteria

Before considering Phase 1 complete:

1. **All 78 existing Pandas tests pass** (no regressions)
2. **New tests pass** (import guards, path resolution)
3. **CI is green** on test-base job (required)
4. **Spark/Azure clearly marked** as experimental in all docs
5. **No breaking changes** to existing Pandas pipelines
6. **Import guards work** - helpful errors when extras not installed
7. **Documentation complete** - CONTRIBUTING, setup guides, examples

---

## 🧪 Testing Instructions

```bash
# Run all base tests (must pass)
pytest -v

# Verify 78 tests still passing
pytest --co -q | grep "test session starts"

# Run with coverage
pytest --cov=odibi --cov-report=term-missing

# Test extras (if pyspark installed)
pytest -k "extras" -v

# Test connections
pytest tests/test_connections_paths.py -v
```

---

## 📦 File Creation Checklist

- [ ] CODE_OF_CONDUCT.md
- [ ] SECURITY.md
- [ ] CODEOWNERS
- [ ] CHANGELOG.md
- [ ] odibi/engine/spark_engine.py
- [ ] odibi/connections/azure_adls.py
- [ ] odibi/connections/azure_sql.py
- [ ] odibi/connections/local_dbfs.py
- [ ] tests/test_extras_imports.py
- [ ] tests/test_connections_paths.py
- [ ] .github/workflows/ci.yml
- [ ] .github/ISSUE_TEMPLATE/bug_report.md
- [ ] .github/ISSUE_TEMPLATE/feature_request.md
- [ ] .github/PULL_REQUEST_TEMPLATE.md
- [ ] .pre-commit-config.yaml
- [ ] docs/setup_databricks.md
- [ ] docs/setup_azure.md
- [ ] examples/example_spark.yaml
- [ ] Update README.md (4 sections)
- [ ] Update odibi/engine/__init__.py
- [ ] Update odibi/connections/__init__.py
- [ ] Update pytest.ini

---

## 🎯 Oracle Recommendations Summary

Key guidance from Oracle consultation:

1. **Keep Pandas untouched** - All new code is additive only
2. **Import guards everywhere** - No top-level imports of optional deps
3. **Stub with clear messages** - NotImplementedError pointing to PHASES.md
4. **No network I/O in Phase 1** - Path resolution and validation only
5. **Mark extras tests optional** - CI can fail on extras job
6. **Flatten pyproject.toml extras** - ✅ Already done
7. **Add py.typed** - ✅ Already done

---

## 📞 Next Thread Actions

**Recommended approach:**

1. **Start with governance** (PHASE A) - Quick wins, no code risk
2. **Then scaffolding** (PHASE B) - Core work, follow patterns exactly
3. **Add tests** (PHASE C) - Ensure quality gates
4. **Setup CI** (PHASE D) - Automation
5. **Finish docs** (PHASE E) - Community enablement
6. **Run full test suite** - Verify nothing broke
7. **Create summary** - Document changes for review

**Time estimate:** 4-6 hours for complete Phase 1 scaffolding.

---

**Files to reference:**
- `/d:/odibi/PHASES.md` - Phase 1 deliverables (lines 21-98)
- `/d:/odibi/CONTRIBUTING.md` - Coding standards (just created)
- `/d:/odibi/pyproject.toml` - Package config (already updated)
- `/d:/odibi/odibi/engine/base.py` - Engine interface to implement
- `/d:/odibi/odibi/connections/base.py` - Connection interface to implement

**Test to verify nothing broke:**
```bash
cd /d:/odibi
pytest -v
# Should show: 78 passed
```

---

**End of handoff. Good luck! 🚀**
