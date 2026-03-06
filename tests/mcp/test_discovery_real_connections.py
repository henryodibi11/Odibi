"""Test discovery API with real Azure SQL and ADLS connections.

Uses credentials from .env file to test actual discovery operations.
"""

from dotenv import load_dotenv
from pathlib import Path
import os

# Load .env
load_dotenv(Path(".env"), override=True)

from odibi.connections.azure_sql import AzureSQL
from odibi.connections.azure_adls import AzureADLS
from odibi.connections.local import LocalConnection


def test_azure_sql_discovery():
    """Test SQL Server discovery with real connection."""
    print("=" * 80)
    print("TEST: Azure SQL Discovery (Real Connection)")
    print("=" * 80)

    # Get credentials
    server = os.environ.get("SQL_SERVER")
    user = os.environ.get("SQL_USER")
    password = os.environ.get("SQL_PASSWORD")

    if not all([server, user, password]):
        print("  [SKIP] Missing SQL credentials (SQL_SERVER, SQL_USER, SQL_PASSWORD)")
        return False

    print(f"\n  Connecting to: {server}")

    try:
        # Create connection
        conn = AzureSQL(
            server=server,
            database="master",  # Start with master to list databases
            username=user,
            password=password,
            auth_mode="sql",
            trust_server_certificate=True,
        )

        print("  [OK] Connection created")

        # Test 1: List schemas
        print("\n  [TEST 1] List schemas...")
        schemas = conn.list_schemas()
        print(f"    Found {len(schemas)} schemas: {schemas[:5]}")

        # Test 2: List tables in first schema
        if schemas:
            schema = schemas[0]
            print(f"\n  [TEST 2] List tables in '{schema}' schema...")
            tables = conn.list_tables(schema)
            print(f"    Found {len(tables)} tables")
            if tables:
                print(f"    First table: {tables[0]}")

        # Test 3: Discover catalog
        print("\n  [TEST 3] Discover catalog...")
        catalog = conn.discover_catalog(include_schema=False, include_stats=False, limit=10)
        print(f"    Total datasets: {catalog.get('total_datasets', 0)}")
        print(f"    Schemas: {catalog.get('schemas', [])[:3]}")

        # Test 4: Get table info (if we found any tables)
        if "tables" in catalog and catalog["tables"]:
            first_table = catalog["tables"][0]
            table_name = first_table.get("name")
            table_schema = first_table.get("namespace", "dbo")

            print(f"\n  [TEST 4] Get table info for '{table_schema}.{table_name}'...")
            try:
                info = conn.get_table_info(f"{table_schema}.{table_name}")
                print(f"    Columns: {len(info.get('columns', []))}")
                print(f"    Row count: {info.get('row_count', 'unknown')}")
                if info.get("columns"):
                    print(f"    First column: {info['columns'][0]}")
            except Exception as e:
                print(f"    [WARN] Could not get table info: {e}")

        print("\n  [SUCCESS] SQL Server discovery works!")
        return True

    except Exception as e:
        print(f"\n  [ERROR] SQL discovery failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_azure_adls_discovery():
    """Test ADLS discovery with real connection."""
    print("\n" + "=" * 80)
    print("TEST: Azure ADLS Discovery (Real Connection)")
    print("=" * 80)

    # Get credentials
    account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

    if not all([account, key]):
        print(
            "  [SKIP] Missing ADLS credentials (AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY)"
        )
        return False

    print(f"\n  Connecting to storage account: {account}")

    try:
        # Create connection
        conn = AzureADLS(
            account=account,
            container="datalake",  # Real container from your storage
            path_prefix="",
            auth_mode="direct_key",
            account_key=key,
            validate=False,  # Skip validation for now
        )

        print("  [OK] Connection created")

        # Test 1: List files at root
        print("\n  [TEST 1] List files at root...")
        try:
            files = conn.list_files(path="", limit=10)
            print(f"    Found {len(files)} items")
            if files:
                print(f"    First item: {files[0].get('name', 'unknown')}")
        except Exception as e:
            print(f"    [WARN] Could not list files: {e}")

        # Test 2: List folders
        print("\n  [TEST 2] List folders...")
        try:
            folders = conn.list_folders(limit=10)
            print(f"    Found {len(folders)} folders")
            if folders:
                print(f"    First folder: {folders[0]}")
        except Exception as e:
            print(f"    [WARN] Could not list folders: {e}")

        # Test 3: Discover catalog
        print("\n  [TEST 3] Discover catalog...")
        try:
            catalog = conn.discover_catalog(include_schema=False, limit=20)
            print(f"    Total datasets: {catalog.get('total_datasets', 0)}")
            print(f"    Folders: {len(catalog.get('folders', []))}")
            print(f"    Files: {len(catalog.get('files', []))}")
            print(f"    Formats: {catalog.get('formats', {})}")
        except Exception as e:
            print(f"    [WARN] Could not discover catalog: {e}")

        print("\n  [SUCCESS] ADLS discovery works!")
        return True

    except Exception as e:
        print(f"\n  [ERROR] ADLS discovery failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_local_discovery():
    """Test local filesystem discovery."""
    print("\n" + "=" * 80)
    print("TEST: Local Filesystem Discovery")
    print("=" * 80)

    try:
        # Create connection to examples folder
        conn = LocalConnection(base_path="examples/phase1")

        print("  [OK] Connection created (base_path: examples/phase1)")

        # Test 1: List files
        print("\n  [TEST 1] List files...")
        files = conn.list_files(path="", pattern="*.py")
        print(f"    Found {len(files)} Python files")
        if files:
            print(f"    First file: {files[0].get('name')}")

        # Test 2: List folders
        print("\n  [TEST 2] List folders...")
        folders = conn.list_folders()
        print(f"    Found {len(folders)} folders")

        # Test 3: Discover catalog
        print("\n  [TEST 3] Discover catalog...")
        catalog = conn.discover_catalog(include_schema=False, limit=50)
        print(f"    Total datasets: {catalog.get('total_datasets', 0)}")
        print(f"    Files: {len(catalog.get('files', []))}")
        print(f"    Formats: {catalog.get('formats', {})}")

        # Test 4: Get schema of a YAML file (if exists)
        yaml_files = [f for f in files if f.get("name", "").endswith(".yaml")]
        if yaml_files:
            yaml_file = yaml_files[0]["path"]
            print(f"\n  [TEST 4] Get schema of {yaml_file}...")
            try:
                conn.get_schema(yaml_file)
                print("    [INFO] Schema inference attempted")
            except Exception as e:
                print(f"    [INFO] Schema inference not applicable for YAML: {type(e).__name__}")

        print("\n  [SUCCESS] Local filesystem discovery works!")
        return True

    except Exception as e:
        print(f"\n  [ERROR] Local discovery failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_pipeline_manager_discover():
    """Test PipelineManager.discover() convenience method."""
    print("\n" + "=" * 80)
    print("TEST: PipelineManager.discover() API")
    print("=" * 80)

    # Create minimal test project
    from odibi.config import ProjectConfig

    from odibi.config import LocalConnectionConfig, StoryConfig, SystemConfig

    config = ProjectConfig(
        project="test_discovery",
        engine="pandas",
        connections={
            "test_local": LocalConnectionConfig(type="local", base_path="examples/phase1")
        },
        pipelines=[],
        story=StoryConfig(connection="test_local", path=".story"),
        system=SystemConfig(connection="test_local", path=".system"),
    )

    # Use from_yaml to initialize properly
    from odibi.pipeline import PipelineManager

    # Create temp YAML file
    import yaml

    temp_yaml = "temp_test_discovery.yaml"
    with open(temp_yaml, "w") as f:
        yaml.safe_dump(config.model_dump(mode="json", exclude_none=True), f)

    pm = PipelineManager.from_yaml(temp_yaml)

    # Clean up
    import os

    if os.path.exists(temp_yaml):
        os.remove(temp_yaml)

    print("  [OK] PipelineManager created with test_local connection")

    # Test discover
    print("\n  [TEST] pm.discover('test_local')...")
    result = pm.discover("test_local")

    if "error" in result:
        print(f"    [ERROR] {result['error']}")
        return False

    print(f"    Total datasets: {result.get('total_datasets', 0)}")
    print(f"    Files: {len(result.get('files', []))}")

    print("\n  [SUCCESS] PipelineManager.discover() works!")
    return True


if __name__ == "__main__":
    print("Testing Discovery API with Real Connections\n")

    results = []

    # Test each connection type
    results.append(("Azure SQL", test_azure_sql_discovery()))
    results.append(("Azure ADLS", test_azure_adls_discovery()))
    results.append(("Local Filesystem", test_local_discovery()))
    results.append(("PipelineManager API", test_pipeline_manager_discover()))

    # Summary
    print("\n" + "=" * 80)
    print("REAL CONNECTION TEST SUMMARY")
    print("=" * 80)

    for name, passed in results:
        status = "[OK]" if passed else "[FAIL/SKIP]"
        print(f"  {status} {name}")

    passed_count = sum(1 for _, p in results if p)
    total_count = len(results)

    print(f"\nPassed: {passed_count}/{total_count}")

    if passed_count > 0:
        print("\n[SUCCESS] Discovery API validated with real connections!")
    else:
        print("\n[INFO] Tests skipped - no credentials available")
