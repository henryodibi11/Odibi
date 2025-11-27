import importlib
import os
from typing import Optional

from odibi.pipeline import PipelineManager


def check_dependencies() -> bool:
    """Check installed dependencies."""
    print("Checking dependencies...")
    deps = [
        ("pandas", "Pandas"),
        ("duckdb", "DuckDB (Local SQL Engine)"),
        ("pyspark", "PySpark (Distributed Engine)"),
        ("fastapi", "FastAPI (UI Backend)"),
        ("uvicorn", "Uvicorn (UI Server)"),
        ("openlineage.client", "OpenLineage (Governance)"),
        ("azure.storage.blob", "Azure Blob Storage"),
        ("delta", "Delta Lake"),
    ]

    all_good = True
    for module, name in deps:
        try:
            mod = importlib.import_module(module)
            version = getattr(mod, "__version__", "installed")
            print(f"  [OK] {name}: {version}")
        except ImportError:
            # Define optional vs required
            optional = ["pyspark", "openlineage.client", "azure.storage.blob", "delta"]

            if module in optional:
                print(f"  [OPTIONAL] {name}: Not installed")
            else:
                print(f"  [MISSING] {name}")
                all_good = False

    return all_good


def check_config() -> Optional[PipelineManager]:
    """Check configuration file."""
    print("\nChecking configuration...")
    config_files = ["odibi.yaml", "project.yaml", "odibi.yml", "project.yml"]
    found_config = None
    for f in config_files:
        if os.path.exists(f):
            found_config = f
            break

    if not found_config:
        print("  [FAIL] No configuration file found (odibi.yaml or project.yaml)")
        return None

    print(f"  [OK] Found {found_config}")

    try:
        # We use PipelineManager to parse and validate
        manager = PipelineManager.from_yaml(found_config)
        print("  [OK] Configuration is valid")
        return manager
    except Exception as e:
        print(f"  [FAIL] Invalid configuration: {e}")
        return None


def check_connections(manager: PipelineManager) -> bool:
    """Check connections."""
    print("\nChecking connections...")
    if not manager:
        print("  [SKIP] Skipping connection checks (no config)")
        return False

    all_good = True
    for name, conn in manager.connections.items():
        try:
            conn.validate()
            print(f"  [OK] {name} ({conn.__class__.__name__})")
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            all_good = False

    return all_good


def check_system_catalog(manager: PipelineManager) -> bool:
    """Check system catalog connectivity and integrity."""
    print("\nChecking System Catalog...")

    if not manager or not manager.catalog_manager:
        print("  [SKIP] System Catalog not configured (Local Only Mode)")
        return True

    print(f"  [INFO] Catalog Path: {manager.catalog_manager.base_path}")

    all_good = True
    required_tables = [
        "meta_tables",
        "meta_runs",
        "meta_patterns",
        "meta_metrics",
        "meta_state",
        "meta_pipelines",
        "meta_nodes",
    ]

    for table in required_tables:
        path = manager.catalog_manager.tables.get(table)
        # Access internal method for check (acceptable for doctor/diagnostic tool)
        exists = manager.catalog_manager._table_exists(path)
        if exists:
            print(f"  [OK] {table}")
        else:
            print(f"  [FAIL] {table} not found at {path}")
            all_good = False

    return all_good


def doctor_command(args) -> int:
    """Run doctor command."""
    try:
        version = __import__("odibi").__version__
    except Exception:
        version = "unknown"

    print(f"Odibi Doctor (v{version})")
    print("=" * 40)

    deps_ok = check_dependencies()
    manager = check_config()
    conns_ok = check_connections(manager)

    catalog_ok = True
    if manager:
        catalog_ok = check_system_catalog(manager)

    if not deps_ok:
        print("\n[WARNING] Some required dependencies are missing.")
        return 1

    if not manager:
        print("\n[WARNING] Configuration issues found.")
        return 1

    if not conns_ok:
        print("\n[WARNING] Some connections failed validation.")
        return 1

    if not catalog_ok:
        print("\n[WARNING] System Catalog issues found.")
        return 1

    print("\n[SUCCESS] You are ready to run pipelines!")
    return 0


def add_doctor_parser(subparsers):
    """Add doctor parser."""
    parser = subparsers.add_parser("doctor", help="Check environment health")
    return parser
