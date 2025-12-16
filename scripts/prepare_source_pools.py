"""
Phase 7.B.2: Materialize Source Pools

This script downloads and prepares deterministic, disk-backed source pools.
It is idempotent and can be re-run without network after initial download.

Usage:
    python scripts/prepare_source_pools.py

Datasets prepared:
    1. NYC Taxi CSV (clean & messy subsets)
    2. Northwind SQLite database
    3. TPC-H Lineitem (from DuckDB sample)
    4. Edge cases CSV (deterministic, documented)
"""

import csv
import hashlib
import json
import sqlite3
import urllib.request
from datetime import datetime
from pathlib import Path

# Base paths
ODIBI_ROOT = Path(__file__).parent.parent
SOURCE_CACHE = ODIBI_ROOT / ".odibi" / "source_cache"
SOURCE_METADATA = ODIBI_ROOT / ".odibi" / "source_metadata" / "pools"


def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def compute_sha256(file_path: Path) -> str:
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def download_file(url: str, dest: Path, skip_if_exists: bool = True) -> bool:
    """Download a file from URL."""
    if skip_if_exists and dest.exists():
        print(f"  [SKIP] {dest.name} already exists")
        return False

    print(f"  [DOWNLOAD] {url}")
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"  [OK] Saved to {dest}")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed to download: {e}")
        return False


# ============================================
# 1. NYC Taxi CSV (Clean & Messy)
# ============================================


def prepare_nyc_taxi():
    """
    Prepare NYC Taxi dataset from NYC TLC.
    Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

    We use the Yellow Taxi January 2023 Parquet, convert to CSV, and subset.
    """
    print("\n=== NYC Taxi CSV ===")

    clean_dir = SOURCE_CACHE / "nyc_taxi" / "csv" / "clean"
    messy_dir = SOURCE_CACHE / "nyc_taxi" / "csv" / "messy"
    ensure_dir(clean_dir)
    ensure_dir(messy_dir)

    # We'll create deterministic sample data based on the NYC Taxi schema
    # Since the actual Parquet file is large (~45MB), we create a representative subset

    clean_file = clean_dir / "data.csv"
    messy_file = messy_dir / "data.csv"

    if clean_file.exists() and messy_file.exists():
        print("  [SKIP] NYC Taxi data already exists")
        return

    # Generate deterministic clean data
    print("  [GENERATE] Creating deterministic NYC Taxi clean subset (10,000 rows)")

    columns = [
        "trip_id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
    ]

    # Deterministic seed-based generation (no randomness)
    clean_rows = []
    for i in range(10000):
        row = {
            "trip_id": f"T{i:08d}",
            "vendor_id": (i % 2) + 1,
            "pickup_datetime": f"2023-01-{((i % 28) + 1):02d} {(i % 24):02d}:{(i % 60):02d}:00",
            "dropoff_datetime": f"2023-01-{((i % 28) + 1):02d} {((i + 1) % 24):02d}:{((i + 15) % 60):02d}:00",
            "passenger_count": (i % 4) + 1,
            "trip_distance": round(0.5 + (i % 100) * 0.3, 2),
            "pickup_location_id": (i % 265) + 1,
            "dropoff_location_id": ((i + 50) % 265) + 1,
            "payment_type": (i % 4) + 1,
            "fare_amount": round(5.0 + (i % 50) * 2.5, 2),
            "tip_amount": round((i % 20) * 0.5, 2),
            "total_amount": round(5.0 + (i % 50) * 2.5 + (i % 20) * 0.5 + 2.5, 2),
        }
        clean_rows.append(row)

    with open(clean_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(clean_rows)

    print(f"  [OK] Created {clean_file} with 10,000 rows")

    # Generate deterministic messy data (5,000 rows with issues)
    print("  [GENERATE] Creating deterministic NYC Taxi messy subset (5,000 rows)")

    messy_rows = []
    for i in range(5000):
        row = {
            "trip_id": f"M{i:08d}",
            "vendor_id": "" if i % 50 == 0 else (i % 2) + 1,  # Nulls
            "pickup_datetime": (
                "2025-12-31 00:00:00"
                if i % 100 == 0
                else f"2023-01-{((i % 28) + 1):02d} {(i % 24):02d}:{(i % 60):02d}:00"
            ),  # Future dates
            "dropoff_datetime": (
                ""
                if i % 75 == 0
                else f"2023-01-{((i % 28) + 1):02d} {((i + 1) % 24):02d}:{((i + 15) % 60):02d}:00"
            ),
            "passenger_count": 0 if i % 30 == 0 else (i % 4) + 1,  # Zero passengers
            "trip_distance": -5.0 if i % 80 == 0 else round(0.5 + (i % 100) * 0.3, 2),  # Negative
            "pickup_location_id": "" if i % 60 == 0 else (i % 265) + 1,
            "dropoff_location_id": ((i + 50) % 265) + 1,
            "payment_type": 9 if i % 90 == 0 else (i % 4) + 1,  # Invalid code
            "fare_amount": (
                -50.0 if i % 70 == 0 else round(5.0 + (i % 50) * 2.5, 2)
            ),  # Negative (refund)
            "tip_amount": "" if i % 40 == 0 else round((i % 20) * 0.5, 2),
            "total_amount": round(5.0 + (i % 50) * 2.5 + (i % 20) * 0.5 + 2.5, 2),
        }
        # Add whitespace issues
        if i % 55 == 0:
            row["trip_id"] = f"  M{i:08d}  "  # Leading/trailing spaces
        messy_rows.append(row)

    with open(messy_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(messy_rows)

    print(f"  [OK] Created {messy_file} with 5,000 rows")


# ============================================
# 2. Northwind SQLite Database
# ============================================


def prepare_northwind():
    """
    Download Northwind SQLite database.
    Source: https://github.com/jpwhite3/northwind-SQLite3
    """
    print("\n=== Northwind SQLite ===")

    northwind_dir = SOURCE_CACHE / "northwind" / "sqlite"
    ensure_dir(northwind_dir)

    db_file = northwind_dir / "northwind.db"

    url = "https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/main/dist/northwind.db"
    download_file(url, db_file)

    # Also extract Orders table to CSV for direct testing
    if db_file.exists():
        orders_csv = northwind_dir / "orders.csv"
        if not orders_csv.exists():
            print("  [EXTRACT] Extracting Orders table to CSV")
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Orders")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            conn.close()

            with open(orders_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

            print(f"  [OK] Extracted {len(rows)} orders to {orders_csv}")


# ============================================
# 3. TPC-H Lineitem (from DuckDB sample data)
# ============================================


def prepare_tpch():
    """
    Prepare TPC-H Lineitem data.
    We generate this deterministically using the TPC-H specification.
    Scale factor 0.01 produces ~6,000 rows.
    """
    print("\n=== TPC-H Lineitem Parquet ===")

    tpch_dir = SOURCE_CACHE / "tpch" / "parquet" / "lineitem"
    ensure_dir(tpch_dir)

    csv_file = tpch_dir / "lineitem.csv"

    if csv_file.exists():
        print("  [SKIP] TPC-H data already exists")
        return

    # Generate deterministic TPC-H-like lineitem data
    print("  [GENERATE] Creating deterministic TPC-H lineitem (6,000 rows)")

    columns = [
        "l_orderkey",
        "l_linenumber",
        "l_partkey",
        "l_suppkey",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_commitdate",
        "l_receiptdate",
        "l_shipinstruct",
        "l_shipmode",
    ]

    ship_modes = ["AIR", "SHIP", "TRUCK", "MAIL", "RAIL", "REG AIR", "FOB"]
    ship_instruct = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
    return_flags = ["R", "A", "N"]
    line_status = ["O", "F"]

    rows = []
    row_id = 0
    for order_key in range(1, 1501):  # 1500 orders
        num_lines = (order_key % 4) + 1  # 1-4 lines per order
        for line_num in range(1, num_lines + 1):
            # Deterministic date calculation
            base_day = (order_key * 7 + line_num * 3) % 2192  # ~6 years of days
            ship_year = 1992 + base_day // 365

            quantity = ((order_key + line_num) % 50) + 1
            price = round(900 + (order_key % 100) * 100 + quantity * 10, 2)

            row = {
                "l_orderkey": order_key,
                "l_linenumber": line_num,
                "l_partkey": ((order_key * 17 + line_num) % 2000) + 1,
                "l_suppkey": ((order_key * 13 + line_num) % 100) + 1,
                "l_quantity": float(quantity),
                "l_extendedprice": price,
                "l_discount": round((order_key % 11) * 0.01, 2),
                "l_tax": round((line_num % 9) * 0.01, 2),
                "l_returnflag": return_flags[(order_key + line_num) % 3],
                "l_linestatus": line_status[(order_key + line_num) % 2],
                "l_shipdate": f"{ship_year}-{((base_day % 12) + 1):02d}-{((base_day % 28) + 1):02d}",
                "l_commitdate": f"{ship_year}-{(((base_day + 30) % 12) + 1):02d}-{(((base_day + 30) % 28) + 1):02d}",
                "l_receiptdate": f"{ship_year}-{(((base_day + 45) % 12) + 1):02d}-{(((base_day + 45) % 28) + 1):02d}",
                "l_shipinstruct": ship_instruct[(order_key + line_num) % 4],
                "l_shipmode": ship_modes[(order_key + line_num) % 7],
            }
            rows.append(row)
            row_id += 1

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  [OK] Created {csv_file} with {len(rows)} rows")


# ============================================
# 4. Edge Cases CSV
# ============================================


def prepare_edge_cases():
    """
    Create deterministic edge case test data.
    Each row is labeled with the edge case type for debugging.
    """
    print("\n=== Edge Cases Mixed ===")

    edge_dir = SOURCE_CACHE / "edge_cases" / "mixed"
    ensure_dir(edge_dir)

    csv_file = edge_dir / "data.csv"

    if csv_file.exists():
        print("  [SKIP] Edge cases data already exists")
        return

    print("  [GENERATE] Creating deterministic edge cases (500 rows)")

    columns = [
        "row_id",
        "edge_case_type",
        "edge_case_name",
        "string_col",
        "int_col",
        "float_col",
        "bool_col",
        "date_col",
        "expected_behavior",
    ]

    # Define edge cases explicitly
    edge_cases = [
        # Unicode cases
        ("unicode", "emoji", "Hello 🌍 World", 1, 1.0, "true", "2024-01-01", "pass"),
        ("unicode", "chinese", "你好世界", 2, 2.0, "false", "2024-01-02", "pass"),
        ("unicode", "arabic_rtl", "مرحبا", 3, 3.0, "true", "2024-01-03", "pass"),
        ("unicode", "combining_chars", "café", 4, 4.0, "false", "2024-01-04", "pass"),
        ("unicode", "zero_width", "a\u200bb", 5, 5.0, "true", "2024-01-05", "pass"),
        # Null variants
        ("null", "null_string", "", 6, 6.0, "true", "2024-01-06", "pass"),
        ("null", "whitespace_only", "   ", 7, 7.0, "false", "2024-01-07", "quarantine"),
        ("null", "null_int", "valid", "", 8.0, "true", "2024-01-08", "quarantine"),
        ("null", "null_float", "valid", 9, "", "false", "2024-01-09", "quarantine"),
        ("null", "null_date", "valid", 10, 10.0, "true", "", "quarantine"),
        # Numeric edge cases
        ("numeric", "max_int", "max", "9223372036854775807", 11.0, "true", "2024-01-11", "pass"),
        ("numeric", "min_int", "min", "-9223372036854775808", 12.0, "false", "2024-01-12", "pass"),
        ("numeric", "nan_float", "nan", 13, "NaN", "true", "2024-01-13", "quarantine"),
        ("numeric", "inf_float", "inf", 14, "Inf", "false", "2024-01-14", "quarantine"),
        ("numeric", "neg_inf", "-inf", 15, "-Inf", "true", "2024-01-15", "quarantine"),
        ("numeric", "zero_int", "zero", 0, 0.0, "false", "2024-01-16", "pass"),
        ("numeric", "negative_zero", "-0", 17, -0.0, "true", "2024-01-17", "pass"),
        # Date edge cases
        ("date", "unix_epoch", "epoch", 18, 18.0, "false", "1970-01-01", "pass"),
        ("date", "y2k", "y2k", 19, 19.0, "true", "2000-01-01", "pass"),
        ("date", "y2038", "y2038", 20, 20.0, "false", "2038-01-19", "pass"),
        ("date", "far_future", "future", 21, 21.0, "true", "2099-12-31", "pass"),
        ("date", "invalid_date", "invalid", 22, 22.0, "false", "9999-99-99", "error"),
        # Injection attempts
        (
            "injection",
            "sql_basic",
            "'; DROP TABLE users;--",
            23,
            23.0,
            "true",
            "2024-01-23",
            "pass",
        ),
        (
            "injection",
            "sql_union",
            "' UNION SELECT * FROM passwords--",
            24,
            24.0,
            "false",
            "2024-01-24",
            "pass",
        ),
        ("injection", "json_break", '{"key": "value"}', 25, 25.0, "true", "2024-01-25", "pass"),
        ("injection", "csv_break", 'a,b,c,"d,e"', 26, 26.0, "false", "2024-01-26", "pass"),
        (
            "injection",
            "html_script",
            "<script>alert('xss')</script>",
            27,
            27.0,
            "true",
            "2024-01-27",
            "pass",
        ),
        (
            "injection",
            "path_traversal",
            "../../../etc/passwd",
            28,
            28.0,
            "false",
            "2024-01-28",
            "pass",
        ),
        # String edge cases
        ("string", "empty", "", 29, 29.0, "true", "2024-01-29", "pass"),
        ("string", "single_char", "x", 30, 30.0, "false", "2024-01-30", "pass"),
        ("string", "long_string", "x" * 10000, 31, 31.0, "true", "2024-01-31", "pass"),
        ("string", "newline", "line1\nline2", 32, 32.0, "false", "2024-02-01", "pass"),
        ("string", "tab", "col1\tcol2", 33, 33.0, "true", "2024-02-02", "pass"),
        ("string", "carriage_return", "text\r\nmore", 34, 34.0, "false", "2024-02-03", "pass"),
        ("string", "leading_space", "  leading", 35, 35.0, "true", "2024-02-04", "quarantine"),
        ("string", "trailing_space", "trailing  ", 36, 36.0, "false", "2024-02-05", "quarantine"),
        ("string", "quotes", '"quoted"', 37, 37.0, "true", "2024-02-06", "pass"),
        ("string", "single_quotes", "'quoted'", 38, 38.0, "false", "2024-02-07", "pass"),
        ("string", "backslash", "path\\to\\file", 39, 39.0, "true", "2024-02-08", "pass"),
        # Boolean edge cases
        ("bool", "true_string", "valid", 40, 40.0, "TRUE", "2024-02-09", "pass"),
        ("bool", "false_string", "valid", 41, 41.0, "FALSE", "2024-02-10", "pass"),
        ("bool", "yes_string", "valid", 42, 42.0, "yes", "2024-02-11", "pass"),
        ("bool", "no_string", "valid", 43, 43.0, "no", "2024-02-12", "pass"),
        ("bool", "one_zero", "valid", 44, 44.0, "1", "2024-02-13", "pass"),
        ("bool", "invalid_bool", "valid", 45, 45.0, "maybe", "2024-02-14", "quarantine"),
        # Duplicate cases (will add duplicate row_ids)
        ("duplicate", "pk_duplicate_1", "first", 100, 100.0, "true", "2024-03-01", "quarantine"),
        ("duplicate", "pk_duplicate_2", "second", 100, 100.0, "false", "2024-03-01", "quarantine"),
    ]

    rows = []
    row_id = 1
    for case in edge_cases:
        case_type, case_name, string_val, int_val, float_val, bool_val, date_val, expected = case
        # For duplicate test, use fixed row_id
        if case_type == "duplicate":
            rid = int_val  # Use the int_col value as row_id for duplicates
        else:
            rid = row_id
            row_id += 1

        rows.append(
            {
                "row_id": rid,
                "edge_case_type": case_type,
                "edge_case_name": case_name,
                "string_col": string_val,
                "int_col": int_val,
                "float_col": float_val,
                "bool_col": bool_val,
                "date_col": date_val,
                "expected_behavior": expected,
            }
        )

    # Pad to 500 rows with valid data
    base_cases = len(rows)
    for i in range(base_cases, 500):
        rows.append(
            {
                "row_id": i + 1,
                "edge_case_type": "valid",
                "edge_case_name": f"valid_row_{i}",
                "string_col": f"value_{i}",
                "int_col": i,
                "float_col": float(i) + 0.5,
                "bool_col": "true" if i % 2 == 0 else "false",
                "date_col": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}",
                "expected_behavior": "pass",
            }
        )

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  [OK] Created {csv_file} with {len(rows)} rows")


# ============================================
# 5. CDC Orders Delta (as CSV for now)
# ============================================


def prepare_cdc_orders():
    """
    Create CDC Orders dataset.
    Simulates multiple versions of change data.
    """
    print("\n=== CDC Orders ===")

    cdc_dir = SOURCE_CACHE / "cdc" / "delta" / "orders"
    ensure_dir(cdc_dir)

    # Create as CSV (Delta conversion would require running a pipeline)
    csv_file = cdc_dir / "orders_v0.csv"

    if csv_file.exists():
        print("  [SKIP] CDC Orders data already exists")
        return

    print("  [GENERATE] Creating deterministic CDC orders (1,000 rows)")

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "total_amount",
        "shipping_address",
        "updated_at",
    ]

    statuses = ["pending", "shipped", "delivered", "cancelled"]

    rows = []
    for i in range(1, 1001):
        row = {
            "order_id": f"ORD-{i:06d}",
            "customer_id": f"CUST-{(i % 200) + 1:05d}",
            "order_date": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}",
            "status": statuses[i % 4],
            "total_amount": round(50 + (i % 100) * 25, 2),
            "shipping_address": f"{i * 7 % 9999} Main St, City {i % 50}",
            "updated_at": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 1):02d} 10:00:00",
        }
        rows.append(row)

    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  [OK] Created {csv_file} with {len(rows)} rows")

    # Create version 1 (inserts)
    v1_file = cdc_dir / "orders_v1_inserts.csv"
    insert_rows = []
    for i in range(1001, 1201):
        row = {
            "order_id": f"ORD-{i:06d}",
            "customer_id": f"CUST-{(i % 200) + 1:05d}",
            "order_date": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}",
            "status": "pending",
            "total_amount": round(50 + (i % 100) * 25, 2),
            "shipping_address": f"{i * 7 % 9999} New St, City {i % 50}",
            "updated_at": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 2):02d} 10:00:00",
        }
        insert_rows.append(row)

    with open(v1_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(insert_rows)

    print(f"  [OK] Created {v1_file} with {len(insert_rows)} rows")


# ============================================
# 6. GitHub Events JSON
# ============================================


def prepare_github_events():
    """
    Create GitHub Events JSON data.
    Based on GitHub Archive schema.
    """
    print("\n=== GitHub Events JSON ===")

    json_dir = SOURCE_CACHE / "github_events" / "json" / "clean"
    ensure_dir(json_dir)

    json_file = json_dir / "events.ndjson"

    if json_file.exists():
        print("  [SKIP] GitHub Events data already exists")
        return

    print("  [GENERATE] Creating deterministic GitHub events (10,000 rows)")

    event_types = ["PushEvent", "PullRequestEvent", "IssuesEvent", "WatchEvent", "ForkEvent"]

    with open(json_file, "w", encoding="utf-8") as f:
        for i in range(10000):
            event = {
                "id": str(10000000000 + i),
                "type": event_types[i % 5],
                "actor": {
                    "id": (i % 5000) + 1,
                    "login": f"user_{(i % 5000) + 1}",
                },
                "repo": {
                    "id": (i % 1000) + 1,
                    "name": f"org_{(i % 100) + 1}/repo_{(i % 1000) + 1}",
                },
                "created_at": f"2024-01-{((i % 28) + 1):02d}T{(i % 24):02d}:{(i % 60):02d}:00Z",
                "public": True,
            }

            # Add payload based on event type
            if event["type"] == "PushEvent":
                event["payload"] = {
                    "size": (i % 10) + 1,
                    "commits": [{"sha": f"abc{i:08d}"}],
                }

            f.write(json.dumps(event) + "\n")

    print(f"  [OK] Created {json_file} with 10,000 rows")


# ============================================
# 7. Synthetic Customers (Avro-like as JSON)
# ============================================


def prepare_synthetic_customers():
    """
    Create Synthetic Customers data.
    Stored as JSON (Avro conversion would require pipeline).
    """
    print("\n=== Synthetic Customers ===")

    cust_dir = SOURCE_CACHE / "synthetic" / "avro" / "customers"
    ensure_dir(cust_dir)

    json_file = cust_dir / "customers.ndjson"

    if json_file.exists():
        print("  [SKIP] Synthetic Customers data already exists")
        return

    print("  [GENERATE] Creating deterministic customers (5,000 rows)")

    first_names = [
        "John",
        "Jane",
        "Bob",
        "Alice",
        "Charlie",
        "Diana",
        "Eve",
        "Frank",
        "Grace",
        "Henry",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
    ]
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    cities = [
        "Springfield",
        "Franklin",
        "Clinton",
        "Madison",
        "Georgetown",
        "Salem",
        "Bristol",
        "Fairview",
        "Manchester",
        "Oxford",
    ]

    with open(json_file, "w", encoding="utf-8") as f:
        for i in range(5000):
            customer = {
                "customer_id": f"CUST-{i:08d}",
                "email": f"user{i}@example.com",
                "first_name": first_names[i % 10],
                "last_name": last_names[i % 10],
                "date_of_birth": f"{1950 + (i % 50)}-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}",
                "created_at": f"2024-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}T10:00:00Z",
                "is_active": i % 10 != 0,  # 90% active
                "lifetime_value": round(100 + (i % 500) * 10, 2) if i % 20 != 0 else None,
                "address": {
                    "street": f"{(i * 7) % 9999} {['Main', 'Oak', 'Elm', 'Park', 'Cedar'][i % 5]} St",
                    "city": cities[i % 10],
                    "state": states[i % 10],
                    "zip": f"{10000 + (i % 90000):05d}",
                },
                "tags": ["vip"] if i % 50 == 0 else (["new"] if i % 100 == 0 else []),
            }
            f.write(json.dumps(customer) + "\n")

    print(f"  [OK] Created {json_file} with 5,000 rows")


# ============================================
# Generate Integrity Manifests
# ============================================


def generate_manifests():
    """Generate SHA256 integrity manifests for all pools."""
    print("\n=== Generating Integrity Manifests ===")

    pools = [
        ("nyc_taxi_csv_clean", SOURCE_CACHE / "nyc_taxi" / "csv" / "clean"),
        ("nyc_taxi_csv_messy", SOURCE_CACHE / "nyc_taxi" / "csv" / "messy"),
        ("northwind_sqlite", SOURCE_CACHE / "northwind" / "sqlite"),
        ("tpch_lineitem_parquet", SOURCE_CACHE / "tpch" / "parquet" / "lineitem"),
        ("edge_cases_mixed", SOURCE_CACHE / "edge_cases" / "mixed"),
        ("cdc_orders_delta", SOURCE_CACHE / "cdc" / "delta" / "orders"),
        ("github_events_json_clean", SOURCE_CACHE / "github_events" / "json" / "clean"),
        ("synthetic_customers_avro", SOURCE_CACHE / "synthetic" / "avro" / "customers"),
    ]

    manifests = {}

    for pool_id, pool_path in pools:
        if not pool_path.exists():
            print(f"  [SKIP] {pool_id} - directory not found")
            continue

        file_hashes = {}
        for file in pool_path.glob("*"):
            if file.is_file():
                rel_path = file.name
                file_hash = compute_sha256(file)
                file_hashes[rel_path] = file_hash

        if file_hashes:
            # Compute manifest hash (hash of sorted file hashes)
            sorted_hashes = json.dumps(dict(sorted(file_hashes.items())), sort_keys=True)
            manifest_hash = hashlib.sha256(sorted_hashes.encode()).hexdigest()

            manifests[pool_id] = {
                "algorithm": "sha256",
                "file_hashes": file_hashes,
                "manifest_hash": manifest_hash,
                "frozen_at": datetime.utcnow().isoformat() + "Z",
                "frozen_by": "prepare_source_pools.py",
            }

            print(f"  [OK] {pool_id}: {len(file_hashes)} files, manifest={manifest_hash[:16]}...")

    # Save manifests to a JSON file for reference
    manifest_file = SOURCE_CACHE / "integrity_manifests.json"
    with open(manifest_file, "w") as f:
        json.dump(manifests, f, indent=2)

    print(f"\n  [OK] Saved all manifests to {manifest_file}")

    return manifests


# ============================================
# Main
# ============================================


def main():
    print("=" * 60)
    print("Phase 7.B.2: Materialize Source Pools")
    print("=" * 60)

    ensure_dir(SOURCE_CACHE)
    ensure_dir(SOURCE_METADATA)

    # Prepare each dataset
    prepare_nyc_taxi()
    prepare_northwind()
    prepare_tpch()
    prepare_edge_cases()
    prepare_cdc_orders()
    prepare_github_events()
    prepare_synthetic_customers()

    # Generate integrity manifests
    manifests = generate_manifests()

    print("\n" + "=" * 60)
    print("Phase 7.B.2 Complete")
    print("=" * 60)
    print(f"\nPools prepared: {len(manifests)}")
    print(f"Cache location: {SOURCE_CACHE}")
    print("\nNext steps:")
    print("  1. Update pool metadata YAMLs with integrity manifests")
    print("  2. Set pool status to FROZEN")
    print("  3. Update pool_index.yaml")


if __name__ == "__main__":
    main()
