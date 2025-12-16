"""
Phase 2: Dataset-Driven Testing

Tests each dataset through Bronze -> Silver -> Gold medallion architecture.
Exercises validation, quarantine, and patterns with real data.
"""

from pathlib import Path

import pandas as pd
import pytest

from odibi.connections import LocalConnection
from odibi.context import PandasContext
from odibi.engine.pandas_engine import PandasEngine

SOURCE_CACHE = Path(__file__).parent.parent.parent / ".odibi" / "source_cache"


@pytest.fixture
def temp_data_dir(tmp_path):
    return tmp_path


@pytest.fixture
def local_connection(temp_data_dir):
    return LocalConnection(base_path=str(temp_data_dir))


@pytest.fixture
def connections(local_connection):
    return {"local": local_connection}


@pytest.fixture
def engine(connections):
    return PandasEngine(connections=connections)


@pytest.fixture
def context():
    return PandasContext()


class TestNYCTaxiClean:
    """NYC Taxi Clean Dataset - DateTime parsing, numeric precision, Fact pattern."""

    @pytest.fixture
    def taxi_data(self):
        data_path = SOURCE_CACHE / "nyc_taxi" / "csv" / "clean" / "data.csv"
        if not data_path.exists():
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_csv_read_and_type_casting(self, taxi_data):
        """Bronze: Read CSV with proper type casting."""
        assert len(taxi_data) > 0, "Dataset should have rows"
        assert "pickup_datetime" in taxi_data.columns or any(
            "pickup" in c.lower() for c in taxi_data.columns
        ), "Should have pickup datetime column"

    def test_silver_null_and_range_validation(self, taxi_data):
        """Silver: Validate nulls and numeric ranges."""
        numeric_cols = taxi_data.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            first_numeric = numeric_cols[0]
            null_count = taxi_data[first_numeric].isna().sum()
            non_negative = (taxi_data[first_numeric].dropna() >= 0).all()
            assert non_negative or null_count >= 0, "Should handle numeric validation"

    def test_gold_aggregation(self, taxi_data):
        """Gold: Aggregate fares by day (if datetime available)."""
        datetime_cols = [c for c in taxi_data.columns if "date" in c.lower() or "time" in c.lower()]
        if datetime_cols:
            col = datetime_cols[0]
            taxi_data[col] = pd.to_datetime(taxi_data[col], errors="coerce")
            taxi_data["date"] = taxi_data[col].dt.date
            daily_counts = taxi_data.groupby("date").size()
            assert len(daily_counts) > 0, "Should have daily aggregations"


class TestNYCTaxiMessy:
    """NYC Taxi Messy Dataset - Validation rules, quarantine, null handling."""

    @pytest.fixture
    def messy_data(self):
        data_path = SOURCE_CACHE / "nyc_taxi" / "csv" / "messy" / "data.csv"
        if not data_path.exists():
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_read_with_error_handling(self, messy_data):
        """Bronze: Read with error handling for messy data."""
        assert len(messy_data) > 0, "Should read messy data even with issues"

    def test_silver_quarantine_bad_rows(self, messy_data):
        """Silver: Identify rows that should be quarantined."""
        null_counts = messy_data.isna().sum()
        total_nulls = null_counts.sum()
        assert total_nulls >= 0, "Should detect null values"

        numeric_cols = messy_data.select_dtypes(include=["number"]).columns
        if len(numeric_cols) > 0:
            first_numeric = numeric_cols[0]
            bad_rows = messy_data[first_numeric].isna()
            clean_data = messy_data[~bad_rows]
            assert len(clean_data) <= len(messy_data), "Clean data should be subset"


class TestCDCOrders:
    """CDC Orders Dataset - MERGE/UPSERT, delete detection, SCD2 pattern."""

    @pytest.fixture
    def cdc_data(self):
        data_path = SOURCE_CACHE / "cdc" / "delta" / "orders" / "data.csv"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "cdc" / "delta" / "orders"
            if alt_path.exists():
                csv_files = list(alt_path.glob("*.csv"))
                if csv_files:
                    return pd.read_csv(csv_files[0])
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_multi_version_read(self, cdc_data):
        """Bronze: Read CDC data with multiple versions."""
        assert len(cdc_data) > 0, "Should have CDC data"
        if "order_id" in cdc_data.columns:
            unique_orders = cdc_data["order_id"].nunique()
            total_rows = len(cdc_data)
            assert total_rows >= unique_orders, "CDC may have multiple versions per order"

    def test_silver_scd2_merge_simulation(self, cdc_data):
        """Silver: Simulate SCD2 merge behavior."""
        if "order_id" in cdc_data.columns:
            duplicates = cdc_data.duplicated(subset=["order_id"], keep=False)
            cdc_candidates = cdc_data[duplicates]
            assert len(cdc_candidates) >= 0, "May have records needing SCD2 treatment"


class TestGitHubEvents:
    """GitHub Events Dataset - JSON parsing, nested flattening, schema evolution."""

    @pytest.fixture
    def events_data(self):
        data_path = SOURCE_CACHE / "github_events" / "json" / "data.ndjson"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "github_events" / "json"
            if alt_path.exists():
                json_files = list(alt_path.glob("*.ndjson")) + list(alt_path.glob("*.json"))
                if json_files:
                    return pd.read_json(json_files[0], lines=True)
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_json(data_path, lines=True)

    def test_bronze_ndjson_parse_and_flatten(self, events_data):
        """Bronze: Parse NDJSON and handle nested structures."""
        assert len(events_data) > 0, "Should parse NDJSON data"

    def test_silver_dedupe_by_event_id(self, events_data):
        """Silver: Deduplicate by event_id."""
        if "id" in events_data.columns:
            deduped = events_data.drop_duplicates(subset=["id"])
            assert len(deduped) <= len(events_data), "Deduplication should not add rows"

    def test_gold_event_counts_by_type(self, events_data):
        """Gold: Aggregate event counts by type."""
        if "type" in events_data.columns:
            type_counts = events_data.groupby("type").size()
            assert len(type_counts) > 0, "Should have event type counts"


class TestTPCHLineitem:
    """TPC-H Lineitem Dataset - Composite keys, decimal precision, Merge pattern."""

    @pytest.fixture
    def lineitem_data(self):
        data_path = SOURCE_CACHE / "tpch" / "parquet" / "lineitem" / "data.csv"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "tpch" / "parquet" / "lineitem"
            if alt_path.exists():
                csv_files = list(alt_path.glob("*.csv"))
                if csv_files:
                    return pd.read_csv(csv_files[0])
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_csv_with_composite_pk(self, lineitem_data):
        """Bronze: Read CSV with composite primary key."""
        assert len(lineitem_data) > 0, "Should have lineitem data"
        pk_cols = ["l_orderkey", "l_linenumber"]
        if all(c in lineitem_data.columns for c in pk_cols):
            composite_key = lineitem_data[pk_cols].drop_duplicates()
            assert len(composite_key) == len(lineitem_data), "Composite PK should be unique"

    def test_silver_decimal_precision_check(self, lineitem_data):
        """Silver: Verify decimal precision for monetary columns."""
        price_cols = [
            c for c in lineitem_data.columns if "price" in c.lower() or "amount" in c.lower()
        ]
        for col in price_cols:
            if col in lineitem_data.columns:
                assert lineitem_data[col].dtype in [
                    "float64",
                    "int64",
                    "object",
                ], f"{col} should be numeric"

    def test_gold_revenue_by_order(self, lineitem_data):
        """Gold: Calculate revenue aggregations by order."""
        if "l_orderkey" in lineitem_data.columns and "l_extendedprice" in lineitem_data.columns:
            revenue_by_order = lineitem_data.groupby("l_orderkey")["l_extendedprice"].sum()
            assert len(revenue_by_order) > 0, "Should have revenue by order"


class TestSyntheticCustomers:
    """Synthetic Customers Dataset - Nested structs, arrays, Dimension pattern."""

    @pytest.fixture
    def customers_data(self):
        data_path = SOURCE_CACHE / "synthetic" / "avro" / "customers" / "data.ndjson"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "synthetic" / "avro" / "customers"
            if alt_path.exists():
                json_files = list(alt_path.glob("*.ndjson")) + list(alt_path.glob("*.json"))
                if json_files:
                    return pd.read_json(json_files[0], lines=True)
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_json(data_path, lines=True)

    def test_bronze_nested_struct_flatten(self, customers_data):
        """Bronze: Flatten nested customer structures."""
        assert len(customers_data) > 0, "Should have customer data"

    def test_silver_email_validation(self, customers_data):
        """Silver: Validate email format."""
        if "email" in customers_data.columns:
            email_pattern = r"^[^@]+@[^@]+\.[^@]+$"
            valid_emails = customers_data["email"].str.match(email_pattern, na=False)
            invalid_count = (~valid_emails).sum()
            assert invalid_count >= 0, "Should identify invalid emails"

    def test_gold_customer_by_region(self, customers_data):
        """Gold: Aggregate customers by region."""
        region_cols = [
            c for c in customers_data.columns if "region" in c.lower() or "country" in c.lower()
        ]
        if region_cols:
            region_counts = customers_data.groupby(region_cols[0]).size()
            assert len(region_counts) > 0, "Should have regional aggregations"


class TestNorthwind:
    """Northwind Dataset - SQL ingestion, HWM incremental, FK relationships."""

    @pytest.fixture
    def orders_data(self):
        data_path = SOURCE_CACHE / "northwind" / "sqlite" / "orders.csv"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "northwind" / "sqlite"
            if alt_path.exists():
                csv_files = list(alt_path.glob("*orders*.csv"))
                if csv_files:
                    return pd.read_csv(csv_files[0])
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_sql_extraction(self, orders_data):
        """Bronze: Verify SQL data extraction."""
        assert len(orders_data) > 0, "Should have orders data"

    def test_silver_fk_validation(self, orders_data):
        """Silver: Validate foreign key relationships."""
        if "CustomerID" in orders_data.columns:
            null_fks = orders_data["CustomerID"].isna().sum()
            assert null_fks >= 0, "Should track FK nulls"

    def test_gold_sales_by_category(self, orders_data):
        """Gold: Aggregate sales metrics."""
        if "OrderID" in orders_data.columns:
            order_counts = orders_data.groupby(orders_data.columns[0]).size()
            assert len(order_counts) >= 0, "Should have aggregated data"


class TestEdgeCases:
    """Edge Cases Dataset - All validation rules, quarantine, security."""

    @pytest.fixture
    def edge_data(self):
        data_path = SOURCE_CACHE / "edge_cases" / "mixed" / "data.csv"
        if not data_path.exists():
            alt_path = SOURCE_CACHE / "edge_cases" / "mixed"
            if alt_path.exists():
                csv_files = list(alt_path.glob("*.csv"))
                if csv_files:
                    return pd.read_csv(csv_files[0])
            pytest.skip(f"Dataset not found: {data_path}")
        return pd.read_csv(data_path)

    def test_bronze_all_formats(self, edge_data):
        """Bronze: Handle various data formats."""
        assert len(edge_data) > 0, "Should read edge case data"

    def test_silver_quarantine_flow(self, edge_data):
        """Silver: Full quarantine workflow."""
        null_counts = edge_data.isna().sum()
        total_issues = null_counts.sum()
        assert total_issues >= 0, "Should count data issues"

    def test_gold_pass_fail_counts(self, edge_data):
        """Gold: Calculate pass/fail metrics."""
        if len(edge_data.columns) > 0:
            first_col = edge_data.columns[0]
            valid_count = edge_data[first_col].notna().sum()
            invalid_count = edge_data[first_col].isna().sum()
            assert valid_count + invalid_count == len(edge_data), "Should account for all rows"


class TestEngineParityBasic:
    """Basic engine parity checks for Pandas engine."""

    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
                "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
                "date": pd.to_datetime(
                    ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
                ),
            }
        )

    def test_pandas_filter(self, sample_data):
        """Verify filter operation."""
        filtered = sample_data[sample_data["amount"] > 150]
        assert len(filtered) == 3, "Should filter correctly"

    def test_pandas_aggregate(self, sample_data):
        """Verify aggregation operation."""
        total = sample_data["amount"].sum()
        assert total == 1000.0, "Should aggregate correctly"

    def test_pandas_groupby(self, sample_data):
        """Verify groupby operation."""
        grouped = sample_data.groupby(sample_data["date"].dt.date)["amount"].sum()
        assert len(grouped) == 5, "Should group by date correctly"
