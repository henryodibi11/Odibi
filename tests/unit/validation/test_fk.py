"""Tests for Foreign Key Validation Module."""

import pytest
import pandas as pd

from odibi.validation.fk import (
    RelationshipConfig,
    RelationshipRegistry,
    OrphanRecord,
    FKValidationResult,
    FKValidationReport,
    FKValidator,
    get_orphan_records,
    validate_fk_on_load,
    parse_relationships_config,
)
from odibi.enums import EngineType


class MockEngineContext:
    """Mock EngineContext for FK validation tests."""

    def __init__(self, data: dict = None, engine_type: EngineType = EngineType.PANDAS):
        self._data = data or {}
        self.engine_type = engine_type

    def get(self, name: str):
        if name not in self._data:
            raise KeyError(f"Table '{name}' not found")
        return self._data[name]

    def set(self, name: str, df):
        self._data[name] = df


# =============================================================================
# RelationshipConfig Tests
# =============================================================================


class TestRelationshipConfig:
    """Tests for RelationshipConfig Pydantic model."""

    def test_valid_config(self):
        """Test creating a valid relationship config."""
        config = RelationshipConfig(
            name="orders_to_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        assert config.name == "orders_to_customers"
        assert config.fact == "fact_orders"
        assert config.dimension == "dim_customer"
        assert config.nullable is False
        assert config.on_violation == "error"

    def test_config_with_nullable(self):
        """Test config with nullable=True."""
        config = RelationshipConfig(
            name="orders_to_promo",
            fact="fact_orders",
            dimension="dim_promo",
            fact_key="promo_sk",
            dimension_key="promo_sk",
            nullable=True,
        )
        assert config.nullable is True

    def test_on_violation_values(self):
        """Test on_violation accepts valid values."""
        for action in ["warn", "error", "quarantine"]:
            config = RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
                on_violation=action,
            )
            assert config.on_violation == action

    def test_on_violation_case_insensitive(self):
        """Test on_violation is case-insensitive."""
        config = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="fk",
            dimension_key="dk",
            on_violation="WARN",
        )
        assert config.on_violation == "warn"

    def test_on_violation_invalid(self):
        """Test on_violation rejects invalid values."""
        with pytest.raises(ValueError, match="Invalid on_violation value"):
            RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
                on_violation="invalid",
            )

    def test_empty_name_rejected(self):
        """Test empty name is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
            )

    def test_whitespace_name_rejected(self):
        """Test whitespace-only name is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="   ",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
            )

    def test_empty_fact_rejected(self):
        """Test empty fact is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="test",
                fact="",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
            )

    def test_empty_dimension_rejected(self):
        """Test empty dimension is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="test",
                fact="fact",
                dimension="",
                fact_key="fk",
                dimension_key="dk",
            )

    def test_empty_fact_key_rejected(self):
        """Test empty fact_key is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="",
                dimension_key="dk",
            )

    def test_empty_dimension_key_rejected(self):
        """Test empty dimension_key is rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="",
            )

    def test_name_strips_whitespace(self):
        """Test names are stripped of whitespace."""
        config = RelationshipConfig(
            name="  test_rel  ",
            fact="  fact_table  ",
            dimension="  dim_table  ",
            fact_key="  fk  ",
            dimension_key="  dk  ",
        )
        assert config.name == "test_rel"
        assert config.fact == "fact_table"
        assert config.dimension == "dim_table"
        assert config.fact_key == "fk"
        assert config.dimension_key == "dk"


# =============================================================================
# RelationshipRegistry Tests
# =============================================================================


class TestRelationshipRegistry:
    """Tests for RelationshipRegistry."""

    @pytest.fixture
    def sample_relationships(self):
        """Create sample relationships."""
        return [
            RelationshipConfig(
                name="orders_to_customers",
                fact="fact_orders",
                dimension="dim_customer",
                fact_key="customer_sk",
                dimension_key="customer_sk",
            ),
            RelationshipConfig(
                name="orders_to_products",
                fact="fact_orders",
                dimension="dim_product",
                fact_key="product_sk",
                dimension_key="product_sk",
            ),
            RelationshipConfig(
                name="inventory_to_products",
                fact="fact_inventory",
                dimension="dim_product",
                fact_key="product_sk",
                dimension_key="product_sk",
            ),
        ]

    def test_empty_registry(self):
        """Test empty registry."""
        registry = RelationshipRegistry()
        assert registry.relationships == []

    def test_registry_with_relationships(self, sample_relationships):
        """Test registry with relationships."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        assert len(registry.relationships) == 3

    def test_get_relationship_found(self, sample_relationships):
        """Test get_relationship finds existing relationship."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rel = registry.get_relationship("orders_to_customers")
        assert rel is not None
        assert rel.name == "orders_to_customers"

    def test_get_relationship_case_insensitive(self, sample_relationships):
        """Test get_relationship is case-insensitive."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rel = registry.get_relationship("ORDERS_TO_CUSTOMERS")
        assert rel is not None
        assert rel.name == "orders_to_customers"

    def test_get_relationship_not_found(self, sample_relationships):
        """Test get_relationship returns None if not found."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rel = registry.get_relationship("nonexistent")
        assert rel is None

    def test_get_fact_relationships(self, sample_relationships):
        """Test get_fact_relationships returns all for a fact table."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rels = registry.get_fact_relationships("fact_orders")
        assert len(rels) == 2
        assert all(r.fact == "fact_orders" for r in rels)

    def test_get_fact_relationships_case_insensitive(self, sample_relationships):
        """Test get_fact_relationships is case-insensitive."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rels = registry.get_fact_relationships("FACT_ORDERS")
        assert len(rels) == 2

    def test_get_fact_relationships_empty(self, sample_relationships):
        """Test get_fact_relationships returns empty for unknown fact."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rels = registry.get_fact_relationships("fact_unknown")
        assert rels == []

    def test_get_dimension_relationships(self, sample_relationships):
        """Test get_dimension_relationships returns all for a dimension."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rels = registry.get_dimension_relationships("dim_product")
        assert len(rels) == 2
        assert all(r.dimension == "dim_product" for r in rels)

    def test_get_dimension_relationships_case_insensitive(self, sample_relationships):
        """Test get_dimension_relationships is case-insensitive."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        rels = registry.get_dimension_relationships("DIM_PRODUCT")
        assert len(rels) == 2

    def test_generate_lineage(self, sample_relationships):
        """Test generate_lineage creates correct lineage map."""
        registry = RelationshipRegistry(relationships=sample_relationships)
        lineage = registry.generate_lineage()
        assert "fact_orders" in lineage
        assert "dim_customer" in lineage["fact_orders"]
        assert "dim_product" in lineage["fact_orders"]
        assert "fact_inventory" in lineage
        assert "dim_product" in lineage["fact_inventory"]

    def test_generate_lineage_no_duplicates(self):
        """Test lineage doesn't duplicate dimensions."""
        rels = [
            RelationshipConfig(
                name="rel1",
                fact="fact_a",
                dimension="dim_x",
                fact_key="fk1",
                dimension_key="dk1",
            ),
            RelationshipConfig(
                name="rel2",
                fact="fact_a",
                dimension="dim_x",
                fact_key="fk2",
                dimension_key="dk2",
            ),
        ]
        registry = RelationshipRegistry(relationships=rels)
        lineage = registry.generate_lineage()
        assert lineage["fact_a"].count("dim_x") == 1


# =============================================================================
# DataClass Tests
# =============================================================================


class TestOrphanRecord:
    """Tests for OrphanRecord dataclass."""

    def test_orphan_record_creation(self):
        """Test creating an OrphanRecord."""
        orphan = OrphanRecord(
            fact_key_value=123,
            fact_key_column="customer_sk",
            dimension_table="dim_customer",
        )
        assert orphan.fact_key_value == 123
        assert orphan.fact_key_column == "customer_sk"
        assert orphan.dimension_table == "dim_customer"
        assert orphan.row_index is None

    def test_orphan_record_with_row_index(self):
        """Test OrphanRecord with row_index."""
        orphan = OrphanRecord(
            fact_key_value=456,
            fact_key_column="product_sk",
            dimension_table="dim_product",
            row_index=10,
        )
        assert orphan.row_index == 10


class TestFKValidationResult:
    """Tests for FKValidationResult dataclass."""

    def test_result_defaults(self):
        """Test FKValidationResult default values."""
        result = FKValidationResult(
            relationship_name="test",
            valid=True,
            total_rows=100,
            orphan_count=0,
            null_count=0,
        )
        assert result.orphan_values == []
        assert result.elapsed_ms == 0.0
        assert result.error is None

    def test_result_with_all_fields(self):
        """Test FKValidationResult with all fields."""
        result = FKValidationResult(
            relationship_name="test",
            valid=False,
            total_rows=100,
            orphan_count=5,
            null_count=2,
            orphan_values=[1, 2, 3],
            elapsed_ms=123.45,
            error="Some error",
        )
        assert result.orphan_values == [1, 2, 3]
        assert result.elapsed_ms == 123.45
        assert result.error == "Some error"


class TestFKValidationReport:
    """Tests for FKValidationReport dataclass."""

    def test_report_defaults(self):
        """Test FKValidationReport default values."""
        report = FKValidationReport(
            fact_table="fact_orders",
            all_valid=True,
            total_relationships=2,
            valid_relationships=2,
        )
        assert report.results == []
        assert report.orphan_records == []
        assert report.elapsed_ms == 0.0


# =============================================================================
# FKValidator Tests - Pandas Engine
# =============================================================================


class TestFKValidatorPandas:
    """Tests for FKValidator with Pandas engine."""

    @pytest.fixture
    def sample_fact_df(self):
        """Create a sample fact DataFrame."""
        return pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "customer_sk": [101, 102, 103, 101, 999],  # 999 is orphan
                "product_sk": [201, 202, 201, 203, 202],
            }
        )

    @pytest.fixture
    def sample_dim_customer(self):
        """Create a sample customer dimension."""
        return pd.DataFrame({"customer_sk": [101, 102, 103, 104]})

    @pytest.fixture
    def sample_dim_product(self):
        """Create a sample product dimension."""
        return pd.DataFrame({"product_sk": [201, 202, 203, 204]})

    def test_validate_relationship_valid(self, sample_dim_customer):
        """Test validating a valid relationship."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, 102, 103]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(fact_df, rel, context)

        assert result.valid is True
        assert result.orphan_count == 0
        assert result.null_count == 0
        assert result.total_rows == 3

    def test_validate_relationship_with_orphans(
        self, sample_fact_df, sample_dim_customer
    ):
        """Test detecting orphan records."""
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(sample_fact_df, rel, context)

        assert result.valid is False
        assert result.orphan_count == 1
        assert 999 in result.orphan_values

    def test_validate_relationship_with_nulls(self, sample_dim_customer):
        """Test handling null values in FK."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, None, 103]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
            nullable=False,
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(fact_df, rel, context)

        assert result.valid is False
        assert result.null_count == 1
        assert result.orphan_count == 0

    def test_validate_relationship_nullable_allowed(self, sample_dim_customer):
        """Test nullable=True allows nulls."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, None, 103]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
            nullable=True,
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(fact_df, rel, context)

        assert result.valid is True
        assert result.null_count == 1
        assert result.orphan_count == 0

    def test_validate_relationship_dimension_not_found(self):
        """Test error when dimension table is missing."""
        fact_df = pd.DataFrame({"order_id": [1], "customer_sk": [101]})
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({})  # No dimension
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(fact_df, rel, context)

        assert result.valid is False
        assert "not found" in result.error

    def test_validate_relationship_exception_handling(self, sample_dim_customer):
        """Test exception handling during validation."""
        fact_df = pd.DataFrame({"order_id": [1], "wrong_column": [101]})
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",  # Column doesn't exist
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        result = validator.validate_relationship(fact_df, rel, context)

        assert result.valid is False
        assert result.error is not None

    def test_validate_fact_all_valid(
        self, sample_dim_customer, sample_dim_product
    ):
        """Test validating all relationships for a fact table."""
        fact_df = pd.DataFrame(
            {
                "order_id": [1, 2],
                "customer_sk": [101, 102],
                "product_sk": [201, 202],
            }
        )
        rels = [
            RelationshipConfig(
                name="orders_customers",
                fact="fact_orders",
                dimension="dim_customer",
                fact_key="customer_sk",
                dimension_key="customer_sk",
            ),
            RelationshipConfig(
                name="orders_products",
                fact="fact_orders",
                dimension="dim_product",
                fact_key="product_sk",
                dimension_key="product_sk",
            ),
        ]
        context = MockEngineContext(
            {"dim_customer": sample_dim_customer, "dim_product": sample_dim_product}
        )
        registry = RelationshipRegistry(relationships=rels)
        validator = FKValidator(registry)

        report = validator.validate_fact(fact_df, "fact_orders", context)

        assert report.all_valid is True
        assert report.total_relationships == 2
        assert report.valid_relationships == 2
        assert len(report.results) == 2

    def test_validate_fact_some_invalid(
        self, sample_fact_df, sample_dim_customer, sample_dim_product
    ):
        """Test validate_fact with some invalid relationships."""
        rels = [
            RelationshipConfig(
                name="orders_customers",
                fact="fact_orders",
                dimension="dim_customer",
                fact_key="customer_sk",
                dimension_key="customer_sk",
            ),
            RelationshipConfig(
                name="orders_products",
                fact="fact_orders",
                dimension="dim_product",
                fact_key="product_sk",
                dimension_key="product_sk",
            ),
        ]
        context = MockEngineContext(
            {"dim_customer": sample_dim_customer, "dim_product": sample_dim_product}
        )
        registry = RelationshipRegistry(relationships=rels)
        validator = FKValidator(registry)

        report = validator.validate_fact(sample_fact_df, "fact_orders", context)

        assert report.all_valid is False
        assert report.total_relationships == 2
        assert report.valid_relationships == 1
        assert len(report.orphan_records) > 0

    def test_validate_fact_no_relationships(self):
        """Test validate_fact when no relationships defined."""
        fact_df = pd.DataFrame({"order_id": [1, 2]})
        context = MockEngineContext({})
        registry = RelationshipRegistry(relationships=[])
        validator = FKValidator(registry)

        report = validator.validate_fact(fact_df, "fact_orders", context)

        assert report.all_valid is True
        assert report.total_relationships == 0
        assert report.valid_relationships == 0

    def test_validate_fact_orphan_records_collected(self, sample_dim_customer):
        """Test orphan records are collected in report."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, 888, 999]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": sample_dim_customer})
        registry = RelationshipRegistry(relationships=[rel])
        validator = FKValidator(registry)

        report = validator.validate_fact(fact_df, "fact_orders", context)

        assert len(report.orphan_records) == 2
        orphan_values = [o.fact_key_value for o in report.orphan_records]
        assert 888 in orphan_values
        assert 999 in orphan_values


# =============================================================================
# get_orphan_records Tests
# =============================================================================


class TestGetOrphanRecords:
    """Tests for get_orphan_records function."""

    def test_get_orphan_records_pandas(self):
        """Test extracting orphan records with Pandas."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, 999, 888]}
        )
        dim_df = pd.DataFrame({"customer_sk": [101, 102, 103]})
        rel = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )

        orphans = get_orphan_records(fact_df, rel, dim_df, EngineType.PANDAS)

        assert len(orphans) == 2
        assert set(orphans["customer_sk"]) == {999, 888}

    def test_get_orphan_records_pandas_with_nulls(self):
        """Test orphan extraction excludes null FKs."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3, 4], "customer_sk": [101, 999, None, 888]}
        )
        dim_df = pd.DataFrame({"customer_sk": [101, 102]})
        rel = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )

        orphans = get_orphan_records(fact_df, rel, dim_df, EngineType.PANDAS)

        assert len(orphans) == 2
        assert None not in orphans["customer_sk"].values

    def test_get_orphan_records_pandas_no_orphans(self):
        """Test no orphans returned when all valid."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2], "customer_sk": [101, 102]}
        )
        dim_df = pd.DataFrame({"customer_sk": [101, 102, 103]})
        rel = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )

        orphans = get_orphan_records(fact_df, rel, dim_df, EngineType.PANDAS)

        assert len(orphans) == 0


# =============================================================================
# validate_fk_on_load Tests
# =============================================================================


class TestValidateFKOnLoad:
    """Tests for validate_fk_on_load convenience function."""

    @pytest.fixture
    def dim_customer(self):
        return pd.DataFrame({"customer_sk": [101, 102, 103]})

    def test_on_load_error_raises(self, dim_customer):
        """Test on_failure='error' raises ValueError."""
        fact_df = pd.DataFrame({"order_id": [1], "customer_sk": [999]})
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": dim_customer})

        with pytest.raises(ValueError, match="FK validation failed"):
            validate_fk_on_load(fact_df, [rel], context, on_failure="error")

    def test_on_load_warn_no_raise(self, dim_customer):
        """Test on_failure='warn' doesn't raise."""
        fact_df = pd.DataFrame({"order_id": [1], "customer_sk": [999]})
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": dim_customer})

        result = validate_fk_on_load(fact_df, [rel], context, on_failure="warn")

        assert len(result) == 1

    def test_on_load_filter_removes_orphans(self, dim_customer):
        """Test on_failure='filter' removes orphan rows."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2, 3], "customer_sk": [101, 999, 102]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": dim_customer})

        result = validate_fk_on_load(fact_df, [rel], context, on_failure="filter")

        assert len(result) == 2
        assert 999 not in result["customer_sk"].values

    def test_on_load_filter_missing_dimension_skips(self):
        """Test filter skips if dimension missing."""
        fact_df = pd.DataFrame({"order_id": [1], "customer_sk": [999]})
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({})  # No dimension

        result = validate_fk_on_load(fact_df, [rel], context, on_failure="filter")

        assert len(result) == 1

    def test_on_load_valid_no_action(self, dim_customer):
        """Test no action when all valid."""
        fact_df = pd.DataFrame(
            {"order_id": [1, 2], "customer_sk": [101, 102]}
        )
        rel = RelationshipConfig(
            name="orders_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        context = MockEngineContext({"dim_customer": dim_customer})

        result = validate_fk_on_load(fact_df, [rel], context, on_failure="error")

        assert len(result) == 2


# =============================================================================
# parse_relationships_config Tests
# =============================================================================


class TestParseRelationshipsConfig:
    """Tests for parse_relationships_config function."""

    def test_parse_valid_config(self):
        """Test parsing valid config dict."""
        config = {
            "relationships": [
                {
                    "name": "orders_customers",
                    "fact": "fact_orders",
                    "dimension": "dim_customer",
                    "fact_key": "customer_sk",
                    "dimension_key": "customer_sk",
                },
                {
                    "name": "orders_products",
                    "fact": "fact_orders",
                    "dimension": "dim_product",
                    "fact_key": "product_sk",
                    "dimension_key": "product_sk",
                },
            ]
        }

        registry = parse_relationships_config(config)

        assert len(registry.relationships) == 2
        assert registry.get_relationship("orders_customers") is not None
        assert registry.get_relationship("orders_products") is not None

    def test_parse_empty_config(self):
        """Test parsing config with no relationships."""
        config = {}
        registry = parse_relationships_config(config)
        assert len(registry.relationships) == 0

    def test_parse_empty_relationships_list(self):
        """Test parsing config with empty relationships list."""
        config = {"relationships": []}
        registry = parse_relationships_config(config)
        assert len(registry.relationships) == 0

    def test_parse_config_with_optionals(self):
        """Test parsing config with optional fields."""
        config = {
            "relationships": [
                {
                    "name": "orders_promo",
                    "fact": "fact_orders",
                    "dimension": "dim_promo",
                    "fact_key": "promo_sk",
                    "dimension_key": "promo_sk",
                    "nullable": True,
                    "on_violation": "warn",
                }
            ]
        }

        registry = parse_relationships_config(config)

        rel = registry.get_relationship("orders_promo")
        assert rel.nullable is True
        assert rel.on_violation == "warn"
