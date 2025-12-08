"""Unit tests for FK Validation module."""

import pandas as pd
import pytest

from odibi.context import EngineContext, PandasContext
from odibi.enums import EngineType
from odibi.validation.fk import (
    RelationshipConfig,
    RelationshipRegistry,
    FKValidator,
    get_orphan_records,
    validate_fk_on_load,
    parse_relationships_config,
)


def create_pandas_context_with_data(main_df, registered_dfs=None):
    """Helper to create a PandasContext with main df and registered tables."""
    pandas_context = PandasContext()

    if registered_dfs:
        for name, df in registered_dfs.items():
            pandas_context.register(name, df)

    ctx = EngineContext(
        context=pandas_context,
        df=main_df,
        engine_type=EngineType.PANDAS,
    )
    return ctx


class TestRelationshipConfig:
    """Test RelationshipConfig model."""

    def test_valid_relationship(self):
        """Test creating a valid relationship."""
        rel = RelationshipConfig(
            name="orders_to_customers",
            fact="fact_orders",
            dimension="dim_customer",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )
        assert rel.name == "orders_to_customers"
        assert rel.fact == "fact_orders"
        assert rel.dimension == "dim_customer"
        assert rel.nullable is False
        assert rel.on_violation == "error"

    def test_relationship_with_nullable(self):
        """Test relationship with nullable FK."""
        rel = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="fk",
            dimension_key="dk",
            nullable=True,
        )
        assert rel.nullable is True

    def test_relationship_on_violation_options(self):
        """Test on_violation options."""
        for action in ["warn", "error", "quarantine"]:
            rel = RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
                on_violation=action,
            )
            assert rel.on_violation == action

    def test_relationship_invalid_on_violation(self):
        """Test invalid on_violation raises error."""
        with pytest.raises(ValueError, match="on_violation"):
            RelationshipConfig(
                name="test",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
                on_violation="invalid",
            )

    def test_relationship_empty_name(self):
        """Test empty name raises error."""
        with pytest.raises(ValueError, match="empty"):
            RelationshipConfig(
                name="",
                fact="fact",
                dimension="dim",
                fact_key="fk",
                dimension_key="dk",
            )

    def test_relationship_whitespace_trimmed(self):
        """Test whitespace is trimmed from fields."""
        rel = RelationshipConfig(
            name="  test  ",
            fact="  fact  ",
            dimension="  dim  ",
            fact_key="  fk  ",
            dimension_key="  dk  ",
        )
        assert rel.name == "test"
        assert rel.fact == "fact"


class TestRelationshipRegistry:
    """Test RelationshipRegistry model."""

    @pytest.fixture
    def sample_registry(self):
        """Create a sample registry."""
        return RelationshipRegistry(
            relationships=[
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
                    name="orders_to_dates",
                    fact="fact_orders",
                    dimension="dim_date",
                    fact_key="date_sk",
                    dimension_key="date_sk",
                ),
                RelationshipConfig(
                    name="inventory_to_products",
                    fact="fact_inventory",
                    dimension="dim_product",
                    fact_key="product_sk",
                    dimension_key="product_sk",
                ),
            ]
        )

    def test_get_relationship(self, sample_registry):
        """Test getting relationship by name."""
        rel = sample_registry.get_relationship("orders_to_customers")
        assert rel is not None
        assert rel.name == "orders_to_customers"

        rel_insensitive = sample_registry.get_relationship("ORDERS_TO_CUSTOMERS")
        assert rel_insensitive is not None

        assert sample_registry.get_relationship("unknown") is None

    def test_get_fact_relationships(self, sample_registry):
        """Test getting all relationships for a fact table."""
        rels = sample_registry.get_fact_relationships("fact_orders")
        assert len(rels) == 3

        rels = sample_registry.get_fact_relationships("fact_inventory")
        assert len(rels) == 1

        rels = sample_registry.get_fact_relationships("unknown")
        assert len(rels) == 0

    def test_get_dimension_relationships(self, sample_registry):
        """Test getting all relationships referencing a dimension."""
        rels = sample_registry.get_dimension_relationships("dim_product")
        assert len(rels) == 2

        rels = sample_registry.get_dimension_relationships("dim_customer")
        assert len(rels) == 1

    def test_generate_lineage(self, sample_registry):
        """Test generating lineage map."""
        lineage = sample_registry.generate_lineage()

        assert "fact_orders" in lineage
        assert "dim_customer" in lineage["fact_orders"]
        assert "dim_product" in lineage["fact_orders"]
        assert "dim_date" in lineage["fact_orders"]

        assert "fact_inventory" in lineage
        assert "dim_product" in lineage["fact_inventory"]


class TestFKValidatorPandas:
    """Test FKValidator with Pandas engine."""

    @pytest.fixture
    def simple_registry(self):
        """Create a simple registry with one relationship."""
        return RelationshipRegistry(
            relationships=[
                RelationshipConfig(
                    name="orders_to_customers",
                    fact="fact_orders",
                    dimension="dim_customer",
                    fact_key="customer_sk",
                    dimension_key="customer_sk",
                ),
            ]
        )

    @pytest.fixture
    def dim_customer(self):
        """Sample customer dimension."""
        return pd.DataFrame(
            {
                "customer_sk": [1, 2, 3],
                "customer_name": ["Alice", "Bob", "Charlie"],
            }
        )

    def test_validate_relationship_valid(self, simple_registry, dim_customer):
        """Test validation passes when all FKs are valid."""
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102],
                "customer_sk": [1, 2, 3],
                "amount": [100, 200, 300],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        validator = FKValidator(simple_registry)
        rel = simple_registry.get_relationship("orders_to_customers")
        result = validator.validate_relationship(fact_orders, rel, context)

        assert result.valid is True
        assert result.orphan_count == 0
        assert result.null_count == 0
        assert result.total_rows == 3

    def test_validate_relationship_orphans(self, simple_registry, dim_customer):
        """Test validation detects orphan records."""
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102],
                "customer_sk": [1, 2, 999],  # 999 doesn't exist
                "amount": [100, 200, 300],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        validator = FKValidator(simple_registry)
        rel = simple_registry.get_relationship("orders_to_customers")
        result = validator.validate_relationship(fact_orders, rel, context)

        assert result.valid is False
        assert result.orphan_count == 1
        assert 999 in result.orphan_values

    def test_validate_relationship_nulls_not_allowed(self, simple_registry, dim_customer):
        """Test validation fails with nulls when not nullable."""
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102],
                "customer_sk": [1, 2, None],
                "amount": [100, 200, 300],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        validator = FKValidator(simple_registry)
        rel = simple_registry.get_relationship("orders_to_customers")
        result = validator.validate_relationship(fact_orders, rel, context)

        assert result.valid is False
        assert result.null_count == 1
        assert result.orphan_count == 0

    def test_validate_relationship_nulls_allowed(self, dim_customer):
        """Test validation passes with nulls when nullable."""
        registry = RelationshipRegistry(
            relationships=[
                RelationshipConfig(
                    name="orders_to_customers",
                    fact="fact_orders",
                    dimension="dim_customer",
                    fact_key="customer_sk",
                    dimension_key="customer_sk",
                    nullable=True,
                ),
            ]
        )

        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102],
                "customer_sk": [1, 2, None],
                "amount": [100, 200, 300],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        validator = FKValidator(registry)
        rel = registry.get_relationship("orders_to_customers")
        result = validator.validate_relationship(fact_orders, rel, context)

        assert result.valid is True
        assert result.null_count == 1

    def test_validate_relationship_dimension_not_found(self, simple_registry):
        """Test validation handles missing dimension."""
        fact_orders = pd.DataFrame(
            {
                "order_id": [100],
                "customer_sk": [1],
            }
        )

        context = create_pandas_context_with_data(fact_orders, {})

        validator = FKValidator(simple_registry)
        rel = simple_registry.get_relationship("orders_to_customers")
        result = validator.validate_relationship(fact_orders, rel, context)

        assert result.valid is False
        assert result.error is not None
        assert "not found" in result.error


class TestFKValidatorFactLevel:
    """Test FKValidator.validate_fact method."""

    @pytest.fixture
    def multi_rel_registry(self):
        """Create registry with multiple relationships."""
        return RelationshipRegistry(
            relationships=[
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
            ]
        )

    def test_validate_fact_all_valid(self, multi_rel_registry):
        """Test validation of entire fact table - all valid."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2]})
        dim_product = pd.DataFrame({"product_sk": [10, 20]})

        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101],
                "customer_sk": [1, 2],
                "product_sk": [10, 20],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer, "dim_product": dim_product},
        )

        validator = FKValidator(multi_rel_registry)
        report = validator.validate_fact(fact_orders, "fact_orders", context)

        assert report.all_valid is True
        assert report.total_relationships == 2
        assert report.valid_relationships == 2
        assert len(report.results) == 2

    def test_validate_fact_partial_failure(self, multi_rel_registry):
        """Test validation with some relationships failing."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2]})
        dim_product = pd.DataFrame({"product_sk": [10]})  # Missing 20

        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101],
                "customer_sk": [1, 2],
                "product_sk": [10, 20],  # 20 is orphan
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer, "dim_product": dim_product},
        )

        validator = FKValidator(multi_rel_registry)
        report = validator.validate_fact(fact_orders, "fact_orders", context)

        assert report.all_valid is False
        assert report.valid_relationships == 1
        assert len(report.orphan_records) == 1

    def test_validate_fact_no_relationships(self):
        """Test validation with no defined relationships."""
        registry = RelationshipRegistry(relationships=[])
        fact_orders = pd.DataFrame({"order_id": [100]})

        context = create_pandas_context_with_data(fact_orders, {})

        validator = FKValidator(registry)
        report = validator.validate_fact(fact_orders, "fact_orders", context)

        assert report.all_valid is True
        assert report.total_relationships == 0


class TestGetOrphanRecords:
    """Test get_orphan_records function."""

    def test_get_orphan_records_pandas(self):
        """Test extracting orphan records with Pandas."""
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102, 103],
                "customer_sk": [1, 2, 999, 888],
                "amount": [100, 200, 300, 400],
            }
        )

        dim_customer = pd.DataFrame(
            {
                "customer_sk": [1, 2, 3],
            }
        )

        rel = RelationshipConfig(
            name="test",
            fact="fact",
            dimension="dim",
            fact_key="customer_sk",
            dimension_key="customer_sk",
        )

        orphans = get_orphan_records(fact_orders, rel, dim_customer, EngineType.PANDAS)

        assert len(orphans) == 2
        assert set(orphans["customer_sk"]) == {999, 888}
        assert set(orphans["order_id"]) == {102, 103}


class TestValidateFKOnLoad:
    """Test validate_fk_on_load convenience function."""

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
        ]

    def test_validate_on_load_error_mode(self, sample_relationships):
        """Test error mode raises on violation."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2]})
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101],
                "customer_sk": [1, 999],  # 999 is orphan
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        with pytest.raises(ValueError, match="FK validation failed"):
            validate_fk_on_load(
                fact_orders,
                sample_relationships,
                context,
                on_failure="error",
            )

    def test_validate_on_load_warn_mode(self, sample_relationships, caplog):
        """Test warn mode logs warning but returns data."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2]})
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101],
                "customer_sk": [1, 999],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        result = validate_fk_on_load(
            fact_orders,
            sample_relationships,
            context,
            on_failure="warn",
        )

        assert len(result) == 2  # All rows returned

    def test_validate_on_load_filter_mode(self, sample_relationships):
        """Test filter mode removes orphan rows."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2]})
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101, 102],
                "customer_sk": [1, 2, 999],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        result = validate_fk_on_load(
            fact_orders,
            sample_relationships,
            context,
            on_failure="filter",
        )

        assert len(result) == 2
        assert 999 not in result["customer_sk"].values

    def test_validate_on_load_valid_data(self, sample_relationships):
        """Test valid data passes without changes."""
        dim_customer = pd.DataFrame({"customer_sk": [1, 2, 3]})
        fact_orders = pd.DataFrame(
            {
                "order_id": [100, 101],
                "customer_sk": [1, 2],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        result = validate_fk_on_load(
            fact_orders,
            sample_relationships,
            context,
            on_failure="error",
        )

        assert len(result) == 2


class TestParseRelationshipsConfig:
    """Test parse_relationships_config function."""

    def test_parse_config(self):
        """Test parsing config dictionary."""
        config_dict = {
            "relationships": [
                {
                    "name": "orders_to_customers",
                    "fact": "fact_orders",
                    "dimension": "dim_customer",
                    "fact_key": "customer_sk",
                    "dimension_key": "customer_sk",
                },
                {
                    "name": "orders_to_products",
                    "fact": "fact_orders",
                    "dimension": "dim_product",
                    "fact_key": "product_sk",
                    "dimension_key": "product_sk",
                    "nullable": True,
                },
            ]
        }

        registry = parse_relationships_config(config_dict)

        assert len(registry.relationships) == 2
        assert registry.get_relationship("orders_to_customers") is not None
        assert registry.get_relationship("orders_to_products").nullable is True

    def test_parse_empty_config(self):
        """Test parsing empty config."""
        registry = parse_relationships_config({})
        assert len(registry.relationships) == 0

        registry = parse_relationships_config({"relationships": []})
        assert len(registry.relationships) == 0


class TestFKValidationIntegration:
    """Integration tests for FK validation."""

    def test_complete_star_schema_validation(self):
        """Test validating a complete star schema."""
        registry = RelationshipRegistry(
            relationships=[
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
                    name="orders_to_dates",
                    fact="fact_orders",
                    dimension="dim_date",
                    fact_key="date_sk",
                    dimension_key="date_sk",
                ),
            ]
        )

        dim_customer = pd.DataFrame(
            {
                "customer_sk": [0, 1, 2, 3],  # 0 is unknown member
                "customer_name": ["Unknown", "Alice", "Bob", "Charlie"],
            }
        )

        dim_product = pd.DataFrame(
            {
                "product_sk": [0, 10, 20, 30],
                "product_name": ["Unknown", "Widget", "Gadget", "Gizmo"],
            }
        )

        dim_date = pd.DataFrame(
            {
                "date_sk": [20240101, 20240102, 20240103],
                "full_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        )

        fact_orders = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5],
                "customer_sk": [1, 2, 0, 3, 1],  # 0 = unknown customer
                "product_sk": [10, 20, 30, 0, 10],  # 0 = unknown product
                "date_sk": [20240101, 20240101, 20240102, 20240102, 20240103],
                "amount": [100, 200, 150, 300, 250],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {
                "dim_customer": dim_customer,
                "dim_product": dim_product,
                "dim_date": dim_date,
            },
        )

        validator = FKValidator(registry)
        report = validator.validate_fact(fact_orders, "fact_orders", context)

        assert report.all_valid is True
        assert report.total_relationships == 3
        assert report.valid_relationships == 3
        assert len(report.orphan_records) == 0

        lineage = registry.generate_lineage()
        assert set(lineage["fact_orders"]) == {"dim_customer", "dim_product", "dim_date"}

    def test_validation_with_orphan_handling(self):
        """Test validation with different orphan handling strategies."""
        registry = RelationshipRegistry(
            relationships=[
                RelationshipConfig(
                    name="orders_to_customers",
                    fact="fact_orders",
                    dimension="dim_customer",
                    fact_key="customer_sk",
                    dimension_key="customer_sk",
                ),
            ]
        )

        dim_customer = pd.DataFrame(
            {
                "customer_sk": [1, 2, 3],
            }
        )

        fact_orders = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "customer_sk": [1, 2, 999, 888],  # 999, 888 are orphans
                "amount": [100, 200, 150, 300],
            }
        )

        context = create_pandas_context_with_data(
            fact_orders,
            {"dim_customer": dim_customer},
        )

        validator = FKValidator(registry)
        report = validator.validate_fact(fact_orders, "fact_orders", context)

        assert report.all_valid is False
        assert report.results[0].orphan_count == 2
        assert set(report.results[0].orphan_values) == {999, 888}

        orphan_df = get_orphan_records(
            fact_orders,
            registry.get_relationship("orders_to_customers"),
            dim_customer,
            EngineType.PANDAS,
        )
        assert len(orphan_df) == 2
        assert set(orphan_df["customer_sk"]) == {999, 888}
