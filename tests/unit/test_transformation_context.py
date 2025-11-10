"""Tests for context passing system."""

from odibi.transformations import TransformationContext, build_context_from_config


class TestTransformationContext:
    """Tests for TransformationContext class."""

    def test_context_creation(self):
        """Should create context with required fields."""
        ctx = TransformationContext(node_name="test_node", operation_name="test_op")

        assert ctx.node_name == "test_node"
        assert ctx.operation_name == "test_op"
        assert ctx.params == {}

    def test_context_with_all_fields(self):
        """Should create context with all fields populated."""
        ctx = TransformationContext(
            node_name="calc_efficiency",
            operation_name="custom_transform",
            params={"threshold": 100},
            pipeline_name="energy_pipeline",
            layer="bronze",
            project="ODIBI",
            plant="NKC",
            asset="Germ Dryer 1",
            business_unit="Starch",
            environment="production",
            extra={"custom_field": "value"},
        )

        assert ctx.node_name == "calc_efficiency"
        assert ctx.operation_name == "custom_transform"
        assert ctx.params == {"threshold": 100}
        assert ctx.pipeline_name == "energy_pipeline"
        assert ctx.layer == "bronze"
        assert ctx.project == "ODIBI"
        assert ctx.plant == "NKC"
        assert ctx.asset == "Germ Dryer 1"
        assert ctx.business_unit == "Starch"
        assert ctx.environment == "production"
        assert ctx.extra == {"custom_field": "value"}

    def test_context_to_dict(self):
        """Should convert context to dictionary."""
        ctx = TransformationContext(
            node_name="test_node",
            operation_name="test_op",
            plant="NKC",
            asset="Germ Dryer 1",
        )

        result = ctx.to_dict()

        assert result["node"] == "test_node"
        assert result["operation"] == "test_op"
        assert result["plant"] == "NKC"
        assert result["asset"] == "Germ Dryer 1"
        assert result["params"] == {}
        assert result["environment"] == "development"

    def test_context_to_dict_with_extra(self):
        """Should include extra fields in to_dict()."""
        ctx = TransformationContext(
            node_name="test",
            operation_name="test_op",
            extra={"custom1": "value1", "custom2": "value2"},
        )

        result = ctx.to_dict()

        assert result["custom1"] == "value1"
        assert result["custom2"] == "value2"


class TestBuildContextFromConfig:
    """Tests for build_context_from_config function."""

    def test_build_context_minimal(self):
        """Should build context from minimal config."""
        node_config = {"node": "my_node", "operation": "my_op"}
        pipeline_config = {}
        project_config = {}

        ctx = build_context_from_config(node_config, pipeline_config, project_config)

        assert ctx.node_name == "my_node"
        assert ctx.operation_name == "my_op"
        assert ctx.params == {}

    def test_build_context_full(self):
        """Should build context from complete config."""
        node_config = {
            "node": "efficiency_calc",
            "operation": "calculate",
            "params": {"fuel_col": "natural_gas", "output_col": "steam"},
        }
        pipeline_config = {"name": "energy_efficiency", "layer": "silver"}
        project_config = {
            "project": "ODIBI",
            "plant": "NKC",
            "asset": "Germ Dryer 1",
            "business_unit": "Starch",
            "environment": "production",
            "metadata": {"team": "data_engineering"},
        }

        ctx = build_context_from_config(node_config, pipeline_config, project_config)

        assert ctx.node_name == "efficiency_calc"
        assert ctx.operation_name == "calculate"
        assert ctx.params == {"fuel_col": "natural_gas", "output_col": "steam"}
        assert ctx.pipeline_name == "energy_efficiency"
        assert ctx.layer == "silver"
        assert ctx.project == "ODIBI"
        assert ctx.plant == "NKC"
        assert ctx.asset == "Germ Dryer 1"
        assert ctx.business_unit == "Starch"
        assert ctx.environment == "production"
        assert ctx.extra == {"team": "data_engineering"}

    def test_build_context_defaults(self):
        """Should handle missing fields with defaults."""
        node_config = {}
        pipeline_config = {}
        project_config = {}

        ctx = build_context_from_config(node_config, pipeline_config, project_config)

        assert ctx.node_name == "unnamed"
        assert ctx.operation_name == "unknown"
        assert ctx.params == {}
        assert ctx.pipeline_name is None
        assert ctx.layer is None
        assert ctx.project is None
        assert ctx.plant is None
        assert ctx.asset is None
        assert ctx.business_unit is None
        assert ctx.environment == "development"
        assert ctx.extra == {}

    def test_build_context_partial(self):
        """Should handle partially populated configs."""
        node_config = {"node": "transform1", "params": {"col": "value"}}
        pipeline_config = {"name": "my_pipeline"}
        project_config = {"plant": "NKC", "environment": "staging"}

        ctx = build_context_from_config(node_config, pipeline_config, project_config)

        assert ctx.node_name == "transform1"
        assert ctx.operation_name == "unknown"  # default
        assert ctx.params == {"col": "value"}
        assert ctx.pipeline_name == "my_pipeline"
        assert ctx.layer is None
        assert ctx.plant == "NKC"
        assert ctx.environment == "staging"
