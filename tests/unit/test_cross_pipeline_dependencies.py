"""Tests for cross-pipeline dependencies feature.

Tests cover:
1. CatalogManager: meta_outputs table and register_outputs_batch
2. References: resolve_input_reference and validation
3. NodeConfig: inputs field validation
4. NodeExecutor: inputs phase execution
"""

import os
from datetime import datetime, timezone

import pytest

from odibi.catalog import CatalogManager
from odibi.config import NodeConfig, ReadConfig, SystemConfig, WriteConfig
from odibi.engine.pandas_engine import PandasEngine
from odibi.references import (
    ReferenceResolutionError,
    is_pipeline_reference,
    resolve_input_reference,
    resolve_inputs,
    validate_references,
)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def catalog_manager(tmp_path):
    """Create a CatalogManager with local Delta backend."""
    config = SystemConfig(connection="local", path="_odibi_system")
    base_path = str(tmp_path / "_odibi_system")
    engine = PandasEngine(config={})
    return CatalogManager(spark=None, config=config, base_path=base_path, engine=engine)


@pytest.fixture
def catalog_with_outputs(catalog_manager):
    """CatalogManager with bootstrapped tables and sample outputs."""
    catalog_manager.bootstrap()

    # Register some sample outputs
    records = [
        {
            "pipeline_name": "read_bronze",
            "node_name": "shift_events",
            "output_type": "external_table",
            "connection_name": "goat_prod",
            "path": "bronze/OEE/shift_events",
            "format": "delta",
            "table_name": "test.shift_events",
            "last_run": datetime.now(timezone.utc),
            "row_count": 1000,
        },
        {
            "pipeline_name": "read_bronze",
            "node_name": "calendar",
            "output_type": "external_table",
            "connection_name": "goat_prod",
            "path": "bronze/OEE/calendar",
            "format": "delta",
            "table_name": None,
            "last_run": datetime.now(timezone.utc),
            "row_count": 365,
        },
        {
            "pipeline_name": "transform_silver",
            "node_name": "enriched_events",
            "output_type": "managed_table",
            "connection_name": None,
            "path": None,
            "format": "delta",
            "table_name": "silver.enriched_events",
            "last_run": datetime.now(timezone.utc),
            "row_count": 500,
        },
    ]
    catalog_manager.register_outputs_batch(records)

    return catalog_manager


# ============================================================================
# CatalogManager Tests: meta_outputs
# ============================================================================


class TestCatalogManagerMetaOutputs:
    """Tests for meta_outputs table functionality."""

    def test_bootstrap_creates_meta_outputs(self, catalog_manager):
        """Test that bootstrap creates meta_outputs table."""
        catalog_manager.bootstrap()

        # Verify meta_outputs path exists
        assert "meta_outputs" in catalog_manager.tables
        output_path = catalog_manager.tables["meta_outputs"]
        assert os.path.exists(output_path), f"meta_outputs path {output_path} not created"

    def test_register_outputs_batch_creates_records(self, catalog_manager):
        """Test batch registration of outputs."""
        catalog_manager.bootstrap()

        records = [
            {
                "pipeline_name": "test_pipeline",
                "node_name": "test_node",
                "output_type": "external_table",
                "connection_name": "conn1",
                "path": "data/test",
                "format": "parquet",
                "table_name": "test.output",
                "last_run": datetime.now(timezone.utc),
                "row_count": 100,
            }
        ]

        catalog_manager.register_outputs_batch(records)

        # Read back and verify
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_outputs"])
        assert not df.empty
        assert len(df) == 1
        row = df.iloc[0]
        assert row["pipeline_name"] == "test_pipeline"
        assert row["node_name"] == "test_node"
        assert row["output_type"] == "external_table"
        assert row["path"] == "data/test"

    def test_register_outputs_batch_upserts(self, catalog_manager):
        """Test that batch registration upserts existing records."""
        catalog_manager.bootstrap()

        # Initial insert
        initial_records = [
            {
                "pipeline_name": "pipeline1",
                "node_name": "node1",
                "output_type": "external_table",
                "connection_name": "conn1",
                "path": "old/path",
                "format": "parquet",
                "table_name": None,
                "last_run": datetime.now(timezone.utc),
                "row_count": 50,
            }
        ]
        catalog_manager.register_outputs_batch(initial_records)

        # Update with new path
        updated_records = [
            {
                "pipeline_name": "pipeline1",
                "node_name": "node1",
                "output_type": "external_table",
                "connection_name": "conn1",
                "path": "new/path",
                "format": "delta",
                "table_name": "updated.table",
                "last_run": datetime.now(timezone.utc),
                "row_count": 100,
            }
        ]
        catalog_manager.register_outputs_batch(updated_records)

        # Verify only one record and it's updated
        df = catalog_manager._read_local_table(catalog_manager.tables["meta_outputs"])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["path"] == "new/path"
        assert row["format"] == "delta"
        assert row["row_count"] == 100

    def test_register_outputs_batch_empty_list(self, catalog_manager):
        """Test that empty list doesn't cause errors."""
        catalog_manager.bootstrap()
        catalog_manager.register_outputs_batch([])
        # Should not raise

    def test_get_node_output_found(self, catalog_with_outputs):
        """Test retrieving an existing node output."""
        output = catalog_with_outputs.get_node_output("read_bronze", "shift_events")

        assert output is not None
        assert output["pipeline_name"] == "read_bronze"
        assert output["node_name"] == "shift_events"
        assert output["path"] == "bronze/OEE/shift_events"
        assert output["format"] == "delta"

    def test_get_node_output_not_found(self, catalog_with_outputs):
        """Test that missing node returns None."""
        output = catalog_with_outputs.get_node_output("fake_pipeline", "fake_node")
        assert output is None

    def test_get_node_output_caching(self, catalog_with_outputs):
        """Test that outputs are cached for performance."""
        # First call populates cache
        output1 = catalog_with_outputs.get_node_output("read_bronze", "shift_events")

        # Cache should be populated
        assert catalog_with_outputs._outputs_cache is not None

        # Second call should use cache
        output2 = catalog_with_outputs.get_node_output("read_bronze", "shift_events")

        assert output1 == output2

    def test_invalidate_cache_clears_outputs(self, catalog_with_outputs):
        """Test that invalidate_cache clears outputs cache."""
        # Populate cache
        catalog_with_outputs.get_node_output("read_bronze", "shift_events")
        assert catalog_with_outputs._outputs_cache is not None

        # Invalidate
        catalog_with_outputs.invalidate_cache()
        assert catalog_with_outputs._outputs_cache is None


# ============================================================================
# References Module Tests
# ============================================================================


class TestReferenceFunctions:
    """Tests for reference resolution functions."""

    def test_is_pipeline_reference_valid(self):
        """Test identifying valid pipeline references."""
        assert is_pipeline_reference("$read_bronze.shift_events") is True
        assert is_pipeline_reference("$pipeline.node") is True
        assert is_pipeline_reference("$a.b") is True

    def test_is_pipeline_reference_invalid(self):
        """Test rejecting non-references."""
        assert is_pipeline_reference("read_bronze.shift_events") is False
        assert is_pipeline_reference("not_a_reference") is False
        assert is_pipeline_reference(123) is False
        assert is_pipeline_reference(None) is False
        assert is_pipeline_reference({"key": "value"}) is False

    def test_resolve_input_reference_external_table(self, catalog_with_outputs):
        """Test resolving reference to external table."""
        result = resolve_input_reference("$read_bronze.shift_events", catalog_with_outputs)

        assert result["connection"] == "goat_prod"
        assert result["path"] == "bronze/OEE/shift_events"
        assert result["format"] == "delta"
        assert "table" not in result  # External table uses path, not table

    def test_resolve_input_reference_managed_table(self, catalog_with_outputs):
        """Test resolving reference to managed table."""
        result = resolve_input_reference("$transform_silver.enriched_events", catalog_with_outputs)

        assert result["table"] == "silver.enriched_events"
        assert result["format"] == "delta"
        assert "connection" not in result  # Managed table uses table name

    def test_resolve_input_reference_invalid_format(self, catalog_with_outputs):
        """Test that invalid reference format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid reference"):
            resolve_input_reference("no_dollar_sign.node", catalog_with_outputs)

    def test_resolve_input_reference_missing_node(self, catalog_with_outputs):
        """Test that missing reference raises error."""
        with pytest.raises(ReferenceResolutionError, match="No output found"):
            resolve_input_reference("$fake_pipeline.fake_node", catalog_with_outputs)

    def test_resolve_input_reference_malformed(self, catalog_with_outputs):
        """Test malformed reference (no dot) raises error."""
        with pytest.raises(ValueError, match="Invalid reference format"):
            resolve_input_reference("$nodot", catalog_with_outputs)

    def test_resolve_inputs_mixed(self, catalog_with_outputs):
        """Test resolving mixed inputs (references and explicit configs)."""
        inputs = {
            "events": "$read_bronze.shift_events",
            "calendar": {
                "connection": "local",
                "path": "data/calendar",
                "format": "parquet",
            },
        }

        resolved = resolve_inputs(inputs, catalog_with_outputs)

        assert "events" in resolved
        assert resolved["events"]["path"] == "bronze/OEE/shift_events"

        assert "calendar" in resolved
        assert resolved["calendar"]["path"] == "data/calendar"
        assert resolved["calendar"]["format"] == "parquet"

    def test_resolve_inputs_invalid_type(self, catalog_with_outputs):
        """Test that invalid input type raises error."""
        inputs = {
            "bad_input": 123,  # Not a string or dict
        }

        with pytest.raises(ValueError, match="Invalid input format"):
            resolve_inputs(inputs, catalog_with_outputs)

    def test_validate_references_success(self, catalog_with_outputs):
        """Test that valid references pass validation."""
        inputs = {
            "events": "$read_bronze.shift_events",
            "calendar": "$read_bronze.calendar",
        }

        # Should not raise
        validate_references(inputs, catalog_with_outputs)

    def test_validate_references_failure(self, catalog_with_outputs):
        """Test that invalid references fail validation."""
        inputs = {
            "events": "$read_bronze.shift_events",
            "missing": "$fake_pipeline.missing_node",
        }

        with pytest.raises(ReferenceResolutionError):
            validate_references(inputs, catalog_with_outputs)


# ============================================================================
# NodeConfig Tests: inputs field
# ============================================================================


class TestNodeConfigInputs:
    """Tests for NodeConfig inputs field validation."""

    def test_node_with_inputs_valid(self):
        """Test that node with inputs field is valid."""
        config = NodeConfig(
            name="test_node",
            inputs={
                "events": "$read_bronze.shift_events",
                "calendar": {"connection": "local", "path": "data/cal"},
            },
            transform={"steps": [{"sql": "SELECT * FROM events JOIN calendar USING (date_id)"}]},
        )
        assert config.inputs is not None
        assert len(config.inputs) == 2

    def test_node_with_both_read_and_inputs_invalid(self):
        """Test that node cannot have both read and inputs."""
        with pytest.raises(ValueError, match="Cannot have both 'read' and 'inputs'"):
            NodeConfig(
                name="test_node",
                read=ReadConfig(connection="local", format="parquet", path="data/test"),
                inputs={"events": "$read_bronze.shift_events"},
                transform={"steps": [{"sql": "SELECT * FROM df"}]},
            )

    def test_node_inputs_counts_as_operation(self):
        """Test that inputs counts as a valid operation."""
        # This should be valid (inputs + write is valid)
        config = NodeConfig(
            name="test_node",
            inputs={"data": "$pipeline.node"},
            write=WriteConfig(connection="local", format="parquet", path="output"),
        )
        assert config.inputs is not None

    def test_node_must_have_operation(self):
        """Test that node must have at least one operation."""
        with pytest.raises(ValueError, match="must have at least one of"):
            NodeConfig(
                name="empty_node",
                depends_on=["other"],
            )


# ============================================================================
# Integration Tests
# ============================================================================


class TestCrossPipelineIntegration:
    """Integration tests for the full cross-pipeline flow."""

    def test_end_to_end_bronze_silver_reference(self, catalog_with_outputs):
        """Test complete flow: bronze writes output, silver references it."""
        # Simulate silver pipeline referencing bronze output
        inputs = {
            "events": "$read_bronze.shift_events",
            "calendar": "$read_bronze.calendar",
        }

        # Validate references at load time (fail fast)
        validate_references(inputs, catalog_with_outputs)

        # Resolve references for execution
        resolved = resolve_inputs(inputs, catalog_with_outputs)

        # Verify we got proper read configs
        assert resolved["events"]["connection"] == "goat_prod"
        assert resolved["events"]["path"] == "bronze/OEE/shift_events"
        assert resolved["calendar"]["path"] == "bronze/OEE/calendar"

    def test_output_registration_flow(self, catalog_manager):
        """Test the complete output registration flow."""
        catalog_manager.bootstrap()

        # Simulate pipeline run with output collection
        output_records = []

        # Node 1 completes with write
        output_records.append(
            {
                "pipeline_name": "test_pipeline",
                "node_name": "node1",
                "output_type": "external_table",
                "connection_name": "conn1",
                "path": "data/node1",
                "format": "delta",
                "table_name": None,
                "last_run": datetime.now(timezone.utc),
                "row_count": 100,
            }
        )

        # Node 2 completes with write
        output_records.append(
            {
                "pipeline_name": "test_pipeline",
                "node_name": "node2",
                "output_type": "external_table",
                "connection_name": "conn1",
                "path": "data/node2",
                "format": "delta",
                "table_name": "table2",
                "last_run": datetime.now(timezone.utc),
                "row_count": 200,
            }
        )

        # Batch write at end of pipeline (performance critical)
        catalog_manager.register_outputs_batch(output_records)

        # Verify both outputs are registered
        output1 = catalog_manager.get_node_output("test_pipeline", "node1")
        output2 = catalog_manager.get_node_output("test_pipeline", "node2")

        assert output1 is not None
        assert output1["row_count"] == 100

        assert output2 is not None
        assert output2["row_count"] == 200

    def test_no_catalog_manager_raises_error(self):
        """Test that resolving reference without catalog raises clear error."""
        # Create a mock that returns None for catalog_manager
        mock_catalog = None

        with pytest.raises(Exception):
            # This should fail because catalog is None
            resolve_input_reference("$pipeline.node", mock_catalog)


# ============================================================================
# NodeExecutor: _extract_output_from_transform_steps Tests
# ============================================================================


class TestExtractOutputFromTransformSteps:
    """Tests for extracting output info from merge/scd2 transform steps."""

    def test_extract_merge_output(self):
        """Test extracting output info from merge function in transform steps."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(
                steps=[
                    TransformStep(sql="SELECT * FROM df"),
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "goat_prod",
                            "path": "OEE/silver/cleaned_data",
                            "register_table": "test.cleaned_data",
                            "keys": ["id"],
                        },
                    ),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is not None
        assert result["connection"] == "goat_prod"
        assert result["path"] == "OEE/silver/cleaned_data"
        assert result["register_table"] == "test.cleaned_data"
        assert result["format"] == "delta"
        assert result["output_type"] == "managed_table"

    def test_extract_scd2_output(self):
        """Test extracting output info from scd2 function using target param."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(
                steps=[
                    TransformStep(
                        function="scd2",
                        params={
                            "connection": "goat_prod",
                            "target": "OEE/silver/dim_product",
                            "keys": ["id"],
                            "track_cols": ["name", "status"],
                            "effective_time_col": "_extracted_at",
                        },
                    ),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is not None
        assert result["connection"] == "goat_prod"
        assert result["path"] == "OEE/silver/dim_product"
        assert result["format"] == "delta"
        assert result["output_type"] == "external_table"

    def test_no_output_when_no_merge_scd2(self):
        """Test that no output is extracted when there's no merge/scd2 step."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(
                steps=[
                    TransformStep(sql="SELECT * FROM df"),
                    TransformStep(function="deduplicate", params={"keys": ["id"]}),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is None

    def test_no_output_when_no_transform(self):
        """Test that no output is extracted when there's no transform config."""
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            write=WriteConfig(connection="conn", path="path", format="delta"),
        )
        config.transform = None

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is None

    def test_uses_last_merge_in_chain(self):
        """Test that the last merge/scd2 in the chain is used for output."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(
                steps=[
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "conn1",
                            "path": "first/path",
                            "keys": ["id"],
                        },
                    ),
                    TransformStep(sql="SELECT * FROM df"),
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "conn2",
                            "path": "second/path",
                            "register_table": "final.table",
                            "keys": ["id"],
                        },
                    ),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is not None
        assert result["connection"] == "conn2"
        assert result["path"] == "second/path"
        assert result["register_table"] == "final.table"

    def test_create_output_record_with_merge(self):
        """Test _create_output_record uses merge params when no write block."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="cleaned_data",
            transform=TransformConfig(
                steps=[
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "goat_prod",
                            "path": "OEE/silver/cleaned_data",
                            "register_table": "test.cleaned_data",
                            "keys": ["id"],
                        },
                    ),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        executor.pipeline_name = "silver"

        result = executor._create_output_record(config, row_count=1000)

        assert result is not None
        assert result["pipeline_name"] == "silver"
        assert result["node_name"] == "cleaned_data"
        assert result["connection_name"] == "goat_prod"
        assert result["path"] == "OEE/silver/cleaned_data"
        assert result["table_name"] == "test.cleaned_data"
        assert result["row_count"] == 1000

    def test_write_block_takes_precedence(self):
        """Test that explicit write block takes precedence over merge params."""
        from odibi.config import TransformConfig, TransformStep, WriteConfig
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transform=TransformConfig(
                steps=[
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "merge_conn",
                            "path": "merge/path",
                            "keys": ["id"],
                        },
                    ),
                ]
            ),
            write=WriteConfig(
                connection="write_conn",
                path="write/path",
                format="delta",
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        executor.pipeline_name = "test"

        result = executor._create_output_record(config, row_count=500)

        assert result["connection_name"] == "write_conn"
        assert result["path"] == "write/path"

    def test_extract_toplevel_merge_transformer(self):
        """Test extracting output from top-level merge transformer."""
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transformer="merge",
            params={
                "connection": "goat_prod",
                "path": "OEE/silver/merged_data",
                "register_table": "test.merged_data",
                "keys": ["id"],
                "strategy": "upsert",
            },
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is not None
        assert result["connection"] == "goat_prod"
        assert result["path"] == "OEE/silver/merged_data"
        assert result["register_table"] == "test.merged_data"

    def test_extract_toplevel_scd2_transformer(self):
        """Test extracting output from top-level scd2 transformer."""
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transformer="scd2",
            params={
                "connection": "goat_prod",
                "target": "OEE/silver/dim_customer",
                "keys": ["customer_id"],
                "track_cols": ["name", "address"],
                "effective_time_col": "_extracted_at",
            },
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result is not None
        assert result["connection"] == "goat_prod"
        assert result["path"] == "OEE/silver/dim_customer"

    def test_transform_step_takes_precedence_over_transformer(self):
        """Test that merge in transform steps takes precedence over top-level transformer."""
        from odibi.config import TransformConfig, TransformStep
        from odibi.node import NodeExecutor

        config = NodeConfig(
            name="test_node",
            transformer="merge",
            params={
                "connection": "toplevel_conn",
                "path": "toplevel/path",
                "keys": ["id"],
            },
            transform=TransformConfig(
                steps=[
                    TransformStep(
                        function="merge",
                        params={
                            "connection": "step_conn",
                            "path": "step/path",
                            "keys": ["id"],
                        },
                    ),
                ]
            ),
        )

        executor = NodeExecutor.__new__(NodeExecutor)
        result = executor._extract_output_from_transform_steps(config)

        assert result["connection"] == "step_conn"
        assert result["path"] == "step/path"


# ============================================================================
# CatalogManager: register_outputs_from_config Tests
# ============================================================================


class TestRegisterOutputsFromConfig:
    """Tests for pre-registering outputs from pipeline config."""

    def test_register_outputs_with_write_block(self, catalog_manager):
        """Test registering outputs from nodes with write blocks."""
        from odibi.config import PipelineConfig, WriteConfig

        catalog_manager.bootstrap()

        pipeline_config = PipelineConfig(
            pipeline="test_pipeline",
            nodes=[
                NodeConfig(
                    name="node_with_write",
                    write=WriteConfig(
                        connection="goat_prod",
                        path="silver/data",
                        format="delta",
                        register_table="test.silver_data",
                    ),
                ),
            ],
        )

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 1
        output = catalog_manager.get_node_output("test_pipeline", "node_with_write")
        assert output is not None
        assert output["path"] == "silver/data"
        assert output["table_name"] == "test.silver_data"

    def test_register_outputs_with_merge_step(self, catalog_manager):
        """Test registering outputs from nodes with merge in transform steps."""
        from odibi.config import PipelineConfig, TransformConfig, TransformStep

        catalog_manager.bootstrap()

        pipeline_config = PipelineConfig(
            pipeline="silver",
            nodes=[
                NodeConfig(
                    name="cleaned_data",
                    transform=TransformConfig(
                        steps=[
                            TransformStep(sql="SELECT * FROM df"),
                            TransformStep(
                                function="merge",
                                params={
                                    "connection": "goat_prod",
                                    "path": "OEE/silver/cleaned_data",
                                    "register_table": "test.cleaned_data",
                                    "keys": ["id"],
                                },
                            ),
                        ]
                    ),
                ),
            ],
        )

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 1
        output = catalog_manager.get_node_output("silver", "cleaned_data")
        assert output is not None
        assert output["path"] == "OEE/silver/cleaned_data"
        assert output["table_name"] == "test.cleaned_data"

    def test_register_outputs_with_scd2_transformer(self, catalog_manager):
        """Test registering outputs from nodes with scd2 as top-level transformer."""
        from odibi.config import PipelineConfig

        catalog_manager.bootstrap()

        pipeline_config = PipelineConfig(
            pipeline="silver",
            nodes=[
                NodeConfig(
                    name="dim_product",
                    transformer="scd2",
                    params={
                        "connection": "goat_prod",
                        "target": "OEE/silver/dim_product",
                        "keys": ["product_id"],
                        "track_cols": ["name", "status"],
                        "effective_time_col": "_extracted_at",
                    },
                ),
            ],
        )

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 1
        output = catalog_manager.get_node_output("silver", "dim_product")
        assert output is not None
        assert output["path"] == "OEE/silver/dim_product"

    def test_register_multiple_nodes(self, catalog_manager):
        """Test registering outputs from multiple nodes in one pipeline."""
        from odibi.config import PipelineConfig, TransformConfig, TransformStep, WriteConfig

        catalog_manager.bootstrap()

        pipeline_config = PipelineConfig(
            pipeline="silver",
            nodes=[
                NodeConfig(
                    name="node1",
                    write=WriteConfig(connection="conn", path="path1", format="delta"),
                ),
                NodeConfig(
                    name="node2",
                    transform=TransformConfig(
                        steps=[
                            TransformStep(
                                function="merge",
                                params={"connection": "conn", "path": "path2", "keys": ["id"]},
                            ),
                        ]
                    ),
                ),
                NodeConfig(
                    name="node3",
                    transformer="scd2",
                    params={
                        "connection": "conn",
                        "target": "path3",
                        "keys": ["id"],
                        "track_cols": ["name"],
                        "effective_time_col": "ts",
                    },
                ),
            ],
        )

        count = catalog_manager.register_outputs_from_config(pipeline_config)

        assert count == 3
        assert catalog_manager.get_node_output("silver", "node1") is not None
        assert catalog_manager.get_node_output("silver", "node2") is not None
        assert catalog_manager.get_node_output("silver", "node3") is not None

    def test_skip_nodes_without_output(self, catalog_manager):
        """Test that nodes without output locations are skipped."""
        from odibi.config import PipelineConfig, TransformConfig, TransformStep

        catalog_manager.bootstrap()

        pipeline_config = PipelineConfig(
            pipeline="test",
            nodes=[
                NodeConfig(
                    name="no_output_node",
                    transform=TransformConfig(
                        steps=[
                            TransformStep(sql="SELECT * FROM df"),
                            TransformStep(function="deduplicate", params={"keys": ["id"]}),
                        ]
                    ),
                ),
            ],
        )

        count = catalog_manager.register_outputs_from_config(pipeline_config)
        assert count == 0
