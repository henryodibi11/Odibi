"""Tests for uncovered validators and edge cases in odibi.config."""

import pytest
from pydantic import ValidationError

from odibi.config import (
    # Time Travel
    TimeTravelConfig,
    # Simulation generators
    CategoricalGeneratorConfig,
    GeoGeneratorConfig,
    # Simulation
    SimulationScope,
    SimulationConfig,
    EntityConfig,
    ColumnGeneratorConfig,
    SimulationDataType,
    # Read / Transform
    ReadConfig,
    ReadFormat,
    TransformStep,
    # Streaming
    TriggerConfig,
    # Schema
    SchemaPolicyConfig,
    SchemaMode,
    OnNewColumns,
    # Node / Pipeline / Project
    NodeConfig,
    PipelineConfig,
    ProjectConfig,
    StoryConfig,
    SystemConfig,
    LocalConnectionConfig,
)


# ── TimeTravelConfig (lines 1222-1228) ───────────────────────────


def test_time_travel_both_version_and_timestamp_raises():
    with pytest.raises(ValidationError, match="Cannot specify both"):
        TimeTravelConfig(as_of_version=10, as_of_timestamp="2023-10-01T12:00:00Z")


def test_time_travel_version_only():
    cfg = TimeTravelConfig(as_of_version=5)
    assert cfg.as_of_version == 5
    assert cfg.as_of_timestamp is None


def test_time_travel_timestamp_only():
    cfg = TimeTravelConfig(as_of_timestamp="2023-10-01")
    assert cfg.as_of_timestamp == "2023-10-01"


# ── CategoricalGeneratorConfig (lines 1315-1327) ────────────────


def test_categorical_empty_values_raises():
    with pytest.raises(ValidationError, match="at least one value"):
        CategoricalGeneratorConfig(type="categorical", values=[])


def test_categorical_weights_length_mismatch():
    with pytest.raises(ValidationError, match="Weights length"):
        CategoricalGeneratorConfig(type="categorical", values=["A", "B"], weights=[0.5])


def test_categorical_weights_dont_sum_to_one():
    with pytest.raises(ValidationError, match="Weights must sum to 1.0"):
        CategoricalGeneratorConfig(type="categorical", values=["A", "B"], weights=[0.5, 0.3])


def test_categorical_valid_with_weights():
    cfg = CategoricalGeneratorConfig(type="categorical", values=["A", "B"], weights=[0.6, 0.4])
    assert cfg.weights == [0.6, 0.4]


# ── GeoGeneratorConfig (lines 1473-1482) ────────────────────────


def test_geo_bbox_wrong_length():
    with pytest.raises(ValidationError, match="exactly 4 values"):
        GeoGeneratorConfig(type="geo", bbox=[1.0, 2.0])


def test_geo_bbox_invalid_latitude():
    with pytest.raises(ValidationError, match="Latitude must be in"):
        GeoGeneratorConfig(type="geo", bbox=[-100.0, 0.0, 50.0, 10.0])


def test_geo_bbox_invalid_longitude():
    with pytest.raises(ValidationError, match="Longitude must be in"):
        GeoGeneratorConfig(type="geo", bbox=[0.0, -200.0, 50.0, 10.0])


def test_geo_bbox_min_lat_gte_max_lat():
    with pytest.raises(ValidationError, match="min_lat"):
        GeoGeneratorConfig(type="geo", bbox=[50.0, 0.0, 10.0, 20.0])


def test_geo_bbox_min_lon_gte_max_lon():
    with pytest.raises(ValidationError, match="min_lon"):
        GeoGeneratorConfig(type="geo", bbox=[0.0, 20.0, 50.0, 10.0])


def test_geo_valid():
    cfg = GeoGeneratorConfig(type="geo", bbox=[-10.0, -20.0, 10.0, 20.0])
    assert cfg.bbox == [-10.0, -20.0, 10.0, 20.0]


# ── SimulationConfig (lines 2233-2266) ──────────────────────────


def _make_sim_config(columns=None, entities=None, scope=None):
    """Helper to build a minimal SimulationConfig."""
    if scope is None:
        scope = SimulationScope(start_time="2026-01-01T00:00:00Z", timestep="5m", row_count=10)
    if entities is None:
        entities = EntityConfig(count=2)
    if columns is None:
        columns = [
            ColumnGeneratorConfig(
                name="val",
                data_type=SimulationDataType.FLOAT,
                generator={"type": "range", "min": 0.0, "max": 100.0},
            )
        ]
    return SimulationConfig(scope=scope, entities=entities, columns=columns)


def test_simulation_get_key_columns_empty():
    cfg = _make_sim_config()
    assert cfg.get_key_columns() == []


def test_simulation_get_key_columns_with_uuid():
    cols = [
        ColumnGeneratorConfig(
            name="id",
            data_type=SimulationDataType.STRING,
            generator={"type": "uuid"},
        ),
        ColumnGeneratorConfig(
            name="val",
            data_type=SimulationDataType.FLOAT,
            generator={"type": "range", "min": 0.0, "max": 10.0},
        ),
    ]
    cfg = _make_sim_config(columns=cols)
    assert cfg.get_key_columns() == ["id"]


def test_simulation_validate_write_compat_no_keys_raises():
    cfg = _make_sim_config()
    with pytest.raises(ValueError, match="requires key columns"):
        cfg.validate_write_compatibility("upsert")


def test_simulation_validate_write_compat_missing_write_keys():
    cols = [
        ColumnGeneratorConfig(
            name="id",
            data_type=SimulationDataType.STRING,
            generator={"type": "uuid"},
        ),
    ]
    cfg = _make_sim_config(columns=cols)
    with pytest.raises(ValueError, match="not found in simulation columns"):
        cfg.validate_write_compatibility("append_once", write_keys=["nonexistent"])


def test_simulation_validate_write_compat_valid():
    cols = [
        ColumnGeneratorConfig(
            name="id",
            data_type=SimulationDataType.STRING,
            generator={"type": "uuid"},
        ),
    ]
    cfg = _make_sim_config(columns=cols)
    cfg.validate_write_compatibility("upsert", write_keys=["id"])


# ── ReadConfig validators (lines 2556-2617) ─────────────────────


def test_readconfig_query_and_sql_file_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        ReadConfig(
            format=ReadFormat.SQL,
            query="SELECT 1",
            sql_file="foo.sql",
        )


def test_readconfig_query_moved_to_options():
    cfg = ReadConfig(format=ReadFormat.SQL, query="SELECT 1")
    assert cfg.options["query"] == "SELECT 1"


def test_readconfig_query_in_both_top_and_options_conflict():
    with pytest.raises(ValidationError, match="different values"):
        ReadConfig(
            format=ReadFormat.SQL,
            query="SELECT 1",
            options={"query": "SELECT 2"},
        )


def test_readconfig_filter_moved_to_options():
    cfg = ReadConfig(format=ReadFormat.SQL, table="t", filter="x > 1")
    assert cfg.options["filter"] == "x > 1"


def test_readconfig_filter_conflict():
    with pytest.raises(ValidationError, match="different values"):
        ReadConfig(
            format=ReadFormat.SQL,
            table="t",
            filter="x > 1",
            options={"filter": "x > 2"},
        )


def test_readconfig_table_and_path_exclusive():
    with pytest.raises(ValidationError, match="mutually exclusive"):
        ReadConfig(format=ReadFormat.PARQUET, table="t", path="/p")


def test_readconfig_sql_requires_source():
    with pytest.raises(ValidationError, match="either 'table', 'query', or 'sql_file'"):
        ReadConfig(format=ReadFormat.SQL)


def test_readconfig_no_source_at_all():
    with pytest.raises(ValidationError, match="No data source specified"):
        ReadConfig(format=ReadFormat.PARQUET)


def test_readconfig_simulation_needs_no_table_or_path():
    cfg = ReadConfig(format=ReadFormat.SIMULATION)
    assert cfg.table is None and cfg.path is None


# ── TransformStep (lines 2698-2708) ─────────────────────────────


def test_transform_step_no_type_raises():
    with pytest.raises(ValidationError, match="No step type specified"):
        TransformStep()


def test_transform_step_multiple_types_raises():
    with pytest.raises(ValidationError, match="Multiple step types"):
        TransformStep(sql="SELECT 1", function="my_func")


def test_transform_step_valid_sql():
    step = TransformStep(sql="SELECT * FROM df")
    assert step.sql == "SELECT * FROM df"


# ── TriggerConfig (lines 3897-3913) ──────────────────────────────


def test_trigger_multiple_types_raises():
    with pytest.raises(ValidationError, match="Multiple trigger types"):
        TriggerConfig(processing_time="10 seconds", once=True)


def test_trigger_processing_time_and_continuous_raises():
    with pytest.raises(ValidationError, match="Multiple trigger types"):
        TriggerConfig(processing_time="10 seconds", continuous="1 second")


def test_trigger_single_processing_time():
    cfg = TriggerConfig(processing_time="10 seconds")
    assert cfg.processing_time == "10 seconds"


def test_trigger_once():
    cfg = TriggerConfig(once=True)
    assert cfg.once is True


def test_trigger_available_now():
    cfg = TriggerConfig(available_now=True)
    assert cfg.available_now is True


# ── SchemaPolicyConfig (lines 4308-4314) ────────────────────────


def test_schema_policy_evolve_defaults_on_new_columns():
    cfg = SchemaPolicyConfig(mode=SchemaMode.EVOLVE)
    assert cfg.on_new_columns == OnNewColumns.ADD_NULLABLE


def test_schema_policy_enforce_defaults_on_new_columns():
    cfg = SchemaPolicyConfig(mode=SchemaMode.ENFORCE)
    assert cfg.on_new_columns == OnNewColumns.IGNORE


def test_schema_policy_explicit_on_new_columns_not_overridden():
    cfg = SchemaPolicyConfig(mode=SchemaMode.EVOLVE, on_new_columns=OnNewColumns.FAIL)
    assert cfg.on_new_columns == OnNewColumns.FAIL


# ── NodeConfig validators (lines 4758-4799) ─────────────────────


def test_node_read_and_inputs_exclusive():
    with pytest.raises(ValidationError, match="Cannot have both 'read' and 'inputs'"):
        NodeConfig(
            name="test_node",
            read=ReadConfig(format=ReadFormat.PARQUET, path="/data"),
            inputs={"a": "$bronze.node1"},
            write={"connection": "c", "format": "parquet", "path": "/out"},
        )


def test_node_transformer_without_params_raises():
    with pytest.raises(ValidationError, match="'transformer' is set but 'params' is empty"):
        NodeConfig(
            name="test_node",
            transformer="deduplicate",
            write={"connection": "c", "format": "parquet", "path": "/out"},
        )


def test_node_explanation_and_file_exclusive():
    with pytest.raises(
        ValidationError, match="Cannot have both 'explanation' and 'explanation_file'"
    ):
        NodeConfig(
            name="test_node",
            explanation="Some text",
            explanation_file="docs/explain.md",
            write={"connection": "c", "format": "parquet", "path": "/out"},
        )


# ── PipelineConfig (lines 4861-4895) ────────────────────────────


def test_pipeline_duplicate_node_names():
    with pytest.raises(ValidationError, match="Duplicate node names"):
        PipelineConfig(
            pipeline="test",
            nodes=[
                NodeConfig(
                    name="dup", write={"connection": "c", "format": "parquet", "path": "/a"}
                ),
                NodeConfig(
                    name="dup", write={"connection": "c", "format": "parquet", "path": "/b"}
                ),
            ],
        )


def test_pipeline_auto_depends_on_from_inputs():
    """Inputs referencing same pipeline auto-populate depends_on (lines 4867-4895)."""
    pipeline = PipelineConfig(
        pipeline="silver",
        nodes=[
            NodeConfig(
                name="source_node", write={"connection": "c", "format": "parquet", "path": "/a"}
            ),
            NodeConfig(
                name="consumer",
                inputs={"src": "$silver.source_node"},
                write={"connection": "c", "format": "parquet", "path": "/b"},
            ),
        ],
    )
    consumer = [n for n in pipeline.nodes if n.name == "consumer"][0]
    assert "source_node" in consumer.depends_on


def test_pipeline_cross_pipeline_input_no_auto_depends():
    """Inputs referencing different pipeline should NOT auto-populate depends_on."""
    pipeline = PipelineConfig(
        pipeline="gold",
        nodes=[
            NodeConfig(
                name="consumer",
                inputs={"src": "$silver.some_node"},
                write={"connection": "c", "format": "parquet", "path": "/b"},
            ),
        ],
    )
    consumer = pipeline.nodes[0]
    assert "some_node" not in consumer.depends_on


# ── ProjectConfig validators (lines 5556-5654) ──────────────────


def _make_project_config(**overrides):
    """Build a minimal valid ProjectConfig, applying overrides."""
    base = dict(
        project="test_project",
        engine="pandas",
        connections={
            "local": LocalConnectionConfig(base_path="./data"),
        },
        pipelines=[
            PipelineConfig(
                pipeline="test",
                nodes=[
                    NodeConfig(
                        name="n1",
                        write={"connection": "local", "format": "parquet", "path": "/out"},
                    )
                ],
            )
        ],
        story=StoryConfig(connection="local", path="stories/"),
        system=SystemConfig(connection="local"),
    )
    base.update(overrides)
    return ProjectConfig(**base)


def test_project_story_connection_missing():
    with pytest.raises(ValidationError, match="Story connection.*not found"):
        _make_project_config(
            story=StoryConfig(connection="nonexistent", path="s/"),
        )


def test_project_system_connection_missing():
    with pytest.raises(ValidationError, match="System connection.*not found"):
        _make_project_config(
            system=SystemConfig(connection="nonexistent"),
        )


def test_project_system_sql_server_connection_raises():
    with pytest.raises(ValidationError, match="requires storage"):
        _make_project_config(
            connections={
                "local": LocalConnectionConfig(base_path="./data"),
                "sql": {"type": "sql_server", "connection_string": "fake"},
            },
            system=SystemConfig(connection="sql"),
        )


def test_project_system_sync_to_missing_connection():
    from odibi.config import SyncToConfig

    with pytest.raises(ValidationError, match="sync_to connection.*not found"):
        _make_project_config(
            system=SystemConfig(
                connection="local",
                sync_to=SyncToConfig(connection="nonexistent"),
            ),
        )


def test_project_valid_minimal():
    cfg = _make_project_config()
    assert cfg.project == "test_project"


def test_project_environments_invalid_field():
    with pytest.raises(ValidationError, match="non-overrideable fields"):
        _make_project_config(
            environments={"dev": {"project": "bad"}},
        )


def test_project_environments_not_dict():
    with pytest.raises(ValidationError, match="valid dictionary"):
        _make_project_config(
            environments={"dev": "not_a_dict"},
        )


def test_project_environments_valid():
    cfg = _make_project_config(
        environments={"dev": {"engine": "polars"}},
    )
    assert "dev" in cfg.environments
