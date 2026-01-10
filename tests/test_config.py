"""Tests for configuration validation (Pydantic schemas)."""

import pytest
from pydantic import ValidationError

from odibi.config import (
    EngineType,
    LocalConnectionConfig,
    NodeConfig,
    PipelineConfig,
    ProjectConfig,
    ReadConfig,
    StreamingWriteConfig,
    TransformConfig,
    TriggerConfig,
    WriteConfig,
    WriteMode,
)


class TestReadConfig:
    """Test ReadConfig validation."""

    def test_valid_read_config_with_path(self):
        """Valid config with path should parse correctly."""
        config = ReadConfig(connection="local", format="csv", path="data/input.csv")
        assert config.connection == "local"
        assert config.format == "csv"
        assert config.path == "data/input.csv"
        assert config.table is None

    def test_valid_read_config_with_table(self):
        """Valid config with table should parse correctly."""
        config = ReadConfig(connection="delta", format="delta", table="sales_bronze")
        assert config.table == "sales_bronze"
        assert config.path is None

    def test_read_config_requires_path_or_table(self):
        """Config must have either path or table or query in options."""
        with pytest.raises(ValidationError) as exc_info:
            ReadConfig(
                connection="local",
                format="csv",
                # Missing path, table, AND query option
            )
        assert "No data source specified" in str(exc_info.value)

    def test_valid_read_config_with_query_option(self):
        """Valid config with query in options should parse correctly without table/path."""
        config = ReadConfig(
            connection="sql",
            format="sql",
            options={"query": "SELECT * FROM users"},
        )
        assert config.table is None
        assert config.path is None
        assert config.options["query"] == "SELECT * FROM users"

    def test_read_config_with_options(self):
        """Config can include format-specific options."""
        config = ReadConfig(
            connection="local",
            format="csv",
            path="data.csv",
            options={"delimiter": ",", "header": True},
        )
        assert config.options["delimiter"] == ","
        assert config.options["header"] is True


class TestWriteConfig:
    """Test WriteConfig validation."""

    def test_valid_write_config(self):
        """Valid write config should parse correctly."""
        config = WriteConfig(
            connection="local",
            format="parquet",
            path="output/result.parquet",
            mode=WriteMode.OVERWRITE,
        )
        assert config.mode == WriteMode.OVERWRITE
        assert config.format == "parquet"

    def test_write_config_default_mode(self):
        """Default write mode should be OVERWRITE."""
        config = WriteConfig(connection="local", format="parquet", path="output.parquet")
        assert config.mode == WriteMode.OVERWRITE

    def test_write_config_append_mode(self):
        """Can set mode to APPEND."""
        config = WriteConfig(
            connection="local", format="csv", path="output.csv", mode=WriteMode.APPEND
        )
        assert config.mode == WriteMode.APPEND

    def test_write_config_with_streaming(self):
        """Write config with streaming configuration."""
        config = WriteConfig(
            connection="delta",
            format="delta",
            table="events_stream",
            streaming=StreamingWriteConfig(
                output_mode="append",
                checkpoint_location="/checkpoints/events",
            ),
        )
        assert config.streaming is not None
        assert config.streaming.output_mode == "append"
        assert config.streaming.checkpoint_location == "/checkpoints/events"

    def test_write_config_streaming_with_trigger(self):
        """Write config with streaming and trigger."""
        config = WriteConfig(
            connection="delta",
            format="delta",
            table="events_stream",
            streaming=StreamingWriteConfig(
                output_mode="append",
                checkpoint_location="/checkpoints/events",
                trigger=TriggerConfig(processing_time="10 seconds"),
            ),
        )
        assert config.streaming.trigger is not None
        assert config.streaming.trigger.processing_time == "10 seconds"

    def test_write_config_streaming_once_trigger(self):
        """Write config with once trigger for batch-like streaming."""
        config = WriteConfig(
            connection="delta",
            format="delta",
            table="events_batch",
            streaming=StreamingWriteConfig(
                output_mode="append",
                checkpoint_location="/checkpoints/events_batch",
                trigger=TriggerConfig(once=True),
                await_termination=True,
            ),
        )
        assert config.streaming.trigger.once is True
        assert config.streaming.await_termination is True


class TestStreamingWriteConfig:
    """Test StreamingWriteConfig validation."""

    def test_valid_streaming_write_config(self):
        """Valid streaming config should parse correctly."""
        config = StreamingWriteConfig(
            output_mode="append",
            checkpoint_location="/checkpoints/test",
        )
        assert config.output_mode == "append"
        assert config.checkpoint_location == "/checkpoints/test"
        assert config.trigger is None
        assert config.await_termination is False

    def test_streaming_write_config_default_output_mode(self):
        """Default output mode should be append."""
        config = StreamingWriteConfig(checkpoint_location="/checkpoints/test")
        assert config.output_mode == "append"

    def test_streaming_write_config_with_query_name(self):
        """Streaming config with query name."""
        config = StreamingWriteConfig(
            checkpoint_location="/checkpoints/test",
            query_name="my_streaming_query",
        )
        assert config.query_name == "my_streaming_query"

    def test_streaming_write_config_complete_mode(self):
        """Streaming config with complete output mode."""
        config = StreamingWriteConfig(
            output_mode="complete",
            checkpoint_location="/checkpoints/test",
        )
        assert config.output_mode == "complete"

    def test_streaming_write_requires_checkpoint_location(self):
        """Streaming config requires checkpoint_location."""
        with pytest.raises(ValidationError):
            StreamingWriteConfig(output_mode="append")


class TestTriggerConfig:
    """Test TriggerConfig validation."""

    def test_processing_time_trigger(self):
        """Processing time trigger should parse correctly."""
        config = TriggerConfig(processing_time="10 seconds")
        assert config.processing_time == "10 seconds"
        assert config.once is None

    def test_once_trigger(self):
        """Once trigger should parse correctly."""
        config = TriggerConfig(once=True)
        assert config.once is True
        assert config.processing_time is None

    def test_available_now_trigger(self):
        """Available now trigger should parse correctly."""
        config = TriggerConfig(available_now=True)
        assert config.available_now is True

    def test_continuous_trigger(self):
        """Continuous trigger should parse correctly."""
        config = TriggerConfig(continuous="1 second")
        assert config.continuous == "1 second"

    def test_multiple_triggers_raises_error(self):
        """Only one trigger type can be specified."""
        with pytest.raises(ValidationError) as exc_info:
            TriggerConfig(processing_time="10 seconds", once=True)
        assert "Multiple trigger types specified" in str(exc_info.value)

    def test_empty_trigger_is_valid(self):
        """Empty trigger config is valid (defaults to processing as fast as possible)."""
        config = TriggerConfig()
        assert config.processing_time is None
        assert config.once is None


class TestTransformConfig:
    """Test TransformConfig validation."""

    def test_transform_with_sql_strings(self):
        """Transform can have SQL string steps."""
        config = TransformConfig(
            steps=[
                "SELECT * FROM input WHERE value > 0",
                "SELECT id, SUM(amount) as total FROM __previous__ GROUP BY id",
            ]
        )
        assert len(config.steps) == 2
        assert isinstance(config.steps[0], str)

    def test_transform_with_dict_steps(self):
        """Transform can have structured dict steps (converted to TransformStep)."""
        config = TransformConfig(
            steps=[
                {"function": "clean_data", "params": {"threshold": 0.5}},
                {"operation": "pivot", "params": {"group_by": ["id"]}},
            ]
        )
        assert len(config.steps) == 2
        # Pydantic converts dict to TransformStep model
        from odibi.config import TransformStep

        assert isinstance(config.steps[0], (dict, TransformStep))
        # Verify it has the function attribute
        if hasattr(config.steps[0], "function"):
            assert config.steps[0].function == "clean_data"

    def test_transform_with_sql_file_step(self):
        """Transform can have sql_file step."""
        from odibi.config import TransformStep

        config = TransformConfig(
            steps=[
                {"sql_file": "sql/transform.sql"},
            ]
        )
        assert len(config.steps) == 1
        step = config.steps[0]
        assert isinstance(step, TransformStep)
        assert step.sql_file == "sql/transform.sql"
        assert step.sql is None
        assert step.function is None
        assert step.operation is None

    def test_transform_step_requires_exactly_one_type(self):
        """TransformStep must have exactly one of sql, sql_file, function, operation."""
        from odibi.config import TransformStep

        # No type specified - should fail
        with pytest.raises(ValidationError):
            TransformStep()

        # Multiple types specified - should fail
        with pytest.raises(ValidationError):
            TransformStep(sql="SELECT 1", sql_file="file.sql")

        with pytest.raises(ValidationError):
            TransformStep(sql_file="file.sql", function="my_func")

    def test_transform_mixed_steps(self):
        """Transform can mix sql, sql_file, function, and operation steps."""
        from odibi.config import TransformStep

        config = TransformConfig(
            steps=[
                "SELECT * FROM df WHERE active = true",
                {"sql_file": "pipelines/silver/sql/aggregate.sql"},
                {"function": "apply_rules", "params": {"threshold": 0.5}},
                {"sql": "SELECT * FROM df ORDER BY id"},
                {"operation": "drop_duplicates", "params": {"subset": ["id"]}},
            ]
        )
        assert len(config.steps) == 5
        assert isinstance(config.steps[0], str)
        assert isinstance(config.steps[1], TransformStep)
        assert config.steps[1].sql_file == "pipelines/silver/sql/aggregate.sql"


class TestNodeConfig:
    """Test NodeConfig validation."""

    def test_valid_node_with_read_only(self):
        """Node can have only read operation."""
        config = NodeConfig(
            name="load_data", read=ReadConfig(connection="local", format="csv", path="input.csv")
        )
        assert config.name == "load_data"
        assert config.read is not None
        assert config.transform is None
        assert config.write is None

    def test_valid_node_with_all_operations(self):
        """Node can have read, transform, and write."""
        config = NodeConfig(
            name="full_pipeline",
            read=ReadConfig(connection="local", format="csv", path="input.csv"),
            transform=TransformConfig(steps=["SELECT * FROM full_pipeline"]),
            write=WriteConfig(connection="local", format="parquet", path="output.parquet"),
        )
        assert config.read is not None
        assert config.transform is not None
        assert config.write is not None

    def test_node_requires_at_least_one_operation(self):
        """Node must have at least one of: read, inputs, transform, write, transformer."""
        with pytest.raises(ValidationError) as exc_info:
            NodeConfig(name="empty_node")
        assert "must have at least one of: read, inputs, transform, write, transformer" in str(
            exc_info.value
        )

    def test_node_with_dependencies(self):
        """Node can declare dependencies."""
        config = NodeConfig(
            name="process",
            depends_on=["load_data", "load_reference"],
            transform=TransformConfig(steps=["SELECT * FROM load_data"]),
        )
        assert config.depends_on == ["load_data", "load_reference"]

    def test_node_with_cache(self):
        """Node can enable caching."""
        config = NodeConfig(
            name="cached_node",
            read=ReadConfig(connection="local", format="csv", path="data.csv"),
            cache=True,
        )
        assert config.cache is True

    def test_node_cache_default_false(self):
        """Cache should default to False."""
        config = NodeConfig(
            name="node", read=ReadConfig(connection="local", format="csv", path="data.csv")
        )
        assert config.cache is False


class TestPipelineConfig:
    """Test PipelineConfig validation."""

    def test_valid_pipeline(self):
        """Valid pipeline with multiple nodes."""
        config = PipelineConfig(
            pipeline="test_pipeline",
            description="Test pipeline",
            nodes=[
                NodeConfig(
                    name="node1",
                    read=ReadConfig(connection="local", format="csv", path="input.csv"),
                ),
                NodeConfig(
                    name="node2",
                    depends_on=["node1"],
                    transform=TransformConfig(steps=["SELECT * FROM node1"]),
                ),
            ],
        )
        assert config.pipeline == "test_pipeline"
        assert len(config.nodes) == 2

    def test_pipeline_rejects_duplicate_node_names(self):
        """Pipeline cannot have duplicate node names."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineConfig(
                pipeline="test",
                nodes=[
                    NodeConfig(
                        name="duplicate",
                        read=ReadConfig(connection="local", format="csv", path="a.csv"),
                    ),
                    NodeConfig(
                        name="duplicate",
                        read=ReadConfig(connection="local", format="csv", path="b.csv"),
                    ),
                ],
            )
        assert "Duplicate node names" in str(exc_info.value)

    def test_pipeline_with_layer(self):
        """Pipeline can specify a layer."""
        config = PipelineConfig(
            pipeline="bronze_pipeline",
            layer="bronze",
            nodes=[
                NodeConfig(
                    name="load", read=ReadConfig(connection="local", format="csv", path="data.csv")
                )
            ],
        )
        assert config.layer == "bronze"


class TestProjectConfig:
    """Test ProjectConfig validation."""

    def test_minimal_project_config(self):
        """Minimal valid project config requires connections, pipelines, story, and system."""
        from odibi.config import StoryConfig, SystemConfig

        config = ProjectConfig(
            project="My Project",
            connections={"data": {"type": "local", "base_path": "./data"}},
            pipelines=[
                PipelineConfig(
                    pipeline="test_pipeline",
                    nodes=[
                        NodeConfig(
                            name="test_node",
                            read=ReadConfig(connection="data", path="test.csv", format="csv"),
                        )
                    ],
                )
            ],
            story=StoryConfig(connection="data", path="stories/"),
            system=SystemConfig(connection="data"),
        )
        assert config.project == "My Project"
        assert config.engine == EngineType.PANDAS  # Default
        assert config.version == "1.0.0"  # Default

    def test_project_with_connections(self):
        """Project can define connections."""
        from odibi.config import StoryConfig, SystemConfig

        config = ProjectConfig(
            project="Test",
            connections={"local": {"type": "local", "base_path": "./data"}},
            pipelines=[
                PipelineConfig(
                    pipeline="test",
                    nodes=[
                        NodeConfig(
                            name="test_node",
                            read=ReadConfig(connection="local", path="test.csv", format="csv"),
                        )
                    ],
                )
            ],
            story=StoryConfig(connection="local", path="stories/"),
            system=SystemConfig(connection="local"),
        )
        assert "local" in config.connections
        # connections are models, so access with dot notation, not dict subscription
        assert config.connections["local"].type.value == "local"

    def test_project_default_engine_is_pandas(self):
        """Default engine should be Pandas."""
        from odibi.config import StoryConfig, SystemConfig

        config = ProjectConfig(
            project="Test",
            connections={"data": {"type": "local"}},
            pipelines=[
                PipelineConfig(
                    pipeline="test",
                    nodes=[
                        NodeConfig(
                            name="node",
                            read=ReadConfig(connection="data", path="test.csv", format="csv"),
                        )
                    ],
                )
            ],
            story=StoryConfig(connection="data", path="stories/"),
            system=SystemConfig(connection="data"),
        )
        assert config.engine == EngineType.PANDAS

    def test_project_can_set_spark_engine(self):
        """Can set engine to Spark."""
        from odibi.config import StoryConfig, SystemConfig

        config = ProjectConfig(
            project="Test",
            engine=EngineType.SPARK,
            connections={"data": {"type": "local"}},
            pipelines=[
                PipelineConfig(
                    pipeline="test",
                    nodes=[
                        NodeConfig(
                            name="node",
                            read=ReadConfig(connection="data", path="test.csv", format="csv"),
                        )
                    ],
                )
            ],
            story=StoryConfig(connection="data", path="stories/"),
            system=SystemConfig(connection="data"),
        )
        assert config.engine == EngineType.SPARK

    def test_project_with_global_settings(self):
        """Project has settings for retry, logging at top level."""
        from odibi.config import StoryConfig, SystemConfig

        config = ProjectConfig(
            project="Test",
            connections={"data": {"type": "local"}},
            pipelines=[
                PipelineConfig(
                    pipeline="test",
                    nodes=[
                        NodeConfig(
                            name="node",
                            read=ReadConfig(connection="data", path="test.csv", format="csv"),
                        )
                    ],
                )
            ],
            story=StoryConfig(connection="data", path="stories/"),
            system=SystemConfig(connection="data"),
        )

        # Check top-level settings exist (no more nested defaults)
        assert config.retry is not None
        assert config.retry.enabled is True
        assert config.retry.max_attempts == 3
        assert config.logging.level.value == "INFO"
        assert config.story is not None
        assert config.story.connection == "data"
        assert config.story.auto_generate is True
        assert config.story.max_sample_rows == 10


class TestConnectionConfigs:
    """Test connection configuration schemas."""

    def test_local_connection_config(self):
        """LocalConnection config should validate."""
        config = LocalConnectionConfig(base_path="/data/local")
        assert config.type.value == "local"
        assert config.base_path == "/data/local"

    def test_local_connection_default_path(self):
        """LocalConnection has default base_path."""
        config = LocalConnectionConfig()
        assert config.base_path == "./data"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
