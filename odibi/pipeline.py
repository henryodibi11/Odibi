"""Pipeline executor and orchestration."""

from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import time
from pathlib import Path
import yaml

from odibi.config import PipelineConfig, ProjectConfig
from odibi.context import create_context
from odibi.graph import DependencyGraph
from odibi.node import Node, NodeResult
from odibi.engine import PandasEngine
from odibi.exceptions import DependencyError
from odibi.story import StoryGenerator
from odibi.connections import LocalConnection


@dataclass
class PipelineResults:
    """Results from pipeline execution."""

    pipeline_name: str
    completed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    node_results: Dict[str, NodeResult] = field(default_factory=dict)
    duration: float = 0.0
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    story_path: Optional[str] = None

    def get_node_result(self, name: str) -> Optional[NodeResult]:
        """Get result for specific node.

        Args:
            name: Node name

        Returns:
            NodeResult if available, None otherwise
        """
        return self.node_results.get(name)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation
        """
        return {
            "pipeline_name": self.pipeline_name,
            "completed": self.completed,
            "failed": self.failed,
            "skipped": self.skipped,
            "duration": self.duration,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "node_count": len(self.node_results),
        }


class Pipeline:
    """Pipeline executor and orchestrator."""

    def __init__(
        self,
        pipeline_config: PipelineConfig,
        engine: str = "pandas",
        connections: Optional[Dict[str, Any]] = None,
        generate_story: bool = True,
        story_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize pipeline.

        Args:
            pipeline_config: Pipeline configuration
            engine: Engine type ('pandas' or 'spark')
            connections: Available connections
            generate_story: Whether to generate execution stories
            story_config: Story generator configuration
        """
        self.config = pipeline_config
        self.engine_type = engine
        self.connections = connections or {}
        self.generate_story = generate_story

        # Initialize story generator
        story_config = story_config or {}
        self.story_generator = StoryGenerator(
            pipeline_name=pipeline_config.pipeline,
            max_sample_rows=story_config.get("max_sample_rows", 10),
            output_path=story_config.get("output_path", "stories/"),
        )

        # Initialize engine
        if engine == "pandas":
            self.engine = PandasEngine()
        else:
            raise ValueError(f"Unsupported engine: {engine}")

        # Initialize context
        self.context = create_context(engine)

        # Build dependency graph
        self.graph = DependencyGraph(pipeline_config.nodes)

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "PipelineManager":
        """Create PipelineManager from YAML file (recommended).

        This method now returns a PipelineManager that can run all or specific pipelines.

        Args:
            yaml_path: Path to YAML configuration file

        Returns:
            PipelineManager instance (use .run() to execute)

        Example:
            >>> from odibi.pipeline import Pipeline
            >>> manager = Pipeline.from_yaml("config.yaml")
            >>> results = manager.run()  # Run all pipelines
            >>> results = manager.run('bronze_to_silver')  # Run specific pipeline

        Note:
            For direct access to PipelineManager class:
            >>> from odibi.pipeline import PipelineManager
            >>> manager = PipelineManager.from_yaml("config.yaml")
        """
        # Delegate to PipelineManager
        return PipelineManager.from_yaml(yaml_path)

    def run(self, parallel: bool = False) -> PipelineResults:
        """Execute the pipeline.

        Args:
            parallel: Whether to use parallel execution (not implemented yet)

        Returns:
            PipelineResults with execution details
        """
        start_time = time.time()
        start_timestamp = datetime.now().isoformat()

        results = PipelineResults(pipeline_name=self.config.pipeline, start_time=start_timestamp)

        # Get execution order
        execution_order = self.graph.topological_sort()

        # Execute nodes in order
        for node_name in execution_order:
            # Skip if dependencies failed
            node_config = self.graph.nodes[node_name]
            deps_failed = any(dep in results.failed for dep in node_config.depends_on)

            if deps_failed:
                results.skipped.append(node_name)
                continue

            # Execute node
            node = Node(
                config=node_config,
                context=self.context,
                engine=self.engine,
                connections=self.connections,
                config_file=None,  # TODO: track config file
            )

            node_result = node.execute()
            results.node_results[node_name] = node_result

            if node_result.success:
                results.completed.append(node_name)
            else:
                results.failed.append(node_name)

        # Calculate duration
        results.duration = time.time() - start_time
        results.end_time = datetime.now().isoformat()

        # Generate story
        if self.generate_story:
            story_path = self.story_generator.generate(
                node_results=results.node_results,
                completed=results.completed,
                failed=results.failed,
                skipped=results.skipped,
                duration=results.duration,
                start_time=results.start_time,
                end_time=results.end_time,
                context=self.context,
            )
            results.story_path = story_path

        return results

    def run_node(self, node_name: str, mock_data: Optional[Dict[str, Any]] = None) -> NodeResult:
        """Execute a single node (for testing/debugging).

        Args:
            node_name: Name of node to execute
            mock_data: Optional mock data to register in context

        Returns:
            NodeResult
        """
        if node_name not in self.graph.nodes:
            raise ValueError(f"Node '{node_name}' not found in pipeline")

        # Register mock data if provided
        if mock_data:
            for name, data in mock_data.items():
                self.context.register(name, data)

        # Execute the node
        node_config = self.graph.nodes[node_name]
        node = Node(
            config=node_config,
            context=self.context,
            engine=self.engine,
            connections=self.connections,
        )

        return node.execute()

    def validate(self) -> Dict[str, Any]:
        """Validate pipeline without executing.

        Returns:
            Validation results
        """
        validation = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "node_count": len(self.graph.nodes),
            "execution_order": [],
        }

        try:
            # Test topological sort
            execution_order = self.graph.topological_sort()
            validation["execution_order"] = execution_order

        except DependencyError as e:
            validation["valid"] = False
            validation["errors"].append(str(e))

        # Check for missing connections
        for node in self.config.nodes:
            if node.read and node.read.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.read.connection}' not configured"
                )
            if node.write and node.write.connection not in self.connections:
                validation["warnings"].append(
                    f"Node '{node.name}': connection '{node.write.connection}' not configured"
                )

        return validation

    def get_execution_layers(self) -> List[List[str]]:
        """Get nodes grouped by execution layers.

        Returns:
            List of layers, where each layer is a list of node names
        """
        return self.graph.get_execution_layers()

    def visualize(self) -> str:
        """Get text visualization of pipeline.

        Returns:
            String representation of pipeline graph
        """
        return self.graph.visualize()


class PipelineManager:
    """Manages multiple pipelines from a YAML configuration."""

    def __init__(
        self,
        yaml_config: Dict[str, Any],
        engine: str = "pandas",
        connections: Optional[Dict[str, Any]] = None,
        story_config: Optional[Dict[str, Any]] = None,
    ):
        """Initialize pipeline manager.

        Args:
            yaml_config: Full YAML configuration dictionary
            engine: Engine type
            connections: Pre-configured connections
            story_config: Story generation configuration
        """
        self.config = yaml_config
        self.engine = engine
        self.connections = connections or {}
        self.story_config = story_config or {}
        self._pipelines: Dict[str, Pipeline] = {}

        # Create all pipeline instances
        for pipeline_config_dict in yaml_config.get("pipelines", []):
            pipeline_config = PipelineConfig(**pipeline_config_dict)
            pipeline_name = pipeline_config.pipeline

            self._pipelines[pipeline_name] = Pipeline(
                pipeline_config=pipeline_config,
                engine=engine,
                connections=connections,
                story_config=story_config,
            )

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "PipelineManager":
        """Create PipelineManager from YAML file.

        Args:
            yaml_path: Path to YAML configuration file

        Returns:
            PipelineManager instance ready to run pipelines

        Example:
            >>> manager = PipelineManager.from_yaml("config.yaml")
            >>> results = manager.run()  # Run all pipelines
        """
        # Load YAML
        yaml_path_obj = Path(yaml_path)
        if not yaml_path_obj.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_path}")

        with open(yaml_path_obj, "r") as f:
            config = yaml.safe_load(f)

        # Parse project config
        project_config = ProjectConfig(**{k: v for k, v in config.items() if k != "pipelines"})

        # Auto-create connections from config
        connections = {}
        for conn_name, conn_config in config.get("connections", {}).items():
            conn_type = conn_config.get("type", "local")

            if conn_type == "local":
                connections[conn_name] = LocalConnection(
                    base_path=conn_config.get("base_path", "./data")
                )
            else:
                raise ValueError(f"Unsupported connection type: {conn_type}")

        # Extract story config
        story_config = config.get("story", {})

        return cls(
            yaml_config=config,
            engine=project_config.engine,
            connections=connections,
            story_config=story_config,
        )

    def run(
        self, pipelines: Optional[Union[str, List[str]]] = None
    ) -> Union[PipelineResults, Dict[str, PipelineResults]]:
        """Run one, multiple, or all pipelines.

        Args:
            pipelines: Pipeline name(s) to run. If None, runs all pipelines.
                      Can be a string (single pipeline) or list of strings.

        Returns:
            If single pipeline: PipelineResults
            If multiple pipelines: Dict[pipeline_name -> PipelineResults]

        Examples:
            >>> manager = PipelineManager.from_yaml("config.yaml")
            >>> results = manager.run()  # Run all
            >>> results = manager.run('bronze_to_silver')  # Run one
            >>> results = manager.run(['bronze_to_silver', 'silver_to_gold'])  # Run multiple
        """
        # Determine which pipelines to run
        if pipelines is None:
            # Run all pipelines in order
            pipeline_names = list(self._pipelines.keys())
        elif isinstance(pipelines, str):
            # Single pipeline
            pipeline_names = [pipelines]
        else:
            # List of pipelines
            pipeline_names = pipelines

        # Validate pipeline names
        for name in pipeline_names:
            if name not in self._pipelines:
                available = ", ".join(self._pipelines.keys())
                raise ValueError(f"Pipeline '{name}' not found. Available pipelines: {available}")

        # Run pipelines
        results = {}
        for name in pipeline_names:
            print(f"\n{'='*60}")
            print(f"Running pipeline: {name}")
            print(f"{'='*60}\n")

            results[name] = self._pipelines[name].run()

            # Print summary
            result = results[name]
            status = "✅ SUCCESS" if not result.failed else "❌ FAILED"
            print(f"\n{status} - {name}")
            print(f"  Completed: {len(result.completed)} nodes")
            print(f"  Failed: {len(result.failed)} nodes")
            print(f"  Duration: {result.duration:.2f}s")
            if result.story_path:
                print(f"  Story: {result.story_path}")

        # Return single result if only one pipeline, otherwise dict
        if len(pipeline_names) == 1:
            return results[pipeline_names[0]]
        else:
            return results

    def list_pipelines(self) -> List[str]:
        """Get list of available pipeline names.

        Returns:
            List of pipeline names
        """
        return list(self._pipelines.keys())

    def get_pipeline(self, name: str) -> Pipeline:
        """Get a specific pipeline instance.

        Args:
            name: Pipeline name

        Returns:
            Pipeline instance

        Raises:
            ValueError: If pipeline not found
        """
        if name not in self._pipelines:
            available = ", ".join(self._pipelines.keys())
            raise ValueError(f"Pipeline '{name}' not found. Available: {available}")
        return self._pipelines[name]
