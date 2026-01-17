from odibi.config import ProjectConfig

try:
    from dagster import (
        AssetExecutionContext,
        Definitions,
        asset,
    )
except ImportError:
    # Dagster is optional
    pass


class DagsterFactory:
    """
    Factory to create Dagster definitions from Odibi configuration.

    Usage in definitions.py:
        from odibi.config import load_config
        from odibi.orchestration.dagster import DagsterFactory

        config = load_config("odibi.yaml")
        defs = DagsterFactory(config).create_definitions()
    """

    def __init__(self, config: ProjectConfig):
        self.config = config

    def create_definitions(self) -> "Definitions":
        if "Definitions" not in globals():
            raise ImportError("Dagster not installed. Run 'pip install dagster'")

        all_assets = []

        for pipeline in self.config.pipelines:
            for node in pipeline.nodes:
                # Create an asset for each node
                # We use dynamic function creation to bind specific node/pipeline

                asset_name = node.name.replace("-", "_")
                deps = [dep.replace("-", "_") for dep in node.depends_on]
                group = pipeline.pipeline

                # Define the asset function
                def make_asset_fn(p_name, n_name):
                    @asset(
                        name=asset_name,
                        deps=deps,
                        group_name=group,
                        description=node.description,
                        compute_kind="odibi",
                        op_tags={"odibi/pipeline": p_name, "odibi/node": n_name},
                    )
                    def _asset_fn(context: AssetExecutionContext):
                        # Run Odibi Node
                        # We shell out to CLI to ensure clean environment,
                        # OR import runner. Shell out is safer for isolation.
                        import subprocess

                        context.log.info(f"Running Odibi node: {n_name} in pipeline {p_name}")
                        cmd = ["odibi", "run", "--pipeline", p_name, "--node", n_name]

                        result = subprocess.run(cmd, capture_output=True, text=True)

                        if result.stdout:
                            context.log.info(result.stdout)
                        if result.stderr:
                            context.log.error(result.stderr)

                        if result.returncode != 0:
                            raise Exception(f"Odibi execution failed: {result.stderr}")

                    return _asset_fn

                all_assets.append(make_asset_fn(pipeline.pipeline, node.name))

        return Definitions(assets=all_assets)
