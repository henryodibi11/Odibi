"""Deploy command implementation."""

from pathlib import Path

from odibi.pipeline import PipelineManager
from odibi.utils.extensions import load_extensions
from odibi.utils.logging import logger


def deploy_command(args):
    """Deploy pipeline definitions to System Catalog."""
    try:
        config_path = Path(args.config).resolve()

        logger.info(f"Deploying configuration from {config_path}...")

        # Load extensions (similar to run command)
        load_extensions(config_path.parent)
        if config_path.parent.parent != config_path.parent:
            load_extensions(config_path.parent.parent)
        if config_path.parent != Path.cwd():
            load_extensions(Path.cwd())

        # Load Pipeline Manager (this validates config and initializes connections/catalog)
        # We don't need environment overrides for deployment typically, but user might want to deploy prod config?
        # The user should pass the correct config file or env vars.
        # We'll respect --env arg if we add it to deploy command.
        env = getattr(args, "env", None)
        manager = PipelineManager.from_yaml(args.config, env=env)

        if not manager.catalog_manager:
            logger.error("System Catalog not configured. Cannot deploy.")
            return 1

        catalog = manager.catalog_manager

        # Bootstrap (ensure tables exist)
        # PipelineManager.__init__ already calls bootstrap(), but calling it again is safe/idempotent
        catalog.bootstrap()

        logger.info(f"Syncing to System Catalog at: {catalog.base_path}")

        project_config = manager.project_config
        pipelines = project_config.pipelines

        total_pipelines = len(pipelines)
        total_nodes = sum(len(p.nodes) for p in pipelines)

        logger.info(f"Found {total_pipelines} pipelines, {total_nodes} nodes.")

        for i, pipeline in enumerate(pipelines, 1):
            logger.info(f"[{i}/{total_pipelines}] Syncing pipeline: {pipeline.pipeline}")

            # Register Pipeline
            catalog.register_pipeline(pipeline, project_config)

            # Register Nodes
            for node in pipeline.nodes:
                # logger.debug(f"  - Syncing node: {node.name}")
                catalog.register_node(pipeline.pipeline, node)

        logger.info("Deployment complete! System Catalog is up to date.")
        return 0

    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        # import traceback
        # traceback.print_exc()
        return 1
