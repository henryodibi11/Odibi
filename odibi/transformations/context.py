"""
Context Passing System
======================

Enables transformations to receive pipeline-level metadata.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class TransformationContext:
    """
    Context passed to transformation functions and explain() methods.

    Contains:
    - Node parameters (from YAML)
    - Pipeline metadata (plant, asset, layer, etc.)
    - Project metadata (project name, business unit, etc.)
    - Runtime information (timestamps, etc.)
    """

    # Node-level
    node_name: str
    operation_name: str
    params: Dict[str, Any] = field(default_factory=dict)

    # Pipeline-level
    pipeline_name: Optional[str] = None
    layer: Optional[str] = None

    # Project-level (from top of YAML)
    project: Optional[str] = None
    plant: Optional[str] = None
    asset: Optional[str] = None
    business_unit: Optional[str] = None

    # Runtime
    environment: str = "development"

    # Additional metadata
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for **kwargs passing."""
        return {
            "node": self.node_name,
            "operation": self.operation_name,
            "params": self.params,
            "pipeline": self.pipeline_name,
            "layer": self.layer,
            "project": self.project,
            "plant": self.plant,
            "asset": self.asset,
            "business_unit": self.business_unit,
            "environment": self.environment,
            **self.extra,
        }


def build_context_from_config(
    node_config: Dict[str, Any],
    pipeline_config: Dict[str, Any],
    project_config: Dict[str, Any],
) -> TransformationContext:
    """
    Build TransformationContext from config dictionaries.

    Args:
        node_config: Node configuration (operation, params, etc.)
        pipeline_config: Pipeline configuration
        project_config: Project-level configuration

    Returns:
        TransformationContext instance
    """
    return TransformationContext(
        node_name=node_config.get("node", "unnamed"),
        operation_name=node_config.get("operation", "unknown"),
        params=node_config.get("params", {}),
        pipeline_name=pipeline_config.get("name"),
        layer=pipeline_config.get("layer"),
        project=project_config.get("project"),
        plant=project_config.get("plant"),
        asset=project_config.get("asset"),
        business_unit=project_config.get("business_unit"),
        environment=project_config.get("environment", "development"),
        extra=project_config.get("metadata", {}),
    )
