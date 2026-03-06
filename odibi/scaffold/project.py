"""Project YAML scaffolding."""

from typing import Any, Dict, List, Optional

import yaml


def generate_project_yaml(
    project_name: str,
    connections: Dict[str, Dict[str, Any]],
    imports: Optional[List[str]] = None,
    story_connection: Optional[str] = None,
    system_connection: Optional[str] = None,
) -> str:
    """Generate a project.yaml file.

    Args:
        project_name: Name of the project
        connections: Dict of connection name -> connection config
        imports: List of pipeline files to import
        story_connection: Connection name for story output
        system_connection: Connection name for system data

    Returns:
        YAML string for project.yaml

    Example:
        >>> connections = {
        ...     "local": {"type": "local", "base_path": "data/"},
        ...     "azure": {"type": "azure_blob", "account_name": "myaccount"}
        ... }
        >>> yaml = generate_project_yaml("my_project", connections)
    """
    if not connections:
        first_connection = "local"
    else:
        first_connection = next(iter(connections.keys()))

    config: Dict[str, Any] = {
        "project": project_name,
        "connections": connections,
    }

    default_conn = story_connection or system_connection or first_connection
    config["story"] = {
        "connection": story_connection or default_conn,
        "path": "stories",
    }
    config["system"] = {
        "connection": system_connection or default_conn,
        "path": "_system",
    }

    if imports:
        config["imports"] = imports

    return yaml.dump(
        config,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=120,
    )
