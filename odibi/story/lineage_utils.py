"""
Lineage Utilities
=================

Shared utilities for generating combined lineage from pipeline stories.

This module provides helper functions that can be used by both PipelineManager
and SemanticLayerRunner to generate lineage without tight coupling.
"""

from typing import Any, Callable, Dict, Optional

from odibi.config import ProjectConfig
from odibi.story.lineage import LineageGenerator, LineageResult
from odibi.utils.logging_context import get_logging_context


def get_full_stories_path(project_config: ProjectConfig) -> str:
    """
    Build the full path to stories, including cloud URL if remote.

    Converts relative paths like "OEE/Stories/" to full cloud URLs:
    - Azure: abfs://container@account.dfs.core.windows.net/OEE/Stories/
    - S3: s3://bucket/OEE/Stories/
    - GCS: gs://bucket/OEE/Stories/

    Args:
        project_config: Project configuration with story settings

    Returns:
        Full stories path (local or remote URL)
    """
    stories_path = project_config.story.path

    # Already a full URL
    if "://" in stories_path:
        return stories_path

    # Get story connection info
    story_conn_name = project_config.story.connection
    story_conn = project_config.connections.get(story_conn_name)

    if not story_conn:
        return stories_path

    conn_type = getattr(story_conn, "type", None)
    if conn_type is None:
        return stories_path

    conn_type_value = conn_type.value if hasattr(conn_type, "value") else str(conn_type)

    # Strip leading/trailing slashes for clean path construction
    clean_path = stories_path.strip("/")

    # Azure Blob Storage / Delta Lake
    if conn_type_value in ("azure_blob", "delta"):
        account_name = getattr(story_conn, "account_name", None)
        container = getattr(story_conn, "container", None)

        if account_name and container:
            return f"abfs://{container}@{account_name}.dfs.core.windows.net/{clean_path}"

    # AWS S3
    elif conn_type_value in ("s3", "aws_s3"):
        bucket = getattr(story_conn, "bucket", None)

        if bucket:
            return f"s3://{bucket}/{clean_path}"

    # Google Cloud Storage
    elif conn_type_value in ("gcs", "google_cloud_storage"):
        bucket = getattr(story_conn, "bucket", None)

        if bucket:
            return f"gs://{bucket}/{clean_path}"

    # HDFS
    elif conn_type_value == "hdfs":
        host = getattr(story_conn, "host", None)
        port = getattr(story_conn, "port", 8020)

        if host:
            return f"hdfs://{host}:{port}/{clean_path}"

    # DBFS (Databricks File System)
    elif conn_type_value == "dbfs":
        return f"dbfs:/{clean_path}"

    return stories_path


def get_storage_options(project_config: ProjectConfig) -> Dict[str, Any]:
    """
    Get storage options from story connection for fsspec/adlfs.

    Handles all Azure auth modes:
    - account_key / direct_key: Returns account_key for fsspec
    - sas: Returns sas_token for fsspec
    - connection_string: Returns connection_string for fsspec
    - aad_msi / managed_identity: Returns empty dict (uses default Azure credential)
    - key_vault: Would need to fetch secret (not implemented here)

    Args:
        project_config: Project configuration with story connection

    Returns:
        Dict of storage options for fsspec
    """
    ctx = get_logging_context()
    story_conn_name = project_config.story.connection
    story_conn = project_config.connections.get(story_conn_name)

    if not story_conn:
        return {}

    # Check for direct credentials on connection
    if hasattr(story_conn, "credentials") and story_conn.credentials:
        return dict(story_conn.credentials)
    if hasattr(story_conn, "account_key") and story_conn.account_key:
        return {"account_key": story_conn.account_key}
    if hasattr(story_conn, "sas_token") and story_conn.sas_token:
        return {"sas_token": story_conn.sas_token}

    # Check nested auth structure
    if hasattr(story_conn, "auth") and story_conn.auth:
        auth = story_conn.auth

        # Helper to get value from auth (handles both dict and Pydantic model)
        def get_auth_value(key: str):
            if isinstance(auth, dict):
                return auth.get(key)
            return getattr(auth, key, None)

        auth_mode = get_auth_value("mode")
        if auth_mode:
            mode_value = auth_mode.value if hasattr(auth_mode, "value") else str(auth_mode)
        else:
            mode_value = None

        # account_key or direct_key mode
        account_key = get_auth_value("account_key")
        if account_key:
            return {"account_key": account_key}

        # SAS token mode
        sas_token = get_auth_value("sas_token")
        if sas_token:
            return {"sas_token": sas_token}

        # Connection string mode
        connection_string = get_auth_value("connection_string")
        if connection_string:
            return {"connection_string": connection_string}

        # MSI / managed identity - uses DefaultAzureCredential, no explicit creds needed
        if mode_value in ("aad_msi", "managed_identity"):
            # Return account_name for adlfs to use with DefaultAzureCredential
            account_name = getattr(story_conn, "account_name", None)
            if account_name:
                return {"account_name": account_name}
            return {}

        # Key Vault mode - would need to fetch from Key Vault
        if mode_value == "key_vault":
            ctx.warning(
                "Key Vault auth for story storage not yet implemented. "
                "Consider using direct_key or aad_msi for story connection."
            )
            return {}

    return {}


def get_write_file(project_config: ProjectConfig) -> Optional[Callable[[str, str], None]]:
    """
    Create a write_file callback for remote storage using story connection.

    Args:
        project_config: Project configuration with story connection

    Returns:
        Callable for writing files, or None if local storage
    """
    storage_options = get_storage_options(project_config)

    story_conn_name = project_config.story.connection
    story_conn = project_config.connections.get(story_conn_name)

    if not story_conn:
        return None

    conn_type = getattr(story_conn, "type", None)
    if conn_type is None:
        return None

    conn_type_value = conn_type.value if hasattr(conn_type, "value") else str(conn_type)

    if conn_type_value == "local":
        base_path = getattr(story_conn, "base_path", "./data")

        def write_file_local(path: str, content: str) -> None:
            import os

            full_path = os.path.join(base_path, path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(content)

        return write_file_local

    elif conn_type_value in ("azure_blob", "delta"):
        if not storage_options:
            return None

        account_name = getattr(story_conn, "account_name", None)
        container = getattr(story_conn, "container", None)

        if not account_name or not container:
            return None

        def write_file_azure(path: str, content: str) -> None:
            import fsspec

            if path.startswith(("abfs://", "az://")):
                full_path = path
            else:
                full_path = f"abfs://{container}@{account_name}.dfs.core.windows.net/{path}"

            fs_options = {"account_name": account_name, **storage_options}
            fs = fsspec.filesystem("abfs", **fs_options)
            with fs.open(full_path, "w") as f:
                f.write(content)

        return write_file_azure

    elif conn_type_value in ("s3", "aws_s3"):
        bucket = getattr(story_conn, "bucket", None)
        if not bucket:
            return None

        def write_file_s3(path: str, content: str) -> None:
            import fsspec

            if path.startswith("s3://"):
                full_path = path
            else:
                full_path = f"s3://{bucket}/{path}"

            fs = fsspec.filesystem("s3", **storage_options)
            with fs.open(full_path, "w") as f:
                f.write(content)

        return write_file_s3

    elif conn_type_value in ("gcs", "google_cloud_storage"):
        bucket = getattr(story_conn, "bucket", None)
        if not bucket:
            return None

        def write_file_gcs(path: str, content: str) -> None:
            import fsspec

            if path.startswith("gs://"):
                full_path = path
            else:
                full_path = f"gs://{bucket}/{path}"

            fs = fsspec.filesystem("gcs", **storage_options)
            with fs.open(full_path, "w") as f:
                f.write(content)

        return write_file_gcs

    return None


def generate_lineage(
    project_config: ProjectConfig,
    date: Optional[str] = None,
    write_file: Optional[Callable[[str, str], None]] = None,
) -> Optional[LineageResult]:
    """
    Generate combined lineage from all pipeline stories.

    This is a standalone function that can be called after any pipeline run
    to generate cross-layer lineage stitching.

    Args:
        project_config: Project configuration
        date: Optional date string for lineage (defaults to today)
        write_file: Optional callback for writing files to remote storage
                    (auto-created from story connection if not provided)

    Returns:
        LineageResult if successful, None if generation fails
    """
    ctx = get_logging_context()

    stories_path = get_full_stories_path(project_config)
    storage_options = get_storage_options(project_config)

    # Auto-create write_file callback if not provided and using remote storage
    if write_file is None:
        write_file = get_write_file(project_config)

    ctx.debug("Generating lineage", stories_path=stories_path)

    try:
        lineage_gen = LineageGenerator(
            stories_path=stories_path,
            storage_options=storage_options,
        )
        result = lineage_gen.generate(date=date)
        lineage_gen.save(result, write_file=write_file)
        ctx.info(
            "Lineage generated successfully",
            nodes=len(result.nodes),
            edges=len(result.edges),
            layers=len(result.layers),
        )
        return result
    except Exception as e:
        ctx.warning(f"Failed to generate lineage: {e}")
        return None
