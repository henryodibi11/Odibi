"""Connection factory for built-in connection types."""

from typing import Any, Dict

from odibi.plugins import register_connection_factory
from odibi.utils.logging import logger


def create_local_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for LocalConnection."""
    from odibi.connections.local import LocalConnection

    return LocalConnection(base_path=config.get("base_path", "./data"))


def create_http_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for HttpConnection."""
    from odibi.connections.http import HttpConnection

    return HttpConnection(
        base_url=config.get("base_url", ""),
        headers=config.get("headers"),
        auth=config.get("auth"),
    )


def create_azure_blob_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for AzureADLS (Blob) Connection."""
    try:
        from odibi.connections.azure_adls import AzureADLS
    except ImportError:
        raise ImportError(
            "Azure ADLS support requires 'pip install odibi[azure]'. "
            "See README.md for installation instructions."
        )

    # Handle config discrepancies
    account = config.get("account_name") or config.get("account")
    if not account:
        raise ValueError(f"Connection '{name}' missing 'account_name'")

    auth_config = config.get("auth", {})

    # Extract auth details
    key_vault_name = auth_config.get("key_vault_name") or config.get("key_vault_name")
    secret_name = auth_config.get("secret_name") or config.get("secret_name")
    account_key = auth_config.get("account_key") or config.get("account_key")
    sas_token = auth_config.get("sas_token") or config.get("sas_token")
    tenant_id = auth_config.get("tenant_id") or config.get("tenant_id")
    client_id = auth_config.get("client_id") or config.get("client_id")
    client_secret = auth_config.get("client_secret") or config.get("client_secret")

    auth_mode = config.get("auth_mode", "key_vault")

    # Auto-detect auth_mode
    if "auth_mode" not in config:
        if sas_token:
            auth_mode = "sas_token"
        elif key_vault_name and secret_name:
            auth_mode = "key_vault"
        elif account_key:
            auth_mode = "direct_key"
        elif tenant_id and client_id and client_secret:
            auth_mode = "service_principal"
        else:
            auth_mode = "managed_identity"

    validation_mode = config.get("validation_mode", "lazy")
    validate = config.get("validate")
    if validate is None:
        validate = True if validation_mode == "eager" else False

    # Register secrets
    if account_key:
        logger.register_secret(account_key)
    if sas_token:
        logger.register_secret(sas_token)
    if client_secret:
        logger.register_secret(client_secret)

    return AzureADLS(
        account=account,
        container=config["container"],
        path_prefix=config.get("path_prefix", ""),
        auth_mode=auth_mode,
        key_vault_name=key_vault_name,
        secret_name=secret_name,
        account_key=account_key,
        sas_token=sas_token,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        validate=validate,
    )


def create_delta_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for Delta Connection."""
    # Local path-based Delta
    if "path" in config:
        from odibi.connections.local import LocalConnection

        return LocalConnection(base_path=config.get("path") or config.get("base_path"))

    # Catalog based (Spark only)
    from odibi.connections.base import BaseConnection

    class DeltaCatalogConnection(BaseConnection):
        def __init__(self, catalog, schema):
            self.catalog = catalog
            self.schema = schema

        def get_path(self, table):
            return f"{self.catalog}.{self.schema}.{table}"

        def validate(self):
            pass

        def pandas_storage_options(self):
            return {}

    return DeltaCatalogConnection(
        catalog=config.get("catalog"),
        schema=config.get("schema") or "default",
    )


def create_sql_server_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for SQL Server / Azure SQL Connection."""
    try:
        from odibi.connections.azure_sql import AzureSQL
    except ImportError:
        raise ImportError(
            "Azure SQL support requires 'pip install odibi[azure]'. "
            "See README.md for installation instructions."
        )

    server = config.get("host") or config.get("server")
    if not server:
        raise ValueError(f"Connection '{name}' missing 'host' or 'server'")

    auth_config = config.get("auth", {})
    username = auth_config.get("username") or config.get("username")
    password = auth_config.get("password") or config.get("password")
    key_vault_name = auth_config.get("key_vault_name") or config.get("key_vault_name")
    secret_name = auth_config.get("secret_name") or config.get("secret_name")

    auth_mode = config.get("auth_mode")
    if not auth_mode:
        if username and password:
            auth_mode = "sql"
        elif key_vault_name and secret_name and username:
            auth_mode = "key_vault"
        else:
            auth_mode = "aad_msi"

    if password:
        logger.register_secret(password)

    return AzureSQL(
        server=server,
        database=config["database"],
        driver=config.get("driver", "ODBC Driver 18 for SQL Server"),
        username=username,
        password=password,
        auth_mode=auth_mode,
        key_vault_name=key_vault_name,
        secret_name=secret_name,
        port=config.get("port", 1433),
        timeout=config.get("timeout", 30),
    )


def register_builtins():
    """Register all built-in connection factories."""
    register_connection_factory("local", create_local_connection)
    register_connection_factory("http", create_http_connection)

    # Azure Blob / ADLS
    register_connection_factory("azure_blob", create_azure_blob_connection)
    register_connection_factory("azure_adls", create_azure_blob_connection)

    # Delta
    register_connection_factory("delta", create_delta_connection)

    # SQL
    register_connection_factory("sql_server", create_sql_server_connection)
    register_connection_factory("azure_sql", create_sql_server_connection)
