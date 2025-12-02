"""Connection factory for built-in connection types."""

from typing import Any, Dict

from odibi.plugins import register_connection_factory
from odibi.utils.logging import logger
from odibi.utils.logging_context import get_logging_context


def create_local_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for LocalConnection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="local", connection_name=name, action="create")

    from odibi.connections.local import LocalConnection

    base_path = config.get("base_path", "./data")
    connection = LocalConnection(base_path=base_path)

    ctx.log_connection(
        connection_type="local", connection_name=name, action="created", base_path=base_path
    )
    return connection


def create_http_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for HttpConnection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="http", connection_name=name, action="create")

    from odibi.connections.http import HttpConnection

    base_url = config.get("base_url", "")
    connection = HttpConnection(
        base_url=base_url,
        headers=config.get("headers"),
        auth=config.get("auth"),
    )

    ctx.log_connection(
        connection_type="http", connection_name=name, action="created", base_url=base_url
    )
    return connection


def create_azure_blob_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for AzureADLS (Blob) Connection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="azure_blob", connection_name=name, action="create")

    try:
        from odibi.connections.azure_adls import AzureADLS
    except ImportError as e:
        ctx.error(
            f"Failed to import AzureADLS for connection '{name}'",
            connection_name=name,
            error=str(e),
        )
        raise ImportError(
            "Azure ADLS support requires 'pip install odibi[azure]'. "
            "See README.md for installation instructions."
        )

    # Handle config discrepancies
    account = config.get("account_name") or config.get("account")
    if not account:
        ctx.error(
            f"Connection '{name}' missing 'account_name'",
            connection_name=name,
            config_keys=list(config.keys()),
        )
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

        ctx.debug(
            f"Auto-detected auth_mode for connection '{name}'",
            connection_name=name,
            auth_mode=auth_mode,
        )

    validation_mode = config.get("validation_mode", "lazy")
    validate = config.get("validate")
    if validate is None:
        validate = True if validation_mode == "eager" else False

    # Register secrets (log that we're registering, not the values)
    if account_key:
        logger.register_secret(account_key)
        ctx.debug(f"Registered account_key secret for connection '{name}'", connection_name=name)
    if sas_token:
        logger.register_secret(sas_token)
        ctx.debug(f"Registered sas_token secret for connection '{name}'", connection_name=name)
    if client_secret:
        logger.register_secret(client_secret)
        ctx.debug(f"Registered client_secret secret for connection '{name}'", connection_name=name)

    try:
        connection = AzureADLS(
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

        ctx.log_connection(
            connection_type="azure_blob",
            connection_name=name,
            action="created",
            account=account,
            container=config["container"],
            auth_mode=auth_mode,
            validation_mode=validation_mode,
        )
        return connection

    except Exception as e:
        ctx.error(
            f"Failed to create Azure Blob connection '{name}'",
            connection_name=name,
            account=account,
            container=config.get("container"),
            auth_mode=auth_mode,
            error=str(e),
        )
        raise


def create_delta_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for Delta Connection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="delta", connection_name=name, action="create")

    # Local path-based Delta
    if "path" in config:
        from odibi.connections.local import LocalConnection

        base_path = config.get("path") or config.get("base_path")
        connection = LocalConnection(base_path=base_path)

        ctx.log_connection(
            connection_type="delta",
            connection_name=name,
            action="created",
            mode="local_path",
            base_path=base_path,
        )
        return connection

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

    catalog = config.get("catalog")
    schema = config.get("schema") or "default"
    connection = DeltaCatalogConnection(catalog=catalog, schema=schema)

    ctx.log_connection(
        connection_type="delta",
        connection_name=name,
        action="created",
        mode="catalog",
        catalog=catalog,
        schema=schema,
    )
    return connection


def create_sql_server_connection(name: str, config: Dict[str, Any]) -> Any:
    """Factory for SQL Server / Azure SQL Connection."""
    ctx = get_logging_context()
    ctx.log_connection(connection_type="sql_server", connection_name=name, action="create")

    try:
        from odibi.connections.azure_sql import AzureSQL
    except ImportError as e:
        ctx.error(
            f"Failed to import AzureSQL for connection '{name}'",
            connection_name=name,
            error=str(e),
        )
        raise ImportError(
            "Azure SQL support requires 'pip install odibi[azure]'. "
            "See README.md for installation instructions."
        )

    server = config.get("host") or config.get("server")
    if not server:
        ctx.error(
            f"Connection '{name}' missing 'host' or 'server'",
            connection_name=name,
            config_keys=list(config.keys()),
        )
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

        ctx.debug(
            f"Auto-detected auth_mode for connection '{name}'",
            connection_name=name,
            auth_mode=auth_mode,
        )

    if password:
        logger.register_secret(password)
        ctx.debug(f"Registered password secret for connection '{name}'", connection_name=name)

    try:
        connection = AzureSQL(
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

        ctx.log_connection(
            connection_type="sql_server",
            connection_name=name,
            action="created",
            server=server,
            database=config["database"],
            auth_mode=auth_mode,
            port=config.get("port", 1433),
        )
        return connection

    except Exception as e:
        ctx.error(
            f"Failed to create SQL Server connection '{name}'",
            connection_name=name,
            server=server,
            database=config.get("database"),
            auth_mode=auth_mode,
            error=str(e),
        )
        raise


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
