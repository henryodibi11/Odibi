"""Databricks Connect session setup; storage and table resolution stay separate."""

from typing import Optional

from odibi.config import DatabricksConnectionConfig
from odibi.connections.base import BaseConnection
from odibi.utils.logging import logger


class DatabricksConnection(BaseConnection):
    """Lazily create and reuse an OAuth M2M remote Spark session.

    Construction and validate() are local-only. The SDK handles OAuth token
    acquisition/refresh. Session creation never falls back to local Spark.
    """

    def __init__(
        self,
        host: str,
        client_id: str,
        client_secret: str,
        cluster_id: Optional[str] = None,
        serverless: bool = False,
    ):
        self.config = DatabricksConnectionConfig(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            cluster_id=cluster_id,
            serverless=serverless,
        )
        logger.register_secret(self.config.client_secret.get_secret_value())
        self._session = None

    def validate(self) -> None:
        """Configuration is validated on construction; no remote probe is made."""

    def get_path(self, relative_path: str) -> str:
        """Compute is not storage: use a Unity Catalog or filesystem connection."""
        raise ValueError(
            "Databricks connection selects Spark compute, not data paths. "
            "Use a unity_catalog or storage connection for reads/writes, system and story."
        )

    def get_spark_session(self):
        """Create a remote session once, with explicit OAuth M2M authentication."""
        if self._session is not None:
            return self._session

        try:
            from databricks.connect import DatabricksSession
            from databricks.sdk.core import Config
        except ImportError:
            raise ImportError(
                "Remote Spark requires databricks-connect matched to your runtime/Python. "
                "Install it in a separate environment without pyspark or delta-spark; "
                "see docs/setup_databricks_connect.md."
            ) from None

        try:
            config = Config(
                host=self.config.host,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret.get_secret_value(),
                auth_type="oauth-m2m",
                # Explicit credentials must not load an unrelated shell profile.
                profile="",
                cluster_id=self.config.cluster_id,
                serverless_compute_id="auto" if self.config.serverless else None,
            )
            # SDK defaults can fill omitted targets from environment/profile.
            # The YAML target is authoritative, even in a developer's configured shell.
            unused_target = "cluster_id" if self.config.serverless else "serverless_compute_id"
            config.as_dict().pop(unused_target, None)
            self._session = DatabricksSession.builder.sdkConfig(config).getOrCreate()
        except Exception:
            # SDK/transport errors may echo credentials or authorization headers.
            # Do not include their text or chained exception in public diagnostics.
            raise ConnectionError(
                "Databricks session setup failed. Check OAuth M2M credentials, workspace "
                "permissions, compute target and Databricks Connect compatibility."
            ) from None
        return self._session
