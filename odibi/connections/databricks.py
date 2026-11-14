"""Databricks Connect session setup; storage and table resolution stay separate."""

import configparser
from importlib.metadata import version
import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Optional

from odibi.config import DatabricksConnectionConfig
from odibi.connections.base import BaseConnection
from odibi.utils.logging import logger


class DatabricksConnection(BaseConnection):
    """Lazily create and reuse an explicit M2M or named CLI OAuth Spark session.

    Construction and validate() are local-only. The SDK handles OAuth token
    acquisition/refresh. Session creation never falls back to local Spark.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        cluster_id: Optional[str] = None,
        serverless: bool = False,
        profile: Optional[str] = None,
    ):
        self.config = DatabricksConnectionConfig(
            host=host,
            client_id=client_id,
            client_secret=client_secret,
            cluster_id=cluster_id,
            serverless=serverless,
            profile=profile,
        )
        if self.config.client_secret is not None:
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

    def _profile_sdk_config(self, Config):
        # Config(profile=...) alone loads environment credentials/targets first.
        # Read only the exact named section, without DEFAULT inheritance, and pin
        # identity/compute inputs through the SDK's public constructor instead.
        if tuple(int(p) for p in version("databricks-sdk").split(".")[:3]) < (0, 166, 0):
            raise ValueError("Named profiles require databricks-sdk>=0.166.0")
        config_file = Path(
            os.environ.get("DATABRICKS_CONFIG_FILE") or "~/.databrickscfg"
        ).expanduser()
        parser = configparser.ConfigParser(interpolation=None, default_section="__no_defaults__")
        with config_file.open(encoding="utf-8") as handle:
            parser.read_file(handle)
        if not parser.has_section(self.config.profile):
            raise ValueError("Named profile is missing")
        profile = parser[self.config.profile]
        host = profile.get("host")
        if not host:
            raise ValueError("Named profile must define its own host")
        DatabricksConnectionConfig.validate_workspace_host(host)
        if profile.get("auth_type") != "databricks-cli":
            raise ValueError("Named profile must use databricks-cli authentication")
        cluster_id = self.config.cluster_id
        serverless = self.config.serverless
        if not cluster_id and not serverless:
            cluster_id = profile.get("cluster_id") or None
            target = profile.get("serverless_compute_id")
            if target not in (None, "", "auto"):
                raise ValueError("Unsupported profile serverless target")
            serverless = target == "auto"
        if bool(cluster_id) == serverless:
            raise ValueError("Named profile must select exactly one compute target")

        cli = shutil.which("databricks")
        if not cli:
            raise ValueError("Databricks CLI is required for named profiles")
        probe = subprocess.run(
            [cli, "version", "--output", "json"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        cli_version = json.loads(probe.stdout)
        if tuple(int(cli_version[k]) for k in ("Major", "Minor", "Patch")) < (0, 207, 1):
            raise ValueError("Databricks CLI>=0.207.1 is required for profile-bound tokens")

        # Empty typed values block environment auth inputs, including alternate
        # Azure/Google providers. Forced CLI auth prevents a credential-chain fallback.
        settings = {
            attr.name: attr.transform()
            for attr in Config.attributes()
            if attr.auth
            or attr.name
            in (
                "account_id",
                "workspace_id",
                "azure_workspace_resource_id",
                "cluster_id",
                "serverless_compute_id",
            )
        }
        settings.update(
            host=host,
            profile=self.config.profile,
            config_file=str(config_file),
            auth_type="databricks-cli",
            databricks_cli_path=cli,
            cluster_id=cluster_id or "",
            serverless_compute_id="auto" if serverless else "",
        )
        return Config(**settings)

    def get_spark_session(self):
        """Create a remote session once, without implicit identity fallback."""
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
            config = (
                self._profile_sdk_config(Config)
                if self.config.profile
                else Config(
                    host=self.config.host,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret.get_secret_value(),
                    auth_type="oauth-m2m",
                    # Explicit credentials must not load an unrelated shell profile.
                    profile="",
                    cluster_id=self.config.cluster_id,
                    serverless_compute_id="auto" if self.config.serverless else None,
                )
            )
            # SDK defaults can fill omitted targets from environment/profile.
            # The YAML target is authoritative, even in a developer's configured shell.
            serverless = (
                config.serverless_compute_id == "auto"
                if self.config.profile
                else self.config.serverless
            )
            unused_target = "cluster_id" if serverless else "serverless_compute_id"
            config.as_dict().pop(unused_target, None)
            self._session = DatabricksSession.builder.sdkConfig(config).getOrCreate()
        except Exception:
            # SDK/transport errors may echo credentials or authorization headers.
            # Do not include their text or chained exception in public diagnostics.
            if self.config.profile:
                message = (
                    "Databricks session setup failed. Named-profile mode requires an exact saved "
                    "profile with host, auth_type=databricks-cli and one compute target, "
                    "databricks-sdk>=0.166.0 and Databricks CLI>=0.207.1 on PATH. "
                    "Verify login for that profile, workspace access and Connect/Python compatibility; "
                    "ambient credentials and targets are not a fallback."
                )
            else:
                message = (
                    "Databricks session setup failed. Check OAuth M2M credentials, workspace "
                    "permissions, compute target and Databricks Connect compatibility."
                )
            raise ConnectionError(message) from None
        return self._session
