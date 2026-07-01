"""Unity Catalog connection for Databricks managed tables.

Provides a connection type that resolves paths to fully qualified
Unity Catalog table names (catalog.schema.table). Writes go through
Spark's ``saveAsTable`` / ``CREATE TABLE`` — no direct filesystem
access is required, making this compatible with Databricks Serverless
and Free Edition environments.
"""

from typing import Any, Dict, List, Optional

from odibi.connections.base import BaseConnection


class UnityCatalogConnection(BaseConnection):
    """Connection to Unity Catalog managed tables on Databricks.

    Tables are addressed as ``catalog.schema.table``.  The UC metastore
    handles backend storage (S3/ADLS/GCS) automatically — Spark never
    needs direct filesystem access.

    Args:
        catalog: UC catalog name (e.g. ``"workspace"``, ``"main"``).
        schema: UC schema/database name (e.g. ``"odibi_logs"``).
        create_schema: If True, issue ``CREATE SCHEMA IF NOT EXISTS``
            on first validate() call.
    """

    def __init__(
        self,
        catalog: str,
        schema: str = "default",
        create_schema: bool = True,
    ):
        self.catalog = catalog
        self.schema = schema
        self.create_schema = create_schema

    def get_path(self, table: str) -> str:
        """Return the fully qualified UC table name.

        Args:
            table: Table name (may already contain dots).

        Returns:
            ``catalog.schema.table`` string.
        """
        if "." in table:
            return table
        return f"{self.catalog}.{self.schema}.{table}"

    def validate(self) -> None:
        """Validate and optionally create the target schema.

        When running inside Databricks with an active SparkSession,
        this will issue ``CREATE SCHEMA IF NOT EXISTS`` if
        ``create_schema`` was set to True.

        Does not raise on failure — schema creation may require
        elevated permissions; downstream writes will surface the
        real error.
        """
        if not self.create_schema:
            return

        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                return
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        except Exception:
            pass

    def pandas_storage_options(self) -> Dict[str, Any]:
        """Return empty dict — UC tables don't use storage options."""
        return {}

    # ---- Discovery API (basic) -----------------------------------------------

    def discover_catalog(
        self,
        include_schema: bool = False,
        include_stats: bool = False,
        limit: int = 200,
        recursive: bool = True,
        path: str = "",
        pattern: str = "",
    ) -> Dict[str, Any]:
        """List tables in the UC schema via ``SHOW TABLES``."""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                return {"datasets": [], "error": "No active SparkSession"}

            rows = spark.sql(f"SHOW TABLES IN {self.catalog}.{self.schema}").collect()
            datasets = []
            for row in rows:
                tbl = row["tableName"]
                if pattern and pattern not in tbl:
                    continue
                entry: Dict[str, Any] = {
                    "name": tbl,
                    "full_name": f"{self.catalog}.{self.schema}.{tbl}",
                    "type": "managed_table",
                }
                datasets.append(entry)
                if len(datasets) >= limit:
                    break
            return {"datasets": datasets}
        except Exception as e:
            return {"datasets": [], "error": str(e)}

    def list_tables(self) -> List[str]:
        """Return table names in the configured schema."""
        result = self.discover_catalog()
        return [d["name"] for d in result.get("datasets", [])]

    def get_freshness(self, dataset: str, timestamp_column: Optional[str] = None) -> Dict[str, Any]:
        """Get freshness of a UC table."""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                return {"error": "No active SparkSession"}
            fqn = self.get_path(dataset)
            hist = spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1").collect()
            if hist:
                return {
                    "last_modified": str(hist[0]["timestamp"]),
                    "operation": hist[0]["operation"],
                }
            return {"last_modified": None}
        except Exception as e:
            return {"error": str(e)}
