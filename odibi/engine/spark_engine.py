"""Spark execution engine (Phase 1: Scaffolding only).

Status: Experimental - Basic introspection implemented.
Full read/write/SQL implementation planned for Phase 3.
See PHASES.md lines 171-173 for roadmap.
"""

from typing import Any, Dict, List, Tuple, Optional
from .base import Engine


class SparkEngine(Engine):
    """Spark execution engine with PySpark backend.

    Phase 1 (Current): Import guards + introspection methods only
    Phase 3 (Planned): read(), write(), execute_sql() implementation
    """

    name = "spark"

    def __init__(self, session=None, config: Optional[Dict[str, Any]] = None):
        """Initialize Spark engine with import guard.

        Args:
            session: Existing SparkSession (optional, creates new if None)
            config: Engine configuration (optional)

        Raises:
            ImportError: If pyspark not installed
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError as e:
            raise ImportError(
                "Spark support requires 'pip install odibi[spark]'. "
                "See docs/setup_databricks.md for setup instructions."
            ) from e

        from pyspark.sql import SparkSession

        self.spark = session or SparkSession.builder.appName("odibi").getOrCreate()
        self.config = config or {}

    def get_schema(self, df) -> List[Tuple[str, str]]:
        """Get DataFrame schema as list of (name, type) tuples."""
        return [(f.name, f.dataType.simpleString()) for f in df.schema]

    def get_shape(self, df) -> Tuple[int, int]:
        """Get DataFrame shape as (rows, columns)."""
        return (df.count(), len(df.columns))

    def count_rows(self, df) -> int:
        """Count rows in DataFrame."""
        return df.count()

    # Phase 3 stubs
    def read(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.read() will be implemented in Phase 3. "
            "See PHASES.md line 171 for implementation plan."
        )

    def write(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.write() will be implemented in Phase 3. "
            "See PHASES.md line 172 for implementation plan."
        )

    def execute_sql(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_sql() will be implemented in Phase 3. "
            "See PHASES.md line 171 for implementation plan."
        )

    def execute_transform(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_transform() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )

    def execute_operation(self, *args, **kwargs):
        raise NotImplementedError(
            "SparkEngine.execute_operation() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )

    def count_nulls(self, df, columns: List[str]) -> Dict[str, int]:
        """Count nulls in specified columns (Phase 1 stub)."""
        raise NotImplementedError(
            "SparkEngine.count_nulls() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )

    def validate_schema(self, df, schema_rules: Dict[str, Any]) -> List[str]:
        """Validate DataFrame schema (Phase 1 stub)."""
        raise NotImplementedError(
            "SparkEngine.validate_schema() will be implemented in Phase 3. "
            "See PHASES.md for implementation plan."
        )
