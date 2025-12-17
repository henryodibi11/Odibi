"""Memory storage backends using Odibi's connection system.

Supports storing memories via any Odibi connection:
- Local filesystem
- Azure Blob Storage (ADLS)
- Delta Lake
- Any connection Odibi supports
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from odibi.engine.base import Engine

logger = logging.getLogger(__name__)


class MemoryBackend(ABC):
    """Abstract base class for memory storage backends."""

    @abstractmethod
    def save(self, memory_id: str, data: dict[str, Any]) -> bool:
        """Save a memory."""
        pass

    @abstractmethod
    def load(self, memory_id: str) -> Optional[dict[str, Any]]:
        """Load a memory by ID."""
        pass

    @abstractmethod
    def delete(self, memory_id: str) -> bool:
        """Delete a memory."""
        pass

    @abstractmethod
    def list_all(self) -> list[str]:
        """List all memory IDs."""
        pass

    @abstractmethod
    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        """Search memories by keyword."""
        pass

    def get_recent(
        self,
        days: int = 7,
        memory_types: Optional[list[str]] = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Get recent memories."""
        cutoff = datetime.now() - timedelta(days=days)
        results = []

        for memory_id in self.list_all():
            data = self.load(memory_id)
            if not data:
                continue

            created_str = data.get("created_at", "")
            if created_str and isinstance(created_str, str):
                try:
                    created = datetime.fromisoformat(created_str)
                    if created < cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

            if memory_types:
                if data.get("memory_type") not in memory_types:
                    continue

            results.append(data)

        results.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return results[:limit]


class LocalFileBackend(MemoryBackend):
    """Store memories as local JSON files."""

    def __init__(self, base_path: str = ".odibi/memories"):
        self.base_path = Path(base_path).resolve()

    def _file_path(self, memory_id: str) -> Path:
        return self.base_path / f"{memory_id}.json"

    def save(self, memory_id: str, data: dict[str, Any]) -> bool:
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)
            with open(self._file_path(memory_id), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception:
            return False

    def load(self, memory_id: str) -> Optional[dict[str, Any]]:
        path = self._file_path(memory_id)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def delete(self, memory_id: str) -> bool:
        path = self._file_path(memory_id)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_all(self) -> list[str]:
        return [p.stem for p in self.base_path.glob("*.json")]

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        query_lower = query.lower()
        results = []

        for memory_id in self.list_all():
            data = self.load(memory_id)
            if not data:
                continue

            content = data.get("content", "").lower()
            summary = data.get("summary", "").lower()
            tags = [t.lower() for t in data.get("tags", [])]

            if (
                query_lower in content
                or query_lower in summary
                or any(query_lower in tag for tag in tags)
            ):
                results.append(data)

            if len(results) >= limit:
                break

        return results


class OdibiConnectionBackend(MemoryBackend):
    """Store memories using an Odibi connection.

    Stores memories as JSON files via Odibi's engine abstraction.
    Works with any connection type: local, azure_blob, delta, etc.

    Example:
        ```python
        # Using existing Odibi connection
        from odibi.connections import load_connections
        from odibi.engine import PandasEngine

        connections = load_connections("project.yaml")
        engine = PandasEngine()

        backend = OdibiConnectionBackend(
            connection=connections["adls_memories"],
            engine=engine,
            path_prefix="agent/memories",
        )
        ```
    """

    def __init__(
        self,
        connection: Any,
        engine: "Engine",
        path_prefix: str = "memories",
        format: str = "json",
    ):
        """Initialize with Odibi connection.

        Args:
            connection: Odibi connection object.
            engine: Odibi engine (PandasEngine, SparkEngine, etc.)
            path_prefix: Path prefix within the connection.
            format: Storage format (json, parquet, delta).
        """
        self.connection = connection
        self.engine = engine
        self.path_prefix = path_prefix
        self.format = format

        self._index_cache: Optional[dict[str, str]] = None
        self._index_path = f"{path_prefix}/_index.json"

    def _memory_path(self, memory_id: str) -> str:
        return f"{self.path_prefix}/{memory_id}.json"

    def _load_index(self) -> dict[str, str]:
        """Load the memory index (maps ID to created_at for quick filtering)."""
        if self._index_cache is not None:
            return self._index_cache

        try:
            df = self.engine.read(
                connection=self.connection,
                format="json",
                path=self._index_path,
                options={},
            )
            if df is not None and len(df) > 0:
                records = df.to_dict(orient="records")
                self._index_cache = {r["id"]: r.get("created_at", "") for r in records}
                return self._index_cache
        except Exception:
            pass

        self._index_cache = {}
        return self._index_cache

    def _save_index(self, index: dict[str, str]) -> None:
        """Save the memory index."""
        import pandas as pd

        records = [{"id": k, "created_at": v} for k, v in index.items()]
        df = pd.DataFrame(records)

        self.engine.write(
            df=df,
            connection=self.connection,
            format="json",
            path=self._index_path,
            mode="overwrite",
            options={},
        )
        self._index_cache = index

    def save(self, memory_id: str, data: dict[str, Any]) -> bool:
        import pandas as pd

        try:
            df = pd.DataFrame([data])

            self.engine.write(
                df=df,
                connection=self.connection,
                format="json",
                path=self._memory_path(memory_id),
                mode="overwrite",
                options={},
            )

            index = self._load_index()
            index[memory_id] = data.get("created_at", datetime.now().isoformat())
            self._save_index(index)

            return True
        except Exception as e:
            logger.error("Failed to save memory %s: %s", memory_id, e, exc_info=True)
            return False

    def load(self, memory_id: str) -> Optional[dict[str, Any]]:
        try:
            df = self.engine.read(
                connection=self.connection,
                format="json",
                path=self._memory_path(memory_id),
                options={},
            )
            if df is not None and len(df) > 0:
                return df.to_dict(orient="records")[0]
        except Exception as e:
            logger.error("Failed to load memory %s: %s", memory_id, e, exc_info=True)
        return None

    def delete(self, memory_id: str) -> bool:
        index = self._load_index()
        if memory_id in index:
            del index[memory_id]
            self._save_index(index)
            return True
        return False

    def list_all(self) -> list[str]:
        return list(self._load_index().keys())

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        try:
            query_lower = query.lower()
            results = []

            for memory_id in self.list_all():
                data = self.load(memory_id)
                if not data:
                    continue

                content = data.get("content", "").lower()
                summary = data.get("summary", "").lower()
                tags = [t.lower() for t in data.get("tags", [])]

                if (
                    query_lower in content
                    or query_lower in summary
                    or any(query_lower in tag for tag in tags)
                ):
                    results.append(data)

                if len(results) >= limit:
                    break

            return results
        except Exception as e:
            logger.error("Failed to search memories for query '%s': %s", query, e, exc_info=True)
            return []


class DeltaTableBackend(MemoryBackend):
    """Store memories as a Delta table.

    All memories in a single Delta table for efficient querying.
    Best for Spark/Databricks environments.

    Example:
        ```python
        backend = DeltaTableBackend(
            connection=connections["silver"],
            engine=spark_engine,
            table_path="system/agent_memories",
        )
        ```
    """

    def __init__(
        self,
        connection: Any,
        engine: "Engine",
        table_path: str = "agent_memories",
    ):
        self.connection = connection
        self.engine = engine
        self.table_path = table_path
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Ensure the Delta table exists."""
        pass

    def save(self, memory_id: str, data: dict[str, Any]) -> bool:
        import pandas as pd

        try:
            data["id"] = memory_id
            if "metadata" in data and isinstance(data["metadata"], dict):
                data["metadata"] = json.dumps(data["metadata"])
            if "tags" in data and isinstance(data["tags"], list):
                data["tags"] = json.dumps(data["tags"])
            if "source_files" in data and isinstance(data["source_files"], list):
                data["source_files"] = json.dumps(data["source_files"])

            df = pd.DataFrame([data])

            self.engine.write(
                df=df,
                connection=self.connection,
                format="delta",
                path=self.table_path,
                mode="append",
                options={"mergeSchema": "true"},
            )
            return True
        except Exception as e:
            print(f"Failed to save memory: {e}")
            return False

    def load(self, memory_id: str) -> Optional[dict[str, Any]]:
        try:
            df = self.engine.read(
                connection=self.connection,
                format="delta",
                path=self.table_path,
                options={
                    "query": f"SELECT * FROM delta.`{self.table_path}` WHERE id = '{memory_id}'"
                },
            )
            if df is not None and len(df) > 0:
                record = df.to_dict(orient="records")[0]
                if "tags" in record and isinstance(record["tags"], str):
                    record["tags"] = json.loads(record["tags"])
                if "source_files" in record and isinstance(record["source_files"], str):
                    record["source_files"] = json.loads(record["source_files"])
                if "metadata" in record and isinstance(record["metadata"], str):
                    record["metadata"] = json.loads(record["metadata"])
                return record
        except Exception:
            pass
        return None

    def delete(self, memory_id: str) -> bool:
        return False

    def list_all(self) -> list[str]:
        try:
            df = self.engine.read(
                connection=self.connection,
                format="delta",
                path=self.table_path,
                options={},
            )
            if df is not None:
                return df["id"].tolist()
        except Exception:
            pass
        return []

    def search(self, query: str, limit: int = 10) -> list[dict[str, Any]]:
        try:
            df = self.engine.read(
                connection=self.connection,
                format="delta",
                path=self.table_path,
                options={},
            )
            if df is None:
                return []

            query_lower = query.lower()
            mask = df["content"].str.lower().str.contains(query_lower, na=False) | df[
                "summary"
            ].str.lower().str.contains(query_lower, na=False)
            results = df[mask].head(limit).to_dict(orient="records")

            for record in results:
                if "tags" in record and isinstance(record["tags"], str):
                    record["tags"] = json.loads(record["tags"])

            return results
        except Exception:
            return []


def create_memory_backend(
    backend_type: str = "local",
    connection: Optional[Any] = None,
    engine: Optional["Engine"] = None,
    **kwargs,
) -> MemoryBackend:
    """Factory function to create a memory backend.

    Args:
        backend_type: Type of backend ("local", "odibi", "delta").
        connection: Odibi connection (for odibi/delta backends).
        engine: Odibi engine (for odibi/delta backends).
        **kwargs: Additional backend-specific arguments.

    Returns:
        MemoryBackend instance.

    Example:
        ```python
        # Local storage
        backend = create_memory_backend("local", base_path=".odibi/memories")

        # ADLS via Odibi connection
        backend = create_memory_backend(
            "odibi",
            connection=connections["adls"],
            engine=pandas_engine,
            path_prefix="agent/memories",
        )

        # Delta table
        backend = create_memory_backend(
            "delta",
            connection=connections["silver"],
            engine=spark_engine,
            table_path="system.agent_memories",
        )
        ```
    """
    if backend_type == "local":
        return LocalFileBackend(base_path=kwargs.get("base_path", ".odibi/memories"))

    if backend_type == "odibi":
        if not connection or not engine:
            raise ValueError("'odibi' backend requires connection and engine")
        return OdibiConnectionBackend(
            connection=connection,
            engine=engine,
            path_prefix=kwargs.get("path_prefix", "memories"),
            format=kwargs.get("format", "json"),
        )

    if backend_type == "delta":
        if not connection or not engine:
            raise ValueError("'delta' backend requires connection and engine")
        return DeltaTableBackend(
            connection=connection,
            engine=engine,
            table_path=kwargs.get("table_path", "agent_memories"),
        )

    raise ValueError(f"Unknown backend type: {backend_type}")
