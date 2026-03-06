"""Profile result caching to avoid re-downloading/re-analyzing sources."""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ProfileCache:
    """Simple file-based cache for profile results."""

    def __init__(self, cache_dir: str = "./.odibi/profile_cache"):
        """Initialize profile cache.

        Args:
            cache_dir: Directory to store cached profiles
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_key(self, connection: str, dataset: str) -> str:
        """Generate cache key from connection and dataset."""
        key = f"{connection}:{dataset}"
        return hashlib.md5(key.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get path to cache file."""
        return self.cache_dir / f"{cache_key}.json"

    def get(
        self, connection: str, dataset: str, max_age_hours: int = 24
    ) -> Optional[Dict[str, Any]]:
        """Retrieve cached profile result if fresh enough.

        Args:
            connection: Connection name
            dataset: Dataset path/name
            max_age_hours: Maximum age in hours (default: 24)

        Returns:
            Cached profile dict or None if not found/expired
        """
        cache_key = self._get_cache_key(connection, dataset)
        cache_path = self._get_cache_path(cache_key)

        if not cache_path.exists():
            logger.debug(f"Cache miss: {connection}:{dataset}")
            return None

        try:
            with open(cache_path) as f:
                cached = json.load(f)

            # Check age
            cached_at = datetime.fromisoformat(cached["cached_at"])
            age = datetime.now() - cached_at

            if age > timedelta(hours=max_age_hours):
                logger.info(
                    f"Cache expired for {connection}:{dataset} (age: {age.total_seconds() / 3600:.1f}h)"
                )
                cache_path.unlink()
                return None

            logger.info(
                f"Cache hit: {connection}:{dataset} (age: {age.total_seconds() / 3600:.1f}h)"
            )
            return cached["profile"]

        except Exception as e:
            logger.warning(f"Failed to read cache for {connection}:{dataset}: {e}")
            return None

    def set(self, connection: str, dataset: str, profile: Dict[str, Any]) -> None:
        """Store profile result in cache.

        Args:
            connection: Connection name
            dataset: Dataset path/name
            profile: Profile result to cache
        """
        cache_key = self._get_cache_key(connection, dataset)
        cache_path = self._get_cache_path(cache_key)

        try:
            cached = {
                "connection": connection,
                "dataset": dataset,
                "cached_at": datetime.now().isoformat(),
                "profile": profile,
            }

            with open(cache_path, "w") as f:
                json.dump(cached, f, indent=2, default=str)

            logger.info(f"Cached profile for {connection}:{dataset}")

        except Exception as e:
            logger.warning(f"Failed to cache profile for {connection}:{dataset}: {e}")

    def clear(self, older_than_hours: Optional[int] = None) -> int:
        """Clear cache entries.

        Args:
            older_than_hours: Only clear entries older than N hours. None = clear all.

        Returns:
            Number of entries cleared
        """
        count = 0
        cutoff = datetime.now() - timedelta(hours=older_than_hours) if older_than_hours else None

        for cache_file in self.cache_dir.glob("*.json"):
            try:
                if cutoff:
                    with open(cache_file) as f:
                        cached = json.load(f)
                    cached_at = datetime.fromisoformat(cached["cached_at"])
                    if cached_at > cutoff:
                        continue

                cache_file.unlink()
                count += 1

            except Exception as e:
                logger.warning(f"Failed to clear cache file {cache_file}: {e}")

        logger.info(f"Cleared {count} cache entries")
        return count


# Global cache instance
_cache = ProfileCache()


def get_cached_profile(
    connection: str, dataset: str, max_age_hours: int = 24
) -> Optional[Dict[str, Any]]:
    """Get cached profile if available and fresh."""
    return _cache.get(connection, dataset, max_age_hours)


def cache_profile(connection: str, dataset: str, profile: Dict[str, Any]) -> None:
    """Cache a profile result."""
    _cache.set(connection, dataset, profile)


def clear_profile_cache(older_than_hours: Optional[int] = None) -> int:
    """Clear profile cache."""
    return _cache.clear(older_than_hours)
