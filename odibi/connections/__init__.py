"""Connection implementations for ODIBI."""

from odibi.connections.base import BaseConnection
from odibi.connections.local import LocalConnection

__all__ = ["BaseConnection", "LocalConnection"]
