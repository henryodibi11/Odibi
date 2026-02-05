"""Connection implementations for ODIBI."""

from odibi.connections.api_fetcher import ApiFetcher, FetchResult, create_api_fetcher
from odibi.connections.azure_adls import AzureADLS
from odibi.connections.azure_sql import AzureSQL
from odibi.connections.base import BaseConnection
from odibi.connections.local import LocalConnection
from odibi.connections.local_dbfs import LocalDBFS

__all__ = [
    "BaseConnection",
    "LocalConnection",
    "AzureADLS",
    "AzureSQL",
    "LocalDBFS",
    "ApiFetcher",
    "FetchResult",
    "create_api_fetcher",
]
