"""Plugin system for Odibi."""

import sys
import logging
from typing import Dict, Any, Protocol, Optional

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

# Type for connection factory function
# (name: str, config: Dict[str, Any]) -> BaseConnection
# We use Any for return type to avoid circular import with BaseConnection
ConnectionFactory = Any 

logger = logging.getLogger(__name__)

_CONNECTION_FACTORIES: Dict[str, ConnectionFactory] = {}

def register_connection_factory(type_name: str, factory: ConnectionFactory):
    """Register a connection factory.
    
    Args:
        type_name: The 'type' string used in config (e.g., 'postgres')
        factory: Function that takes (name, config) and returns a Connection instance
    """
    _CONNECTION_FACTORIES[type_name] = factory
    logger.debug(f"Registered connection factory: {type_name}")

def get_connection_factory(type_name: str) -> Optional[ConnectionFactory]:
    """Get a registered connection factory.
    
    Args:
        type_name: The connection type
        
    Returns:
        Factory function or None
    """
    return _CONNECTION_FACTORIES.get(type_name)

def load_plugins():
    """Load plugins from entry points.
    
    Scans 'odibi.connections' entry points.
    The entry point value should be a callable (factory).
    The entry point name is used as the connection type.
    """
    try:
        # Handle different entry_points API versions
        if sys.version_info < (3, 10):
            eps = entry_points().get('odibi.connections', [])
        else:
            eps = entry_points(group='odibi.connections')
            
        for ep in eps:
            try:
                factory = ep.load()
                register_connection_factory(ep.name, factory)
                logger.info(f"Loaded plugin: {ep.name}")
            except Exception as e:
                logger.error(f"Failed to load plugin {ep.name}: {e}")
                
    except Exception as e:
        logger.error(f"Plugin discovery failed: {e}")
