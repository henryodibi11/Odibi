"""Engine registry for dynamic engine loading."""

from typing import Type, Dict, Any
from odibi.engine.base import Engine
from odibi.engine.pandas_engine import PandasEngine

_ENGINES: Dict[str, Type[Engine]] = {
    "pandas": PandasEngine
}

try:
    from odibi.engine.spark_engine import SparkEngine
    _ENGINES["spark"] = SparkEngine
except ImportError:
    pass

def register_engine(name: str, engine_cls: Type[Engine]) -> None:
    """Register a new engine class.
    
    Args:
        name: Engine name (e.g. 'duckdb')
        engine_cls: Engine class inheriting from Engine
    """
    _ENGINES[name] = engine_cls

def get_engine_class(name: str) -> Type[Engine]:
    """Get engine class by name.
    
    Args:
        name: Engine name
        
    Returns:
        Engine class
        
    Raises:
        ValueError: If engine not found
    """
    if name not in _ENGINES:
        raise ValueError(f"Unsupported engine: {name}. Available: {list(_ENGINES.keys())}")
    return _ENGINES[name]
