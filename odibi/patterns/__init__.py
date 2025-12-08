from typing import Dict, Type

from odibi.patterns.aggregation import AggregationPattern
from odibi.patterns.base import Pattern
from odibi.patterns.date_dimension import DateDimensionPattern
from odibi.patterns.dimension import DimensionPattern
from odibi.patterns.fact import FactPattern
from odibi.patterns.merge import MergePattern
from odibi.patterns.scd2 import SCD2Pattern

_PATTERNS: Dict[str, Type[Pattern]] = {
    "scd2": SCD2Pattern,
    "merge": MergePattern,
    "dimension": DimensionPattern,
    "date_dimension": DateDimensionPattern,
    "aggregation": AggregationPattern,
    "fact": FactPattern,
}


def get_pattern_class(name: str) -> Type[Pattern]:
    if name not in _PATTERNS:
        raise ValueError(f"Unknown pattern: '{name}'. Available: {list(_PATTERNS.keys())}")
    return _PATTERNS[name]
