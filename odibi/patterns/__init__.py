from typing import Dict, Type

from odibi.patterns.base import Pattern
from odibi.patterns.fact import FactPattern
from odibi.patterns.merge import MergePattern
from odibi.patterns.scd2 import SCD2Pattern
from odibi.patterns.snapshot import SnapshotPattern

_PATTERNS: Dict[str, Type[Pattern]] = {
    "snapshot": SnapshotPattern,
    "fact": FactPattern,
    "scd2": SCD2Pattern,
    "merge": MergePattern,
    # "incremental": IncrementalPattern, # TODO
}


def get_pattern_class(name: str) -> Type[Pattern]:
    if name not in _PATTERNS:
        raise ValueError(f"Unknown pattern: '{name}'. Available: {list(_PATTERNS.keys())}")
    return _PATTERNS[name]
