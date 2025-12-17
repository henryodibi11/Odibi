"""Explorer: Untrusted experimentation sandbox for Odibi.

TRUST BOUNDARY NOTICE:
======================
This component is explicitly UNTRUSTED by design.

- Operates ONLY on cloned, sandboxed copies of the Odibi repo
- NEVER touches the trusted Odibi codebase
- Generates experiments and records outcomes
- Produces candidate diffs for human review
- Has NO authority to promote changes

The Explorer cannot:
- Modify any trusted codebase files
- Access or modify regression tests or golden projects
- Auto-promote any changes
- Bypass human review gates

All memory is append-only and advisory.
"""

from odibi.agents.explorer.clone_manager import RepoCloneManager
from odibi.agents.explorer.experiment_runner import ExperimentRunner
from odibi.agents.explorer.memory import ExplorerMemory
from odibi.agents.explorer.promotion_bucket import PromotionBucket

__all__ = [
    "RepoCloneManager",
    "ExperimentRunner",
    "ExplorerMemory",
    "PromotionBucket",
]
