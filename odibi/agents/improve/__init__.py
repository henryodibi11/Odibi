"""Odibi Agent Improvement Environment.

This module provides infrastructure for autonomous agent improvement campaigns.

TRUST HIERARCHY:
- Sacred: Source repository (NEVER touched)
- Master: Blessed working copy (promoted changes land here)
- Sandboxes: Disposable experiment clones (destroyed after each cycle)
"""

from odibi.agents.improve.config import (
    CampaignConfig,
    CommandConfig,
    EnvironmentConfig,
    GateConfig,
    SnapshotConfig,
    StopConfig,
)
from odibi.agents.improve.environment import ImprovementEnvironment
from odibi.agents.improve.results import (
    CommandResult,
    CycleResult,
    GateCheckResult,
    GoldenResult,
    LintResult,
    PipelineResult,
    TestResult,
    ValidationResult,
)
from odibi.agents.improve.runner import OdibiPipelineRunner
from odibi.agents.improve.sandbox import SandboxInfo
from odibi.agents.improve.story_parser import NodeFailure, ParsedStory, StoryParser
from odibi.agents.improve.harness_generator import CoverageGap, HarnessGenerator

__all__ = [
    # Config
    "CampaignConfig",
    "CommandConfig",
    "EnvironmentConfig",
    "GateConfig",
    "SnapshotConfig",
    "StopConfig",
    # Environment
    "ImprovementEnvironment",
    "SandboxInfo",
    # Runner
    "OdibiPipelineRunner",
    # Results
    "CommandResult",
    "CycleResult",
    "GateCheckResult",
    "GoldenResult",
    "LintResult",
    "PipelineResult",
    "TestResult",
    "ValidationResult",
    # Story Parser
    "NodeFailure",
    "ParsedStory",
    "StoryParser",
    # Harness Generator
    "CoverageGap",
    "HarnessGenerator",
]
