"""Core simulation engine for generating synthetic data."""

import hashlib
import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np

from odibi.config import (
    BooleanGeneratorConfig,
    CategoricalGeneratorConfig,
    ConstantGeneratorConfig,
    DailyProfileGeneratorConfig,
    DerivedGeneratorConfig,
    EmailGeneratorConfig,
    GeoGeneratorConfig,
    IPGeneratorConfig,
    RandomWalkGeneratorConfig,
    RangeGeneratorConfig,
    ScheduledEvent,
    ScheduledEventType,
    SequentialGeneratorConfig,
    SimulationConfig,
    TimestampGeneratorConfig,
    UUIDGeneratorConfig,
)


@dataclass
class PreparedScheduledEvent:
    """Pre-parsed scheduled event with resolved datetime/timedelta values."""

    event_id: int
    config: ScheduledEvent
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    duration_td: Optional[timedelta] = None
    recurrence_td: Optional[timedelta] = None
    jitter_td: Optional[timedelta] = None
    cooldown_td: Optional[timedelta] = None
    sustain_steps: int = 0
    # Pre-expanded occurrence windows for recurring events: list of (start, end)
    occurrence_windows: List[Tuple[datetime, datetime]] = field(default_factory=list)


@dataclass
class ResolvedScheduledEvent:
    """Result of resolving a scheduled event at a specific timestamp."""

    config: ScheduledEvent
    event_id: int
    window_start: datetime
    window_end: Optional[datetime]
    is_ramp: bool = False
    ramp_duration_seconds: float = 0.0


class EntityProxy:
    """Proxy object for cross-entity column references.

    Allows expressions like "Tank_A.level" to reference another entity's
    column value at the same timestamp.

    Example:
        # In Tank_B's derived expression:
        expression: "Tank_A.flow_out * 0.5"
    """

    def __init__(self, entity_name: str):
        """Initialize entity proxy.

        Args:
            entity_name: Name of the entity this proxy represents
        """
        self.entity_name = entity_name
        self._data: Optional[Dict[str, Any]] = None

    def bind(self, row_data: Optional[Dict[str, Any]]):
        """Bind proxy to a specific row's data for the current timestamp.

        Args:
            row_data: Dictionary of column values for this entity at current timestamp,
                     or None to unbind
        """
        self._data = row_data

    def __getattr__(self, column_name: str):
        """Access column value via dot notation.

        Args:
            column_name: Name of column to access

        Returns:
            Column value from bound row data

        Raises:
            AttributeError: If proxy not bound or column doesn't exist
        """
        # Prevent infinite recursion for _data and entity_name
        if column_name in ("_data", "entity_name"):
            return object.__getattribute__(self, column_name)

        if self._data is None:
            raise AttributeError(
                f"Entity '{self.entity_name}' row not yet available at this timestamp. "
                f"Check entity generation order - '{self.entity_name}' must be generated "
                f"before entities that reference it."
            )

        if column_name in self._data:
            return self._data[column_name]

        raise AttributeError(
            f"Entity '{self.entity_name}' has no column '{column_name}'. "
            f"Available columns: {list(self._data.keys())}"
        )


class SimulationEngine:
    """Engine for generating simulated data according to YAML configuration."""

    def __init__(
        self,
        config: SimulationConfig,
        hwm_timestamp: Optional[str] = None,
        random_walk_state: Optional[Dict[str, Dict[str, float]]] = None,
        scheduled_event_state: Optional[Dict[str, Dict[int, Dict[str, Any]]]] = None,
    ):
        """Initialize simulation engine.

        Args:
            config: Simulation configuration
            hwm_timestamp: High-water mark timestamp for incremental generation
            random_walk_state: Last random walk values per entity per column from previous run
            scheduled_event_state: Condition-based event state per entity per event from previous run
        """
        self.config = config
        self.hwm_timestamp = hwm_timestamp
        # Random walk state: last values per entity per column from previous run
        # Format: {"entity_name": {"column_name": last_value, ...}, ...}
        self.random_walk_state = random_walk_state or {}
        self.rng = np.random.default_rng(config.scope.seed)

        # NEW: Per-entity state tracking for prev() and stateful functions
        # Format: {"entity_name": {"column_name": prev_value, "_pid_column_name": {"integral": 0.0, "prev_error": 0.0}, ...}}
        self.entity_state: Dict[str, Dict[str, Any]] = {}

        # Parse timestep
        self.timestep_seconds = self._parse_timestep(config.scope.timestep)

        # Generate entity names
        self.entity_names = self._generate_entity_names()

        # Parse start time
        self.start_time = datetime.fromisoformat(config.scope.start_time.replace("Z", "+00:00"))

        # Determine effective start time for incremental mode
        if hwm_timestamp:
            hwm_dt = datetime.fromisoformat(hwm_timestamp.replace("Z", "+00:00"))
            self.effective_start_time = hwm_dt + timedelta(seconds=self.timestep_seconds)
            self.rows_before_hwm = int(
                (self.effective_start_time - self.start_time).total_seconds()
                / self.timestep_seconds
            )
        else:
            self.effective_start_time = self.start_time
            self.rows_before_hwm = 0

        # Calculate total rows and end time
        if config.scope.row_count:
            self.total_rows = config.scope.row_count
            self.end_time = self.effective_start_time + timedelta(
                seconds=self.timestep_seconds * (self.total_rows - 1)
            )
        else:
            self.end_time = datetime.fromisoformat(config.scope.end_time.replace("Z", "+00:00"))
            time_diff = (self.end_time - self.effective_start_time).total_seconds()
            # Use max(0, ...) not max(1, ...) to allow empty datasets when end < start
            self.total_rows = max(0, int(time_diff / self.timestep_seconds) + 1)

        # Resolve column dependencies for derived columns
        self.column_order = self._resolve_column_dependencies()

        # Detect cross-entity references and build entity dependency DAG
        self._detect_cross_entity_references()
        self._build_entity_dependency_dag()

        # Prepare scheduled events (pre-parse times, expand recurrences)
        self.prepared_events = self._prepare_scheduled_events()
        # Per-entity per-event runtime state for conditions/ramp
        # Format: {entity_name: {event_id: {key: value}}}
        self.scheduled_event_state: Dict[str, Dict[int, Dict[str, Any]]] = (
            self._deserialize_event_state(scheduled_event_state) if scheduled_event_state else {}
        )

    @staticmethod
    def _deserialize_event_state(
        raw: Dict[str, Dict[Any, Dict[str, Any]]],
    ) -> Dict[str, Dict[int, Dict[str, Any]]]:
        """Deserialize scheduled event state from JSON-safe format.

        JSON round-trips convert int keys to strings and datetime objects to
        ISO strings. This restores the original types.
        """
        _dt_keys = {"last_trigger_time", "active_until", "ramp_started_at"}
        result: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for entity, events in raw.items():
            result[entity] = {}
            for eid, state in events.items():
                deserialized = {}
                for k, v in state.items():
                    if k in _dt_keys and isinstance(v, str):
                        try:
                            v = datetime.fromisoformat(v)
                        except ValueError:
                            pass
                    deserialized[k] = v
                result[entity][int(eid)] = deserialized
        return result

    def _parse_timestep(self, timestep: str) -> float:
        """Parse timestep string into seconds.

        Args:
            timestep: Timestep string (e.g., '5m', '1h', '30s')

        Returns:
            Timestep in seconds

        Raises:
            ValueError: If timestep format is invalid
        """
        match = re.match(r"^(\d+)(s|m|h|d)$", timestep)
        if not match:
            raise ValueError(
                f"Invalid timestep format: '{timestep}'. "
                f"Expected format: <number><unit> where unit is s, m, h, or d"
            )

        value, unit = match.groups()
        value = int(value)

        unit_to_seconds = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
        }

        seconds = value * unit_to_seconds[unit]

        if seconds <= 0:
            raise ValueError(
                f"Timestep must be positive, got '{timestep}' ({seconds}s). "
                f"Use positive values like '5m', '1h', '30s'."
            )

        return seconds

    def _resolve_column_dependencies(self) -> List[str]:
        """Resolve column dependencies for derived columns using topological sort.

        Returns:
            List of column names in dependency order

        Raises:
            ValueError: If circular dependencies detected
        """
        # Build dependency graph
        dependencies: Dict[str, Set[str]] = {}
        all_columns = {col.name for col in self.config.columns}

        for col_config in self.config.columns:
            generator = col_config.generator
            if isinstance(generator, DerivedGeneratorConfig):
                # Extract column references from expression
                deps = self._extract_column_references(generator.expression, all_columns)
                dependencies[col_config.name] = deps
            else:
                dependencies[col_config.name] = set()

        # Topological sort
        sorted_cols = []
        visited = set()
        visiting = set()

        def visit(col_name: str):
            if col_name in visited:
                return
            if col_name in visiting:
                raise ValueError(
                    f"Circular dependency detected in derived columns involving '{col_name}'"
                )

            visiting.add(col_name)
            for dep in dependencies.get(col_name, set()):
                visit(dep)
            visiting.remove(col_name)
            visited.add(col_name)
            sorted_cols.append(col_name)

        for col_name in dependencies:
            visit(col_name)

        return sorted_cols

    def _extract_column_references(self, expression: str, all_columns: Set[str]) -> Set[str]:
        """Extract column names referenced in a derived expression.

        Args:
            expression: Python expression string
            all_columns: Set of all valid column names

        Returns:
            Set of column names referenced in the expression

        Note:
            Columns referenced within prev() calls are NOT considered dependencies
            since they reference previous rows, not current row (no circular dependency).
        """
        # Remove prev() calls to exclude them from dependency analysis
        # prev('column_name', ...) references PREVIOUS row, not current row
        expression_without_prev = re.sub(r"prev\s*\([^)]+\)", "", expression)

        # Also remove ema() calls - they reference previous state, not current row
        expression_without_prev = re.sub(r"ema\s*\([^)]+\)", "", expression_without_prev)

        # Also remove delay() calls - they reference previous state, not current row
        expression_without_prev = re.sub(r"delay\s*\([^)]+\)", "", expression_without_prev)

        # Remove _timestamp.attr patterns - these are context variables, not column refs
        expression_without_prev = re.sub(
            r"_timestamp\.[a-zA-Z_][a-zA-Z0-9_]*", "", expression_without_prev
        )

        # Find all identifiers in the remaining expression
        # Match valid Python identifiers
        identifiers = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", expression_without_prev)

        # Filter to only valid column names (exclude keywords and functions)
        python_keywords = {
            "if",
            "else",
            "and",
            "or",
            "not",
            "True",
            "False",
            "None",
            "abs",
            "round",
            "min",
            "max",
            "int",
            "float",
            "str",
            "bool",
            "coalesce",
            "safe_div",
            "safe_mul",
            # Stateful functions
            "prev",
            "ema",
            "delay",
            "pid",
            # Context variables
            "_timestamp",
            "_row_index",
            "entity_id",
            "random",
        }

        return {name for name in identifiers if name in all_columns and name not in python_keywords}

    def _detect_cross_entity_references(self):
        """Detect cross-entity references in derived expressions.

        Scans all derived expressions for patterns like 'EntityName.column_name'
        and validates that referenced entities and columns exist.

        Sets:
            self.cross_entity_enabled: True if any cross-entity refs found
            self.entity_proxies: Dict of entity name -> EntityProxy (if enabled)

        Raises:
            ValueError: If invalid cross-entity references detected
        """
        entity_set = set(self.entity_names)
        cross_entity_refs = set()

        for col_config in self.config.columns:
            # Check base generator
            generator = col_config.generator
            if isinstance(generator, DerivedGeneratorConfig):
                refs = self._extract_entity_references(generator.expression, entity_set)
                cross_entity_refs.update(refs)

                if re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*prev\s*\(", generator.expression):
                    raise ValueError(
                        f"Column '{col_config.name}' uses cross-entity prev() which is not supported. "
                        f"Expression: {generator.expression}\n"
                        f"Use prev() only for the current entity's own columns."
                    )

            # Check entity_overrides for cross-entity references
            for override_gen in col_config.entity_overrides.values():
                if isinstance(override_gen, DerivedGeneratorConfig):
                    refs = self._extract_entity_references(override_gen.expression, entity_set)
                    cross_entity_refs.update(refs)

                    if re.search(
                        r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*prev\s*\(", override_gen.expression
                    ):
                        raise ValueError(
                            f"Column '{col_config.name}' uses cross-entity prev() which is not supported. "
                            f"Expression: {override_gen.expression}\n"
                            f"Use prev() only for the current entity's own columns."
                        )

        self.cross_entity_enabled = len(cross_entity_refs) > 0

        if self.cross_entity_enabled:
            # Create entity proxies (reused across all timestamps)
            self.entity_proxies = {name: EntityProxy(name) for name in self.entity_names}
        else:
            self.entity_proxies = {}

    def _extract_entity_references(self, expression: str, known_entities: Set[str]) -> Set[str]:
        """Extract entity names from cross-entity references in expression.

        Args:
            expression: Python expression string
            known_entities: Set of valid entity names

        Returns:
            Set of (entity_name, column_name) tuples for cross-entity references

        Example:
            expression = "Tank_A.level + Tank_B.flow * 0.5"
            known_entities = {"Tank_A", "Tank_B", "Tank_C"}
            returns: {("Tank_A", "level"), ("Tank_B", "flow")}
        """
        # Pattern: EntityName.column_name
        # Match: identifier followed by dot and another identifier
        pattern = r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\.\s*([A-Za-z_][A-Za-z0-9_]*)\b"
        matches = re.findall(pattern, expression)

        refs = set()
        for entity_name, column_name in matches:
            if entity_name in known_entities:
                refs.add((entity_name, column_name))

        return refs

    def _extract_entity_id_guard(self, expression: str) -> Optional[str]:
        """Extract entity name from an entity_id guard in a conditional expression.

        Detects patterns like:
            "... if entity_id == 'SystemB_Processor' else ..."

        Returns:
            The guarded entity name, or None if no guard found.
        """
        match = re.search(r"\bif\s+entity_id\s*==\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", expression)
        return match.group(1) if match else None

    def _build_entity_dependency_dag(self):
        """Build entity dependency DAG and compute generation order.

        Analyzes cross-entity references to determine which entities must be
        generated before others at each timestamp.

        Sets:
            self.entity_generation_order: List of entity names in topological order

        Raises:
            ValueError: If circular cross-entity dependencies detected
        """
        if not self.cross_entity_enabled:
            # No cross-entity refs: use original entity order (doesn't matter)
            self.entity_generation_order = self.entity_names
            return

        # Build dependency graph: entity -> set of entities it depends on
        dependencies: Dict[str, Set[str]] = {name: set() for name in self.entity_names}
        entity_set = set(self.entity_names)

        # For each entity, find which other entities it references
        for entity_name in self.entity_names:
            for col_config in self.config.columns:
                # Determine the generator this entity will use for this column
                generator = col_config.entity_overrides.get(entity_name, col_config.generator)
                if isinstance(generator, DerivedGeneratorConfig):
                    # For base generators (not overrides), check for entity_id guards
                    # e.g. "SystemA.output * 0.85 if entity_id == 'SystemB' else 0"
                    # Only assign refs to the guarded entity, not all entities
                    if entity_name not in col_config.entity_overrides:
                        guarded_entity = self._extract_entity_id_guard(generator.expression)
                        if guarded_entity and guarded_entity != entity_name:
                            # This expression is guarded for a different entity; skip
                            continue
                    refs = self._extract_entity_references(generator.expression, entity_set)
                    for ref_entity, ref_column in refs:
                        if ref_entity != entity_name:
                            dependencies[entity_name].add(ref_entity)

        # Topological sort using DFS
        sorted_entities = []
        visited = set()
        visiting = set()

        def visit(entity_name: str, path: List[str]):
            if entity_name in visited:
                return
            if entity_name in visiting:
                # Circular dependency detected
                cycle_path = path + [entity_name]
                cycle_str = " -> ".join(cycle_path)
                raise ValueError(
                    f"Circular cross-entity dependency detected: {cycle_str}\n"
                    f"Entities cannot reference each other in a cycle. "
                    f"Consider using prev() for feedback loops within an entity."
                )

            visiting.add(entity_name)
            for dep_entity in dependencies[entity_name]:
                visit(dep_entity, path + [entity_name])
            visiting.remove(entity_name)
            visited.add(entity_name)
            sorted_entities.append(entity_name)

        for entity_name in self.entity_names:
            visit(entity_name, [])

        self.entity_generation_order = sorted_entities

    def _generate_entity_names(self) -> List[str]:
        """Generate entity names based on configuration.

        Returns:
            List of entity names
        """
        if self.config.entities.names:
            return sorted(self.config.entities.names)

        # Generate entity names
        entity_count = self.config.entities.count
        prefix = self.config.entities.id_prefix

        if self.config.entities.id_format == "sequential":
            # Zero-pad based on entity count (minimum 2 digits)
            width = max(2, len(str(entity_count)))
            return [f"{prefix}{str(i + 1).zfill(width)}" for i in range(entity_count)]
        else:  # uuid
            # Generate deterministic UUIDs from seed
            temp_rng = np.random.default_rng(self.config.scope.seed)
            return [
                f"{prefix}{uuid.UUID(int=temp_rng.integers(0, 2**128))}"
                for _ in range(entity_count)
            ]

    def generate(self) -> List[Dict[str, Any]]:
        """Generate simulated data.

        Returns:
            List of row dictionaries
        """
        # Choose generation strategy based on cross-entity references
        if self.cross_entity_enabled:
            return self._generate_timestamp_major()
        else:
            return self._generate_entity_major()

    def _generate_entity_major(self) -> List[Dict[str, Any]]:
        """Generate data using entity-major strategy (all rows for entity 1, then entity 2, etc.).

        This is the original/default strategy, used when no cross-entity references exist.

        Returns:
            List of row dictionaries
        """
        from odibi.utils.logging_context import get_logging_context

        ctx = get_logging_context()
        rows = []

        total_entities = len(self.entity_names)

        # Generate rows for each entity
        for entity_idx, entity_name in enumerate(self.entity_names):
            # Progress logging for large datasets
            if entity_idx > 0 and entity_idx % 10 == 0:
                progress_pct = int((entity_idx / total_entities) * 100)
                ctx.info(
                    "Simulation progress",
                    entities_completed=entity_idx,
                    total_entities=total_entities,
                    progress_pct=progress_pct,
                    rows_generated=len(rows),
                )
            # Create entity-specific RNG state for determinism
            # Use stable hash (hashlib.md5) instead of Python's hash() which is process-randomized
            entity_hash = hashlib.md5(entity_name.encode()).hexdigest()[:8]
            base_entity_seed = self.config.scope.seed + int(entity_hash, 16) % (2**31)

            # Advance RNG for incremental mode
            # This ensures new runs don't repeat the same random sequence
            entity_seed = base_entity_seed + self.rows_before_hwm

            entity_rng = np.random.default_rng(entity_seed)

            # Generate rows for this entity
            entity_rows = self._generate_entity_rows(entity_name, entity_idx, entity_rng)
            rows.extend(entity_rows)

        # Apply chaos parameters
        if self.config.chaos:
            rows = self._apply_chaos(rows)

        # Sort by timestamp if timestamp column exists
        timestamp_cols = [c.name for c in self.config.columns if c.generator.type == "timestamp"]
        if timestamp_cols:
            rows.sort(key=lambda r: r.get(timestamp_cols[0], ""))

        return rows

    def _generate_timestamp_major(self) -> List[Dict[str, Any]]:
        """Generate data using timestamp-major strategy (row 0 for all entities, then row 1, etc.).

        This strategy is used when cross-entity references exist, as entities must be
        generated in dependency order at each timestamp to allow Entity.column references.

        Returns:
            List of row dictionaries
        """
        from odibi.utils.logging_context import get_logging_context

        ctx = get_logging_context()
        rows = []

        # Create entity-specific RNGs (deterministic based on entity name + seed)
        entity_rngs = {}
        for entity_name in self.entity_names:
            entity_hash = hashlib.md5(entity_name.encode()).hexdigest()[:8]
            base_entity_seed = self.config.scope.seed + int(entity_hash, 16) % (2**31)

            # Advance RNG for incremental mode
            if self.hwm_timestamp:
                rows_before_hwm = int(
                    (self.effective_start_time - self.start_time).total_seconds()
                    / self.timestep_seconds
                )
                entity_seed = base_entity_seed + rows_before_hwm
            else:
                entity_seed = base_entity_seed

            entity_rngs[entity_name] = np.random.default_rng(entity_seed)

        # Initialize random walk state per entity
        random_walk_current = {}
        col_map = {col.name: col for col in self.config.columns}
        for entity_name in self.entity_names:
            random_walk_current[entity_name] = {}
            for col_config in self.config.columns:
                gen = col_config.entity_overrides.get(entity_name, col_config.generator)
                if isinstance(gen, RandomWalkGeneratorConfig):
                    if (
                        entity_name in self.random_walk_state
                        and col_config.name in self.random_walk_state[entity_name]
                    ):
                        random_walk_current[entity_name][col_config.name] = self.random_walk_state[
                            entity_name
                        ][col_config.name]
                    else:
                        random_walk_current[entity_name][col_config.name] = gen.start

        # Initialize entity state for prev() and stateful functions
        for entity_name in self.entity_names:
            if entity_name not in self.entity_state:
                self.entity_state[entity_name] = {}

        # Generate row by row (timestamp-major loop)
        for row_idx in range(self.total_rows):
            row_timestamp = self.effective_start_time + timedelta(
                seconds=self.timestep_seconds * row_idx
            )

            # Progress logging
            if row_idx > 0 and row_idx % 100 == 0:
                progress_pct = int((row_idx / self.total_rows) * 100)
                ctx.info(
                    "Simulation progress (timestamp-major)",
                    rows_completed=row_idx,
                    total_rows=self.total_rows,
                    progress_pct=progress_pct,
                    entities=len(self.entity_names),
                )

            # Unbind all entity proxies at start of timestamp
            for proxy in self.entity_proxies.values():
                proxy.bind(None)

            current_tick_rows = {}

            # Generate row for each entity in dependency order
            for entity_idx, entity_name in enumerate(self.entity_generation_order):
                # Check downtime for this entity
                if self._is_in_downtime(entity_name, row_timestamp):
                    continue

                row = {}
                entity_rng = entity_rngs[entity_name]
                entity_state = self.entity_state[entity_name]
                entity_random_walk = random_walk_current[entity_name]

                # Bind proxy to current row early so self-references work
                if entity_name in self.entity_proxies:
                    self.entity_proxies[entity_name].bind(row)

                # Generate columns in dependency order
                for col_name in self.column_order:
                    col_config = col_map[col_name]
                    generator = col_config.entity_overrides.get(entity_name, col_config.generator)

                    # Generate value (passing entity_proxies for cross-entity refs)
                    value = self._generate_value(
                        generator,
                        entity_name,
                        entity_idx,
                        row_timestamp,
                        row_idx,
                        entity_rng,
                        row,
                        entity_random_walk,
                        entity_state,
                    )

                    # Check for scheduled events that override this value
                    event_context = {**row, col_name: value}
                    resolved = self._resolve_scheduled_event(
                        entity_name,
                        col_name,
                        row_timestamp,
                        row_data=event_context,
                        entity_state=entity_state,
                        row_idx=row_idx,
                    )
                    if resolved:
                        value = self._apply_scheduled_event(
                            base_value=value,
                            resolved=resolved,
                            entity_name=entity_name,
                            column_name=col_name,
                            timestamp=row_timestamp,
                            random_walk_current=entity_random_walk,
                        )

                    # Apply null_rate
                    if col_config.null_rate > 0 and entity_rng.random() < col_config.null_rate:
                        value = None

                    # Cast to declared data_type
                    if value is not None and col_config.data_type == "int":
                        value = int(round(value)) if isinstance(value, float) else value

                    row[col_name] = value

                # Update entity state with current row values for next iteration
                for col_name in row.keys():
                    if row[col_name] is not None:
                        entity_state[col_name] = row[col_name]

                # Store row and bind proxy for downstream entities
                current_tick_rows[entity_name] = row
                if entity_name in self.entity_proxies:
                    self.entity_proxies[entity_name].bind(row)

            # Collect all rows from this timestamp
            for entity_name in self.entity_generation_order:
                if entity_name in current_tick_rows:
                    rows.append(current_tick_rows[entity_name])

        # Apply chaos parameters
        if self.config.chaos:
            rows = self._apply_chaos(rows)

        # Sort by timestamp if timestamp column exists
        timestamp_cols = [c.name for c in self.config.columns if c.generator.type == "timestamp"]
        if timestamp_cols:
            rows.sort(key=lambda r: r.get(timestamp_cols[0], ""))

        return rows

    def _generate_entity_rows(
        self, entity_name: str, entity_idx: int, entity_rng: np.random.Generator
    ) -> List[Dict[str, Any]]:
        """Generate rows for a single entity.

        Args:
            entity_name: Entity name
            entity_idx: Entity index (0-based)
            entity_rng: Entity-specific random number generator

        Returns:
            List of row dictionaries for this entity
        """
        rows = []

        # Initialize random walk current values from state or config defaults
        random_walk_current = {}
        col_map = {col.name: col for col in self.config.columns}
        for col_config in self.config.columns:
            gen = col_config.entity_overrides.get(entity_name, col_config.generator)
            if isinstance(gen, RandomWalkGeneratorConfig):
                # Restore from state if available, otherwise use start value
                if (
                    entity_name in self.random_walk_state
                    and col_config.name in self.random_walk_state[entity_name]
                ):
                    random_walk_current[col_config.name] = self.random_walk_state[entity_name][
                        col_config.name
                    ]
                else:
                    random_walk_current[col_config.name] = gen.start

        # NEW: Initialize entity state for prev() and stateful functions
        if entity_name not in self.entity_state:
            self.entity_state[entity_name] = {}
        entity_state = self.entity_state[entity_name]

        for row_idx in range(self.total_rows):
            row_timestamp = self.effective_start_time + timedelta(
                seconds=self.timestep_seconds * row_idx
            )

            # Check downtime events
            if self._is_in_downtime(entity_name, row_timestamp):
                continue

            row = {}

            # Generate columns in dependency order
            for col_name in self.column_order:
                col_config = col_map[col_name]

                # Check for entity-specific override
                generator = col_config.entity_overrides.get(entity_name, col_config.generator)

                # Generate value
                value = self._generate_value(
                    generator,
                    entity_name,
                    entity_idx,
                    row_timestamp,
                    row_idx,
                    entity_rng,
                    row,  # Pass current row for derived columns
                    random_walk_current,
                    entity_state,  # NEW: Pass entity state for stateful functions
                )

                # Check for scheduled events that override this value
                event_context = {**row, col_name: value}
                resolved = self._resolve_scheduled_event(
                    entity_name,
                    col_name,
                    row_timestamp,
                    row_data=event_context,
                    entity_state=entity_state,
                    row_idx=row_idx,
                )
                if resolved:
                    value = self._apply_scheduled_event(
                        base_value=value,
                        resolved=resolved,
                        entity_name=entity_name,
                        column_name=col_name,
                        timestamp=row_timestamp,
                        random_walk_current=random_walk_current,
                    )

                # Apply null_rate
                if col_config.null_rate > 0 and entity_rng.random() < col_config.null_rate:
                    value = None

                # Cast to declared data_type
                if value is not None and col_config.data_type == "int":
                    value = int(round(value)) if isinstance(value, float) else value

                row[col_config.name] = value

            # NEW: Update entity state with current row values for next iteration
            for col_name in row.keys():
                if row[col_name] is not None:  # Only store non-null values
                    entity_state[col_name] = row[col_name]

            rows.append(row)

        return rows

    def _is_in_downtime(self, entity_name: str, timestamp: datetime) -> bool:
        """Check if entity is in downtime at given timestamp.

        Args:
            entity_name: Entity name
            timestamp: Timestamp to check

        Returns:
            True if in downtime
        """
        if not self.config.chaos or not self.config.chaos.downtime_events:
            return False

        for event in self.config.chaos.downtime_events:
            # Check if event applies to this entity
            if event.entity is not None and event.entity != entity_name:
                continue

            # Parse event times
            start_dt = datetime.fromisoformat(event.start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(event.end_time.replace("Z", "+00:00"))

            # Check if timestamp is in downtime window
            if start_dt <= timestamp <= end_dt:
                return True

        return False

    @staticmethod
    def _parse_interval(interval: str) -> timedelta:
        """Parse interval string like '30d', '4h', '5m', '10s' into timedelta."""
        match = re.match(r"^(\d+)([smhd])$", interval)
        if not match:
            raise ValueError(f"Invalid interval format: '{interval}'")
        value, unit = int(match.group(1)), match.group(2)
        return timedelta(seconds=value * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit])

    def _prepare_scheduled_events(self) -> List[PreparedScheduledEvent]:
        """Pre-parse all scheduled events into PreparedScheduledEvent objects."""
        if not self.config.scheduled_events:
            return []

        prepared = []
        for idx, event in enumerate(self.config.scheduled_events):
            start_dt = (
                datetime.fromisoformat(event.start_time.replace("Z", "+00:00"))
                if event.start_time
                else None
            )
            end_dt = (
                datetime.fromisoformat(event.end_time.replace("Z", "+00:00"))
                if event.end_time
                else None
            )
            duration_td = self._parse_interval(event.duration) if event.duration else None
            recurrence_td = self._parse_interval(event.recurrence) if event.recurrence else None
            jitter_td = self._parse_interval(event.jitter) if event.jitter else None
            cooldown_td = self._parse_interval(event.cooldown) if event.cooldown else None

            # Calculate sustain in timesteps
            sustain_steps = 0
            if event.sustain:
                sustain_td = self._parse_interval(event.sustain)
                sustain_steps = max(
                    1, math.ceil(sustain_td.total_seconds() / self.timestep_seconds)
                )

            # Compute effective end for non-recurring single events with duration
            if duration_td and not recurrence_td and start_dt and not end_dt:
                end_dt = start_dt + duration_td

            # Pre-expand recurring events into occurrence windows
            occurrence_windows: List[Tuple[datetime, datetime]] = []
            if recurrence_td and start_dt:
                # Determine per-occurrence active window length
                if duration_td:
                    window_len = duration_td
                elif end_dt and not duration_td:
                    # Use original end_time - start_time as template window
                    window_len = end_dt - start_dt
                else:
                    # Recurring with no duration/end_time: instantaneous pulse (1 timestep)
                    window_len = timedelta(seconds=self.timestep_seconds)

                max_occ = event.max_occurrences or 10000  # safety cap
                occ_idx = 0
                while occ_idx < max_occ:
                    occ_start = start_dt + recurrence_td * occ_idx
                    # Apply jitter deterministically per occurrence
                    if jitter_td:
                        jitter_seed = self.config.scope.seed + idx * 1000 + occ_idx
                        jitter_rng = np.random.default_rng(jitter_seed)
                        jitter_offset = jitter_rng.uniform(
                            -jitter_td.total_seconds(), jitter_td.total_seconds()
                        )
                        occ_start = occ_start + timedelta(seconds=jitter_offset)
                    occ_end = occ_start + window_len
                    # Stop if occurrence is entirely past simulation horizon
                    if occ_start > self.end_time:
                        break
                    # Only include if occurrence overlaps simulation window
                    if occ_end >= self.effective_start_time:
                        occurrence_windows.append((occ_start, occ_end))
                    occ_idx += 1

            pe = PreparedScheduledEvent(
                event_id=idx,
                config=event,
                start_dt=start_dt,
                end_dt=end_dt,
                duration_td=duration_td,
                recurrence_td=recurrence_td,
                jitter_td=jitter_td,
                cooldown_td=cooldown_td,
                sustain_steps=sustain_steps,
                occurrence_windows=occurrence_windows,
            )
            prepared.append(pe)

        return prepared

    def _get_event_state(self, entity_name: str, event_id: int) -> Dict[str, Any]:
        """Get or initialize runtime state for an entity/event pair."""
        if entity_name not in self.scheduled_event_state:
            self.scheduled_event_state[entity_name] = {}
        if event_id not in self.scheduled_event_state[entity_name]:
            self.scheduled_event_state[entity_name][event_id] = {
                "consecutive_true_steps": 0,
                "last_trigger_time": None,
                "active_until": None,
                "ramp_start_value": None,
                "ramp_started_at": None,
            }
        return self.scheduled_event_state[entity_name][event_id]

    def _eval_condition(
        self,
        condition_expr: str,
        row_data: Dict[str, Any],
        entity_name: str,
        timestamp: datetime,
    ) -> bool:
        """Evaluate a condition expression against current row data."""
        safe_builtins = {
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "True": True,
            "False": False,
            "true": True,
            "false": False,
            "None": None,
        }
        namespace = {**row_data, **safe_builtins, **self.entity_proxies}
        namespace["entity_id"] = entity_name
        namespace["_timestamp"] = timestamp
        try:
            return bool(eval(condition_expr, {"__builtins__": {}}, namespace))
        except Exception:
            return False

    def _resolve_scheduled_event(
        self,
        entity_name: str,
        column_name: str,
        timestamp: datetime,
        row_data: Optional[Dict[str, Any]] = None,
        entity_state: Optional[Dict[str, Any]] = None,
        row_idx: int = 0,
    ) -> Optional[ResolvedScheduledEvent]:
        """Resolve the highest-priority active scheduled event for entity/column/timestamp.

        Handles time-based windows, recurrence, condition-based triggers, and sustain/cooldown.
        """
        if not self.prepared_events:
            return None

        active: List[Tuple[PreparedScheduledEvent, datetime, Optional[datetime]]] = []

        for pe in self.prepared_events:
            event = pe.config

            # Filter by entity
            if event.entity is not None and event.entity != entity_name:
                continue
            # Filter by column
            if event.column != column_name:
                continue

            es = self._get_event_state(entity_name, pe.event_id)

            # --- Condition-based events ---
            if event.condition is not None:
                cond_result = self._eval_condition(
                    event.condition,
                    row_data or {},
                    entity_name,
                    timestamp,
                )

                # Check if currently in a held duration window
                if es["active_until"] is not None and timestamp <= es["active_until"]:
                    active.append((pe, es.get("trigger_time", timestamp), es["active_until"]))
                    continue

                if es["active_until"] is not None and timestamp > es["active_until"]:
                    # Duration window expired, reset
                    es["active_until"] = None
                    es["ramp_start_value"] = None
                    es["ramp_started_at"] = None

                if cond_result:
                    es["consecutive_true_steps"] += 1
                else:
                    es["consecutive_true_steps"] = 0
                    continue

                # Check sustain
                if pe.sustain_steps > 0 and es["consecutive_true_steps"] < pe.sustain_steps:
                    continue

                # Check cooldown
                if pe.cooldown_td and es["last_trigger_time"] is not None:
                    if timestamp < es["last_trigger_time"] + pe.cooldown_td:
                        continue

                # Trigger!
                es["last_trigger_time"] = timestamp
                es["trigger_time"] = timestamp
                es["consecutive_true_steps"] = 0  # Reset after trigger

                if pe.duration_td:
                    es["active_until"] = timestamp + pe.duration_td

                window_end = es["active_until"]
                active.append((pe, timestamp, window_end))
                continue

            # --- Recurring time-based events ---
            if pe.occurrence_windows:
                for occ_start, occ_end in pe.occurrence_windows:
                    if occ_start <= timestamp <= occ_end:
                        active.append((pe, occ_start, occ_end))
                        break
                continue

            # --- Simple time-based events ---
            if pe.start_dt is not None:
                if pe.end_dt is not None:
                    if pe.start_dt <= timestamp <= pe.end_dt:
                        active.append((pe, pe.start_dt, pe.end_dt))
                else:
                    # Permanent event (no end time)
                    if timestamp >= pe.start_dt:
                        active.append((pe, pe.start_dt, None))

        if not active:
            return None

        # Pick highest priority
        best_pe, best_start, best_end = max(active, key=lambda t: t[0].config.priority)
        event = best_pe.config

        # Determine ramp info
        is_ramp = event.transition == "ramp"
        ramp_duration_seconds = 0.0
        if is_ramp:
            if best_pe.duration_td:
                ramp_duration_seconds = best_pe.duration_td.total_seconds()
            elif best_end and best_start:
                ramp_duration_seconds = (best_end - best_start).total_seconds()

        return ResolvedScheduledEvent(
            config=event,
            event_id=best_pe.event_id,
            window_start=best_start,
            window_end=best_end,
            is_ramp=is_ramp,
            ramp_duration_seconds=ramp_duration_seconds,
        )

    def _apply_scheduled_event(
        self,
        base_value: Any,
        resolved: ResolvedScheduledEvent,
        entity_name: str,
        column_name: str,
        timestamp: datetime,
        random_walk_current: Dict[str, float],
    ) -> Any:
        """Apply a resolved scheduled event, returning the overridden value."""
        event = resolved.config
        target_value = event.value
        es = self._get_event_state(entity_name, resolved.event_id)

        # Handle ramp transition
        if resolved.is_ramp and resolved.ramp_duration_seconds > 0:
            # Capture ramp start value on first encounter
            if es.get("ramp_start_value") is None:
                es["ramp_start_value"] = base_value
                es["ramp_started_at"] = resolved.window_start

            ramp_start = es["ramp_start_value"]
            if ramp_start is not None and target_value is not None:
                elapsed = (timestamp - resolved.window_start).total_seconds()
                progress = min(1.0, max(0.0, elapsed / resolved.ramp_duration_seconds))
                value = float(ramp_start) + progress * (float(target_value) - float(ramp_start))
            else:
                value = target_value
        else:
            value = target_value

        # Reset random walk state for parameter_override
        if event.type == ScheduledEventType.PARAMETER_OVERRIDE:
            if column_name in random_walk_current:
                random_walk_current[column_name] = float(value) if value is not None else 0.0

        return value

    def _get_active_scheduled_event(
        self, entity_name: str, column_name: str, timestamp: datetime
    ) -> Optional[ScheduledEvent]:
        """Legacy wrapper: get active scheduled event (time-based only, no conditions/ramp).

        Used by call sites that haven't been updated to the new resolver.
        """
        resolved = self._resolve_scheduled_event(entity_name, column_name, timestamp)
        return resolved.config if resolved else None

    def _generate_value(
        self,
        generator,
        entity_name: str,
        entity_idx: int,
        timestamp: datetime,
        row_idx: int,
        rng: np.random.Generator,
        current_row: Dict[str, Any] = None,
        random_walk_current: Dict[str, float] = None,
        entity_state: Dict[str, Any] = None,
    ) -> Any:
        """Generate a single value based on generator configuration.

        Args:
            generator: Generator configuration
            entity_name: Entity name
            entity_idx: Entity index
            timestamp: Row timestamp
            row_idx: Row index
            rng: Random number generator
            current_row: Current row data (for derived columns)
            entity_state: Entity-specific state for stateful functions

        Returns:
            Generated value
        """
        if isinstance(generator, RangeGeneratorConfig):
            return self._generate_range(generator, rng)
        elif isinstance(generator, RandomWalkGeneratorConfig):
            return self._generate_random_walk(generator, rng, random_walk_current, current_row)
        elif isinstance(generator, CategoricalGeneratorConfig):
            return self._generate_categorical(generator, rng)
        elif isinstance(generator, BooleanGeneratorConfig):
            return self._generate_boolean(generator, rng)
        elif isinstance(generator, TimestampGeneratorConfig):
            # Return Zulu time format (Z suffix) for consistency
            return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(generator, SequentialGeneratorConfig):
            global_row = row_idx + self.rows_before_hwm
            if generator.unique_across_entities:
                global_row += entity_idx * self.total_rows
            return generator.start + (global_row * generator.step)
        elif isinstance(generator, ConstantGeneratorConfig):
            # Support magic variables
            value = str(generator.value)
            value = value.replace("{entity_id}", entity_name)
            value = value.replace("{entity_index}", str(entity_idx))
            value = value.replace("{timestamp}", timestamp.isoformat())
            value = value.replace("{row_number}", str(row_idx + self.rows_before_hwm))
            # Convert back to original type if it wasn't a template
            if value == str(generator.value):
                return generator.value
            return value
        elif isinstance(generator, UUIDGeneratorConfig):
            return self._generate_uuid(generator, entity_name, row_idx, rng)
        elif isinstance(generator, EmailGeneratorConfig):
            return self._generate_email(generator, entity_name, entity_idx, row_idx)
        elif isinstance(generator, IPGeneratorConfig):
            return self._generate_ipv4(generator, rng)
        elif isinstance(generator, GeoGeneratorConfig):
            return self._generate_geo(generator, rng)
        elif isinstance(generator, DailyProfileGeneratorConfig):
            return self._generate_daily_profile(generator, timestamp, rng)
        elif isinstance(generator, DerivedGeneratorConfig):
            return self._generate_derived(
                generator,
                current_row or {},
                entity_name,
                entity_state if entity_state is not None else {},
                row_idx,
                timestamp,
            )
        else:
            raise ValueError(f"Unknown generator type: {type(generator)}")

    def _generate_range(self, config: RangeGeneratorConfig, rng: np.random.Generator) -> float:
        """Generate value from range generator.

        Args:
            config: Range generator configuration
            rng: Random number generator

        Returns:
            Generated value
        """
        if config.distribution == "uniform":
            return float(rng.uniform(config.min, config.max))
        else:  # normal
            mean = config.mean if config.mean is not None else (config.min + config.max) / 2
            std_dev = (
                config.std_dev if config.std_dev is not None else (config.max - config.min) / 6
            )

            # Generate normal value and clip to range
            value = rng.normal(mean, std_dev)
            return float(np.clip(value, config.min, config.max))

    def _generate_random_walk(
        self,
        config: RandomWalkGeneratorConfig,
        rng: np.random.Generator,
        current_values: Dict[str, float],
        current_row: Dict[str, Any] = None,
    ) -> float:
        """Generate value using random walk with mean reversion.

        Implements an Ornstein-Uhlenbeck process with optional trend.
        Each value depends on the previous value, creating smooth, realistic
        time-series data suitable for process simulation.

        Args:
            config: Random walk generator configuration
            rng: Random number generator
            current_values: Dict tracking current value per column name.
                            Updated in-place with new value.
            current_row: Current row data (for mean_reversion_to lookups)

        Returns:
            Generated value
        """
        # Find the column name for this generator to track state
        col_name = None
        for col in self.config.columns:
            if col.generator is config or any(ov is config for ov in col.entity_overrides.values()):
                col_name = col.name
                break

        if col_name is None:
            # Fallback: use start value if we can't identify the column
            current = config.start
        else:
            current = current_values.get(col_name, config.start)

        # Determine reversion target (static start or dynamic column)
        if config.mean_reversion_to is not None and current_row is not None:
            # Dynamic setpoint from another column
            if config.mean_reversion_to in current_row:
                reversion_target = float(current_row[config.mean_reversion_to])
            else:
                # Column not yet available in this row (dependency order issue)
                # Fall back to static start value
                reversion_target = config.start
        else:
            # Static setpoint
            reversion_target = config.start

        # Ornstein-Uhlenbeck step:
        # dx = mean_reversion * (target - current) * dt + volatility * dW + trend
        # Where dW is random noise, target can be static or dynamic
        noise = rng.normal(0, config.volatility)
        reversion_pull = config.mean_reversion * (reversion_target - current)
        new_value = current + reversion_pull + noise + config.trend

        # Apply shock event (sudden process upset)
        if config.shock_rate > 0 and rng.random() < config.shock_rate:
            shock_size = rng.uniform(0, config.shock_magnitude)
            # Determine direction based on bias
            if config.shock_bias >= 1.0:
                direction = 1.0
            elif config.shock_bias <= -1.0:
                direction = -1.0
            else:
                # Bias shifts probability: 0.0 = 50/50, 0.7 = 85% up, -0.7 = 85% down
                up_probability = (1.0 + config.shock_bias) / 2.0
                direction = 1.0 if rng.random() < up_probability else -1.0
            new_value += shock_size * direction

        # Clamp to physical bounds
        new_value = float(np.clip(new_value, config.min, config.max))

        # Apply precision rounding
        if config.precision is not None:
            new_value = round(new_value, config.precision)

        # Update current value for next row
        if col_name is not None and current_values is not None:
            current_values[col_name] = new_value

        return new_value

    def _generate_categorical(
        self, config: CategoricalGeneratorConfig, rng: np.random.Generator
    ) -> Any:
        """Generate value from categorical generator.

        Args:
            config: Categorical generator configuration
            rng: Random number generator

        Returns:
            Generated value
        """
        if config.weights:
            return rng.choice(config.values, p=config.weights)
        else:
            return rng.choice(config.values)

    def _generate_boolean(self, config: BooleanGeneratorConfig, rng: np.random.Generator) -> bool:
        """Generate boolean value.

        Args:
            config: Boolean generator configuration
            rng: Random number generator

        Returns:
            Generated boolean
        """
        return bool(rng.random() < config.true_probability)

    def _generate_uuid(
        self,
        config: UUIDGeneratorConfig,
        entity_name: str,
        row_idx: int,
        rng: np.random.Generator,
    ) -> str:
        """Generate UUID value.

        Args:
            config: UUID generator configuration
            entity_name: Entity name
            row_idx: Row index
            rng: Random number generator

        Returns:
            UUID string
        """
        if config.version == 4:
            # UUID4: Random (uses RNG for determinism)
            # Generate random bytes from RNG for reproducibility
            random_bytes = rng.bytes(16)
            # Set version and variant bits for UUID4
            random_bytes = bytearray(random_bytes)
            random_bytes[6] = (random_bytes[6] & 0x0F) | 0x40  # Version 4
            random_bytes[8] = (random_bytes[8] & 0x3F) | 0x80  # Variant
            return str(uuid.UUID(bytes=bytes(random_bytes)))
        else:  # version 5
            # UUID5: Deterministic from namespace + name
            namespace_uuid = uuid.NAMESPACE_DNS
            if config.namespace:
                try:
                    namespace_uuid = uuid.UUID(config.namespace)
                except ValueError:
                    # Use as string namespace
                    namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, config.namespace)

            # Combine entity and row for uniqueness
            name = f"{entity_name}:{row_idx}"
            return str(uuid.uuid5(namespace_uuid, name))

    def _generate_email(
        self,
        config: EmailGeneratorConfig,
        entity_name: str,
        entity_idx: int,
        row_idx: int,
    ) -> str:
        """Generate email address.

        Args:
            config: Email generator configuration
            entity_name: Entity name
            entity_idx: Entity index
            row_idx: Row index

        Returns:
            Email address string
        """
        # Process pattern with substitutions
        username = config.pattern.replace("{entity}", entity_name)
        username = username.replace("{index}", str(entity_idx))
        username = username.replace("{row}", str(row_idx))

        # Sanitize for email (replace underscores/spaces with dots)
        username = username.replace("_", ".").replace(" ", ".")

        return f"{username}@{config.domain}"

    def _generate_ipv4(self, config: IPGeneratorConfig, rng: np.random.Generator) -> str:
        """Generate IPv4 address.

        Args:
            config: IP generator configuration
            rng: Random number generator

        Returns:
            IPv4 address string
        """
        if config.subnet:
            # Parse CIDR notation
            import ipaddress

            network = ipaddress.IPv4Network(config.subnet, strict=False)

            # Generate random IP in subnet
            num_addresses = network.num_addresses
            random_offset = rng.integers(0, num_addresses)
            ip = network.network_address + random_offset

            return str(ip)
        else:
            # Full range (avoid reserved ranges for realism)
            # Generate in 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 private ranges
            octets = rng.integers(0, 256, size=4)
            return f"{octets[0]}.{octets[1]}.{octets[2]}.{octets[3]}"

    def _generate_geo(self, config: GeoGeneratorConfig, rng: np.random.Generator) -> Any:
        """Generate geographic coordinates.

        Args:
            config: Geo generator configuration
            rng: Random number generator

        Returns:
            Tuple of (lat, lon) or separate values based on format
        """
        min_lat, min_lon, max_lat, max_lon = config.bbox

        lat = float(rng.uniform(min_lat, max_lat))
        lon = float(rng.uniform(min_lon, max_lon))

        if config.format == "tuple":
            return (lat, lon)
        else:
            # This will be handled specially in column generation
            # For now return tuple, caller can split if needed
            return {"lat": lat, "lon": lon}

    def _generate_daily_profile(
        self,
        config: DailyProfileGeneratorConfig,
        timestamp: datetime,
        rng: np.random.Generator,
    ) -> float:
        """Generate value from daily profile based on time of day.

        Interpolates between anchor points in the profile to produce a smooth
        daily curve, then adds noise and clamps to [min, max].

        Args:
            config: Daily profile generator configuration
            timestamp: Current row timestamp
            rng: Random number generator

        Returns:
            Generated value following the daily curve
        """
        # Convert profile keys to sorted list of (minutes_since_midnight, value)
        anchors = []
        for time_key, value in config.profile.items():
            parts = time_key.split(":")
            minutes = int(parts[0]) * 60 + int(parts[1])
            anchors.append((minutes, value))
        anchors.sort(key=lambda x: x[0])

        # Apply day-to-day volatility: shift each anchor point independently per day
        if config.volatility > 0:
            # Use date as seed so same day always gets same shifts (deterministic)
            day_seed = timestamp.year * 10000 + timestamp.month * 100 + timestamp.day
            day_rng = np.random.default_rng(seed=day_seed)
            anchors = [
                (minutes, val + day_rng.normal(0, config.volatility)) for minutes, val in anchors
            ]

        # Current time in minutes since midnight
        current_minutes = timestamp.hour * 60 + timestamp.minute

        # Apply weekend scaling
        is_weekend = timestamp.weekday() >= 5  # Saturday=5, Sunday=6

        # Find surrounding anchor points
        if len(anchors) == 1:
            value = anchors[0][1]
        elif current_minutes <= anchors[0][0]:
            # Before first anchor — interpolate between last anchor (wrapped) and first
            if config.interpolation == "step":
                value = anchors[-1][1]
            else:
                prev_minutes = anchors[-1][0] - 1440  # wrap previous day
                prev_value = anchors[-1][1]
                next_minutes = anchors[0][0]
                next_value = anchors[0][1]
                span = next_minutes - prev_minutes
                if span == 0:
                    value = next_value
                else:
                    t = (current_minutes - prev_minutes) / span
                    value = prev_value + t * (next_value - prev_value)
        elif current_minutes >= anchors[-1][0]:
            # After last anchor — interpolate between last anchor and first (wrapped)
            if config.interpolation == "step":
                value = anchors[-1][1]
            else:
                prev_minutes = anchors[-1][0]
                prev_value = anchors[-1][1]
                next_minutes = anchors[0][0] + 1440  # wrap next day
                next_value = anchors[0][1]
                span = next_minutes - prev_minutes
                if span == 0:
                    value = prev_value
                else:
                    t = (current_minutes - prev_minutes) / span
                    value = prev_value + t * (next_value - prev_value)
        else:
            # Between two anchors
            for i in range(len(anchors) - 1):
                if anchors[i][0] <= current_minutes < anchors[i + 1][0]:
                    if config.interpolation == "step":
                        value = anchors[i][1]
                    else:
                        prev_minutes, prev_value = anchors[i]
                        next_minutes, next_value = anchors[i + 1]
                        span = next_minutes - prev_minutes
                        if span == 0:
                            value = prev_value
                        else:
                            t = (current_minutes - prev_minutes) / span
                            value = prev_value + t * (next_value - prev_value)
                    break
            else:
                value = anchors[-1][1]

        # Apply weekend scaling
        if is_weekend and config.weekend_scale is not None:
            value = value * config.weekend_scale

        # Add noise
        if config.noise > 0:
            value += rng.uniform(-config.noise, config.noise)

        # Clamp to [min, max]
        value = float(np.clip(value, config.min, config.max))

        # Apply precision
        if config.precision is not None:
            value = round(value, config.precision)
            if config.precision == 0:
                value = int(value)

        return value

    def _generate_derived(
        self,
        config: DerivedGeneratorConfig,
        row_data: Dict[str, Any],
        entity_name: str,
        entity_state: Dict[str, Any],
        row_idx: int = 0,
        timestamp: datetime = None,
    ) -> Any:
        """Generate derived value from expression.

        Args:
            config: Derived generator configuration
            row_data: Current row data with already-generated columns
            entity_name: Current entity name for state tracking
            entity_state: Entity-specific state dict for stateful functions
            row_idx: Row index (0-based) for _row_index variable
            timestamp: Current row timestamp for _timestamp variable

        Returns:
            Calculated value

        Raises:
            ValueError: If expression references undefined columns or is unsafe
        """
        # Create safe evaluation namespace
        # Only allow specific functions and the row data

        # Null-safe helper functions
        def coalesce(*args):
            """Return first non-None value."""
            return next((a for a in args if a is not None), None)

        def safe_div(a, b, default=None):
            """Safe division that handles None and zero."""
            if a is None or b is None or b == 0:
                return default
            return a / b

        def safe_mul(a, b, default=None):
            """Safe multiplication that handles None."""
            if a is None or b is None:
                return default
            return a * b

        # NEW: Stateful functions
        def prev(column_name: str, default=None):
            """
            Get previous row value for a column.

            Args:
                column_name: Name of column to retrieve previous value
                default: Value to return if no previous row exists

            Returns:
                Previous row value or default

            Example:
                # First-order lag: PV moves 10% toward SP each timestep
                expression: "prev('pv', 0) + 0.1 * (sp - prev('pv', 0))"
            """
            return entity_state.get(column_name, default)

        def ema(column_name: str, alpha: float, default=None):
            """
            Exponential moving average with smoothing factor alpha.

            Args:
                column_name: Column to smooth
                alpha: Smoothing factor (0-1). Higher = more weight to current value
                default: Initial value if no previous EMA exists

            Returns:
                Smoothed value

            Example:
                # Smooth noisy sensor reading
                expression: "ema('raw_temp', alpha=0.1, default=raw_temp)"
            """
            ema_key = f"_ema_{column_name}"
            current_value = row_data.get(column_name)

            if current_value is None:
                return entity_state.get(ema_key, default)

            prev_ema = entity_state.get(ema_key)
            if prev_ema is None:
                # First value - initialize EMA
                ema_value = current_value if default is None else default
            else:
                # EMA formula: EMA_t = alpha * value_t + (1 - alpha) * EMA_{t-1}
                ema_value = alpha * current_value + (1 - alpha) * prev_ema

            # Store for next iteration
            entity_state[ema_key] = ema_value
            return ema_value

        def delay(column_name: str, steps: int, default=None):
            """
            Return the value of a column from N steps ago (true transport delay).

            Maintains a ring buffer in entity_state to store the last N values.

            Args:
                column_name: Column to delay (must be quoted)
                steps: Number of timesteps to delay (must be >= 1)
                default: Value to return during the initial fill period

            Returns:
                Value from N steps ago, or default if buffer not yet full

            Example:
                # Pipeline transport delay: output follows input from 5 steps ago
                expression: "delay('input_flow', 5, 50.0)"
            """
            delay_key = f"_delay_{column_name}_{steps}"
            buffer = entity_state.get(delay_key, [])

            # Get current value of the column
            current_value = row_data.get(column_name)
            if current_value is None:
                current_value = entity_state.get(column_name, default)

            # Append current value to buffer
            buffer.append(current_value)

            # If buffer has more than steps+1 entries, trim from front
            if len(buffer) > steps + 1:
                buffer = buffer[-(steps + 1) :]

            # Store updated buffer
            entity_state[delay_key] = buffer

            # Return value from N steps ago if available, else default
            if len(buffer) > steps:
                return buffer[0]  # oldest value in the buffer
            else:
                return default

        def pid(
            pv: float,
            sp: float,
            Kp: float = 1.0,
            Ki: float = 0.0,
            Kd: float = 0.0,
            dt: float = 1.0,
            output_min: float = 0.0,
            output_max: float = 100.0,
            anti_windup: bool = True,
        ):
            """
            PID controller with anti-windup.

            Args:
                pv: Process variable (current measurement)
                sp: Setpoint (target value)
                Kp: Proportional gain
                Ki: Integral gain
                Kd: Derivative gain
                dt: Time step in seconds (should match simulation timestep)
                output_min: Minimum output value
                output_max: Maximum output value
                anti_windup: Enable anti-windup (stops integral when saturated)

            Returns:
                Control output (clamped to [output_min, output_max])

            Example:
                # PID temperature controller
                expression: "pid(pv=module_temp_c, sp=temp_setpoint_c, Kp=2.0, Ki=0.1, Kd=0.5, dt=60)"
            """
            # Handle None values
            if pv is None or sp is None:
                return None

            # Calculate error
            error = sp - pv

            # Get PID state for this combination
            pid_key = f"_pid_{id(config)}"  # Unique key per PID expression
            pid_state = entity_state.get(pid_key, {"integral": 0.0, "prev_error": 0.0})

            # Proportional term
            p_term = Kp * error

            # Integral term
            integral = pid_state["integral"]
            i_term = Ki * integral

            # Derivative term
            prev_error = pid_state["prev_error"]
            derivative = (error - prev_error) / dt if dt > 0 else 0.0
            d_term = Kd * derivative

            # Calculate output
            output = p_term + i_term + d_term

            # Clamp output
            clamped_output = max(output_min, min(output_max, output))

            # Update integral (with anti-windup)
            if anti_windup:
                # Only update integral if output is not saturated
                if output_min < clamped_output < output_max:
                    integral += error * dt
            else:
                integral += error * dt

            # Store state for next iteration
            entity_state[pid_key] = {
                "integral": integral,
                "prev_error": error,
            }

            return clamped_output

        def _random():
            """Generate a random float in [0, 1) for use in expressions."""
            return float(np.random.default_rng().random())

        safe_builtins = {
            "abs": abs,
            "round": round,
            "min": min,
            "max": max,
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "True": True,
            "False": False,
            "true": True,
            "false": False,
            "None": None,
            "coalesce": coalesce,
            "safe_div": safe_div,
            "safe_mul": safe_mul,
            # Stateful functions
            "prev": prev,
            "ema": ema,
            "delay": delay,
            "pid": pid,
            # Utility functions
            "random": _random,
        }

        # Combine row data with safe builtins, entity proxies, and context variables
        namespace = {**row_data, **safe_builtins, **self.entity_proxies}
        namespace["entity_id"] = entity_name
        namespace["_row_index"] = row_idx + self.rows_before_hwm
        namespace["_timestamp"] = timestamp

        try:
            # Evaluate expression in restricted namespace
            expr = " ".join(config.expression.split())
            result = eval(expr, {"__builtins__": {}}, namespace)
            return result
        except NameError as e:
            # Column not yet generated (dependency issue) or undefined
            raise ValueError(
                f"Derived column expression '{config.expression}' references undefined column: {e}"
            )
        except Exception as e:
            # Other evaluation errors
            raise ValueError(
                f"Error evaluating derived expression '{config.expression}': {type(e).__name__}: {e}"
            )

    def _apply_chaos(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply chaos parameters to generated data.

        Args:
            rows: Generated rows

        Returns:
            Rows with chaos applied
        """
        chaos = self.config.chaos

        # Apply outliers
        if chaos.outlier_rate > 0:
            rows = self._apply_outliers(rows, chaos.outlier_rate, chaos.outlier_factor)

        # Apply duplicates
        if chaos.duplicate_rate > 0:
            rows = self._apply_duplicates(rows, chaos.duplicate_rate)

        return rows

    def _apply_outliers(
        self, rows: List[Dict[str, Any]], outlier_rate: float, outlier_factor: float
    ) -> List[Dict[str, Any]]:
        """Apply outliers to numeric columns.

        Args:
            rows: Input rows
            outlier_rate: Probability of outlier
            outlier_factor: Multiplier for outliers

        Returns:
            Rows with outliers applied
        """
        # Find numeric columns (exclude derived — their values follow source formulas)
        numeric_cols = [
            c.name
            for c in self.config.columns
            if c.data_type in ["int", "float"]
            and not isinstance(c.generator, DerivedGeneratorConfig)
        ]
        int_cols = {
            c.name
            for c in self.config.columns
            if c.data_type == "int" and not isinstance(c.generator, DerivedGeneratorConfig)
        }

        if not numeric_cols:
            return rows

        for row in rows:
            for col in numeric_cols:
                if row[col] is not None and self.rng.random() < outlier_rate:
                    row[col] *= outlier_factor
                    if col in int_cols:
                        row[col] = int(round(row[col]))

        return rows

    def _apply_duplicates(
        self, rows: List[Dict[str, Any]], duplicate_rate: float
    ) -> List[Dict[str, Any]]:
        """Duplicate random rows.

        Args:
            rows: Input rows
            duplicate_rate: Probability of duplicating a row

        Returns:
            Rows with duplicates added
        """
        duplicates = []

        for row in rows:
            if self.rng.random() < duplicate_rate:
                duplicates.append(row.copy())

        return rows + duplicates

    def get_max_timestamp(self, rows: List[Dict[str, Any]]) -> Optional[str]:
        """Get maximum timestamp from generated rows.

        Args:
            rows: Generated rows

        Returns:
            Maximum timestamp string or None
        """
        timestamp_cols = [c.name for c in self.config.columns if c.generator.type == "timestamp"]
        if not timestamp_cols or not rows:
            return None

        timestamp_col = timestamp_cols[0]
        timestamps = [row[timestamp_col] for row in rows if row.get(timestamp_col)]

        if not timestamps:
            return None

        return max(timestamps)

    def get_random_walk_final_state(
        self, rows: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, float]]:
        """Get the final random walk values per entity for state persistence.

        Args:
            rows: Generated rows

        Returns:
            Dict mapping entity_name -> {column_name: last_value}
        """
        # Identify random walk columns
        rw_columns = [
            col.name
            for col in self.config.columns
            if isinstance(col.generator, RandomWalkGeneratorConfig)
        ]

        if not rw_columns or not rows:
            return {}

        # Find the entity ID column (constant with {entity_id})
        entity_col = None
        for col in self.config.columns:
            if isinstance(col.generator, ConstantGeneratorConfig) and "{entity_id}" in str(
                col.generator.value
            ):
                entity_col = col.name
                break

        if not entity_col:
            return {}

        # Group by entity and get last values
        state = {}
        for row in rows:
            entity = row.get(entity_col)
            if entity:
                if entity not in state:
                    state[entity] = {}
                for col in rw_columns:
                    if row.get(col) is not None:
                        state[entity][col] = row[col]

        return state

    def get_scheduled_event_final_state(self) -> Dict[str, Dict[int, Dict[str, Any]]]:
        """Get the final scheduled event state for persistence across incremental runs.

        Serializes datetime objects to ISO strings for JSON compatibility.

        Returns:
            Dict mapping entity_name -> {event_id: {state_key: value}}
        """
        if not self.scheduled_event_state:
            return {}

        def _serialize(val: Any) -> Any:
            if isinstance(val, datetime):
                return val.isoformat()
            return val

        serialized: Dict[str, Dict[int, Dict[str, Any]]] = {}
        for entity, events in self.scheduled_event_state.items():
            serialized[entity] = {}
            for event_id, state in events.items():
                serialized[entity][event_id] = {k: _serialize(v) for k, v in state.items()}
        return serialized
