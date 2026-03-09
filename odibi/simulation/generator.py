"""Core simulation engine for generating synthetic data."""

import hashlib
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import numpy as np

from odibi.config import (
    BooleanGeneratorConfig,
    CategoricalGeneratorConfig,
    ConstantGeneratorConfig,
    DerivedGeneratorConfig,
    EmailGeneratorConfig,
    GeoGeneratorConfig,
    IPGeneratorConfig,
    RandomWalkGeneratorConfig,
    RangeGeneratorConfig,
    SequentialGeneratorConfig,
    SimulationConfig,
    TimestampGeneratorConfig,
    UUIDGeneratorConfig,
)


class SimulationEngine:
    """Engine for generating simulated data according to YAML configuration."""

    def __init__(
        self,
        config: SimulationConfig,
        hwm_timestamp: Optional[str] = None,
        random_walk_state: Optional[Dict[str, Dict[str, float]]] = None,
    ):
        """Initialize simulation engine.

        Args:
            config: Simulation configuration
            hwm_timestamp: High-water mark timestamp for incremental generation
            random_walk_state: Last random walk values per entity per column from previous run
        """
        self.config = config
        self.hwm_timestamp = hwm_timestamp
        # Random walk state: last values per entity per column from previous run
        # Format: {"entity_name": {"column_name": last_value, ...}, ...}
        self.random_walk_state = random_walk_state or {}
        self.rng = np.random.default_rng(config.scope.seed)

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
        else:
            self.effective_start_time = self.start_time

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
        """
        # Find all identifiers in the expression
        # Match valid Python identifiers
        identifiers = re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", expression)

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
        }

        return {name for name in identifiers if name in all_columns and name not in python_keywords}

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
            if self.hwm_timestamp:
                # Calculate how many rows were already generated (before HWM)
                rows_before_hwm = int(
                    (self.effective_start_time - self.start_time).total_seconds()
                    / self.timestep_seconds
                )
                # Advance seed based on rows already generated
                # This ensures new runs don't repeat the same random sequence
                entity_seed = base_entity_seed + rows_before_hwm
            else:
                entity_seed = base_entity_seed

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
                )

                # Apply null_rate
                if col_config.null_rate > 0 and entity_rng.random() < col_config.null_rate:
                    value = None

                row[col_config.name] = value

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

        Returns:
            Generated value
        """
        if isinstance(generator, RangeGeneratorConfig):
            return self._generate_range(generator, rng)
        elif isinstance(generator, RandomWalkGeneratorConfig):
            return self._generate_random_walk(generator, rng, random_walk_current)
        elif isinstance(generator, CategoricalGeneratorConfig):
            return self._generate_categorical(generator, rng)
        elif isinstance(generator, BooleanGeneratorConfig):
            return self._generate_boolean(generator, rng)
        elif isinstance(generator, TimestampGeneratorConfig):
            # Return Zulu time format (Z suffix) for consistency
            return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(generator, SequentialGeneratorConfig):
            return generator.start + (row_idx * generator.step)
        elif isinstance(generator, ConstantGeneratorConfig):
            # Support magic variables
            value = str(generator.value)
            value = value.replace("{entity_id}", entity_name)
            value = value.replace("{entity_index}", str(entity_idx))
            value = value.replace("{timestamp}", timestamp.isoformat())
            value = value.replace("{row_number}", str(row_idx))
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
        elif isinstance(generator, DerivedGeneratorConfig):
            return self._generate_derived(generator, current_row or {})
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

        # Ornstein-Uhlenbeck step:
        # dx = mean_reversion * (start - current) * dt + volatility * dW + trend
        # Where dW is random noise
        noise = rng.normal(0, config.volatility)
        reversion_pull = config.mean_reversion * (config.start - current)
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

    def _generate_derived(self, config: DerivedGeneratorConfig, row_data: Dict[str, Any]) -> Any:
        """Generate derived value from expression.

        Args:
            config: Derived generator configuration
            row_data: Current row data with already-generated columns

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
            "None": None,
            "coalesce": coalesce,
            "safe_div": safe_div,
            "safe_mul": safe_mul,
        }

        # Combine row data with safe builtins
        namespace = {**row_data, **safe_builtins}

        try:
            # Evaluate expression in restricted namespace
            result = eval(config.expression, {"__builtins__": {}}, namespace)
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
        # Find numeric columns
        numeric_cols = [
            c.name
            for c in self.config.columns
            if c.data_type in ["int", "float"] and c.generator.type == "range"
        ]

        if not numeric_cols:
            return rows

        for row in rows:
            for col in numeric_cols:
                if row[col] is not None and self.rng.random() < outlier_rate:
                    row[col] *= outlier_factor

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
