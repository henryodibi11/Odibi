"""
Foreign Key Validation Module
=============================

Declare and validate referential integrity between fact and dimension tables.

Features:
- Declare relationships in YAML
- Validate referential integrity on fact load
- Detect orphan records
- Generate lineage from relationships
- Integration with FactPattern

Example Config:
    relationships:
      - name: orders_to_customers
        fact: fact_orders
        dimension: dim_customer
        fact_key: customer_sk
        dimension_key: customer_sk

      - name: orders_to_products
        fact: fact_orders
        dimension: dim_product
        fact_key: product_sk
        dimension_key: product_sk
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from odibi.context import EngineContext
from odibi.enums import EngineType
from odibi.utils.logging_context import get_logging_context


class RelationshipConfig(BaseModel):
    """
    Configuration for a foreign key relationship.

    Attributes:
        name: Unique relationship identifier
        fact: Fact table name
        dimension: Dimension table name
        fact_key: Foreign key column in fact table
        dimension_key: Primary/surrogate key column in dimension
        nullable: Whether nulls are allowed in fact_key
        on_violation: Action on violation ("warn", "error", "quarantine")
    """

    name: str = Field(..., description="Unique relationship identifier")
    fact: str = Field(..., description="Fact table name")
    dimension: str = Field(..., description="Dimension table name")
    fact_key: str = Field(..., description="FK column in fact table")
    dimension_key: str = Field(..., description="PK/SK column in dimension")
    nullable: bool = Field(default=False, description="Allow nulls in fact_key")
    on_violation: str = Field(default="error", description="Action on violation")

    @field_validator("name", "fact", "dimension", "fact_key", "dimension_key")
    @classmethod
    def validate_not_empty(cls, v: str, info) -> str:
        if not v or not v.strip():
            raise ValueError(f"{info.field_name} cannot be empty")
        return v.strip()

    @field_validator("on_violation")
    @classmethod
    def validate_on_violation(cls, v: str) -> str:
        valid = ("warn", "error", "quarantine")
        if v.lower() not in valid:
            raise ValueError(f"on_violation must be one of {valid}")
        return v.lower()


class RelationshipRegistry(BaseModel):
    """
    Registry of all declared relationships.

    Attributes:
        relationships: List of relationship configurations
    """

    relationships: List[RelationshipConfig] = Field(
        default_factory=list, description="Relationship definitions"
    )

    def get_relationship(self, name: str) -> Optional[RelationshipConfig]:
        """Get a relationship by name."""
        for rel in self.relationships:
            if rel.name.lower() == name.lower():
                return rel
        return None

    def get_fact_relationships(self, fact_table: str) -> List[RelationshipConfig]:
        """Get all relationships for a fact table."""
        return [rel for rel in self.relationships if rel.fact.lower() == fact_table.lower()]

    def get_dimension_relationships(self, dim_table: str) -> List[RelationshipConfig]:
        """Get all relationships referencing a dimension."""
        return [rel for rel in self.relationships if rel.dimension.lower() == dim_table.lower()]

    def generate_lineage(self) -> Dict[str, List[str]]:
        """
        Generate lineage map from relationships.

        Returns:
            Dict mapping fact tables to their dimension dependencies
        """
        lineage: Dict[str, List[str]] = {}
        for rel in self.relationships:
            if rel.fact not in lineage:
                lineage[rel.fact] = []
            if rel.dimension not in lineage[rel.fact]:
                lineage[rel.fact].append(rel.dimension)
        return lineage


@dataclass
class OrphanRecord:
    """Details of an orphan record."""

    fact_key_value: Any
    fact_key_column: str
    dimension_table: str
    row_index: Optional[int] = None


@dataclass
class FKValidationResult:
    """Result of FK validation."""

    relationship_name: str
    valid: bool
    total_rows: int
    orphan_count: int
    null_count: int
    orphan_values: List[Any] = field(default_factory=list)
    elapsed_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class FKValidationReport:
    """Complete FK validation report for a fact table."""

    fact_table: str
    all_valid: bool
    total_relationships: int
    valid_relationships: int
    results: List[FKValidationResult] = field(default_factory=list)
    orphan_records: List[OrphanRecord] = field(default_factory=list)
    elapsed_ms: float = 0.0


class FKValidator:
    """
    Validate foreign key relationships between fact and dimension tables.

    Usage:
        registry = RelationshipRegistry(relationships=[...])
        validator = FKValidator(registry)
        report = validator.validate_fact(fact_df, "fact_orders", context)
    """

    def __init__(self, registry: RelationshipRegistry):
        """
        Initialize with relationship registry.

        Args:
            registry: RelationshipRegistry with relationship definitions
        """
        self.registry = registry

    def validate_relationship(
        self,
        fact_df: Any,
        relationship: RelationshipConfig,
        context: EngineContext,
    ) -> FKValidationResult:
        """
        Validate a single FK relationship.

        Args:
            fact_df: Fact DataFrame to validate
            relationship: Relationship configuration
            context: EngineContext with dimension data

        Returns:
            FKValidationResult with validation details
        """
        ctx = get_logging_context()
        start_time = time.time()

        ctx.debug(
            "Validating FK relationship",
            relationship=relationship.name,
            fact=relationship.fact,
            dimension=relationship.dimension,
        )

        try:
            dim_df = context.get(relationship.dimension)
        except KeyError:
            elapsed_ms = (time.time() - start_time) * 1000
            return FKValidationResult(
                relationship_name=relationship.name,
                valid=False,
                total_rows=0,
                orphan_count=0,
                null_count=0,
                elapsed_ms=elapsed_ms,
                error=f"Dimension table '{relationship.dimension}' not found",
            )

        try:
            if context.engine_type == EngineType.SPARK:
                result = self._validate_spark(fact_df, dim_df, relationship)
            else:
                result = self._validate_pandas(fact_df, dim_df, relationship)

            elapsed_ms = (time.time() - start_time) * 1000
            result.elapsed_ms = elapsed_ms

            if result.valid:
                ctx.debug(
                    "FK validation passed",
                    relationship=relationship.name,
                    total_rows=result.total_rows,
                )
            else:
                ctx.warning(
                    "FK validation failed",
                    relationship=relationship.name,
                    orphan_count=result.orphan_count,
                    null_count=result.null_count,
                )

            return result

        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.error(
                f"FK validation error: {e}",
                relationship=relationship.name,
            )
            return FKValidationResult(
                relationship_name=relationship.name,
                valid=False,
                total_rows=0,
                orphan_count=0,
                null_count=0,
                elapsed_ms=elapsed_ms,
                error=str(e),
            )

    def _validate_spark(
        self,
        fact_df: Any,
        dim_df: Any,
        relationship: RelationshipConfig,
    ) -> FKValidationResult:
        """Validate using Spark."""
        from pyspark.sql import functions as F

        fk_col = relationship.fact_key
        dk_col = relationship.dimension_key

        total_rows = fact_df.count()

        null_count = fact_df.filter(F.col(fk_col).isNull()).count()

        dim_keys = dim_df.select(F.col(dk_col).alias("_dim_key")).distinct()

        non_null_facts = fact_df.filter(F.col(fk_col).isNotNull())
        orphans = non_null_facts.join(
            dim_keys,
            non_null_facts[fk_col] == dim_keys["_dim_key"],
            "left_anti",
        )

        orphan_count = orphans.count()

        orphan_values = []
        if orphan_count > 0 and orphan_count <= 100:
            orphan_values = [
                row[fk_col] for row in orphans.select(fk_col).distinct().limit(100).collect()
            ]

        is_valid = orphan_count == 0 and (relationship.nullable or null_count == 0)

        return FKValidationResult(
            relationship_name=relationship.name,
            valid=is_valid,
            total_rows=total_rows,
            orphan_count=orphan_count,
            null_count=null_count,
            orphan_values=orphan_values,
        )

    def _validate_pandas(
        self,
        fact_df: Any,
        dim_df: Any,
        relationship: RelationshipConfig,
    ) -> FKValidationResult:
        """Validate using Pandas."""

        fk_col = relationship.fact_key
        dk_col = relationship.dimension_key

        total_rows = len(fact_df)

        null_count = int(fact_df[fk_col].isna().sum())

        dim_keys = set(dim_df[dk_col].dropna().unique())

        non_null_fks = fact_df[fk_col].dropna()
        orphan_mask = ~non_null_fks.isin(dim_keys)
        orphan_count = int(orphan_mask.sum())

        orphan_values = []
        if orphan_count > 0:
            orphan_values = list(non_null_fks[orphan_mask].unique()[:100])

        is_valid = orphan_count == 0 and (relationship.nullable or null_count == 0)

        return FKValidationResult(
            relationship_name=relationship.name,
            valid=is_valid,
            total_rows=total_rows,
            orphan_count=orphan_count,
            null_count=null_count,
            orphan_values=orphan_values,
        )

    def validate_fact(
        self,
        fact_df: Any,
        fact_table: str,
        context: EngineContext,
    ) -> FKValidationReport:
        """
        Validate all FK relationships for a fact table.

        Args:
            fact_df: Fact DataFrame to validate
            fact_table: Fact table name
            context: EngineContext with dimension data

        Returns:
            FKValidationReport with all validation results
        """
        ctx = get_logging_context()
        start_time = time.time()

        ctx.info("Starting FK validation", fact_table=fact_table)

        relationships = self.registry.get_fact_relationships(fact_table)

        if not relationships:
            ctx.warning(
                "No FK relationships defined",
                fact_table=fact_table,
            )
            return FKValidationReport(
                fact_table=fact_table,
                all_valid=True,
                total_relationships=0,
                valid_relationships=0,
                elapsed_ms=(time.time() - start_time) * 1000,
            )

        results = []
        all_orphans = []

        for relationship in relationships:
            result = self.validate_relationship(fact_df, relationship, context)
            results.append(result)

            if result.orphan_count > 0:
                for orphan_val in result.orphan_values:
                    all_orphans.append(
                        OrphanRecord(
                            fact_key_value=orphan_val,
                            fact_key_column=relationship.fact_key,
                            dimension_table=relationship.dimension,
                        )
                    )

        all_valid = all(r.valid for r in results)
        valid_count = sum(1 for r in results if r.valid)
        elapsed_ms = (time.time() - start_time) * 1000

        if all_valid:
            ctx.info(
                "FK validation passed",
                fact_table=fact_table,
                relationships=len(relationships),
            )
        else:
            ctx.warning(
                "FK validation failed",
                fact_table=fact_table,
                valid=valid_count,
                total=len(relationships),
            )

        return FKValidationReport(
            fact_table=fact_table,
            all_valid=all_valid,
            total_relationships=len(relationships),
            valid_relationships=valid_count,
            results=results,
            orphan_records=all_orphans,
            elapsed_ms=elapsed_ms,
        )


def get_orphan_records(
    fact_df: Any,
    relationship: RelationshipConfig,
    dim_df: Any,
    engine_type: EngineType,
) -> Any:
    """
    Extract orphan records from a fact table.

    Args:
        fact_df: Fact DataFrame
        relationship: Relationship configuration
        dim_df: Dimension DataFrame
        engine_type: Engine type (SPARK or PANDAS)

    Returns:
        DataFrame containing orphan records
    """
    fk_col = relationship.fact_key
    dk_col = relationship.dimension_key

    if engine_type == EngineType.SPARK:
        from pyspark.sql import functions as F

        dim_keys = dim_df.select(F.col(dk_col).alias("_dim_key")).distinct()
        non_null_facts = fact_df.filter(F.col(fk_col).isNotNull())
        orphans = non_null_facts.join(
            dim_keys,
            non_null_facts[fk_col] == dim_keys["_dim_key"],
            "left_anti",
        )
        return orphans
    else:
        dim_keys = set(dim_df[dk_col].dropna().unique())
        non_null_mask = fact_df[fk_col].notna()
        orphan_mask = ~fact_df[fk_col].isin(dim_keys) & non_null_mask
        return fact_df[orphan_mask].copy()


def validate_fk_on_load(
    fact_df: Any,
    relationships: List[RelationshipConfig],
    context: EngineContext,
    on_failure: str = "error",
) -> Any:
    """
    Validate FK constraints and optionally filter orphans.

    This is a convenience function for use in FactPattern.

    Args:
        fact_df: Fact DataFrame to validate
        relationships: List of relationship configs
        context: EngineContext with dimension data
        on_failure: Action on failure ("error", "warn", "filter")

    Returns:
        fact_df (possibly filtered if on_failure="filter")

    Raises:
        ValueError: If on_failure="error" and validation fails
    """
    ctx = get_logging_context()

    registry = RelationshipRegistry(relationships=relationships)
    validator = FKValidator(registry)

    for rel in relationships:
        result = validator.validate_relationship(fact_df, rel, context)

        if not result.valid:
            if on_failure == "error":
                raise ValueError(
                    f"FK validation failed for '{rel.name}': "
                    f"{result.orphan_count} orphans, {result.null_count} nulls. "
                    f"Sample orphan values: {result.orphan_values[:5]}"
                )
            elif on_failure == "warn":
                ctx.warning(
                    f"FK validation warning for '{rel.name}': "
                    f"{result.orphan_count} orphans, {result.null_count} nulls"
                )
            elif on_failure == "filter":
                try:
                    dim_df = context.get(rel.dimension)
                except KeyError:
                    continue

                if context.engine_type == EngineType.SPARK:
                    from pyspark.sql import functions as F

                    dim_keys = dim_df.select(F.col(rel.dimension_key).alias("_fk_key")).distinct()
                    fact_df = fact_df.join(
                        dim_keys,
                        fact_df[rel.fact_key] == dim_keys["_fk_key"],
                        "inner",
                    ).drop("_fk_key")
                else:
                    dim_keys = set(dim_df[rel.dimension_key].dropna().unique())
                    fact_df = fact_df[fact_df[rel.fact_key].isin(dim_keys)].copy()

                ctx.info(
                    f"Filtered orphans for '{rel.name}'",
                    remaining_rows=len(fact_df) if hasattr(fact_df, "__len__") else "N/A",
                )

    return fact_df


def parse_relationships_config(config_dict: Dict[str, Any]) -> RelationshipRegistry:
    """
    Parse relationships from a configuration dictionary.

    Args:
        config_dict: Config dict with "relationships" key

    Returns:
        RelationshipRegistry instance
    """
    relationships = []
    for rel_dict in config_dict.get("relationships", []):
        relationships.append(RelationshipConfig(**rel_dict))
    return RelationshipRegistry(relationships=relationships)
