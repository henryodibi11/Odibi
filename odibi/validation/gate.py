"""
Quality Gate support for batch-level validation.

Gates evaluate the entire batch before writing, ensuring
data quality thresholds are met at the aggregate level.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from odibi.config import GateConfig, GateOnFail

logger = logging.getLogger(__name__)


@dataclass
class GateResult:
    """Result of gate evaluation."""

    passed: bool
    pass_rate: float
    total_rows: int
    passed_rows: int
    failed_rows: int
    details: Dict[str, Any] = field(default_factory=dict)
    action: GateOnFail = GateOnFail.ABORT
    failure_reasons: List[str] = field(default_factory=list)


def evaluate_gate(
    df: Any,
    validation_results: Dict[str, List[bool]],
    gate_config: GateConfig,
    engine: Any,
    catalog: Optional[Any] = None,
    node_name: Optional[str] = None,
) -> GateResult:
    """
    Evaluate quality gate on validation results.

    Args:
        df: DataFrame being validated
        validation_results: Dict of test_name -> per-row boolean results (True=passed)
        gate_config: Gate configuration
        engine: Engine instance
        catalog: Optional CatalogManager for historical row count checks
        node_name: Optional node name for historical lookups

    Returns:
        GateResult with pass/fail status and action to take
    """
    is_spark = False

    try:
        import pyspark

        if hasattr(engine, "spark") or isinstance(df, pyspark.sql.DataFrame):
            is_spark = True
    except ImportError:
        pass

    if is_spark:
        total_rows = df.count()
    elif hasattr(engine, "count_rows"):
        total_rows = engine.count_rows(df)
    else:
        total_rows = len(df)

    if total_rows == 0:
        return GateResult(
            passed=True,
            pass_rate=1.0,
            total_rows=0,
            passed_rows=0,
            failed_rows=0,
            action=gate_config.on_fail,
            details={"message": "Empty dataset - gate passed by default"},
        )

    passed_rows = total_rows
    if validation_results:
        all_pass_mask = None
        for test_name, results in validation_results.items():
            if len(results) == total_rows:
                if all_pass_mask is None:
                    all_pass_mask = results.copy()
                else:
                    all_pass_mask = [a and b for a, b in zip(all_pass_mask, results)]

        if all_pass_mask:
            passed_rows = sum(all_pass_mask)

    pass_rate = round(passed_rows / total_rows, 10) if total_rows > 0 else 1.0
    failed_rows = total_rows - passed_rows

    details: Dict[str, Any] = {
        "overall_pass_rate": pass_rate,
        "per_test_rates": {},
        "row_count_check": None,
    }

    gate_passed = True
    failure_reasons: List[str] = []

    if pass_rate < gate_config.require_pass_rate:
        gate_passed = False
        failure_reasons.append(
            f"Overall pass rate {pass_rate:.1%} < required {gate_config.require_pass_rate:.1%}"
        )

    for threshold in gate_config.thresholds:
        test_results = validation_results.get(threshold.test)
        if test_results:
            test_total = len(test_results)
            test_passed = sum(test_results)
            test_pass_rate = test_passed / test_total if test_total > 0 else 1.0
            details["per_test_rates"][threshold.test] = test_pass_rate

            if test_pass_rate < threshold.min_pass_rate:
                gate_passed = False
                failure_reasons.append(
                    f"Test '{threshold.test}' pass rate {test_pass_rate:.1%} "
                    f"< required {threshold.min_pass_rate:.1%}"
                )

    if gate_config.row_count:
        row_check = _check_row_count(
            total_rows,
            gate_config.row_count,
            catalog,
            node_name,
        )
        details["row_count_check"] = row_check

        if not row_check["passed"]:
            gate_passed = False
            failure_reasons.append(row_check["reason"])

    details["failure_reasons"] = failure_reasons

    if gate_passed:
        logger.info(f"Gate passed: {pass_rate:.1%} pass rate ({passed_rows}/{total_rows} rows)")
    else:
        logger.warning(f"Gate failed: {', '.join(failure_reasons)}")

    return GateResult(
        passed=gate_passed,
        pass_rate=pass_rate,
        total_rows=total_rows,
        passed_rows=passed_rows,
        failed_rows=failed_rows,
        details=details,
        action=gate_config.on_fail,
        failure_reasons=failure_reasons,
    )


def _check_row_count(
    current_count: int,
    row_count_config: Any,
    catalog: Optional[Any],
    node_name: Optional[str],
) -> Dict[str, Any]:
    """
    Check row count against thresholds and historical data.

    Args:
        current_count: Current row count
        row_count_config: RowCountGate configuration
        catalog: CatalogManager for historical lookups
        node_name: Node name for historical lookups

    Returns:
        Dict with passed status, reason, and details
    """
    result: Dict[str, Any] = {
        "passed": True,
        "reason": "",
        "current_count": current_count,
        "min": row_count_config.min,
        "max": row_count_config.max,
        "change_threshold": row_count_config.change_threshold,
        "previous_count": None,
        "change_percent": None,
    }

    if row_count_config.min is not None and current_count < row_count_config.min:
        result["passed"] = False
        result["reason"] = f"Row count {current_count} < minimum {row_count_config.min}"
        return result

    if row_count_config.max is not None and current_count > row_count_config.max:
        result["passed"] = False
        result["reason"] = f"Row count {current_count} > maximum {row_count_config.max}"
        return result

    if row_count_config.change_threshold is not None and catalog and node_name:
        try:
            previous_count = _get_previous_row_count(catalog, node_name)
            if previous_count is not None and previous_count > 0:
                result["previous_count"] = previous_count
                change_percent = abs(current_count - previous_count) / previous_count
                result["change_percent"] = change_percent

                if change_percent > row_count_config.change_threshold:
                    result["passed"] = False
                    result["reason"] = (
                        f"Row count changed {change_percent:.1%} vs previous ({previous_count}), "
                        f"exceeds threshold {row_count_config.change_threshold:.1%}"
                    )
                    return result
        except Exception as e:
            logger.warning(f"Failed to check historical row count: {e}")

    return result


def _get_previous_row_count(
    catalog: Any,
    node_name: str,
) -> Optional[int]:
    """
    Get the previous row count for a node from the catalog.

    Args:
        catalog: CatalogManager instance
        node_name: Name of the node

    Returns:
        Previous row count or None if not available
    """
    try:
        if hasattr(catalog, "get_last_run_metrics"):
            metrics = catalog.get_last_run_metrics(node_name)
            if metrics and "rows_processed" in metrics:
                return metrics["rows_processed"]

        if hasattr(catalog, "query"):
            results = catalog.query(
                "meta_runs",
                filter=f"node_name = '{node_name}' AND status = 'SUCCESS'",
                order_by="started_at DESC",
                limit=1,
            )
            if results and len(results) > 0:
                return results[0].get("rows_processed")

    except Exception as e:
        logger.debug(f"Could not fetch previous row count: {e}")

    return None
