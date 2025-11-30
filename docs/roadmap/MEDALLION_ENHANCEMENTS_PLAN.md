# Medallion Architecture Enhancements Plan

> **Status:** Draft  
> **Created:** 2024-01-30  
> **Total Effort:** ~6-7 days  
> **Priority:** High - Production readiness for medallion pipelines

---

## Executive Summary

This plan addresses 5 key gaps in Odibi's medallion architecture support:

| # | Enhancement | Effort | Priority |
|---|-------------|--------|----------|
| 1 | [Quarantine Tables](#1-quarantine-tables) | 1.5 days | 🔴 Critical |
| 2 | [Quality Gates](#2-quality-gates) | 1 day | 🔴 Critical |
| 3 | [Alerting Enhancements](#3-alerting-enhancements) | 1 day | 🟡 High |
| 4 | [Schema Version Tracking](#4-schema-version-tracking) | 0.5 days | 🟢 Medium |
| 5 | [Cross-Pipeline Lineage](#5-cross-pipeline-lineage) | 2-3 days | 🟢 Medium |

---

## 1. Quarantine Tables

### Problem
When validation tests fail, bad rows are either:
- Logged and written anyway (`on_fail: warn`)
- Cause the entire pipeline to fail (`on_fail: fail`)

Neither option preserves bad data for later analysis/reprocessing.

### Solution
Route failed rows to a dedicated quarantine table with rejection metadata.

### YAML Schema

```yaml
nodes:
  - name: process_customers
    read:
      connection: bronze
      path: customers_raw

    validation:
      tests:
        - type: not_null
          columns: [customer_id, email]
          on_fail: quarantine  # NEW: Route to quarantine instead of fail/warn
        - type: regex_match
          column: email
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
          on_fail: quarantine

      # NEW: Quarantine configuration
      quarantine:
        connection: silver
        path: customers_quarantine  # Or table: customers_quarantine
        add_columns:
          _rejection_reason: true    # Which test(s) failed
          _rejected_at: true         # Timestamp
          _source_batch_id: true     # Link back to source batch
          _failed_tests: true        # List of failed test names
```

### Config Model Changes

```python
# odibi/config.py

class QuarantineConfig(BaseModel):
    """Configuration for quarantine table routing."""

    connection: str = Field(description="Connection for quarantine writes")
    path: Optional[str] = Field(default=None, description="Path for quarantine data")
    table: Optional[str] = Field(default=None, description="Table name for quarantine")

    # Metadata columns to add
    add_columns: QuarantineColumnsConfig = Field(
        default_factory=QuarantineColumnsConfig,
        description="Metadata columns to add to quarantined rows"
    )

    # Retention
    retention_days: Optional[int] = Field(
        default=90,
        ge=1,
        description="Days to retain quarantined data (auto-cleanup)"
    )

class QuarantineColumnsConfig(BaseModel):
    """Columns added to quarantined rows."""
    rejection_reason: bool = Field(default=True, alias="_rejection_reason")
    rejected_at: bool = Field(default=True, alias="_rejected_at")
    source_batch_id: bool = Field(default=True, alias="_source_batch_id")
    failed_tests: bool = Field(default=True, alias="_failed_tests")
    original_node: bool = Field(default=False, alias="_original_node")


class ValidationConfig(BaseModel):
    """Updated validation config with quarantine support."""
    tests: List[TestConfig] = Field(default_factory=list)
    quarantine: Optional[QuarantineConfig] = Field(
        default=None,
        description="Quarantine configuration for failed rows"
    )
```

### Implementation

**File:** `odibi/validation/quarantine.py` (new)

```python
from typing import Any, Dict, List, Tuple
from datetime import datetime
import hashlib

def split_valid_invalid(
    df: Any,
    tests: List[TestConfig],
    engine: Engine,
    context: EngineContext
) -> Tuple[Any, Any, List[Dict]]:
    """
    Split DataFrame into valid and invalid portions.

    Returns:
        Tuple of (valid_df, invalid_df, rejection_details)
    """
    # Build combined validity mask
    validity_masks = []
    test_results = {}  # row_index -> [failed_test_names]

    for test in tests:
        if test.on_fail == ContractSeverity.QUARANTINE:
            mask = _evaluate_test(df, test, engine)
            validity_masks.append(mask)
            test_results[test.name or test.type] = ~mask

    # Combine masks (row is valid if passes ALL quarantine tests)
    combined_valid = engine.all_true(validity_masks)

    valid_df = engine.filter(df, combined_valid)
    invalid_df = engine.filter(df, ~combined_valid)

    return valid_df, invalid_df, test_results


def add_quarantine_metadata(
    invalid_df: Any,
    test_results: Dict,
    config: QuarantineConfig,
    engine: Engine,
    node_name: str,
    run_id: str
) -> Any:
    """Add metadata columns to quarantined rows."""

    if config.add_columns.rejection_reason:
        invalid_df = engine.add_column(
            invalid_df,
            "_rejection_reason",
            _get_rejection_reasons(invalid_df, test_results, engine)
        )

    if config.add_columns.rejected_at:
        invalid_df = engine.add_column(
            invalid_df,
            "_rejected_at",
            datetime.utcnow().isoformat()
        )

    if config.add_columns.source_batch_id:
        invalid_df = engine.add_column(
            invalid_df,
            "_source_batch_id",
            run_id
        )

    if config.add_columns.failed_tests:
        invalid_df = engine.add_column(
            invalid_df,
            "_failed_tests",
            _get_failed_test_list(invalid_df, test_results, engine)
        )

    if config.add_columns.original_node:
        invalid_df = engine.add_column(
            invalid_df,
            "_original_node",
            node_name
        )

    return invalid_df


def write_quarantine(
    invalid_df: Any,
    config: QuarantineConfig,
    engine: Engine,
    connections: Dict[str, Any]
) -> Dict[str, Any]:
    """Write quarantined rows to destination."""

    connection = connections[config.connection]

    result = engine.write(
        invalid_df,
        connection=connection,
        format="delta",
        path=config.path,
        table=config.table,
        mode="append"  # Always append
    )

    return {
        "rows_quarantined": engine.count(invalid_df),
        "quarantine_path": config.path or config.table,
        "write_info": result
    }
```

### Node Execution Changes

**File:** `odibi/node.py` - Update validation flow

```python
# In _execute_validation method:

def _execute_validation(self, df, config, context):
    """Execute validation with quarantine support."""

    validator = Validator()

    # Check if any tests use quarantine
    has_quarantine_tests = any(
        t.on_fail == ContractSeverity.QUARANTINE
        for t in config.validation.tests
    )

    if has_quarantine_tests and config.validation.quarantine:
        # Split valid/invalid
        valid_df, invalid_df, test_results = split_valid_invalid(
            df, config.validation.tests, self.engine, context
        )

        invalid_count = self.engine.count(invalid_df)

        if invalid_count > 0:
            # Add metadata
            invalid_df = add_quarantine_metadata(
                invalid_df,
                test_results,
                config.validation.quarantine,
                self.engine,
                config.name,
                context.run_id
            )

            # Write to quarantine
            quarantine_result = write_quarantine(
                invalid_df,
                config.validation.quarantine,
                self.engine,
                context.connections
            )

            # Emit alert if configured
            self._emit_quarantine_alert(config, quarantine_result, context)

            logger.info(
                f"Quarantined {invalid_count} rows to "
                f"{config.validation.quarantine.path or config.validation.quarantine.table}"
            )

        # Continue with valid rows only
        return valid_df, []

    else:
        # Original validation logic (no quarantine)
        failures = validator.validate(df, config.validation, context)
        return df, failures
```

### Tests

```python
# tests/test_quarantine.py

def test_quarantine_splits_valid_invalid():
    """Valid rows continue, invalid rows go to quarantine."""

def test_quarantine_adds_metadata_columns():
    """Quarantined rows have _rejection_reason, _rejected_at, etc."""

def test_quarantine_appends_not_overwrites():
    """Multiple runs append to same quarantine table."""

def test_quarantine_triggers_alert():
    """Alert sent when rows are quarantined."""

def test_multiple_failed_tests_captured():
    """Row failing 3 tests has all 3 in _failed_tests column."""
```

---

## 2. Quality Gates

### Problem
Validation tests run per-row, but there's no batch-level check that says "abort if too many rows fail."

### Solution
Add a `gate` configuration that evaluates the batch as a whole before writing.

### YAML Schema

```yaml
nodes:
  - name: load_silver_customers
    read:
      connection: bronze
      path: customers

    validation:
      tests:
        - type: not_null
          columns: [customer_id]
        - type: unique
          columns: [customer_id]

    # NEW: Quality Gate
    gate:
      # Minimum percentage of rows that must pass ALL tests
      require_pass_rate: 0.95  # 95%

      # What to do if gate fails
      on_fail: abort  # abort | warn_and_write | write_valid_only

      # Optional: Specific thresholds per test type
      thresholds:
        - test: not_null
          min_pass_rate: 0.99  # 99% must have non-null customer_id
        - test: unique
          min_pass_rate: 1.0   # 100% must be unique (no duplicates)

      # Row count anomaly detection
      row_count:
        min: 100              # At least 100 rows expected
        max: 1000000          # At most 1M rows
        change_threshold: 0.5 # Fail if row count changes >50% vs last run
```

### Config Model

```python
# odibi/config.py

class GateThreshold(BaseModel):
    """Threshold for a specific test."""
    test: str = Field(description="Test name or type")
    min_pass_rate: float = Field(ge=0.0, le=1.0, description="Minimum pass rate")


class RowCountGate(BaseModel):
    """Row count anomaly detection."""
    min: Optional[int] = Field(default=None, ge=0)
    max: Optional[int] = Field(default=None, ge=0)
    change_threshold: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Max allowed change vs previous run (e.g., 0.5 = 50%)"
    )


class GateOnFail(str, Enum):
    """Action when gate fails."""
    ABORT = "abort"                    # Stop, write nothing
    WARN_AND_WRITE = "warn_and_write"  # Log warning, write all rows
    WRITE_VALID_ONLY = "write_valid_only"  # Write only passing rows


class GateConfig(BaseModel):
    """Quality gate configuration."""

    require_pass_rate: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Minimum percentage of rows passing ALL tests"
    )

    on_fail: GateOnFail = Field(
        default=GateOnFail.ABORT,
        description="Action when gate fails"
    )

    thresholds: List[GateThreshold] = Field(
        default_factory=list,
        description="Per-test thresholds (overrides global require_pass_rate)"
    )

    row_count: Optional[RowCountGate] = Field(
        default=None,
        description="Row count anomaly detection"
    )
```

### Implementation

**File:** `odibi/validation/gate.py` (new)

```python
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class GateResult:
    """Result of gate evaluation."""
    passed: bool
    pass_rate: float
    total_rows: int
    passed_rows: int
    failed_rows: int
    details: Dict[str, Any]
    action: GateOnFail


def evaluate_gate(
    df: Any,
    validation_results: Dict[str, List[bool]],  # test_name -> per-row results
    gate_config: GateConfig,
    engine: Engine,
    catalog: Optional[CatalogManager] = None,
    node_name: Optional[str] = None
) -> GateResult:
    """
    Evaluate quality gate on validation results.

    Returns GateResult with pass/fail status and action to take.
    """
    total_rows = engine.count(df)

    # Calculate overall pass rate (row passes if ALL tests pass)
    all_pass_mask = _combine_test_results(validation_results, engine)
    passed_rows = engine.count_true(all_pass_mask)
    pass_rate = passed_rows / total_rows if total_rows > 0 else 1.0

    details = {
        "overall_pass_rate": pass_rate,
        "per_test_rates": {},
        "row_count_check": None
    }

    gate_passed = True
    failure_reasons = []

    # Check global threshold
    if pass_rate < gate_config.require_pass_rate:
        gate_passed = False
        failure_reasons.append(
            f"Overall pass rate {pass_rate:.1%} < required {gate_config.require_pass_rate:.1%}"
        )

    # Check per-test thresholds
    for threshold in gate_config.thresholds:
        test_results = validation_results.get(threshold.test)
        if test_results:
            test_pass_rate = sum(test_results) / len(test_results)
            details["per_test_rates"][threshold.test] = test_pass_rate

            if test_pass_rate < threshold.min_pass_rate:
                gate_passed = False
                failure_reasons.append(
                    f"Test '{threshold.test}' pass rate {test_pass_rate:.1%} "
                    f"< required {threshold.min_pass_rate:.1%}"
                )

    # Check row count anomalies
    if gate_config.row_count:
        row_check = _check_row_count(
            total_rows,
            gate_config.row_count,
            catalog,
            node_name
        )
        details["row_count_check"] = row_check

        if not row_check["passed"]:
            gate_passed = False
            failure_reasons.append(row_check["reason"])

    details["failure_reasons"] = failure_reasons

    return GateResult(
        passed=gate_passed,
        pass_rate=pass_rate,
        total_rows=total_rows,
        passed_rows=passed_rows,
        failed_rows=total_rows - passed_rows,
        details=details,
        action=gate_config.on_fail if not gate_passed else None
    )


def _check_row_count(
    current_count: int,
    config: RowCountGate,
    catalog: Optional[CatalogManager],
    node_name: Optional[str]
) -> Dict[str, Any]:
    """Check row count against thresholds and history."""

    result = {"passed": True, "reason": None, "current": current_count}

    if config.min is not None and current_count < config.min:
        result["passed"] = False
        result["reason"] = f"Row count {current_count} < minimum {config.min}"
        return result

    if config.max is not None and current_count > config.max:
        result["passed"] = False
        result["reason"] = f"Row count {current_count} > maximum {config.max}"
        return result

    # Historical comparison
    if config.change_threshold is not None and catalog and node_name:
        previous_count = catalog.get_last_row_count(node_name)
        if previous_count:
            change = abs(current_count - previous_count) / previous_count
            result["previous"] = previous_count
            result["change"] = change

            if change > config.change_threshold:
                result["passed"] = False
                result["reason"] = (
                    f"Row count changed {change:.1%} "
                    f"({previous_count} → {current_count}), "
                    f"exceeds threshold {config.change_threshold:.1%}"
                )

    return result
```

### Node Integration

```python
# In node.py - after validation, before write:

def _check_gate(self, df, validation_results, config, context):
    """Check quality gate before proceeding to write."""

    if not config.gate:
        return df, True  # No gate configured

    gate_result = evaluate_gate(
        df,
        validation_results,
        config.gate,
        self.engine,
        context.catalog,
        config.name
    )

    if gate_result.passed:
        logger.info(f"Gate passed: {gate_result.pass_rate:.1%} pass rate")
        return df, True

    # Gate failed - take action
    logger.warning(
        f"Gate FAILED: {gate_result.pass_rate:.1%} pass rate. "
        f"Reasons: {gate_result.details['failure_reasons']}"
    )

    # Emit alert
    self._emit_gate_alert(config, gate_result, context)

    if gate_result.action == GateOnFail.ABORT:
        raise GateFailedError(
            f"Quality gate blocked write for node '{config.name}'. "
            f"Pass rate: {gate_result.pass_rate:.1%}"
        )

    elif gate_result.action == GateOnFail.WARN_AND_WRITE:
        return df, True  # Write all, warning already logged

    elif gate_result.action == GateOnFail.WRITE_VALID_ONLY:
        valid_df = self.engine.filter(df, validation_results["_all_passed"])
        return valid_df, True
```

---

## 3. Alerting Enhancements

### Current Gaps

| Feature | Teams | Slack | Gap |
|---------|-------|-------|-----|
| Timestamp | ✅ | ❌ | Slack missing |
| Status-based styling | ✅ (Adaptive Card styles) | ⚠️ (emoji only) | Slack needs color |
| Structured facts | ✅ (FactSet) | ⚠️ (inline fields) | Minor |
| Action buttons | ❌ | ❌ | Both missing |
| Validation events | ❌ | ❌ | Both missing |
| Throttling | ❌ | ❌ | Both missing |

### New Alert Events

```python
# odibi/config.py

class AlertEvent(str, Enum):
    """Events that trigger alerts."""

    # Existing
    ON_START = "on_start"
    ON_SUCCESS = "on_success"
    ON_FAILURE = "on_failure"

    # NEW: Validation & Quality
    ON_VALIDATION_FAIL = "on_validation_fail"    # Any validation test failed
    ON_GATE_BLOCK = "on_gate_block"              # Quality gate blocked write
    ON_QUARANTINE = "on_quarantine"              # Rows sent to quarantine
    ON_THRESHOLD_BREACH = "on_threshold_breach"  # Row count anomaly, etc.

    # NEW: Data Quality
    ON_SCHEMA_DRIFT = "on_schema_drift"          # Schema changed vs previous
    ON_FRESHNESS_FAIL = "on_freshness_fail"      # Data too old
```

### Enhanced Slack Payload

```python
# odibi/utils/alerting.py

def _build_slack_payload(config: AlertConfig, message: str, context: Dict[str, Any]) -> Dict:
    """Enhanced Slack Block Kit payload with parity to Teams."""

    pipeline = context.get("pipeline", "Unknown")
    status = context.get("status", "UNKNOWN")
    event_type = context.get("event_type", "on_failure")
    duration = context.get("duration", 0.0)
    timestamp = context.get("timestamp", datetime.utcnow().isoformat())
    project_config = context.get("project_config")

    # Status styling
    status_config = {
        "SUCCESS": {"icon": "✅", "color": "#36a64f"},
        "STARTED": {"icon": "🚀", "color": "#1e90ff"},
        "FAILED": {"icon": "❌", "color": "#dc3545"},
        "GATE_BLOCKED": {"icon": "🚫", "color": "#ff6b35"},
        "QUARANTINED": {"icon": "🔒", "color": "#ffc107"},
        "WARNING": {"icon": "⚠️", "color": "#ffc107"},
    }.get(status, {"icon": "❓", "color": "#6c757d"})

    # Build blocks
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{status_config['icon']} ODIBI: {pipeline} - {status}"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Project:*\n{project_config.project if project_config else 'Unknown'}"},
                {"type": "mrkdwn", "text": f"*Status:*\n{status}"},
                {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                {"type": "mrkdwn", "text": f"*Timestamp:*\n{timestamp}"},  # NEW
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Message:*\n{message}"}
        }
    ]

    # Add owner if present
    if project_config and project_config.owner:
        blocks[1]["fields"].append(
            {"type": "mrkdwn", "text": f"*Owner:*\n{project_config.owner}"}
        )

    # NEW: Add event-specific details
    if event_type == "on_quarantine":
        quarantine_details = context.get("quarantine_details", {})
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Rows Quarantined:*\n{quarantine_details.get('rows_quarantined', 0):,}"},
                {"type": "mrkdwn", "text": f"*Quarantine Table:*\n`{quarantine_details.get('quarantine_path', 'N/A')}`"},
                {"type": "mrkdwn", "text": f"*Failed Tests:*\n{', '.join(quarantine_details.get('failed_tests', []))}"},
            ]
        })

    elif event_type == "on_gate_block":
        gate_details = context.get("gate_details", {})
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Pass Rate:*\n{gate_details.get('pass_rate', 0):.1%}"},
                {"type": "mrkdwn", "text": f"*Required:*\n{gate_details.get('required_rate', 0.95):.1%}"},
                {"type": "mrkdwn", "text": f"*Rows Failed:*\n{gate_details.get('failed_rows', 0):,}"},
            ]
        })
        if gate_details.get("failure_reasons"):
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Failure Reasons:*\n• " + "\n• ".join(gate_details["failure_reasons"])}
            })

    # NEW: Action buttons (link to story)
    story_url = context.get("story_url")
    if story_url:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "📊 View Story"},
                    "url": story_url,
                    "style": "primary"
                }
            ]
        })

    # Add divider and context footer
    blocks.extend([
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"Odibi v{__version__} | {event_type}"}
            ]
        }
    ])

    # Use attachment for color sidebar
    return {
        "attachments": [{
            "color": status_config["color"],
            "blocks": blocks
        }]
    }
```

### Enhanced Teams Payload

```python
def _build_teams_payload(config: AlertConfig, message: str, context: Dict[str, Any]) -> Dict:
    """Enhanced Teams Adaptive Card with event-specific details."""

    # ... existing header/status logic ...

    # NEW: Event-specific sections
    event_type = context.get("event_type", "on_failure")

    body_items = [
        # ... existing header container ...
    ]

    # Standard facts
    facts = [
        {"title": "⏱ Duration", "value": f"{duration:.2f}s"},
        {"title": "📅 Time", "value": context.get("timestamp", "")},
        {"title": "📝 Message", "value": message},
    ]

    # NEW: Event-specific facts
    if event_type == "on_quarantine":
        qd = context.get("quarantine_details", {})
        facts.extend([
            {"title": "🔒 Rows Quarantined", "value": f"{qd.get('rows_quarantined', 0):,}"},
            {"title": "📍 Quarantine Table", "value": qd.get("quarantine_path", "N/A")},
            {"title": "❌ Failed Tests", "value": ", ".join(qd.get("failed_tests", []))},
        ])

    elif event_type == "on_gate_block":
        gd = context.get("gate_details", {})
        facts.extend([
            {"title": "📊 Pass Rate", "value": f"{gd.get('pass_rate', 0):.1%}"},
            {"title": "🎯 Required", "value": f"{gd.get('required_rate', 0.95):.1%}"},
            {"title": "❌ Rows Failed", "value": f"{gd.get('failed_rows', 0):,}"},
        ])

    body_items.append({"type": "Container", "items": [{"type": "FactSet", "facts": facts}]})

    # NEW: Action button for story
    story_url = context.get("story_url")
    if story_url:
        body_items.append({
            "type": "ActionSet",
            "actions": [
                {
                    "type": "Action.OpenUrl",
                    "title": "📊 View Story",
                    "url": story_url,
                    "style": "positive"
                }
            ]
        })

    # ... rest of card construction ...
```

### Alert Throttling

```python
# odibi/utils/alerting.py

class AlertThrottler:
    """Prevent alert spam by throttling repeated alerts."""

    def __init__(self):
        self._last_alerts: Dict[str, datetime] = {}
        self._alert_counts: Dict[str, int] = {}

    def should_send(
        self,
        alert_key: str,
        throttle_minutes: int = 15,
        max_per_hour: int = 10
    ) -> bool:
        """Check if alert should be sent based on throttling rules."""

        now = datetime.utcnow()
        last = self._last_alerts.get(alert_key)

        # Check time-based throttle
        if last and (now - last).total_seconds() < throttle_minutes * 60:
            logger.debug(f"Alert throttled: {alert_key} (within {throttle_minutes}m)")
            return False

        # Check rate limit
        hour_key = f"{alert_key}:{now.strftime('%Y%m%d%H')}"
        count = self._alert_counts.get(hour_key, 0)
        if count >= max_per_hour:
            logger.debug(f"Alert rate-limited: {alert_key} ({count}/{max_per_hour} per hour)")
            return False

        # Update tracking
        self._last_alerts[alert_key] = now
        self._alert_counts[hour_key] = count + 1

        return True


# Global throttler instance
_throttler = AlertThrottler()


def send_alert(config: AlertConfig, message: str, context: Dict[str, Any]) -> None:
    """Send alert with throttling."""

    # Build throttle key
    pipeline = context.get("pipeline", "unknown")
    event = context.get("event_type", "unknown")
    throttle_key = f"{pipeline}:{event}"

    # Check throttling (configurable per alert)
    throttle_minutes = config.metadata.get("throttle_minutes", 15)
    if not _throttler.should_send(throttle_key, throttle_minutes):
        return

    # ... existing send logic ...
```

### YAML Config for Enhanced Alerting

```yaml
alerts:
  - type: slack
    url: "${SLACK_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_gate_block
      - on_quarantine
    metadata:
      throttle_minutes: 15  # Don't repeat same alert within 15 min
      channel: "#data-alerts"

  - type: teams
    url: "${TEAMS_WEBHOOK_URL}"
    on_events:
      - on_failure
      - on_gate_block
      - on_threshold_breach
    metadata:
      throttle_minutes: 30
```

---

## 4. Schema Version Tracking

### Problem
Schema changes between runs are shown in stories but not tracked historically in the catalog.

### Solution
Store schema snapshots in `meta_schemas` table for audit trail.

### Catalog Schema

```python
# odibi/catalog.py

SCHEMA_TABLE_SCHEMA = StructType([
    StructField("table_path", StringType(), False),      # e.g., "silver/customers"
    StructField("schema_version", LongType(), False),    # Auto-incrementing
    StructField("schema_hash", StringType(), False),     # MD5 of column definitions
    StructField("columns", StringType(), False),         # JSON: {"col": "type", ...}
    StructField("captured_at", TimestampType(), False),
    StructField("pipeline", StringType(), True),
    StructField("node", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("columns_added", ArrayType(StringType()), True),
    StructField("columns_removed", ArrayType(StringType()), True),
    StructField("columns_type_changed", ArrayType(StringType()), True),
])
```

### Implementation

```python
# odibi/catalog.py

def track_schema(
    self,
    table_path: str,
    schema: Dict[str, str],
    pipeline: str,
    node: str,
    run_id: str
) -> Dict[str, Any]:
    """
    Track schema version for a table.

    Returns:
        Dict with version info and detected changes
    """
    schema_hash = self._hash_schema(schema)

    # Get previous version
    previous = self._get_latest_schema(table_path)

    if previous and previous["schema_hash"] == schema_hash:
        # No change
        return {"changed": False, "version": previous["schema_version"]}

    # Detect changes
    changes = {}
    if previous:
        prev_cols = json.loads(previous["columns"])
        changes = {
            "columns_added": list(set(schema.keys()) - set(prev_cols.keys())),
            "columns_removed": list(set(prev_cols.keys()) - set(schema.keys())),
            "columns_type_changed": [
                col for col in schema
                if col in prev_cols and schema[col] != prev_cols[col]
            ]
        }
        new_version = previous["schema_version"] + 1
    else:
        new_version = 1

    # Insert new version
    record = {
        "table_path": table_path,
        "schema_version": new_version,
        "schema_hash": schema_hash,
        "columns": json.dumps(schema),
        "captured_at": datetime.utcnow(),
        "pipeline": pipeline,
        "node": node,
        "run_id": run_id,
        **changes
    }

    self._append_to_table("meta_schemas", record)

    return {
        "changed": True,
        "version": new_version,
        "previous_version": previous["schema_version"] if previous else None,
        **changes
    }


def get_schema_history(
    self,
    table_path: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """Get schema version history for a table."""

    return self._query_table(
        "meta_schemas",
        filter=f"table_path = '{table_path}'",
        order_by="schema_version DESC",
        limit=limit
    )
```

### CLI Command

```bash
# Show schema history
$ odibi schema history silver/customers

Version  Captured At          Changes
-------  -------------------  ----------------------------------------
v5       2024-01-30 10:15:00  +loyalty_tier (new column)
v4       2024-01-15 08:30:00  email: VARCHAR→STRING (type change)
v3       2024-01-01 12:00:00  -legacy_id (removed)
v2       2023-12-15 09:00:00  +created_at, +updated_at
v1       2023-12-01 10:00:00  Initial schema (12 columns)

# Compare two versions
$ odibi schema diff silver/customers --from v3 --to v5

  customer_id    STRING     (unchanged)
  email          STRING     (unchanged)
- legacy_id      STRING     (removed in v3)
+ loyalty_tier   STRING     (added in v5)
```

---

## 5. Cross-Pipeline Lineage

### Problem
Lineage is tracked within a single pipeline. No visibility into how pipelines connect (e.g., Bronze pipeline feeds Silver pipeline).

### Solution
Track table-level lineage in the catalog, linking pipelines through shared tables.

### Catalog Schema

```python
# odibi/catalog.py

LINEAGE_TABLE_SCHEMA = StructType([
    StructField("source_table", StringType(), False),    # Full path
    StructField("target_table", StringType(), False),
    StructField("source_pipeline", StringType(), True),
    StructField("source_node", StringType(), True),
    StructField("target_pipeline", StringType(), True),
    StructField("target_node", StringType(), True),
    StructField("relationship", StringType(), False),    # "feeds" | "derived_from"
    StructField("last_observed", TimestampType(), False),
    StructField("run_id", StringType(), True),
])
```

### Implementation

```python
# odibi/lineage.py (extend existing)

class LineageTracker:
    """Track cross-pipeline lineage relationships."""

    def __init__(self, catalog: CatalogManager):
        self.catalog = catalog

    def record_lineage(
        self,
        read_config: Optional[ReadConfig],
        write_config: Optional[WriteConfig],
        pipeline: str,
        node: str,
        run_id: str,
        connections: Dict[str, Any]
    ):
        """Record lineage from node's read/write config."""

        if not write_config:
            return  # No output = no lineage to record

        target_table = self._resolve_table_path(write_config, connections)

        # Record read source -> write target
        if read_config:
            source_table = self._resolve_table_path(read_config, connections)
            self._upsert_lineage(
                source_table=source_table,
                target_table=target_table,
                target_pipeline=pipeline,
                target_node=node,
                run_id=run_id
            )

        # Record depends_on -> write target (if depends_on reads from another pipeline's output)
        # This is discovered by matching table paths

    def get_upstream(self, table_path: str, depth: int = 3) -> List[Dict]:
        """Get all upstream sources for a table."""

        upstream = []
        visited = set()
        queue = [(table_path, 0)]

        while queue:
            current, level = queue.pop(0)
            if current in visited or level > depth:
                continue
            visited.add(current)

            sources = self.catalog.query(
                "meta_lineage",
                filter=f"target_table = '{current}'"
            )

            for source in sources:
                upstream.append({**source, "depth": level})
                queue.append((source["source_table"], level + 1))

        return upstream

    def get_downstream(self, table_path: str, depth: int = 3) -> List[Dict]:
        """Get all downstream consumers of a table."""

        downstream = []
        visited = set()
        queue = [(table_path, 0)]

        while queue:
            current, level = queue.pop(0)
            if current in visited or level > depth:
                continue
            visited.add(current)

            targets = self.catalog.query(
                "meta_lineage",
                filter=f"source_table = '{current}'"
            )

            for target in targets:
                downstream.append({**target, "depth": level})
                queue.append((target["target_table"], level + 1))

        return downstream
```

### CLI Commands

```bash
# Trace upstream lineage
$ odibi lineage upstream gold.customer_360

gold.customer_360
└── silver.dim_customers (silver_pipeline.dedupe_customers)
    └── bronze.customers_raw (bronze_pipeline.ingest_customers)
        └── [external] azure_sql.dbo.Customers

# Trace downstream lineage
$ odibi lineage downstream bronze.customers_raw

bronze.customers_raw
├── silver.dim_customers (silver_pipeline.dedupe_customers)
│   ├── gold.customer_360 (gold_pipeline.build_360)
│   └── gold.churn_features (gold_pipeline.churn_model)
└── silver.customer_events (silver_pipeline.process_events)

# Impact analysis
$ odibi lineage impact bronze.customers_raw --if-schema-changes

⚠️  Schema change to bronze.customers_raw would affect:
  - silver.dim_customers (2 pipelines depend on this)
  - gold.customer_360 (production dashboard)
  - gold.churn_features (ML model input)

  Total: 3 downstream tables in 2 pipelines
```

### Story Integration

```html
<!-- In run_story.html, add cross-pipeline lineage section -->

{% if metadata.cross_pipeline_lineage %}
<div class="node-card" style="padding: 20px;">
    <h3>🔗 Cross-Pipeline Lineage</h3>
    <div class="mermaid">
    graph LR
        subgraph "This Pipeline"
            {% for node in metadata.nodes %}
            {{ node.node_name }}
            {% endfor %}
        end

        subgraph "Upstream Pipelines"
            {% for source in metadata.upstream_sources %}
            {{ source.pipeline }}.{{ source.node }}
            {% endfor %}
        end

        subgraph "Downstream Pipelines"
            {% for target in metadata.downstream_targets %}
            {{ target.pipeline }}.{{ target.node }}
            {% endfor %}
        end

        {% for link in metadata.cross_pipeline_links %}
        {{ link.source }} --> {{ link.target }}
        {% endfor %}
    </div>
</div>
{% endif %}
```

---

## Implementation Order

```
Week 1:
├── Day 1-2: Quarantine Tables
│   ├── Config models
│   ├── split_valid_invalid()
│   ├── Quarantine metadata columns
│   ├── Integration with node.py
│   └── Tests
│
├── Day 3: Quality Gates
│   ├── Gate config model
│   ├── evaluate_gate()
│   ├── Row count anomaly detection
│   ├── Integration with node.py
│   └── Tests

Week 2:
├── Day 4: Alerting Enhancements
│   ├── Slack parity (timestamp, colors, action buttons)
│   ├── New alert events (quarantine, gate_block)
│   ├── Event-specific payloads
│   ├── Throttling
│   └── Tests
│
├── Day 5 (half): Schema Version Tracking
│   ├── meta_schemas table
│   ├── track_schema() method
│   ├── CLI: odibi schema history
│   └── Tests
│
├── Day 5-7: Cross-Pipeline Lineage
│   ├── meta_lineage table
│   ├── LineageTracker class
│   ├── get_upstream() / get_downstream()
│   ├── CLI commands
│   ├── Story integration
│   └── Tests
```

---

## Success Criteria

| Enhancement | Success Criteria |
|-------------|------------------|
| Quarantine | Bad rows written to quarantine table with metadata; valid rows continue |
| Gates | Pipeline aborts when pass rate below threshold; alert sent |
| Alerting | Slack/Teams parity; event-specific details; throttling works |
| Schema Tracking | Schema history queryable; changes detected between runs |
| Lineage | Can trace 3 levels up/down across pipelines; visualized in stories |

---

## Dependencies

- No external dependencies required
- All features build on existing infrastructure
- Backward compatible (new config fields are optional)
