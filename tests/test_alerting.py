"""Tests for alerting functionality including throttling and event-specific payloads."""

import json
from unittest.mock import MagicMock, patch

from odibi.config import AlertConfig, AlertEvent, AlertType
from odibi.pipeline import Pipeline
from odibi.utils.alerting import (
    AlertThrottler,
    get_throttler,
    send_alert,
    send_gate_block_alert,
    send_quarantine_alert,
    send_threshold_breach_alert,
)


class TestAlerting:
    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_send_alert_webhook(self, mock_request, mock_urlopen):
        """Test basic webhook alert."""
        config = AlertConfig(type=AlertType.WEBHOOK, url="http://example.com")
        context = {"pipeline": "test", "status": "SUCCESS"}

        get_throttler().reset()

        send_alert(config, "Test message", context)

        mock_request.assert_called_once()
        args, kwargs = mock_request.call_args
        assert args[0] == "http://example.com"
        assert b"Test message" in kwargs["data"]

    @patch("odibi.pipeline.send_alert")
    def test_pipeline_triggers_alert(self, mock_send_alert):
        """Test that pipeline triggers alerts."""

        alert_config = AlertConfig(
            type=AlertType.WEBHOOK, url="http://test", on_events=["on_start", "on_success"]
        )

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "alert_test"
        pipeline_config.nodes = []

        with patch("odibi.pipeline.DependencyGraph") as mock_graph:
            mock_graph.return_value.topological_sort.return_value = []
            mock_graph.return_value.nodes = {}

            pipeline = Pipeline(
                pipeline_config=pipeline_config, alerts=[alert_config], generate_story=False
            )

            pipeline.graph = mock_graph.return_value

            pipeline.run()

            assert mock_send_alert.call_count == 2

            args1, _ = mock_send_alert.call_args_list[0]
            assert args1[0] == alert_config
            assert "STARTED" in args1[2]["status"]

            args2, _ = mock_send_alert.call_args_list[1]
            assert args2[0] == alert_config
            assert "SUCCESS" in args2[2]["status"]


class TestAlertThrottler:
    """Tests for AlertThrottler class."""

    def test_throttler_allows_first_alert(self):
        """First alert should always be allowed."""
        throttler = AlertThrottler()
        assert throttler.should_send("test_key") is True

    def test_throttler_blocks_immediate_repeat(self):
        """Immediate repeat of same alert should be blocked."""
        throttler = AlertThrottler()
        assert throttler.should_send("test_key") is True
        assert throttler.should_send("test_key") is False

    def test_throttler_allows_different_keys(self):
        """Different alert keys should be independent."""
        throttler = AlertThrottler()
        assert throttler.should_send("key_a") is True
        assert throttler.should_send("key_b") is True
        assert throttler.should_send("key_a") is False
        assert throttler.should_send("key_b") is False

    def test_throttler_respects_throttle_minutes(self):
        """Alert should be allowed after throttle period."""
        throttler = AlertThrottler()
        assert throttler.should_send("test_key", throttle_minutes=0) is True
        assert throttler.should_send("test_key", throttle_minutes=0) is True

    def test_throttler_rate_limit(self):
        """Max per hour limit should be enforced."""
        throttler = AlertThrottler()

        for i in range(5):
            assert throttler.should_send(f"unique_key_{i}", max_per_hour=5) is True

    def test_throttler_reset(self):
        """Reset should clear all state."""
        throttler = AlertThrottler()
        throttler.should_send("test_key")
        throttler.reset()
        assert throttler.should_send("test_key") is True


class TestSlackPayload:
    """Tests for Slack payload generation."""

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_slack_basic_payload(self, mock_request, mock_urlopen):
        """Test basic Slack payload structure."""
        config = AlertConfig(type=AlertType.SLACK, url="http://slack.test")
        context = {"pipeline": "test_pipeline", "status": "SUCCESS", "duration": 10.5}

        get_throttler().reset()
        send_alert(config, "Test message", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert "blocks" in payload
        assert payload["blocks"][0]["type"] == "header"

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_slack_quarantine_payload(self, mock_request, mock_urlopen):
        """Test Slack payload with quarantine details."""
        config = AlertConfig(type=AlertType.SLACK, url="http://slack.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "QUARANTINE",
            "event_type": AlertEvent.ON_QUARANTINE.value,
            "quarantine_details": {
                "rows_quarantined": 150,
                "quarantine_path": "silver/quarantine/customers",
                "failed_tests": ["not_null_email", "regex_email"],
            },
        }

        get_throttler().reset()
        send_alert(config, "Rows quarantined", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        payload_str = json.dumps(payload)
        assert "150" in payload_str or "Rows Quarantined" in payload_str

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_slack_gate_block_payload(self, mock_request, mock_urlopen):
        """Test Slack payload with gate block details."""
        config = AlertConfig(type=AlertType.SLACK, url="http://slack.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "GATE_BLOCKED",
            "event_type": AlertEvent.ON_GATE_BLOCK.value,
            "gate_details": {
                "pass_rate": 0.85,
                "required_rate": 0.95,
                "failed_rows": 150,
                "failure_reasons": ["Pass rate below threshold"],
            },
        }

        get_throttler().reset()
        send_alert(config, "Gate blocked", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert "blocks" in payload

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_slack_with_story_path(self, mock_request, mock_urlopen):
        """Test Slack payload includes story path in context block."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.SLACK, url="http://slack.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "SUCCESS",
            "story_path": "/path/to/stories/pipeline/2025-11-30/run.html",
        }

        get_throttler().reset()
        send_alert(config, "Pipeline complete", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        has_context = any(block.get("type") == "context" for block in payload.get("blocks", []))
        assert has_context


class TestTeamsPayload:
    """Tests for Teams Adaptive Card payload generation."""

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_teams_basic_payload(self, mock_request, mock_urlopen):
        """Test basic Teams Adaptive Card structure."""
        config = AlertConfig(type=AlertType.TEAMS, url="http://teams.test")
        context = {"pipeline": "test_pipeline", "status": "SUCCESS", "duration": 10.5}

        get_throttler().reset()
        send_alert(config, "Test message", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert "attachments" in payload
        card = payload["attachments"][0]["content"]
        assert card["type"] == "AdaptiveCard"
        assert card["version"] == "1.4"

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_teams_quarantine_payload(self, mock_request, mock_urlopen):
        """Test Teams payload with quarantine details."""
        config = AlertConfig(type=AlertType.TEAMS, url="http://teams.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "QUARANTINE",
            "event_type": AlertEvent.ON_QUARANTINE.value,
            "quarantine_details": {
                "rows_quarantined": 150,
                "quarantine_path": "silver/quarantine/customers",
                "failed_tests": ["not_null_email"],
            },
        }

        get_throttler().reset()
        send_alert(config, "Rows quarantined", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))
        card = payload["attachments"][0]["content"]

        facts = []
        for item in card["body"]:
            if item.get("type") == "Container":
                for sub_item in item.get("items", []):
                    if sub_item.get("type") == "FactSet":
                        facts.extend(sub_item.get("facts", []))

        fact_titles = [f["title"] for f in facts]
        assert any("Quarantined" in t for t in fact_titles)

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_teams_gate_block_payload(self, mock_request, mock_urlopen):
        """Test Teams payload with gate block details."""
        config = AlertConfig(type=AlertType.TEAMS, url="http://teams.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "GATE_BLOCKED",
            "event_type": AlertEvent.ON_GATE_BLOCK.value,
            "gate_details": {
                "pass_rate": 0.85,
                "required_rate": 0.95,
                "failed_rows": 150,
                "failure_reasons": ["Pass rate below threshold"],
            },
        }

        get_throttler().reset()
        send_alert(config, "Gate blocked", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))
        card = payload["attachments"][0]["content"]

        facts = []
        for item in card["body"]:
            if item.get("type") == "Container":
                for sub_item in item.get("items", []):
                    if sub_item.get("type") == "FactSet":
                        facts.extend(sub_item.get("facts", []))

        fact_titles = [f["title"] for f in facts]
        assert any("Pass Rate" in t for t in fact_titles)
        assert any("Required" in t for t in fact_titles)

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_teams_with_story_path(self, mock_request, mock_urlopen):
        """Test Teams payload includes story path in text block."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.TEAMS, url="http://teams.test")
        context = {
            "pipeline": "test_pipeline",
            "status": "SUCCESS",
            "story_path": "/path/to/stories/pipeline/2025-11-30/run.html",
        }

        get_throttler().reset()
        send_alert(config, "Pipeline complete", context, throttle=False)

        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))
        card = payload["attachments"][0]["content"]

        has_text_block = any(
            item.get("type") == "TextBlock" and "Story" in item.get("text", "")
            for item in card["body"]
        )
        assert has_text_block


class TestConvenienceFunctions:
    """Tests for convenience alert functions."""

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_send_quarantine_alert(self, mock_request, mock_urlopen):
        """Test send_quarantine_alert convenience function."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")

        get_throttler().reset()
        result = send_quarantine_alert(
            config=config,
            pipeline="test_pipeline",
            node_name="clean_customers",
            rows_quarantined=100,
            quarantine_path="silver/quarantine/customers",
            failed_tests=["not_null_email", "regex_match"],
        )

        assert result is True
        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert payload["event_type"] == AlertEvent.ON_QUARANTINE.value
        assert payload["quarantine_details"]["rows_quarantined"] == 100

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_send_gate_block_alert(self, mock_request, mock_urlopen):
        """Test send_gate_block_alert convenience function."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")

        get_throttler().reset()
        result = send_gate_block_alert(
            config=config,
            pipeline="test_pipeline",
            node_name="process_customers",
            pass_rate=0.85,
            required_rate=0.95,
            failed_rows=150,
            total_rows=1000,
            failure_reasons=["Pass rate 85.0% < required 95.0%"],
        )

        assert result is True
        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert payload["event_type"] == AlertEvent.ON_GATE_BLOCK.value
        assert payload["gate_details"]["pass_rate"] == 0.85
        assert payload["gate_details"]["required_rate"] == 0.95

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_send_threshold_breach_alert(self, mock_request, mock_urlopen):
        """Test send_threshold_breach_alert convenience function."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")

        get_throttler().reset()
        result = send_threshold_breach_alert(
            config=config,
            pipeline="test_pipeline",
            node_name="process_customers",
            metric="row_count_change",
            threshold="50%",
            actual_value="75%",
        )

        assert result is True
        args, kwargs = mock_request.call_args
        payload = json.loads(kwargs["data"].decode("utf-8"))

        assert payload["event_type"] == AlertEvent.ON_THRESHOLD_BREACH.value
        assert payload["threshold_details"]["metric"] == "row_count_change"
        assert payload["threshold_details"]["threshold"] == "50%"
        assert payload["threshold_details"]["actual_value"] == "75%"


class TestAlertEventEnum:
    """Tests for new AlertEvent enum values."""

    def test_new_event_types_exist(self):
        """Verify new alert event types are defined."""
        assert AlertEvent.ON_QUARANTINE.value == "on_quarantine"
        assert AlertEvent.ON_GATE_BLOCK.value == "on_gate_block"
        assert AlertEvent.ON_THRESHOLD_BREACH.value == "on_threshold_breach"

    def test_alert_config_accepts_new_events(self):
        """AlertConfig should accept new event types."""
        config = AlertConfig(
            type=AlertType.SLACK,
            url="http://test.com",
            on_events=[
                AlertEvent.ON_FAILURE,
                AlertEvent.ON_QUARANTINE,
                AlertEvent.ON_GATE_BLOCK,
                AlertEvent.ON_THRESHOLD_BREACH,
            ],
        )

        assert AlertEvent.ON_QUARANTINE in config.on_events
        assert AlertEvent.ON_GATE_BLOCK in config.on_events
        assert AlertEvent.ON_THRESHOLD_BREACH in config.on_events


class TestThrottlingIntegration:
    """Tests for throttling integration with send_alert."""

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_throttling_applies_to_send_alert(self, mock_request, mock_urlopen):
        """Throttling should prevent repeated alerts."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")
        context = {"pipeline": "test_pipeline", "status": "FAILURE", "event_type": "on_failure"}

        get_throttler().reset()

        result1 = send_alert(config, "First alert", context, throttle=True)
        result2 = send_alert(config, "Second alert", context, throttle=True)

        assert result1 is True
        assert result2 is False
        assert mock_request.call_count == 1

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_throttle_can_be_disabled(self, mock_request, mock_urlopen):
        """throttle=False should allow all alerts."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")
        context = {"pipeline": "test_pipeline", "status": "FAILURE"}

        get_throttler().reset()

        result1 = send_alert(config, "First alert", context, throttle=False)
        result2 = send_alert(config, "Second alert", context, throttle=False)

        assert result1 is True
        assert result2 is True
        assert mock_request.call_count == 2

    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_custom_throttle_config(self, mock_request, mock_urlopen):
        """Custom throttle settings from metadata should be respected."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_response

        config = AlertConfig(
            type=AlertType.WEBHOOK,
            url="http://test.com",
            metadata={"throttle_minutes": 0},
        )
        context = {"pipeline": "test_pipeline", "status": "FAILURE", "event_type": "on_failure"}

        get_throttler().reset()

        result1 = send_alert(config, "First alert", context, throttle=True)
        result2 = send_alert(config, "Second alert", context, throttle=True)

        assert result1 is True
        assert result2 is True


class TestNodeDetailsAndRunHealth:
    """Tests for node details and run health enrichments in alerts."""

    def test_slack_payload_includes_node_details(self):
        """Alert payload should include per-node row counts and durations."""
        from odibi.utils.alerting import _build_slack_payload

        context = {
            "pipeline": "test",
            "status": "SUCCESS",
            "event_type": "on_success",
            "node_details": [
                {
                    "node": "load_data",
                    "success": True,
                    "duration": 1.23,
                    "rows_processed": 1000,
                    "error": None,
                },
                {
                    "node": "transform",
                    "success": False,
                    "duration": 0.5,
                    "rows_processed": None,
                    "error": "KeyError: 'col'",
                },
            ],
        }
        config = AlertConfig(type=AlertType.SLACK, url="http://test.com")
        payload = _build_slack_payload(
            pipeline="test",
            project_name="Test",
            status="SUCCESS",
            duration=1.73,
            message="done",
            owner=None,
            event_type="on_success",
            timestamp="2024-01-01",
            context=context,
            config=config,
            color="#36a64f",
            icon="✅",
        )
        blocks_text = json.dumps(payload["blocks"])
        assert "load_data" in blocks_text
        assert "1,000 rows" in blocks_text
        assert "KeyError" in blocks_text

    def test_slack_payload_includes_run_health(self):
        """Alert payload should include run health summary on failure."""
        from odibi.utils.alerting import _build_slack_payload

        context = {
            "pipeline": "test",
            "status": "FAILED",
            "event_type": "on_failure",
            "run_health": {
                "has_failures": True,
                "failed_count": 1,
                "failed_nodes": ["bad_node"],
                "first_failure_error": "Connection refused",
                "anomaly_count": 0,
                "anomalous_nodes": [],
            },
        }
        config = AlertConfig(type=AlertType.SLACK, url="http://test.com")
        payload = _build_slack_payload(
            pipeline="test",
            project_name="Test",
            status="FAILED",
            duration=5.0,
            message="failed",
            owner=None,
            event_type="on_failure",
            timestamp="2024-01-01",
            context=context,
            config=config,
            color="#FF0000",
            icon="❌",
        )
        blocks_text = json.dumps(payload["blocks"])
        assert "1 node(s) failed" in blocks_text
        assert "Connection refused" in blocks_text

    def test_teams_payload_includes_node_details(self):
        """Teams payload should include per-node details."""
        from odibi.utils.alerting import _build_teams_workflow_payload

        context = {
            "pipeline": "test",
            "status": "SUCCESS",
            "event_type": "on_success",
            "node_details": [
                {
                    "node": "ingest",
                    "success": True,
                    "duration": 2.5,
                    "rows_processed": 5000,
                    "error": None,
                },
            ],
        }
        config = AlertConfig(type=AlertType.TEAMS_WORKFLOW, url="http://test.com")
        payload = _build_teams_workflow_payload(
            pipeline="test",
            project_name="Test",
            status="SUCCESS",
            duration=2.5,
            message="done",
            owner=None,
            event_type="on_success",
            timestamp="2024-01-01",
            context=context,
            config=config,
            style="Good",
            icon="✅",
        )
        card_text = json.dumps(payload)
        assert "ingest" in card_text
        assert "5,000 rows" in card_text

    def test_send_alerts_enriches_context_with_node_details(self):
        """_send_alerts should include node_details in context."""
        from odibi.node import NodeResult

        alert_config = AlertConfig(
            type=AlertType.WEBHOOK, url="http://test", on_events=["on_success"]
        )

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "enrich_test"
        pipeline_config.nodes = []

        results = MagicMock(
            spec=[
                "failed",
                "completed",
                "skipped",
                "duration",
                "node_results",
                "start_time",
                "end_time",
                "story_path",
                "pipeline_name",
            ]
        )
        results.failed = []
        results.completed = ["node_a"]
        results.skipped = []
        results.duration = 1.0
        results.node_results = {
            "node_a": NodeResult(
                node_name="node_a", success=True, duration=0.5, rows_processed=100
            ),
        }

        with (
            patch("odibi.pipeline.DependencyGraph") as mock_graph,
            patch("odibi.pipeline.send_alert") as mock_send,
        ):
            mock_graph.return_value.topological_sort.return_value = []
            mock_graph.return_value.nodes = {}

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                alerts=[alert_config],
                generate_story=False,
            )
            pipeline.graph = mock_graph.return_value
            pipeline._send_alerts("on_success", results)

            mock_send.assert_called_once()
            ctx = mock_send.call_args[0][2]
            assert "node_details" in ctx
            assert ctx["node_details"][0]["node"] == "node_a"
            assert ctx["node_details"][0]["rows_processed"] == 100
            assert ctx["nodes_passed"] == 1
            assert ctx["nodes_total"] == 1

    def test_slack_scoreboard_in_status_field(self):
        """Slack payload should show pass/fail/skip scoreboard in status field."""
        from odibi.utils.alerting import _build_slack_payload

        context = {
            "pipeline": "test",
            "status": "FAILED",
            "event_type": "on_failure",
            "nodes_passed": 8,
            "nodes_failed": 1,
            "nodes_skipped": 1,
            "nodes_total": 10,
        }
        config = AlertConfig(type=AlertType.SLACK, url="http://test.com")
        payload = _build_slack_payload(
            pipeline="test",
            project_name="Test",
            status="FAILED",
            duration=5.0,
            message="failed",
            owner=None,
            event_type="on_failure",
            timestamp="2024-01-01",
            context=context,
            config=config,
            color="#FF0000",
            icon="❌",
        )
        blocks_text = json.dumps(payload["blocks"], ensure_ascii=False)
        assert "✅ 8" in blocks_text
        assert "❌ 1" in blocks_text
        assert "⏭ 1" in blocks_text

    def test_teams_scoreboard_in_facts(self):
        """Teams payload should show nodes scoreboard fact."""
        from odibi.utils.alerting import _build_teams_workflow_payload

        context = {
            "pipeline": "test",
            "status": "SUCCESS",
            "event_type": "on_success",
            "nodes_passed": 5,
            "nodes_failed": 0,
            "nodes_skipped": 0,
            "nodes_total": 5,
        }
        config = AlertConfig(type=AlertType.TEAMS_WORKFLOW, url="http://test.com")
        payload = _build_teams_workflow_payload(
            pipeline="test",
            project_name="Test",
            status="SUCCESS",
            duration=3.0,
            message="done",
            owner=None,
            event_type="on_success",
            timestamp="2024-01-01",
            context=context,
            config=config,
            style="Good",
            icon="✅",
        )
        card_text = json.dumps(payload, ensure_ascii=False)
        assert "✅ 5" in card_text
        assert "📊 Nodes" in card_text

    def test_slack_error_type_shown(self):
        """Slack node details should show the exception class name."""
        from odibi.utils.alerting import _build_slack_payload

        context = {
            "pipeline": "test",
            "status": "FAILED",
            "event_type": "on_failure",
            "node_details": [
                {
                    "node": "bad_node",
                    "success": False,
                    "duration": 0.1,
                    "rows_processed": None,
                    "error": "'missing_col'",
                    "error_type": "KeyError",
                },
            ],
        }
        config = AlertConfig(type=AlertType.SLACK, url="http://test.com")
        payload = _build_slack_payload(
            pipeline="test",
            project_name="Test",
            status="FAILED",
            duration=0.1,
            message="failed",
            owner=None,
            event_type="on_failure",
            timestamp="2024-01-01",
            context=context,
            config=config,
            color="#FF0000",
            icon="❌",
        )
        blocks_text = json.dumps(payload["blocks"])
        assert "KeyError: " in blocks_text

    def test_slack_data_quality_section(self):
        """Slack payload should render data quality issues when present."""
        from odibi.utils.alerting import _build_slack_payload

        context = {
            "pipeline": "test",
            "status": "SUCCESS",
            "event_type": "on_success",
            "data_quality": {
                "total_validations_failed": 3,
                "total_failed_rows": 150,
                "nodes_with_warnings": ["node_a", "node_b"],
                "has_quality_issues": True,
            },
        }
        config = AlertConfig(type=AlertType.SLACK, url="http://test.com")
        payload = _build_slack_payload(
            pipeline="test",
            project_name="Test",
            status="SUCCESS",
            duration=2.0,
            message="done",
            owner=None,
            event_type="on_success",
            timestamp="2024-01-01",
            context=context,
            config=config,
            color="#36a64f",
            icon="✅",
        )
        blocks_text = json.dumps(payload["blocks"])
        assert "Data Quality" in blocks_text
        assert "3 validation(s) failed" in blocks_text
        assert "150 failed rows" in blocks_text
        assert "node_a" in blocks_text

    def test_teams_data_quality_section(self):
        """Teams payload should render data quality issues when present."""
        from odibi.utils.alerting import _build_teams_workflow_payload

        context = {
            "pipeline": "test",
            "status": "FAILED",
            "event_type": "on_failure",
            "data_quality": {
                "total_validations_failed": 2,
                "total_failed_rows": 50,
                "nodes_with_warnings": ["ingest"],
                "has_quality_issues": True,
            },
        }
        config = AlertConfig(type=AlertType.TEAMS_WORKFLOW, url="http://test.com")
        payload = _build_teams_workflow_payload(
            pipeline="test",
            project_name="Test",
            status="FAILED",
            duration=4.0,
            message="failed",
            owner=None,
            event_type="on_failure",
            timestamp="2024-01-01",
            context=context,
            config=config,
            style="Attention",
            icon="❌",
        )
        card_text = json.dumps(payload)
        assert "Data Quality" in card_text
        assert "2 validation(s) failed" in card_text

    def test_richer_failure_message_includes_counts(self):
        """_send_alerts msg should include pass/total counts on failure."""
        from odibi.node import NodeResult

        alert_config = AlertConfig(
            type=AlertType.WEBHOOK, url="http://test", on_events=["on_failure"]
        )

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "msg_test"
        pipeline_config.nodes = []

        results = MagicMock(
            spec=[
                "failed",
                "completed",
                "skipped",
                "duration",
                "node_results",
                "start_time",
                "end_time",
                "story_path",
                "pipeline_name",
            ]
        )
        results.failed = ["bad_node"]
        results.completed = ["good1", "good2"]
        results.skipped = []
        results.duration = 2.0
        results.node_results = {
            "good1": NodeResult(node_name="good1", success=True, duration=0.5),
            "good2": NodeResult(node_name="good2", success=True, duration=0.5),
            "bad_node": NodeResult(
                node_name="bad_node",
                success=False,
                duration=1.0,
                error=KeyError("x"),
            ),
        }

        with (
            patch("odibi.pipeline.DependencyGraph") as mock_graph,
            patch("odibi.pipeline.send_alert") as mock_send,
        ):
            mock_graph.return_value.topological_sort.return_value = []
            mock_graph.return_value.nodes = {}

            pipeline = Pipeline(
                pipeline_config=pipeline_config,
                alerts=[alert_config],
                generate_story=False,
            )
            pipeline.graph = mock_graph.return_value
            pipeline._send_alerts("on_failure", results)

            mock_send.assert_called_once()
            msg = mock_send.call_args[0][1]
            assert "2/3 nodes passed" in msg
            assert "bad_node" in msg
            ctx = mock_send.call_args[0][2]
            assert ctx["node_details"][2]["error_type"] == "KeyError"

    def test_generic_payload_includes_scoreboard(self):
        """Generic webhook payload should include scoreboard counts."""
        from odibi.utils.alerting import _build_generic_payload

        context = {
            "event_type": "on_success",
            "nodes_passed": 3,
            "nodes_failed": 0,
            "nodes_skipped": 1,
            "nodes_total": 4,
            "node_details": [{"node": "a", "success": True, "duration": 1.0}],
            "run_health": {"overall_status": "success"},
        }
        config = AlertConfig(type=AlertType.WEBHOOK, url="http://test.com")
        payload = _build_generic_payload(
            pipeline="test",
            status="SUCCESS",
            duration=2.0,
            message="done",
            timestamp="2024-01-01",
            context=context,
            config=config,
        )
        assert payload["nodes_passed"] == 3
        assert payload["nodes_total"] == 4
        assert payload["node_details"][0]["node"] == "a"
        assert payload["run_health"]["overall_status"] == "success"
