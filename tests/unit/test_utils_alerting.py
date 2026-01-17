import datetime
import json
from datetime import timezone

import pytest
from unittest.mock import patch, MagicMock

from odibi.utils import alerting
from odibi.config import AlertType


# DummyAlertConfig is a simple substitute for AlertConfig in tests.
class DummyAlertConfig:
    def __init__(self, url, alert_type, metadata):
        self.url = url
        self.type = alert_type
        self.metadata = metadata


@pytest.fixture(autouse=True)
def reset_global_throttler():
    # Reset the global throttler state before and after each test.
    throttler = alerting.get_throttler()
    throttler.reset()
    yield
    throttler.reset()


@pytest.fixture
def dummy_config():
    # Creating a dummy configuration with SLACK type and custom throttling settings.
    return DummyAlertConfig(
        url="http://dummy-alert",
        alert_type=AlertType.SLACK,
        metadata={
            "throttle_minutes": 1,
            "max_per_hour": 2,
            "mention": [],
            "mention_on_failure": [],
        },
    )


@pytest.fixture
def dummy_context():
    # Provide a typical context dictionary with common keys.
    return {
        "pipeline": "TestPipeline",
        "event_type": "TEST_EVENT",
        "status": "SUCCESS",
        "duration": 1.23,
        "timestamp": datetime.datetime.now(timezone.utc).isoformat(),
        "total_rows_processed": 100,
        "rows_dropped": 5,
        "final_output_rows": 95,
    }


def test_alert_throttler_allows_first_send():
    throttler = alerting.AlertThrottler()
    # First call should allow send.
    assert throttler.should_send("key", throttle_minutes=1, max_per_hour=2) is True
    # Immediate second call (without waiting) should be throttled.
    assert throttler.should_send("key", throttle_minutes=1, max_per_hour=2) is False


def test_alert_throttler_reset():
    throttler = alerting.AlertThrottler()
    throttler.should_send("key", throttle_minutes=1, max_per_hour=2)
    throttler.reset()
    # After reset, sending should be allowed again.
    assert throttler.should_send("key", throttle_minutes=1, max_per_hour=2) is True


def test_get_throttler_returns_singleton():
    th1 = alerting.get_throttler()
    th2 = alerting.get_throttler()
    assert th1 is th2


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_success(mock_urlopen, dummy_config, dummy_context):
    # Simulate a successful HTTP response.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_alert(dummy_config, "Test message", dummy_context)
    assert result is True
    mock_urlopen.assert_called_once()


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_http_failure(mock_urlopen, dummy_config, dummy_context):
    # Simulate an HTTP response with failure status.
    response = MagicMock()
    response.status = 500
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    # Disable throttling to ensure the request is attempted.
    result = alerting.send_alert(dummy_config, "Test message", dummy_context, throttle=False)
    assert result is False


@patch("odibi.utils.alerting.urllib.request.urlopen", side_effect=Exception("Network error"))
def test_send_alert_exception(mock_urlopen, dummy_config, dummy_context):
    # Simulate an exception during the sending process.
    result = alerting.send_alert(dummy_config, "Test message", dummy_context, throttle=False)
    assert result is False


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_throttling_prevents_duplicate_calls(mock_urlopen, dummy_config, dummy_context):
    # Simulate a successful HTTP response.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    # First alert should be sent.
    result1 = alerting.send_alert(dummy_config, "First message", dummy_context)
    # Second alert with the same throttle key should be blocked.
    result2 = alerting.send_alert(dummy_config, "Second message", dummy_context)
    assert result1 is True
    assert result2 is False
    assert mock_urlopen.call_count == 1


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_no_throttle_sends_always(mock_urlopen, dummy_config, dummy_context):
    # Test that sending with throttle disabled always attempts the request.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_alert(dummy_config, "No throttle alert", dummy_context, throttle=False)
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_with_missing_context_keys(mock_urlopen, dummy_config):
    # Test that send_alert uses default values when context is missing keys.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_alert(dummy_config, "Missing context", {})
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_quarantine_alert_success(mock_urlopen, dummy_config):
    # Test the convenience function for sending a quarantine alert.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_quarantine_alert(
        dummy_config, "PipelineA", "NodeX", 10, "table_quarantine", ["test_fail"]
    )
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_gate_block_alert_success(mock_urlopen, dummy_config):
    # Test the convenience function for sending a gate block alert.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_gate_block_alert(
        dummy_config, "PipelineB", "NodeY", 0.80, 0.95, 5, 100, ["reason1", "reason2"]
    )
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_threshold_breach_alert_success(mock_urlopen, dummy_config):
    # Test the convenience function for sending a threshold breach alert.
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_threshold_breach_alert(
        dummy_config, "PipelineC", "NodeZ", "MetricX", 50, 75
    )
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_generic_payload_branch(mock_urlopen, dummy_config, dummy_context):
    # Set config type to a value that triggers the generic payload branch.
    dummy_config.type = "OTHER"
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    result = alerting.send_alert(dummy_config, "Generic alert", dummy_context, throttle=False)
    assert result is True


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_teams_payload_branch(mock_urlopen, dummy_config, dummy_context):
    # Test the TEAMS payload branch for Power Automate integration.
    dummy_config.type = AlertType.TEAMS
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    alerting.send_alert(dummy_config, "Teams alert", dummy_context, throttle=False)
    args, _ = mock_urlopen.call_args
    request = args[0]
    data = request.data
    payload = json.loads(data.decode("utf-8"))
    assert "attachments" in payload
    attachment = payload["attachments"][0]
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"


@patch("odibi.utils.alerting.urllib.request.urlopen")
def test_send_alert_teams_invalid_recipients(mock_urlopen, dummy_config, dummy_context):
    # Test that a single string in 'mention' metadata is converted to a list and processed correctly.
    dummy_config.type = AlertType.TEAMS
    dummy_config.metadata["mention"] = "invalid@example.com"
    response = MagicMock()
    response.status = 200
    response.__enter__.return_value = response
    mock_urlopen.return_value = response

    alerting.send_alert(dummy_config, "Teams alert", dummy_context, throttle=False)
    args, _ = mock_urlopen.call_args
    request = args[0]
    data = request.data
    payload = json.loads(data.decode("utf-8"))
    adaptive_card = payload["attachments"][0]["content"]
    # Check that the adaptive card contains msteams entities with the correct mention.
    assert "msteams" in adaptive_card
    entities = adaptive_card["msteams"].get("entities", [])
    assert any(
        entity.get("mentioned", {}).get("id") == "invalid@example.com" for entity in entities
    )
