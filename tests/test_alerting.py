from unittest.mock import MagicMock, patch
from odibi.config import AlertConfig, AlertType
from odibi.pipeline import Pipeline
from odibi.utils.alerting import send_alert


class TestAlerting:
    @patch("odibi.utils.alerting.urllib.request.urlopen")
    @patch("odibi.utils.alerting.urllib.request.Request")
    def test_send_alert_webhook(self, mock_request, mock_urlopen):
        """Test basic webhook alert."""
        config = AlertConfig(type=AlertType.WEBHOOK, url="http://example.com")
        context = {"pipeline": "test", "status": "SUCCESS"}

        send_alert(config, "Test message", context)

        mock_request.assert_called_once()
        args, kwargs = mock_request.call_args
        assert args[0] == "http://example.com"
        assert b"Test message" in kwargs["data"]

    @patch("odibi.pipeline.send_alert")
    def test_pipeline_triggers_alert(self, mock_send_alert):
        """Test that pipeline triggers alerts."""

        # Create pipeline with alert config
        alert_config = AlertConfig(
            type=AlertType.WEBHOOK, url="http://test", on_events=["on_start", "on_success"]
        )

        pipeline_config = MagicMock()
        pipeline_config.pipeline = "alert_test"
        pipeline_config.nodes = []  # Empty pipeline

        # Mock dependency graph to allow empty pipeline run
        with patch("odibi.pipeline.DependencyGraph") as mock_graph:
            mock_graph.return_value.topological_sort.return_value = []
            mock_graph.return_value.nodes = {}

            pipeline = Pipeline(
                pipeline_config=pipeline_config, alerts=[alert_config], generate_story=False
            )

            # Mock graph on instance too
            pipeline.graph = mock_graph.return_value

            pipeline.run()

            # Should trigger on_start and on_success
            assert mock_send_alert.call_count == 2

            # Check first call (on_start)
            args1, _ = mock_send_alert.call_args_list[0]
            assert args1[0] == alert_config
            assert "STARTED" in args1[2]["status"]

            # Check second call (on_success)
            args2, _ = mock_send_alert.call_args_list[1]
            assert args2[0] == alert_config
            assert "SUCCESS" in args2[2]["status"]
