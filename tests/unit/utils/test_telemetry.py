"""Comprehensive unit tests for odibi/utils/telemetry.py."""

from unittest.mock import MagicMock, patch


from odibi.utils.telemetry import (
    MockCounter,
    MockHistogram,
    MockMeter,
    MockSpan,
    MockTracer,
    Status,
    StatusCode,
    get_meter,
    get_tracer,
    meter,
    node_duration,
    nodes_executed,
    rows_processed,
    setup_telemetry,
    tracer,
)


class TestStatusCode:
    """Test StatusCode mock class."""

    def test_status_code_ok(self):
        """Test StatusCode.OK constant."""
        assert StatusCode.OK == 1

    def test_status_code_error(self):
        """Test StatusCode.ERROR constant."""
        assert StatusCode.ERROR == 2


class TestStatus:
    """Test Status mock class."""

    def test_status_instantiation(self):
        """Test Status can be instantiated with status_code."""
        status = Status(StatusCode.OK)
        assert status is not None

    def test_status_with_description(self):
        """Test Status can be instantiated with status_code and description."""
        status = Status(StatusCode.ERROR, description="Error occurred")
        assert status is not None

    def test_status_without_description(self):
        """Test Status can be instantiated without description."""
        status = Status(StatusCode.OK)
        assert status is not None


class TestMockSpan:
    """Test MockSpan class."""

    def test_context_manager(self):
        """Test MockSpan can be used as context manager."""
        span = MockSpan()
        with span as s:
            assert s is span

    def test_set_attribute(self):
        """Test set_attribute method doesn't raise."""
        span = MockSpan()
        span.set_attribute("key", "value")
        span.set_attribute("number", 42)

    def test_set_status(self):
        """Test set_status method doesn't raise."""
        span = MockSpan()
        status = Status(StatusCode.OK)
        span.set_status(status)

    def test_record_exception(self):
        """Test record_exception method doesn't raise."""
        span = MockSpan()
        exception = ValueError("test error")
        span.record_exception(exception)

    def test_add_event(self):
        """Test add_event method doesn't raise."""
        span = MockSpan()
        span.add_event("test_event")
        span.add_event("test_event_with_attrs", attributes={"key": "value"})


class TestMockTracer:
    """Test MockTracer class."""

    def test_start_as_current_span(self):
        """Test start_as_current_span returns MockSpan."""
        tracer = MockTracer()
        span = tracer.start_as_current_span("test_span")
        assert isinstance(span, MockSpan)

    def test_start_as_current_span_with_kind(self):
        """Test start_as_current_span with kind parameter."""
        tracer = MockTracer()
        span = tracer.start_as_current_span("test_span", kind="INTERNAL")
        assert isinstance(span, MockSpan)

    def test_start_as_current_span_with_attributes(self):
        """Test start_as_current_span with attributes parameter."""
        tracer = MockTracer()
        span = tracer.start_as_current_span("test_span", attributes={"key": "value"})
        assert isinstance(span, MockSpan)

    def test_span_context_manager(self):
        """Test MockSpan returned by tracer works as context manager."""
        tracer = MockTracer()
        with tracer.start_as_current_span("test_span") as span:
            assert isinstance(span, MockSpan)
            span.set_attribute("test", "value")


class TestMockCounter:
    """Test MockCounter class."""

    def test_add_without_attributes(self):
        """Test add method without attributes."""
        counter = MockCounter()
        counter.add(1)
        counter.add(10)

    def test_add_with_attributes(self):
        """Test add method with attributes."""
        counter = MockCounter()
        counter.add(1, attributes={"key": "value"})
        counter.add(5, attributes={"status": "success"})


class TestMockHistogram:
    """Test MockHistogram class."""

    def test_record_without_attributes(self):
        """Test record method without attributes."""
        histogram = MockHistogram()
        histogram.record(1.5)
        histogram.record(10.2)

    def test_record_with_attributes(self):
        """Test record method with attributes."""
        histogram = MockHistogram()
        histogram.record(1.5, attributes={"key": "value"})
        histogram.record(5.0, attributes={"node": "test_node"})


class TestMockMeter:
    """Test MockMeter class."""

    def test_create_counter(self):
        """Test create_counter returns MockCounter."""
        meter = MockMeter()
        counter = meter.create_counter("test_counter")
        assert isinstance(counter, MockCounter)

    def test_create_counter_with_unit(self):
        """Test create_counter with unit parameter."""
        meter = MockMeter()
        counter = meter.create_counter("test_counter", unit="1")
        assert isinstance(counter, MockCounter)

    def test_create_counter_with_description(self):
        """Test create_counter with description parameter."""
        meter = MockMeter()
        counter = meter.create_counter("test_counter", description="Test counter")
        assert isinstance(counter, MockCounter)

    def test_create_histogram(self):
        """Test create_histogram returns MockHistogram."""
        meter = MockMeter()
        histogram = meter.create_histogram("test_histogram")
        assert isinstance(histogram, MockHistogram)

    def test_create_histogram_with_unit(self):
        """Test create_histogram with unit parameter."""
        meter = MockMeter()
        histogram = meter.create_histogram("test_histogram", unit="s")
        assert isinstance(histogram, MockHistogram)

    def test_create_histogram_with_description(self):
        """Test create_histogram with description parameter."""
        meter = MockMeter()
        histogram = meter.create_histogram("test_histogram", description="Test histogram")
        assert isinstance(histogram, MockHistogram)


class TestGetTracer:
    """Test get_tracer function."""

    @patch("odibi.utils.telemetry.AVAILABLE", False)
    def test_get_tracer_when_unavailable(self):
        """Test get_tracer returns MockTracer when OTel unavailable."""
        tracer = get_tracer("test_service")
        assert isinstance(tracer, MockTracer)

    def test_get_tracer_when_available(self):
        """Test get_tracer uses trace.get_tracer when available."""
        import odibi.utils.telemetry as telemetry_module

        with patch.object(telemetry_module, "AVAILABLE", True):
            # Mock the trace module within telemetry
            mock_trace = MagicMock()
            mock_real_tracer = MagicMock()
            mock_trace.get_tracer.return_value = mock_real_tracer

            with patch.dict("sys.modules", {"opentelemetry.trace": mock_trace}):
                # Temporarily replace the trace reference
                old_trace = getattr(telemetry_module, "trace", None)
                telemetry_module.trace = mock_trace

                try:
                    tracer = get_tracer("test_service")
                    mock_trace.get_tracer.assert_called_once_with("test_service")
                    assert tracer is mock_real_tracer
                finally:
                    if old_trace is not None:
                        telemetry_module.trace = old_trace
                    else:
                        delattr(telemetry_module, "trace")


class TestGetMeter:
    """Test get_meter function."""

    @patch("odibi.utils.telemetry.AVAILABLE", False)
    def test_get_meter_when_unavailable(self):
        """Test get_meter returns MockMeter when OTel unavailable."""
        meter = get_meter("test_service")
        assert isinstance(meter, MockMeter)

    def test_get_meter_when_available(self):
        """Test get_meter uses metrics.get_meter when available."""
        import odibi.utils.telemetry as telemetry_module

        with patch.object(telemetry_module, "AVAILABLE", True):
            # Mock the metrics module within telemetry
            mock_metrics = MagicMock()
            mock_real_meter = MagicMock()
            mock_metrics.get_meter.return_value = mock_real_meter

            with patch.dict("sys.modules", {"opentelemetry.metrics": mock_metrics}):
                # Temporarily replace the metrics reference
                old_metrics = getattr(telemetry_module, "metrics", None)
                telemetry_module.metrics = mock_metrics

                try:
                    meter = get_meter("test_service")
                    mock_metrics.get_meter.assert_called_once_with("test_service")
                    assert meter is mock_real_meter
                finally:
                    if old_metrics is not None:
                        telemetry_module.metrics = old_metrics
                    else:
                        delattr(telemetry_module, "metrics")


class TestSetupTelemetry:
    """Test setup_telemetry function."""

    @patch("odibi.utils.telemetry.AVAILABLE", False)
    def test_setup_when_unavailable(self):
        """Test setup_telemetry returns early when OTel unavailable."""
        # Should not raise any exception
        setup_telemetry()
        setup_telemetry("custom_service")

    @patch("odibi.utils.telemetry.AVAILABLE", True)
    def test_setup_without_endpoint(self, monkeypatch):
        """Test setup_telemetry returns early when endpoint not set."""
        # Remove endpoint if it exists
        monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

        # Should not raise any exception
        setup_telemetry()

    def test_setup_with_endpoint_success(self, monkeypatch):
        """Test setup_telemetry configures OTel when endpoint is set."""
        import odibi.utils.telemetry as telemetry_module

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

        # Create mock modules
        mock_trace_module = MagicMock()
        mock_resource = MagicMock()
        mock_provider = MagicMock()
        mock_exporter = MagicMock()
        mock_processor = MagicMock()

        mock_exporter_module = MagicMock()
        mock_exporter_module.OTLPSpanExporter.return_value = mock_exporter

        mock_resource_module = MagicMock()
        mock_resource_module.Resource.create.return_value = mock_resource

        mock_trace_sdk_module = MagicMock()
        mock_trace_sdk_module.TracerProvider.return_value = mock_provider

        mock_export_module = MagicMock()
        mock_export_module.BatchSpanProcessor.return_value = mock_processor

        with patch.object(telemetry_module, "AVAILABLE", True):
            # Ensure the telemetry module has a trace reference
            old_trace = getattr(telemetry_module, "trace", None)
            telemetry_module.trace = mock_trace_module

            try:
                with patch.dict(
                    "sys.modules",
                    {
                        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": mock_exporter_module,
                        "opentelemetry.sdk.resources": mock_resource_module,
                        "opentelemetry.sdk.trace": mock_trace_sdk_module,
                        "opentelemetry.sdk.trace.export": mock_export_module,
                        "opentelemetry.trace": mock_trace_module,
                    },
                ):
                    setup_telemetry("test_service")

                    # Verify components were initialized
                    mock_resource_module.Resource.create.assert_called_once_with(
                        attributes={"service.name": "test_service"}
                    )
                    mock_trace_sdk_module.TracerProvider.assert_called_once_with(
                        resource=mock_resource
                    )
                    mock_exporter_module.OTLPSpanExporter.assert_called_once_with(
                        endpoint="http://localhost:4317"
                    )
                    mock_export_module.BatchSpanProcessor.assert_called_once_with(mock_exporter)
                    mock_provider.add_span_processor.assert_called_once_with(mock_processor)
                    mock_trace_module.set_tracer_provider.assert_called_once_with(mock_provider)
            finally:
                if old_trace is not None:
                    telemetry_module.trace = old_trace
                elif hasattr(telemetry_module, "trace"):
                    delattr(telemetry_module, "trace")

    def test_setup_with_endpoint_import_error(self, monkeypatch, capsys):
        """Test setup_telemetry handles ImportError for OTLP exporter."""
        import builtins
        import odibi.utils.telemetry as telemetry_module

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

        # Mock the Resource module to raise ImportError during import
        def mock_import(name, *args, **kwargs):
            if "opentelemetry.exporter.otlp" in name:
                raise ImportError("OTLP not installed")
            return original_import(name, *args, **kwargs)

        original_import = builtins.__import__

        with patch.object(telemetry_module, "AVAILABLE", True):
            with patch("builtins.__import__", side_effect=mock_import):
                # Should not raise, just silently pass
                setup_telemetry()

                # Should not print warning for ImportError
                captured = capsys.readouterr()
                assert "Warning: Failed to initialize OpenTelemetry" not in captured.err

    def test_setup_with_endpoint_generic_exception(self, monkeypatch, capsys):
        """Test setup_telemetry handles generic exceptions without crashing."""
        import odibi.utils.telemetry as telemetry_module

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

        # Create mock modules that succeed on import but fail on execution
        mock_trace_module = MagicMock()
        mock_resource_module = MagicMock()
        mock_resource_module.Resource.create.side_effect = RuntimeError("Setup failed")

        mock_trace_sdk_module = MagicMock()
        mock_exporter_module = MagicMock()
        mock_export_module = MagicMock()

        with patch.object(telemetry_module, "AVAILABLE", True):
            # Ensure the telemetry module has a trace reference
            old_trace = getattr(telemetry_module, "trace", None)
            telemetry_module.trace = mock_trace_module

            try:
                with patch.dict(
                    "sys.modules",
                    {
                        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": mock_exporter_module,
                        "opentelemetry.sdk.resources": mock_resource_module,
                        "opentelemetry.sdk.trace": mock_trace_sdk_module,
                        "opentelemetry.sdk.trace.export": mock_export_module,
                        "opentelemetry.trace": mock_trace_module,
                    },
                ):
                    # Should not raise
                    setup_telemetry()

                    # Should print warning for non-ImportError exceptions
                    captured = capsys.readouterr()
                    assert "Warning: Failed to initialize OpenTelemetry" in captured.err
                    assert "Setup failed" in captured.err
            finally:
                if old_trace is not None:
                    telemetry_module.trace = old_trace
                elif hasattr(telemetry_module, "trace"):
                    delattr(telemetry_module, "trace")

    def test_setup_default_service_name(self, monkeypatch):
        """Test setup_telemetry uses default service name."""
        import odibi.utils.telemetry as telemetry_module

        monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")

        # Create mock modules
        mock_trace_module = MagicMock()
        mock_resource_module = MagicMock()
        mock_resource = MagicMock()
        mock_resource_module.Resource.create.return_value = mock_resource

        mock_trace_sdk_module = MagicMock()
        mock_provider = MagicMock()
        mock_trace_sdk_module.TracerProvider.return_value = mock_provider

        mock_exporter_module = MagicMock()
        mock_exporter = MagicMock()
        mock_exporter_module.OTLPSpanExporter.return_value = mock_exporter

        mock_export_module = MagicMock()
        mock_processor = MagicMock()
        mock_export_module.BatchSpanProcessor.return_value = mock_processor

        with patch.object(telemetry_module, "AVAILABLE", True):
            # Ensure the telemetry module has a trace reference
            old_trace = getattr(telemetry_module, "trace", None)
            telemetry_module.trace = mock_trace_module

            try:
                with patch.dict(
                    "sys.modules",
                    {
                        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": mock_exporter_module,
                        "opentelemetry.sdk.resources": mock_resource_module,
                        "opentelemetry.sdk.trace": mock_trace_sdk_module,
                        "opentelemetry.sdk.trace.export": mock_export_module,
                        "opentelemetry.trace": mock_trace_module,
                    },
                ):
                    setup_telemetry()

                    # Verify default service name "odibi" was used
                    mock_resource_module.Resource.create.assert_called_once_with(
                        attributes={"service.name": "odibi"}
                    )
            finally:
                if old_trace is not None:
                    telemetry_module.trace = old_trace
                elif hasattr(telemetry_module, "trace"):
                    delattr(telemetry_module, "trace")


class TestGlobalInstances:
    """Test global telemetry instances."""

    def test_tracer_exists(self):
        """Test global tracer instance exists."""
        assert tracer is not None

    def test_meter_exists(self):
        """Test global meter instance exists."""
        assert meter is not None

    def test_nodes_executed_counter(self):
        """Test nodes_executed counter exists and is usable."""
        assert nodes_executed is not None
        # Should not raise
        nodes_executed.add(1)
        nodes_executed.add(5, attributes={"node": "test"})

    def test_rows_processed_counter(self):
        """Test rows_processed counter exists and is usable."""
        assert rows_processed is not None
        # Should not raise
        rows_processed.add(100)
        rows_processed.add(1000, attributes={"node": "test"})

    def test_node_duration_histogram(self):
        """Test node_duration histogram exists and is usable."""
        assert node_duration is not None
        # Should not raise
        node_duration.record(1.5)
        node_duration.record(10.0, attributes={"node": "test"})


class TestTelemetryIntegration:
    """Integration tests for telemetry usage patterns."""

    def test_tracer_span_workflow(self):
        """Test typical span workflow with tracer."""
        test_tracer = get_tracer("test")
        with test_tracer.start_as_current_span("test_operation") as span:
            span.set_attribute("test_key", "test_value")
            span.set_attribute("node_id", "node_1")
            span.add_event("processing_started")
            span.set_status(Status(StatusCode.OK))

    def test_meter_counter_workflow(self):
        """Test typical counter workflow with meter."""
        test_meter = get_meter("test")
        counter = test_meter.create_counter(
            "test_operations", description="Test operations counter"
        )
        counter.add(1, attributes={"status": "success"})
        counter.add(1, attributes={"status": "failure"})

    def test_meter_histogram_workflow(self):
        """Test typical histogram workflow with meter."""
        test_meter = get_meter("test")
        histogram = test_meter.create_histogram(
            "test_duration", unit="s", description="Test operation duration"
        )
        histogram.record(1.5, attributes={"operation": "load"})
        histogram.record(2.3, attributes={"operation": "transform"})

    def test_span_with_exception(self):
        """Test span recording exception."""
        test_tracer = get_tracer("test")
        with test_tracer.start_as_current_span("test_operation") as span:
            try:
                raise ValueError("Test error")
            except ValueError as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, description=str(e)))
