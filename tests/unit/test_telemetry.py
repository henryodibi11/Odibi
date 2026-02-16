from unittest.mock import patch

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
    setup_telemetry,
    tracer,
    meter,
    nodes_executed,
    rows_processed,
    node_duration,
)


class TestStatusCode:
    def test_ok_value(self):
        assert StatusCode.OK == 1

    def test_error_value(self):
        assert StatusCode.ERROR == 2


class TestStatus:
    def test_init_with_code_only(self):
        s = Status(StatusCode.OK)
        assert s is not None

    def test_init_with_description(self):
        s = Status(StatusCode.ERROR, description="something failed")
        assert s is not None


class TestMockSpan:
    def test_context_manager(self):
        span = MockSpan()
        with span as s:
            assert s is span

    def test_set_attribute(self):
        span = MockSpan()
        span.set_attribute("key", "value")

    def test_set_status(self):
        span = MockSpan()
        span.set_status(Status(StatusCode.OK))

    def test_record_exception(self):
        span = MockSpan()
        span.record_exception(ValueError("test"))

    def test_add_event_without_attributes(self):
        span = MockSpan()
        span.add_event("my_event")

    def test_add_event_with_attributes(self):
        span = MockSpan()
        span.add_event("my_event", attributes={"k": "v"})

    def test_exit_with_exception_info(self):
        span = MockSpan()
        span.__exit__(ValueError, ValueError("err"), None)


class TestMockTracer:
    def test_start_as_current_span_returns_mock_span(self):
        t = MockTracer()
        result = t.start_as_current_span("test_span")
        assert isinstance(result, MockSpan)

    def test_start_as_current_span_with_kwargs(self):
        t = MockTracer()
        result = t.start_as_current_span("span", kind="client", attributes={"a": 1})
        assert isinstance(result, MockSpan)

    def test_span_usable_as_context_manager(self):
        t = MockTracer()
        with t.start_as_current_span("op") as span:
            span.set_attribute("x", 1)
            span.add_event("done")


class TestMockCounter:
    def test_add_without_attributes(self):
        c = MockCounter()
        c.add(1)

    def test_add_with_attributes(self):
        c = MockCounter()
        c.add(5, attributes={"node": "test"})


class TestMockHistogram:
    def test_record_without_attributes(self):
        h = MockHistogram()
        h.record(0.5)

    def test_record_with_attributes(self):
        h = MockHistogram()
        h.record(1.23, attributes={"unit": "s"})


class TestMockMeter:
    def test_create_counter_returns_mock_counter(self):
        m = MockMeter()
        c = m.create_counter("test.counter")
        assert isinstance(c, MockCounter)

    def test_create_counter_with_kwargs(self):
        m = MockMeter()
        c = m.create_counter("c", unit="1", description="desc")
        assert isinstance(c, MockCounter)

    def test_create_histogram_returns_mock_histogram(self):
        m = MockMeter()
        h = m.create_histogram("test.histogram")
        assert isinstance(h, MockHistogram)

    def test_create_histogram_with_kwargs(self):
        m = MockMeter()
        h = m.create_histogram("h", unit="ms", description="desc")
        assert isinstance(h, MockHistogram)


class TestGetTracer:
    def test_returns_mock_tracer_when_otel_unavailable(self):
        with patch("odibi.utils.telemetry.AVAILABLE", False):
            result = get_tracer("test")
            assert isinstance(result, MockTracer)


class TestGetMeter:
    def test_returns_mock_meter_when_otel_unavailable(self):
        with patch("odibi.utils.telemetry.AVAILABLE", False):
            result = get_meter("test")
            assert isinstance(result, MockMeter)


class TestSetupTelemetry:
    def test_returns_none_when_otel_unavailable(self):
        with patch("odibi.utils.telemetry.AVAILABLE", False):
            result = setup_telemetry()
            assert result is None

    def test_returns_none_when_no_endpoint(self):
        with patch("odibi.utils.telemetry.AVAILABLE", True):
            with patch.dict("os.environ", {}, clear=True):
                result = setup_telemetry("test_service")
                assert result is None


class TestSetupTelemetryWithEndpoint:
    def test_handles_import_error_for_otlp(self):
        with patch("odibi.utils.telemetry.AVAILABLE", True):
            with patch.dict("os.environ", {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4317"}):
                setup_telemetry("test_service")


class TestGlobalInstances:
    def test_tracer_exists(self):
        assert tracer is not None

    def test_meter_exists(self):
        assert meter is not None

    def test_nodes_executed_exists(self):
        assert nodes_executed is not None

    def test_rows_processed_exists(self):
        assert rows_processed is not None

    def test_node_duration_exists(self):
        assert node_duration is not None

    def test_nodes_executed_is_usable(self):
        nodes_executed.add(1)

    def test_rows_processed_is_usable(self):
        rows_processed.add(100, attributes={"node": "test"})

    def test_node_duration_is_usable(self):
        node_duration.record(0.5, attributes={"node": "test"})
