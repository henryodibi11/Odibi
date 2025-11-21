"""Telemetry utilities for OpenTelemetry integration."""

import os
import sys

try:
    from opentelemetry import trace, metrics
    from opentelemetry.trace import Status, StatusCode

    AVAILABLE = True
except ImportError:
    AVAILABLE = False

# --- Mock Classes for when OTel is missing ---


class StatusCode:
    OK = 1
    ERROR = 2


class Status:
    def __init__(self, status_code, description=""):
        pass


class MockSpan:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def set_attribute(self, key, value):
        pass

    def set_status(self, status):
        pass

    def record_exception(self, exception):
        pass

    def add_event(self, name, attributes=None):
        pass


class MockTracer:
    def start_as_current_span(self, name, kind=None, attributes=None):
        return MockSpan()


class MockCounter:
    def add(self, amount, attributes=None):
        pass


class MockHistogram:
    def record(self, amount, attributes=None):
        pass


class MockMeter:
    def create_counter(self, name, unit="", description=""):
        return MockCounter()

    def create_histogram(self, name, unit="", description=""):
        return MockHistogram()


# --- Public API ---


def get_tracer(name: str):
    """Get a tracer (real or mock)."""
    if AVAILABLE:
        return trace.get_tracer(name)
    return MockTracer()


def get_meter(name: str):
    """Get a meter (real or mock)."""
    if AVAILABLE:
        return metrics.get_meter(name)
    return MockMeter()


def setup_telemetry(service_name: str = "odibi"):
    """Configure OpenTelemetry if available and configured.

    Checks OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
    If set, configures OTLP exporter.
    """
    if not AVAILABLE:
        return

    # Check for OTLP endpoint
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource

        # Initialize Provider
        resource = Resource.create(attributes={"service.name": service_name})
        provider = TracerProvider(resource=resource)

        # OTLP Exporter
        exporter = OTLPSpanExporter(endpoint=endpoint)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)

        # Set Global
        trace.set_tracer_provider(provider)

    except ImportError:
        # OTLP exporter might not be installed
        pass
    except Exception as e:
        print(f"Warning: Failed to initialize OpenTelemetry: {e}", file=sys.stderr)


# --- Global Instances ---

tracer = get_tracer("odibi")
meter = get_meter("odibi")

# Metrics
nodes_executed = meter.create_counter(
    "odibi.nodes_executed", description="Number of nodes executed"
)

rows_processed = meter.create_counter("odibi.rows_processed", description="Total rows processed")

node_duration = meter.create_histogram(
    "odibi.node_duration", unit="s", description="Duration of node execution"
)
