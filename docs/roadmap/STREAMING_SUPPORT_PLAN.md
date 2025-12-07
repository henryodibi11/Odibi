# Spark Structured Streaming Support Plan

## Overview

This document outlines the plan to fully support Spark Structured Streaming in Odibi. Currently, the `streaming` config flag creates a streaming DataFrame but the rest of the pipeline isn't designed to handle it.

## Current State

### What Works
- `SparkEngine.read()` uses `spark.readStream` when `streaming=True`
- Merge transformer has `foreachBatch` support for streaming sources

### What's Broken
1. **Row counting fails** - `df.count()` doesn't work on streaming DataFrames
2. **No streaming write** - `engine.write()` doesn't handle streaming DataFrames
3. **No checkpoint configuration** - Streaming requires checkpoint locations
4. **No trigger configuration** - No way to specify processing intervals
5. **No output mode configuration** - append/update/complete modes not exposed
6. **Validation/contracts fail** - Can't run validations on streaming data
7. **Sample collection fails** - Can't get samples from streaming DataFrames

---

## Phase 1: Core Infrastructure

### 1.1 Add StreamingConfig to config.py

```python
class StreamingConfig(BaseModel):
    """Configuration for Spark Structured Streaming."""

    enabled: bool = Field(
        default=False,
        description="Enable streaming mode for this node"
    )
    checkpoint_location: Optional[str] = Field(
        default=None,
        description="Path for streaming checkpoints. Required for fault tolerance."
    )
    trigger: Optional[str] = Field(
        default=None,
        description="Trigger interval. Options: 'once', '10 seconds', '1 minute', 'availableNow'"
    )
    output_mode: Literal["append", "update", "complete"] = Field(
        default="append",
        description="Output mode for streaming writes"
    )
    watermark: Optional[str] = Field(
        default=None,
        description="Watermark column and delay, e.g., 'event_time, 10 minutes'"
    )
    processing_time: Optional[str] = Field(
        default=None,
        description="Processing time trigger, e.g., '10 seconds'"
    )
```

### 1.2 Move streaming from ReadConfig to NodeConfig

The `streaming` flag should be at the node level since it affects the entire execution flow:

```yaml
nodes:
  - name: ingest_events
    streaming:
      enabled: true
      checkpoint_location: "/checkpoints/ingest_events"
      trigger: "10 seconds"
      output_mode: append
    read:
      connection: kafka
      format: kafka
      options:
        subscribe: events_topic
    write:
      connection: silver
      format: delta
      table: events_stream
```

### 1.3 Update NodeExecutor to detect streaming mode

```python
def execute(self, config: NodeConfig, ...):
    is_streaming = config.streaming and config.streaming.enabled

    if is_streaming:
        return self._execute_streaming(config, ...)
    else:
        return self._execute_batch(config, ...)
```

---

## Phase 2: Streaming-Aware Execution

### 2.1 Skip incompatible operations for streaming

| Operation | Streaming Behavior |
|-----------|-------------------|
| Row counting | Skip (return None) |
| Sample collection | Skip |
| Contracts/validation | Skip or use foreachBatch |
| Schema capture | Use schema from DataFrame |
| HWM updates | Not applicable (Kafka offsets managed by checkpoints) |

### 2.2 Add streaming write to SparkEngine

```python
def write(self, df, connection, format, ..., streaming_config=None):
    if df.isStreaming:
        if not streaming_config:
            raise ValueError("Streaming DataFrame requires streaming_config")

        writer = df.writeStream \
            .format(format) \
            .outputMode(streaming_config.output_mode) \
            .option("checkpointLocation", streaming_config.checkpoint_location)

        if streaming_config.trigger == "once":
            writer = writer.trigger(once=True)
        elif streaming_config.trigger == "availableNow":
            writer = writer.trigger(availableNow=True)
        elif streaming_config.trigger:
            writer = writer.trigger(processingTime=streaming_config.trigger)

        if table:
            query = writer.toTable(table)
        else:
            query = writer.start(path)

        return {"streaming_query": query}
    else:
        # existing batch logic
        ...
```

### 2.3 Handle streaming query lifecycle

```python
def _execute_streaming(self, config, ...):
    # Read (returns streaming DataFrame)
    df = self._execute_read_phase(config, ...)

    # Transform (must be streaming-compatible)
    df = self._execute_transform_phase(config, df, ...)

    # Write (starts streaming query)
    query_info = self._execute_write_phase(config, df, ...)

    # Return query handle for management
    return NodeResult(
        node_name=config.name,
        success=True,
        metadata={
            "streaming": True,
            "query_id": query_info["streaming_query"].id,
            "query_name": query_info["streaming_query"].name,
        }
    )
```

---

## Phase 3: Streaming Sources & Sinks

### 3.1 Kafka Connection Type

```python
class KafkaConnection(BaseConnection):
    """Kafka connection for streaming."""

    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    def get_spark_options(self) -> Dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "kafka.security.protocol": self.security_protocol,
            ...
        }
```

### 3.2 Event Hubs Connection Type

```python
class EventHubsConnection(BaseConnection):
    """Azure Event Hubs connection for streaming."""

    connection_string: str
    consumer_group: str = "$Default"

    def get_spark_options(self) -> Dict[str, str]:
        return {
            "eventhubs.connectionString": self.connection_string,
            "eventhubs.consumerGroup": self.consumer_group,
        }
```

### 3.3 Supported Streaming Formats

| Source | Read | Write |
|--------|------|-------|
| Kafka | ✓ | ✓ |
| Event Hubs | ✓ | ✓ |
| Delta | ✓ | ✓ |
| Rate (testing) | ✓ | - |
| File (continuous) | ✓ | ✓ |

---

## Phase 4: Streaming Transforms

### 4.1 Watermarking Support

```yaml
nodes:
  - name: windowed_aggregation
    streaming:
      enabled: true
      watermark: "event_time, 10 minutes"
    transform:
      steps:
        - type: sql
          query: |
            SELECT
              window(event_time, '5 minutes') as window,
              COUNT(*) as event_count
            FROM {input}
            GROUP BY window(event_time, '5 minutes')
```

### 4.2 Streaming-Compatible Transforms

| Transform | Streaming Support |
|-----------|------------------|
| SQL (stateless) | ✓ |
| SQL (windowed aggregation) | ✓ with watermark |
| filter_rows | ✓ |
| derive_columns | ✓ |
| join (stream-static) | ✓ |
| join (stream-stream) | ✓ with watermark |
| deduplicate | ✓ with watermark |
| aggregate | ✓ with watermark |

### 4.3 Incompatible Transforms (Fail Fast)

- `sort` (requires complete data)
- `limit` (requires complete data)
- `distinct` without watermark
- Any transform requiring `.collect()`

---

## Phase 5: Monitoring & Management

### 5.1 Query Status Tracking

```python
class StreamingQueryManager:
    """Manage active streaming queries."""

    def __init__(self, spark):
        self.spark = spark

    def list_queries(self) -> List[Dict]:
        return [
            {
                "id": q.id,
                "name": q.name,
                "status": q.status,
                "recent_progress": q.recentProgress,
            }
            for q in self.spark.streams.active
        ]

    def stop_query(self, query_id: str):
        for q in self.spark.streams.active:
            if q.id == query_id:
                q.stop()
                return True
        return False

    def await_termination(self, query_id: str, timeout: Optional[int] = None):
        for q in self.spark.streams.active:
            if q.id == query_id:
                q.awaitTermination(timeout)
                return
```

### 5.2 CLI Commands

```bash
# List active streaming queries
odibi streaming list

# Stop a streaming query
odibi streaming stop <query-id>

# Show query progress
odibi streaming status <query-id>
```

### 5.3 Metrics Integration

```python
# OpenTelemetry metrics for streaming
streaming_records_processed = meter.create_counter(
    "odibi.streaming.records_processed",
    description="Records processed by streaming query"
)

streaming_batch_duration = meter.create_histogram(
    "odibi.streaming.batch_duration_ms",
    description="Duration of each micro-batch"
)
```

---

## Phase 6: Testing

### 6.1 Unit Tests

- Test streaming config validation
- Test checkpoint path generation
- Test trigger parsing
- Mock streaming DataFrame handling

### 6.2 Integration Tests (Spark Required)

- Rate source → Delta sink
- File source → File sink (continuous)
- Watermark and windowed aggregation
- Stream-static join

### 6.3 E2E Tests (Kafka Required)

- Kafka → Delta pipeline
- Event Hubs → Delta pipeline
- Exactly-once semantics verification

---

## Implementation Order

| Phase | Effort | Priority | Dependencies |
|-------|--------|----------|--------------|
| 1.1 StreamingConfig | 2h | P0 | None |
| 1.2 Move to NodeConfig | 1h | P0 | 1.1 |
| 1.3 Detect streaming mode | 2h | P0 | 1.2 |
| 2.1 Skip incompatible ops | 2h | P0 | 1.3 |
| 2.2 Streaming write | 4h | P0 | 2.1 |
| 2.3 Query lifecycle | 3h | P0 | 2.2 |
| 3.1 Kafka connection | 3h | P1 | 2.3 |
| 3.2 Event Hubs connection | 2h | P1 | 2.3 |
| 4.1 Watermarking | 2h | P1 | 2.3 |
| 4.2 Transform compatibility | 4h | P1 | 4.1 |
| 5.1 Query manager | 3h | P2 | 2.3 |
| 5.2 CLI commands | 2h | P2 | 5.1 |
| 5.3 Metrics | 2h | P2 | 5.1 |
| 6.x Testing | 8h | P1 | All |

**Total Estimate: ~40 hours**

---

## Example: Complete Streaming Pipeline

```yaml
# project.yaml
connections:
  kafka:
    type: kafka
    bootstrap_servers: "localhost:9092"

  silver:
    type: databricks
    catalog: main
    schema: silver

# pipeline.yaml
pipelines:
  - pipeline: realtime_events
    description: "Process events in real-time from Kafka to Delta"

    nodes:
      - name: ingest_events
        description: "Ingest raw events from Kafka"
        streaming:
          enabled: true
          checkpoint_location: "abfss://checkpoints/ingest_events"
          trigger: "10 seconds"
          output_mode: append
          watermark: "event_time, 5 minutes"

        read:
          connection: kafka
          format: kafka
          options:
            subscribe: raw_events
            startingOffsets: earliest

        transform:
          steps:
            - type: sql
              query: |
                SELECT
                  CAST(key AS STRING) as event_key,
                  CAST(value AS STRING) as event_json,
                  timestamp as kafka_timestamp,
                  from_json(CAST(value AS STRING), 'event_type STRING, event_time TIMESTAMP, payload STRING') as parsed
                FROM {input}

            - type: derive
              columns:
                event_type: "parsed.event_type"
                event_time: "parsed.event_time"
                payload: "parsed.payload"

        write:
          connection: silver
          format: delta
          table: events_stream
          partition_by: ["event_type"]
```

---

## Open Questions

1. **Should streaming nodes run indefinitely or support batch-like "run once"?**
   - Spark supports `trigger(once=True)` for processing all available data then stopping
   - Useful for testing and scheduled streaming jobs

2. **How to handle streaming in the Pipeline orchestrator?**
   - Streaming nodes don't "complete" in the traditional sense
   - May need separate `run_streaming()` method

3. **Should we support foreachBatch for custom sinks?**
   - Enables writing to non-streaming sinks (JDBC, etc.)
   - More complex but more flexible

4. **How to expose streaming metrics in Stories?**
   - Streaming runs continuously, no single "run" to report on
   - May need real-time dashboard integration

---

## References

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Streaming](https://docs.delta.io/latest/delta-streaming.html)
- [Kafka + Spark Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
