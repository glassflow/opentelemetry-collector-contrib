## glassflowexporter

The glassflow exporter publishes OpenTelemetry telemetry to Kafka, formatting payloads using the same ClickHouse-compatible JSON shape as `clickhouseexporter`.

It reuses the shared Kafka configuration from `pkg/kafka/configkafka` and exporter lifecycle from `exporterhelper`. Data transformation follows the `clickhouseexporter` mappings for logs, traces, and metrics (including gauge, sum, histogram, exponential histogram, and summary).

### Status

- Class: exporter
- Stability: development (logs, metrics, traces)

### Features

- ClickHouse-shaped JSON for logs, traces, and metrics
- Per-signal Kafka topics and partition keys
- Configurable partition strategy (trace_id or service.name)
- Kafka producer configuration via `pkg/kafka/configkafka` (TLS/SASL, retries, compression, acks)
- Dry run mode to validate transformation without sending to Kafka

### Configuration

Top-level parameters:
- `dry_run` (default: true)
  - When true, the exporter does not send to Kafka (uses a no-op producer) but still performs all formatting/routing. Recommended for tests.
- `sending_queue.enabled` (default: true)
  - Enable an internal queue to decouple Collector ingestion from Kafka availability.
- `sending_queue.num_consumers` (default: 10)
  - Number of concurrent consumers draining the sending queue.
- `sending_queue.queue_size` (default: 5120)
  - Max items buffered in the sending queue.
- `retry_on_failure.enabled` (default: true)
  - Enable retries on transient export failures.
- `retry_on_failure.initial_interval` (default: 1s)
  - Initial backoff interval for retries.
- `retry_on_failure.max_interval` (default: 30s)
  - Maximum backoff interval.
- `retry_on_failure.max_elapsed_time` (default: 5m)
  - Total time spent retrying before giving up.

Kafka client parameters:
- `brokers` (required)
  - List of Kafka broker addresses, e.g. `["kafka:9092"]`.
- `tls` (optional)
  - TLS settings for encrypted connections (enable/disable, certificates).
- `auth` (optional)
  - SASL authentication settings (mechanism, username, password, etc.).
- `protocol_version`, other advanced client fields (optional)
  - Advanced Kafka client tuning for compatibility/metadata behavior.

Kafka producer parameters:
- `producer.compression` (default: none)
  - Message compression: `none|gzip|snappy|lz4|zstd`.
- `producer.required_acks` (default: -1)
  - Acknowledgement level: `-1` (all replicas), `1` (leader), `0` (no ack).
- `producer.max_message_bytes` (default: 1000000)
  - Maximum size (bytes) of a single record sent to Kafka.
- Other producer knobs (optional)
  - Flush thresholds and batching behavior, when configured.

Per-signal parameters:
- `traces.enabled` (default: true)
- `traces.encoding` (default: json)
- `traces.topic.name` (default: otel-traces)
- `traces.topic.create` (default: true)
- `traces.topic.num_partitions` (default: 1)
- `traces.topic.replication_factor` (default: 1)

- `logs.enabled` (default: true)
- `logs.encoding` (default: json)
- `logs.topic.name` (default: otel-logs)
- `logs.topic.create` (default: true)
- `logs.topic.num_partitions` (default: 1)
- `logs.topic.replication_factor` (default: 1)

Metrics parameters (per type; choose which to enable):
- `metrics.gauge.enabled` (default: false)
- `metrics.gauge.encoding` (default: json)
- `metrics.gauge.topic.name` (no default)
- `metrics.gauge.topic.create` (default: true)
- `metrics.gauge.topic.num_partitions` (default: 1)
- `metrics.gauge.topic.replication_factor` (default: 1)

- `metrics.sum.enabled` (default: true)
- `metrics.sum.encoding` (default: json)
- `metrics.sum.topic.name` (default: otel-metrics-sum)
- `metrics.sum.topic.create` (default: true)
- `metrics.sum.topic.num_partitions` (default: 1)
- `metrics.sum.topic.replication_factor` (default: 1)

- `metrics.histogram.enabled` (default: false)
- `metrics.histogram.encoding` (default: json)
- `metrics.histogram.topic.name` (no default)
- `metrics.histogram.topic.create` (default: true)
- `metrics.histogram.topic.num_partitions` (default: 1)
- `metrics.histogram.topic.replication_factor` (default: 1)

- `metrics.exponential_histogram.enabled` (default: false)
- `metrics.exponential_histogram.encoding` (default: json)
- `metrics.exponential_histogram.topic.name` (no default)
- `metrics.exponential_histogram.topic.create` (default: true)
- `metrics.exponential_histogram.topic.num_partitions` (default: 1)
- `metrics.exponential_histogram.topic.replication_factor` (default: 1)

- `metrics.summary.enabled` (default: false)
- `metrics.summary.encoding` (default: json)
- `metrics.summary.topic.name` (no default)
- `metrics.summary.topic.create` (default: true)
- `metrics.summary.topic.num_partitions` (default: 1)
- `metrics.summary.topic.replication_factor` (default: 1)

Partitioning (defaults):
- Traces are keyed by the trace ID.
- Logs and Metrics are keyed by resource `service.name` when present; otherwise no key is used.

Notes:
- Message payloads are JSON objects that mirror ClickHouse table column names and types. Downstream consumers can insert into ClickHouse without re-mapping.
- When `create_topic.enabled` is true, topics listed in `topic` and `metrics.topics.*` are ensured at startup.

Notes:
- Encoding currently targets JSON that matches the ClickHouse table schemas used by `clickhouseexporter`.
- Partitioning is handled in the messenger layer:
  - Traces: key by trace ID
  - Logs/Metrics: key by `service.name` (if present), otherwise empty

### Example

```yaml
exporters:
  glassflow:
    # exporterhelper
    sending_queue:
      enabled: true
      num_consumers: 10
      queue_size: 5120
    retry_on_failure:
      enabled: true
      initial_interval: 1s
      max_interval: 30s
      max_elapsed_time: 5m

    # Kafka client
    brokers: ["localhost:9092"]
    protocol_version: "2.8.0"
    tls:
      insecure: true
    auth:
      # example (SASL/PLAIN). Leave empty if not needed.
      # sasl:
      #   mechanism: PLAIN
      #   username: user
      #   password: pass

    # Producer
    producer:
      compression: gzip
      required_acks: -1   # all replicas
      flush_bytes: 1048576
      max_message_bytes: 1000000

    # Per-signal configuration
    traces:
      enabled: true
      encoding: json
      topic:
        name: otel-traces
        create: true
        num_partitions: 1
        replication_factor: 1
    metrics:
      sum:
        enabled: true
        encoding: json
        topic:
          name: otel-metrics-sum
          create: true
          num_partitions: 1
          replication_factor: 1
      gauge:
        enabled: false
        encoding: json
        topic:
          name: ""
          create: true
          num_partitions: 1
          replication_factor: 1
      histogram:
        enabled: false
        encoding: json
        topic:
          name: ""
          create: true
          num_partitions: 1
          replication_factor: 1
      exponential_histogram:
        enabled: false
        encoding: json
        topic:
          name: ""
          create: true
          num_partitions: 1
          replication_factor: 1
      summary:
        enabled: false
        encoding: json
        topic:
          name: ""
          create: true
          num_partitions: 1
          replication_factor: 1
    logs:
      enabled: true
      encoding: json
      topic:
        name: otel-logs
        create: true
        num_partitions: 1
        replication_factor: 1

    # Useful for local verification without Kafka
    # dry_run: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [glassflow]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [glassflow]
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [glassflow]
```

### End-to-end example (Docker Compose)

An example environment is provided under `exporter/glassflowexporter/example`:

- `docker-compose.yml`: Starts a single Kafka broker and a `kcat` consumer for the topics `otel-traces`, `otel-metrics*`, and `otel-logs`.
- `otel-collector.yaml`: Minimal collector pipeline using the `glassflow` exporter.

Usage:

```bash
cd exporter/glassflowexporter/example
docker compose up -d

# The compose file includes:
# - kafka (broker)
# - otelcol (OpenTelemetry Collector with glassflow exporter)
# - telemetry generators (traces, metrics, logs)
# - kcat consumers for otel-* topics

# Observe messages on Kafka topics in kcat container output.
# You can also curl OTLP HTTP endpoints to send your own data:
# curl -v http://localhost:4318/v1/traces ...
```

### Payload format

Messages are JSON-encoded objects matching the column layout used by the ClickHouse exporter.

- Logs include fields like `Timestamp`, `TraceId`, `SpanId`, `SeverityText`, `SeverityNumber`, `ServiceName`, `Body`, `ResourceAttributes`, `Scope*`, `LogAttributes`.
- Traces include `Timestamp`, `TraceId`, `SpanId`, `ParentSpanId`, `TraceState`, `SpanName`, `SpanKind`, `ServiceName`, `ResourceAttributes`, `Scope*`, `SpanAttributes`, `Duration`, `Status*`, `Events.*`, `Links.*`.
- Metrics generate one message per data point and map to the respective ClickHouse table shapes:
  - Gauge/Sum: value, timestamps, flags, exemplars, plus `AggregationTemporality` and `IsMonotonic` for Sum
  - Histogram: count, sum, min, max, bucket counts, explicit bounds, exemplars, flags, temporality
  - Exponential Histogram: count, sum, scale, zero count, positive/negative offsets and bucket counts, exemplars, flags, temporality
  - Summary: count, sum, `ValueAtQuantiles` (quantile/value arrays), flags

### Partitioning

- Traces are keyed by trace ID
- Logs/Metrics are keyed by `service.name` (if present); otherwise no key is set

### Headers

Future versions may add Kafka headers (e.g., signal type, schema/encoding version). Current implementation focuses on value payload and key.

### Testing

- Unit tests validate transformation parity with ClickHouse-like JSON using golden-style assertions.
- Default configuration sets `dry_run: true` to avoid Kafka connectivity during tests.

### Limitations / Roadmap

- Headers: add schema/version markers
- Additional partition strategies (e.g., resource-hash)
- More configuration validation and error messages

# glassflowexporter
<!-- status autogenerated section -->
| Status        |           |
| ------------- |-----------|
| Stability     | [development]: logs, metrics, traces   |
| Distributions | [] |
| Issues        | [![Open issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aopen%20label%3Aexporter%2Fglassflow%20&label=open&color=orange&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aopen+is%3Aissue+label%3Aexporter%2Fglassflow) [![Closed issues](https://img.shields.io/github/issues-search/open-telemetry/opentelemetry-collector-contrib?query=is%3Aissue%20is%3Aclosed%20label%3Aexporter%2Fglassflow%20&label=closed&color=blue&logo=opentelemetry)](https://github.com/open-telemetry/opentelemetry-collector-contrib/issues?q=is%3Aclosed+is%3Aissue+label%3Aexporter%2Fglassflow) |
| Code coverage | [![codecov](https://codecov.io/github/open-telemetry/opentelemetry-collector-contrib/graph/main/badge.svg?component=exporter_glassflow)](https://app.codecov.io/gh/open-telemetry/opentelemetry-collector-contrib/tree/main/?components%5B0%5D=exporter_glassflow&displayType=list) |
| [Code Owners](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md#becoming-a-code-owner)    |  |

[development]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/component-stability.md#development
<!-- end autogenerated section -->

Kafka exporter that serializes telemetry into ClickHouse-compatible row shapes (JSON),
mirroring the transformation used by `clickhouseexporter`.

This component is in Development. Configuration subject to change.
EOF
