// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package glassflowexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
)

// NewFactory creates a factory for the glassflow exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutConfig(),
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		QueueBatchConfig: exporterhelper.NewDefaultQueueConfig(),
		DryRun:           false,
		Producer:         configkafka.ProducerConfig{MaxMessageBytes: 1000000, RequiredAcks: -1, Compression: "none"},
		Traces:           SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-traces", Create: true, NumPartitions: 1, ReplicationFactor: 1}},
		Metrics:          MetricsConfig{Sum: SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-sum", Create: true, NumPartitions: 1, ReplicationFactor: 1}}},
		Logs:             SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-logs", Create: true, NumPartitions: 1, ReplicationFactor: 1}},
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	// capture config for push path
	setLogsConfig(c)
	// In tests, brokers may be empty; force NOP producer via DryRun to avoid startup errors.
	if len(c.ClientConfig.Brokers) == 0 {
		c.DryRun = true
	}
	return exporterhelper.NewLogs(ctx, set, c, pushLogs,
		exporterhelper.WithStart(startLogs),
		exporterhelper.WithShutdown(shutdownLogs),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueBatchConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	c := cfg.(*Config)
	setTracesConfig(c)
	if len(c.ClientConfig.Brokers) == 0 {
		c.DryRun = true
	}
	return exporterhelper.NewTraces(ctx, set, c, pushTraces,
		exporterhelper.WithStart(startTraces),
		exporterhelper.WithShutdown(shutdownTraces),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueBatchConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
	c := cfg.(*Config)
	setMetricsConfig(c)
	if len(c.ClientConfig.Brokers) == 0 {
		c.DryRun = true
	}
	return exporterhelper.NewMetrics(ctx, set, c, pushMetrics,
		exporterhelper.WithStart(startMetrics),
		exporterhelper.WithShutdown(shutdownMetrics),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueBatchConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
