package glassflowexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"
)

// Simple routing smoke test: just ensures exporter can start; full routing validation would require a mock producer.
func TestMetricsExporter_StartStop(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.DryRun = true
	cfg.Metrics.Gauge = SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-gauge"}}
	cfg.Metrics.Sum = SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-sum"}}
	cfg.Metrics.Histogram = SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-histogram"}}
	cfg.Metrics.ExponentialHistogram = SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-exp"}}
	cfg.Metrics.Summary = SignalConfig{Enabled: true, Encoding: "json", Topic: TopicSpec{Name: "otel-metrics-summary"}}

	exp, err := createMetricsExporter(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, exp)
	assert.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	assert.NoError(t, exp.Shutdown(context.Background()))
}
