package glassflowexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateTracesExporter(t *testing.T) {
	cfg := createDefaultConfig()
	exp, err := createTracesExporter(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
}

func TestCreateMetricsExporter(t *testing.T) {
	cfg := createDefaultConfig()
	exp, err := createMetricsExporter(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
}

func TestCreateLogsExporter(t *testing.T) {
	cfg := createDefaultConfig()
	exp, err := createLogsExporter(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
}

func TestSASLv0Configuration(t *testing.T) {
	// Test that SASL v0 configuration works without ApiVersionsRequest error
	cfg := createDefaultConfig().(*Config)
	cfg.Brokers = []string{"localhost:9092"}
	cfg.Authentication = configkafka.AuthenticationConfig{
		SASL: &configkafka.SASLConfig{
			Username:  "test",
			Password:  "test",
			Mechanism: "PLAIN",
			Version:   0, // This should trigger the ApiVersionsRequest fix
		},
	}

	// This should not fail with the ApiVersionsRequest error
	exp, err := createTracesExporter(
		t.Context(),
		exportertest.NewNopSettings(metadata.Type),
		cfg,
	)
	assert.NoError(t, err)
	require.NotNil(t, exp)
}
