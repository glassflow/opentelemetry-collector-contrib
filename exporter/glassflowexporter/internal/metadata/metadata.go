// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package metadata contains telemetry metadata for the glassflowexporter.
package metadata // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var (
	// Type is the component type of the glassflowexporter.
	Type = component.MustNewType("glassflow")

	// TracesStability is the stability level of the traces functionality.
	TracesStability = component.StabilityLevelDevelopment

	// MetricsStability is the stability level of the metrics functionality.
	MetricsStability = component.StabilityLevelDevelopment

	// LogsStability is the stability level of the logs functionality.
	LogsStability = component.StabilityLevelDevelopment
)

// TelemetryBuilder provides an interface for providing telemetry for the
// glassflowexporter.
type TelemetryBuilder struct {
	meter                         metric.Meter
	tracer                        trace.Tracer
	logger                        *zap.Logger
	disableHighCardinalityMetrics bool
}

// TelemetryBuilderOption applies changes to TelemetryBuilder.
type TelemetryBuilderOption func(*TelemetryBuilder)

// WithMeterProvider sets the metric.MeterProvider for the TelemetryBuilder.
func WithMeterProvider(mp metric.MeterProvider) TelemetryBuilderOption {
	return func(builder *TelemetryBuilder) {
		builder.meter = mp.Meter("github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter")
	}
}

// WithTracerProvider sets the trace.TracerProvider for the TelemetryBuilder.
func WithTracerProvider(tp trace.TracerProvider) TelemetryBuilderOption {
	return func(builder *TelemetryBuilder) {
		builder.tracer = tp.Tracer("github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter")
	}
}

// WithLogger sets the zap.Logger for the TelemetryBuilder.
func WithLogger(logger *zap.Logger) TelemetryBuilderOption {
	return func(builder *TelemetryBuilder) {
		builder.logger = logger
	}
}

// WithDisableHighCardinalityMetrics sets the disableHighCardinalityMetrics for the TelemetryBuilder.
func WithDisableHighCardinalityMetrics(disableHighCardinalityMetrics bool) TelemetryBuilderOption {
	return func(builder *TelemetryBuilder) {
		builder.disableHighCardinalityMetrics = disableHighCardinalityMetrics
	}
}

// NewTelemetryBuilder creates a new TelemetryBuilder.
func NewTelemetryBuilder(settings component.TelemetrySettings, options ...TelemetryBuilderOption) (*TelemetryBuilder, error) {
	builder := &TelemetryBuilder{
		meter:  settings.MeterProvider.Meter("github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"),
		tracer: settings.TracerProvider.Tracer("github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"),
		logger: settings.Logger,
	}

	for _, op := range options {
		op(builder)
	}

	return builder, nil
}

// Shutdown is called when the component is shutting down.
func (tb *TelemetryBuilder) Shutdown() {
	// No-op for now
}
