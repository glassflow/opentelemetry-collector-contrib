// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics contains the metrics for the glassflowexporter.
type Metrics struct {
	// GlassflowExporterRecords records the number of records exported.
	GlassflowExporterRecords metric.Int64Counter
	// GlassflowExporterBytes records the number of bytes exported.
	GlassflowExporterBytes metric.Int64Counter
	// GlassflowExporterLatency records the latency of export operations.
	GlassflowExporterLatency metric.Float64Histogram
	// GlassflowExporterErrors records the number of errors.
	GlassflowExporterErrors metric.Int64Counter
}

// NewMetrics creates a new Metrics instance.
func (tb *TelemetryBuilder) NewMetrics() *Metrics {
	records, _ := tb.meter.Int64Counter(
		"glassflow_exporter_records",
		metric.WithDescription("Number of records exported by the glassflowexporter"),
		metric.WithUnit("1"),
	)
	bytes, _ := tb.meter.Int64Counter(
		"glassflow_exporter_bytes",
		metric.WithDescription("Number of bytes exported by the glassflowexporter"),
		metric.WithUnit("By"),
	)
	latency, _ := tb.meter.Float64Histogram(
		"glassflow_exporter_latency",
		metric.WithDescription("Latency of export operations in the glassflowexporter"),
		metric.WithUnit("s"),
	)
	errors, _ := tb.meter.Int64Counter(
		"glassflow_exporter_errors",
		metric.WithDescription("Number of errors in the glassflowexporter"),
		metric.WithUnit("1"),
	)

	return &Metrics{
		GlassflowExporterRecords: records,
		GlassflowExporterBytes:   bytes,
		GlassflowExporterLatency: latency,
		GlassflowExporterErrors:  errors,
	}
}

// RecordExportMetrics records metrics for an export operation.
func (m *Metrics) RecordExportMetrics(
	ctx context.Context,
	records int64,
	bytes int64,
	latency float64,
	outcome string,
	topic string,
	partition int32,
) {
	attrs := []attribute.KeyValue{
		attribute.String("outcome", outcome),
		attribute.String("topic", topic),
		attribute.Int("partition", int(partition)),
	}

	m.GlassflowExporterRecords.Add(ctx, records, metric.WithAttributes(attrs...))
	m.GlassflowExporterBytes.Add(ctx, bytes, metric.WithAttributes(attrs...))
	m.GlassflowExporterLatency.Record(ctx, latency, metric.WithAttributes(attrs...))

	if outcome == "failure" {
		m.GlassflowExporterErrors.Add(ctx, records, metric.WithAttributes(attrs...))
	}
}
