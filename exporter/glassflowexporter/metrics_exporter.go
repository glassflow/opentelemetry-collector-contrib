// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package glassflowexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/messenger"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"
)

func pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	msgs, _, err := messenger.Metrics(md, messenger.PartitionByService)
	if err != nil {
		return err
	}
	var records []producer.Message
	for _, mm := range msgs {
		if metricsCfg == nil {
			continue
		}
		var sc SignalConfig
		switch mm.Type {
		case pmetric.MetricTypeGauge:
			sc = metricsCfg.Metrics.Gauge
		case pmetric.MetricTypeSum:
			sc = metricsCfg.Metrics.Sum
		case pmetric.MetricTypeHistogram:
			sc = metricsCfg.Metrics.Histogram
		case pmetric.MetricTypeExponentialHistogram:
			sc = metricsCfg.Metrics.ExponentialHistogram
		case pmetric.MetricTypeSummary:
			sc = metricsCfg.Metrics.Summary
		}
		if !sc.Enabled || sc.Topic.Name == "" {
			continue
		}
		records = append(records, producer.Message{Topic: sc.Topic.Name, Key: mm.Key, Value: mm.Value})
	}
	return metricsProd.ExportData(ctx, records)
}

func startMetrics(ctx context.Context, _ component.Host) error {
	if metricsCfg != nil && !metricsCfg.DryRun {
		_ = producer.EnsureTopics(ctx, metricsCfg.ClientConfig, map[string]producer.TopicSpec{
			metricsCfg.Metrics.Gauge.Topic.Name: {
				Enabled:           metricsCfg.Metrics.Gauge.Enabled && metricsCfg.Metrics.Gauge.Topic.Name != "" && metricsCfg.Metrics.Gauge.Topic.Create,
				NumPartitions:     metricsCfg.Metrics.Gauge.Topic.NumPartitions,
				ReplicationFactor: metricsCfg.Metrics.Gauge.Topic.ReplicationFactor,
			},
			metricsCfg.Metrics.Sum.Topic.Name: {
				Enabled:           metricsCfg.Metrics.Sum.Enabled && metricsCfg.Metrics.Sum.Topic.Name != "" && metricsCfg.Metrics.Sum.Topic.Create,
				NumPartitions:     metricsCfg.Metrics.Sum.Topic.NumPartitions,
				ReplicationFactor: metricsCfg.Metrics.Sum.Topic.ReplicationFactor,
			},
			metricsCfg.Metrics.Histogram.Topic.Name: {
				Enabled:           metricsCfg.Metrics.Histogram.Enabled && metricsCfg.Metrics.Histogram.Topic.Name != "" && metricsCfg.Metrics.Histogram.Topic.Create,
				NumPartitions:     metricsCfg.Metrics.Histogram.Topic.NumPartitions,
				ReplicationFactor: metricsCfg.Metrics.Histogram.Topic.ReplicationFactor,
			},
			metricsCfg.Metrics.ExponentialHistogram.Topic.Name: {
				Enabled:           metricsCfg.Metrics.ExponentialHistogram.Enabled && metricsCfg.Metrics.ExponentialHistogram.Topic.Name != "" && metricsCfg.Metrics.ExponentialHistogram.Topic.Create,
				NumPartitions:     metricsCfg.Metrics.ExponentialHistogram.Topic.NumPartitions,
				ReplicationFactor: metricsCfg.Metrics.ExponentialHistogram.Topic.ReplicationFactor,
			},
			metricsCfg.Metrics.Summary.Topic.Name: {
				Enabled:           metricsCfg.Metrics.Summary.Enabled && metricsCfg.Metrics.Summary.Topic.Name != "" && metricsCfg.Metrics.Summary.Topic.Create,
				NumPartitions:     metricsCfg.Metrics.Summary.Topic.NumPartitions,
				ReplicationFactor: metricsCfg.Metrics.Summary.Topic.ReplicationFactor,
			},
		})
		p, err := producer.NewSaramaProducer(ctx, metricsCfg.ClientConfig, metricsCfg.Producer, metricsCfg.TimeoutSettings.Timeout, metricsLogger, metricsDebug, metricsMetrics)
		if err != nil {
			return err
		}
		metricsProd = p
		return nil
	}
	metricsProd = producer.NopProducer{}
	return nil
}

func shutdownMetrics(context.Context) error {
	if metricsProd == nil {
		return nil
	}
	return metricsProd.Close()
}

var _ = exporterhelper.NewMetrics

var metricsCfg *Config
var metricsProd producer.Producer
var metricsLogger *zap.Logger
var metricsDebug bool
var metricsMetrics *metadata.Metrics

func setMetricsConfig(c *Config, logger *zap.Logger, metrics *metadata.Metrics) {
	metricsCfg = c
	metricsLogger = logger
	metricsDebug = c.Debug
	metricsMetrics = metrics
}
