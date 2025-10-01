// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package glassflowexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/messenger"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"
)

func pushLogs(ctx context.Context, ld plog.Logs) error {
	msgs, _, err := messenger.Logs(ld, messenger.PartitionByService)
	if err != nil {
		return err
	}
	var records []producer.Message
	topic := ""
	if logsCfg != nil {
		topic = logsCfg.Logs.Topic.Name
	}
	for _, m := range msgs {
		records = append(records, producer.Message{Topic: topic, Key: m.Key, Value: m.Value})
	}
	return logsProd.ExportData(ctx, records)
}

func startLogs(ctx context.Context, _ component.Host) error {
	if logsCfg != nil && !logsCfg.DryRun {
		if logsCfg.Logs.Enabled && logsCfg.Logs.Topic.Name != "" && logsCfg.Logs.Topic.Create {
			_ = producer.EnsureTopics(ctx, logsCfg.ClientConfig, map[string]producer.TopicSpec{
				logsCfg.Logs.Topic.Name: {
					Enabled:           true,
					NumPartitions:     logsCfg.Logs.Topic.NumPartitions,
					ReplicationFactor: logsCfg.Logs.Topic.ReplicationFactor,
				},
			})
		}
		p, err := producer.NewSaramaProducer(ctx, logsCfg.ClientConfig, logsCfg.Producer, logsCfg.TimeoutSettings.Timeout, logsLogger, logsDebug, logsMetrics)
		if err != nil {
			return err
		}
		logsProd = p
		return nil
	}
	logsProd = producer.NopProducer{}
	return nil
}
func shutdownLogs(context.Context) error {
	if logsProd == nil {
		return nil
	}
	return logsProd.Close()
}

var _ = exporterhelper.NewLogs

var logsCfg *Config
var logsProd producer.Producer
var logsLogger *zap.Logger
var logsDebug bool
var logsMetrics *metadata.Metrics

func setLogsConfig(c *Config, logger *zap.Logger, metrics *metadata.Metrics) {
	logsCfg = c
	logsLogger = logger
	logsDebug = c.Debug
	logsMetrics = metrics
}
