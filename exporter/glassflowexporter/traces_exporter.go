// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package glassflowexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/messenger"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/producer"
)

func pushTraces(ctx context.Context, td ptrace.Traces) error {
	msgs, _, err := messenger.Traces(td, messenger.PartitionByTraceID)
	if err != nil {
		return err
	}
	var records []producer.Message
	topic := ""
	if tracesCfg != nil {
		topic = tracesCfg.Traces.Topic.Name
	}
	for _, m := range msgs {
		records = append(records, producer.Message{Topic: topic, Key: m.Key, Value: m.Value})
	}
	return tracesProd.ExportData(ctx, records)
}

func startTraces(ctx context.Context, _ component.Host) error {
	if tracesCfg != nil && !tracesCfg.DryRun {
		if tracesCfg.Traces.Enabled && tracesCfg.Traces.Topic.Name != "" && tracesCfg.Traces.Topic.Create {
			_ = producer.EnsureTopics(ctx, tracesCfg.ClientConfig, map[string]producer.TopicSpec{
				tracesCfg.Traces.Topic.Name: {
					Enabled:           true,
					NumPartitions:     tracesCfg.Traces.Topic.NumPartitions,
					ReplicationFactor: tracesCfg.Traces.Topic.ReplicationFactor,
				},
			})
		}
		p, err := producer.NewSaramaProducer(ctx, tracesCfg.ClientConfig, tracesCfg.Producer, tracesCfg.TimeoutSettings.Timeout)
		if err != nil {
			return err
		}
		tracesProd = p
		return nil
	}
	tracesProd = producer.NopProducer{}
	return nil
}
func shutdownTraces(context.Context) error {
	if tracesProd == nil {
		return nil
	}
	return tracesProd.Close()
}

var _ = exporterhelper.NewTraces

var tracesCfg *Config
var tracesProd producer.Producer

func setTracesConfig(c *Config) { tracesCfg = c }
