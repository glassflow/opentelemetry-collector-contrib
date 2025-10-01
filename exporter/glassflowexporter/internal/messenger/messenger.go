// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package messenger // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/messenger"

import (
	"encoding/hex"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/formatter"
)

// PartitionStrategy enumerates key strategies.
type PartitionStrategy string

const (
	PartitionNone      PartitionStrategy = "none"
	PartitionByTraceID PartitionStrategy = "trace_id"
	PartitionByService PartitionStrategy = "service_name"
)

// MetricsMessage extends formatter.Message with metric type for routing.
type MetricsMessage struct {
	formatter.Message
	Type pmetric.MetricType
}

// Logs marshaling using formatter + key selection.
func Logs(ld plog.Logs, strat PartitionStrategy) ([]formatter.Message, int, error) {
	msgs, n, err := formatter.LogsToJSON(ld)
	if err != nil {
		return nil, 0, err
	}

	// compute keys in the same iteration order as formatter.LogsToJSON
	if strat == PartitionByService {
		idx := 0
		rs := ld.ResourceLogs()
		for i := 0; i < rs.Len(); i++ {
			rl := rs.At(i)
			serviceName := ""
			if v, ok := rl.Resource().Attributes().Get("service.name"); ok {
				serviceName = v.AsString()
			}
			sls := rl.ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				lrs := sls.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					if idx < len(msgs) && serviceName != "" {
						msgs[idx].Key = []byte(serviceName)
					}
					idx++
				}
			}
		}
	}
	return msgs, n, nil
}

// Traces marshaling using formatter + key selection.
func Traces(td ptrace.Traces, strat PartitionStrategy) ([]formatter.Message, int, error) {
	msgs, n, err := formatter.TracesToJSON(td)
	if err != nil {
		return nil, 0, err
	}

	if strat == PartitionByTraceID {
		idx := 0
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			sss := rs.ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					s := spans.At(k)
					if idx < len(msgs) {
						var buf [32]byte
						tid := s.TraceID()
						hex.Encode(buf[:], tid[:])
						msgs[idx].Key = append([]byte(nil), buf[:]...)
					}
					idx++
				}
			}
		}
	}
	return msgs, n, nil
}

// Metrics marshaling using formatter + key selection + type extraction for routing.
func Metrics(md pmetric.Metrics, strat PartitionStrategy) ([]MetricsMessage, int, error) {
	msgs, n, err := formatter.MetricsToJSON(md)
	if err != nil {
		return nil, 0, err
	}
	out := make([]MetricsMessage, 0, len(msgs))
	idx := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		serviceName := ""
		if v, ok := rm.Resource().Attributes().Get("service.name"); ok {
			serviceName = v.AsString()
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if strat == PartitionByService && idx < len(msgs) && serviceName != "" {
					msgs[idx].Key = []byte(serviceName)
				}
				if idx < len(msgs) {
					out = append(out, MetricsMessage{Message: msgs[idx], Type: m.Type()})
				}
				idx++
			}
		}
	}
	return out, n, nil
}
