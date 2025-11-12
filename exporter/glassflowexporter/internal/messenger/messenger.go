// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package messenger // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/messenger"

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
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
	// PartitionByResourceHash uses a deterministic hash of the resource attributes
	// to create the Kafka message key, matching kafkaexporter behavior.
	PartitionByResourceHash PartitionStrategy = "resource_hash"
)

// mapHash16 produces a deterministic 16-byte hash from a pcommon.Map of attributes.
// It sorts keys and writes key+stringified-value into a SHA-256 digest, returning the first 16 bytes.
// This mirrors the intention of kafkaexporter partitioning by resource attributes without introducing new deps.
func mapHash16(m pcommon.Map) [16]byte {
	var out [16]byte
	if m.Len() == 0 {
		return out
	}
	// Collect and sort keys for deterministic ordering
	keys := make([]string, 0, m.Len())
	for k := range m.All() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		v, _ := m.Get(k)
		h.Write([]byte{0xF4}) // key prefix marker (arbitrary)
		h.Write([]byte(k))
		// string representation is sufficient for stable hashing across types here
		h.Write([]byte{0xF7}) // value prefix marker (arbitrary)
		h.Write([]byte(v.AsString()))
	}
	sum := h.Sum(nil)
	copy(out[:], sum[:16])
	return out
}

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
	if strat == PartitionByService || strat == PartitionByResourceHash {
		idx := 0
		rs := ld.ResourceLogs()
		for i := 0; i < rs.Len(); i++ {
			rl := rs.At(i)
			serviceName := ""
			if v, ok := rl.Resource().Attributes().Get("service.name"); ok {
				serviceName = v.AsString()
			}
			var resHash [16]byte
			if strat == PartitionByResourceHash {
				resHash = mapHash16(rl.Resource().Attributes())
			}
			sls := rl.ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				lrs := sls.At(j).LogRecords()
				for k := 0; k < lrs.Len(); k++ {
					if idx < len(msgs) {
						if strat == PartitionByService && serviceName != "" {
							msgs[idx].Key = []byte(serviceName)
						} else if strat == PartitionByResourceHash {
							// copy hash bytes to a new slice to avoid aliasing
							msgs[idx].Key = append([]byte(nil), resHash[:]...)
						}
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
		var resHash [16]byte
		if strat == PartitionByResourceHash {
			resHash = mapHash16(rm.Resource().Attributes())
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if idx < len(msgs) {
					if strat == PartitionByService && serviceName != "" {
						msgs[idx].Key = []byte(serviceName)
					} else if strat == PartitionByResourceHash {
						msgs[idx].Key = append([]byte(nil), resHash[:]...)
					}
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
