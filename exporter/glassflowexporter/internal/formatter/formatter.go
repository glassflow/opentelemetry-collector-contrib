// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package formatter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/glassflowexporter/internal/formatter"

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Message represents a Kafka-ready payload and an optional key.
type Message struct {
	Key   []byte
	Value []byte
}

// LogsToJSON returns ClickHouse-shaped JSON messages for logs.
func LogsToJSON(ld plog.Logs) ([]Message, int, error) {
	var out []Message
	count := 0
	rs := ld.ResourceLogs()
	for i := 0; i < rs.Len(); i++ {
		rl := rs.At(i)
		resURL := rl.SchemaUrl()
		resAttr := rl.Resource().Attributes()
		serviceName := getServiceName(resAttr)
		resMap := attributesToStringMap(resAttr)
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scopeURL := sl.SchemaUrl()
			scope := sl.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeMap := attributesToStringMap(scope.Attributes())
			lrs := sl.LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				lr := lrs.At(k)
				ts := lr.Timestamp()
				if ts == 0 {
					ts = lr.ObservedTimestamp()
				}
				obj := map[string]any{
					"TimestampTime":      ts.AsTime(),
					"TraceId":            traceIDHex(lr.TraceID()),
					"SpanId":             spanIDHex(lr.SpanID()),
					"Flags":              uint8(lr.Flags()),
					"SeverityText":       lr.SeverityText(),
					"SeverityNumber":     uint8(lr.SeverityNumber()),
					"ServiceName":        serviceName,
					"Body":               lr.Body().AsString(),
					"ResourceSchemaUrl":  resURL,
					"ResourceAttributes": resMap,
					"ScopeSchemaUrl":     scopeURL,
					"ScopeName":          scopeName,
					"ScopeVersion":       scopeVersion,
					"ScopeAttributes":    scopeMap,
					"LogAttributes":      attributesToStringMap(lr.Attributes()),
				}
				b, _ := json.Marshal(obj)
				out = append(out, Message{Value: b})
				count++
			}
		}
	}
	return out, count, nil
}

// TracesToJSON returns ClickHouse-shaped JSON messages for traces.
func TracesToJSON(td ptrace.Traces) ([]Message, int, error) {
	var out []Message
	count := 0
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resMap := attributesToStringMap(rs.Resource().Attributes())
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope := ss.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				dur := span.EndTimestamp() - span.StartTimestamp()
				evTimes, evNames, evAttrs := convertEvents(span.Events())
				linkTIDs, linkSIDs, linkStates, linkAttrs := convertLinks(span.Links())
				obj := map[string]any{
					"Timestamp":          span.StartTimestamp().AsTime(),
					"TraceId":            traceIDHex(span.TraceID()),
					"SpanId":             spanIDHex(span.SpanID()),
					"ParentSpanId":       spanIDHex(span.ParentSpanID()),
					"TraceState":         span.TraceState().AsRaw(),
					"SpanName":           span.Name(),
					"SpanKind":           span.Kind().String(),
					"ServiceName":        getServiceName(rs.Resource().Attributes()),
					"ResourceAttributes": resMap,
					"ScopeName":          scopeName,
					"ScopeVersion":       scopeVersion,
					"SpanAttributes":     attributesToStringMap(span.Attributes()),
					"Duration":           int64(dur),
					"StatusCode":         span.Status().Code().String(),
					"StatusMessage":      span.Status().Message(),
					"Events.Time":        evTimes,
					"Events.Name":        evNames,
					"Events.Attrs":       evAttrs,
					"Links.TraceId":      linkTIDs,
					"Links.SpanId":       linkSIDs,
					"Links.TraceState":   linkStates,
					"Links.Attrs":        linkAttrs,
				}
				b, _ := json.Marshal(obj)
				out = append(out, Message{Value: b})
				count++
			}
		}
	}
	return out, count, nil
}

// MetricsToJSON returns ClickHouse-shaped JSON messages for metrics by datapoint.
func MetricsToJSON(md pmetric.Metrics) ([]Message, int, error) {
	var out []Message
	count := 0
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resMap := attributesToStringMap(rm.Resource().Attributes())
		serviceName := getServiceName(rm.Resource().Attributes())
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			scopeName := scope.Name()
			scopeVersion := scope.Version()
			scopeMap := attributesToStringMap(scope.Attributes())
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				// Process each metric type according to ClickHouse table structure
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					msgs, n, err := processGaugeMetric(m, resMap, rm.SchemaUrl(), scopeName, scopeVersion, scopeMap, sm.SchemaUrl(), serviceName)
					if err != nil {
						return nil, 0, err
					}
					out = append(out, msgs...)
					count += n
				case pmetric.MetricTypeSum:
					msgs, n, err := processSumMetric(m, resMap, rm.SchemaUrl(), scopeName, scopeVersion, scopeMap, sm.SchemaUrl(), serviceName)
					if err != nil {
						return nil, 0, err
					}
					out = append(out, msgs...)
					count += n
				case pmetric.MetricTypeHistogram:
					msgs, n, err := processHistogramMetric(m, resMap, rm.SchemaUrl(), scopeName, scopeVersion, scopeMap, sm.SchemaUrl(), serviceName)
					if err != nil {
						return nil, 0, err
					}
					out = append(out, msgs...)
					count += n
				case pmetric.MetricTypeExponentialHistogram:
					msgs, n, err := processExpHistogramMetric(m, resMap, rm.SchemaUrl(), scopeName, scopeVersion, scopeMap, sm.SchemaUrl(), serviceName)
					if err != nil {
						return nil, 0, err
					}
					out = append(out, msgs...)
					count += n
				case pmetric.MetricTypeSummary:
					msgs, n, err := processSummaryMetric(m, resMap, rm.SchemaUrl(), scopeName, scopeVersion, scopeMap, sm.SchemaUrl(), serviceName)
					if err != nil {
						return nil, 0, err
					}
					out = append(out, msgs...)
					count += n
				}
			}
		}
	}
	return out, count, nil
}

// helpers
func attributesToStringMap(attrs pcommon.Map) map[string]string {
	out := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		out[k] = toString(v.AsRaw())
		return true
	})
	return out
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case int, int32, int64, uint, uint32, uint64, float32, float64:
		b, _ := json.Marshal(t)
		return string(b)
	case []byte:
		return hex.EncodeToString(t)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func traceIDHex(id pcommon.TraceID) string {
	var b [32]byte
	hex.Encode(b[:], id[:])
	return string(b[:])
}

func spanIDHex(id pcommon.SpanID) string {
	var b [16]byte
	hex.Encode(b[:], id[:])
	return string(b[:])
}

func convertEvents(events ptrace.SpanEventSlice) (times []time.Time, names []string, attrs []map[string]string) {
	for i := 0; i < events.Len(); i++ {
		e := events.At(i)
		times = append(times, e.Timestamp().AsTime())
		names = append(names, e.Name())
		attrs = append(attrs, attributesToStringMap(e.Attributes()))
	}
	return
}

func convertLinks(links ptrace.SpanLinkSlice) (traceIDs, spanIDs, states []string, attrs []map[string]string) {
	for i := 0; i < links.Len(); i++ {
		l := links.At(i)
		traceIDs = append(traceIDs, traceIDHex(l.TraceID()))
		spanIDs = append(spanIDs, spanIDHex(l.SpanID()))
		states = append(states, l.TraceState().AsRaw())
		attrs = append(attrs, attributesToStringMap(l.Attributes()))
	}
	return
}

func getServiceName(attrs pcommon.Map) string {
	if v, ok := attrs.Get("service.name"); ok {
		return v.AsString()
	}
	return ""
}

// processGaugeMetric processes gauge metrics according to ClickHouse gauge table structure
func processGaugeMetric(m pmetric.Metric, resMap map[string]string, resURL, scopeName, scopeVersion string, scopeMap map[string]string, scopeURL, serviceName string) ([]Message, int, error) {
	var out []Message
	count := 0
	gauge := m.Gauge()
	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dp := gauge.DataPoints().At(i)
		exemplars := convertExemplars(dp.Exemplars())
		obj := map[string]any{
			"ResourceAttributes":    resMap,
			"ResourceSchemaUrl":     resURL,
			"ScopeName":             scopeName,
			"ScopeVersion":          scopeVersion,
			"ScopeAttributes":       scopeMap,
			"ScopeDroppedAttrCount": dp.Attributes().Len(), // This is a simplification
			"ScopeSchemaUrl":        scopeURL,
			"ServiceName":           serviceName,
			"MetricName":            m.Name(),
			"MetricDescription":     m.Description(),
			"MetricUnit":            m.Unit(),
			"Attributes":            attributesToStringMap(dp.Attributes()),
			"StartTimeUnix":         dp.StartTimestamp().AsTime(),
			"TimeUnix":              dp.Timestamp().AsTime(),
			"Value":                 getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
			"Flags":                 uint32(dp.Flags()),
			"Exemplars": map[string]any{
				"FilteredAttributes": exemplars.Attrs,
				"TimeUnix":           exemplars.Times,
				"Value":              exemplars.Values,
				"SpanId":             exemplars.SpanIDs,
				"TraceId":            exemplars.TraceIDs,
			},
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Message{Value: b})
		count++
	}
	return out, count, nil
}

// processSumMetric processes sum metrics according to ClickHouse sum table structure
func processSumMetric(m pmetric.Metric, resMap map[string]string, resURL, scopeName, scopeVersion string, scopeMap map[string]string, scopeURL, serviceName string) ([]Message, int, error) {
	var out []Message
	count := 0
	sum := m.Sum()
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dp := sum.DataPoints().At(i)
		exemplars := convertExemplars(dp.Exemplars())
		obj := map[string]any{
			"ResourceAttributes":    resMap,
			"ResourceSchemaUrl":     resURL,
			"ScopeName":             scopeName,
			"ScopeVersion":          scopeVersion,
			"ScopeAttributes":       scopeMap,
			"ScopeDroppedAttrCount": dp.Attributes().Len(), // This is a simplification
			"ScopeSchemaUrl":        scopeURL,
			"ServiceName":           serviceName,
			"MetricName":            m.Name(),
			"MetricDescription":     m.Description(),
			"MetricUnit":            m.Unit(),
			"Attributes":            attributesToStringMap(dp.Attributes()),
			"StartTimeUnix":         dp.StartTimestamp().AsTime(),
			"TimeUnix":              dp.Timestamp().AsTime(),
			"Value":                 getValue(dp.IntValue(), dp.DoubleValue(), dp.ValueType()),
			"Flags":                 uint32(dp.Flags()),
			"Exemplars": map[string]any{
				"FilteredAttributes": exemplars.Attrs,
				"TimeUnix":           exemplars.Times,
				"Value":              exemplars.Values,
				"SpanId":             exemplars.SpanIDs,
				"TraceId":            exemplars.TraceIDs,
			},
			"AggregationTemporality": int32(sum.AggregationTemporality()),
			"IsMonotonic":            sum.IsMonotonic(),
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Message{Value: b})
		count++
	}
	return out, count, nil
}

// processHistogramMetric processes histogram metrics according to ClickHouse histogram table structure
func processHistogramMetric(m pmetric.Metric, resMap map[string]string, resURL, scopeName, scopeVersion string, scopeMap map[string]string, scopeURL, serviceName string) ([]Message, int, error) {
	var out []Message
	count := 0
	histogram := m.Histogram()
	for i := 0; i < histogram.DataPoints().Len(); i++ {
		dp := histogram.DataPoints().At(i)
		exemplars := convertExemplars(dp.Exemplars())
		obj := map[string]any{
			"ResourceAttributes":    resMap,
			"ResourceSchemaUrl":     resURL,
			"ScopeName":             scopeName,
			"ScopeVersion":          scopeVersion,
			"ScopeAttributes":       scopeMap,
			"ScopeDroppedAttrCount": dp.Attributes().Len(), // This is a simplification
			"ScopeSchemaUrl":        scopeURL,
			"ServiceName":           serviceName,
			"MetricName":            m.Name(),
			"MetricDescription":     m.Description(),
			"MetricUnit":            m.Unit(),
			"Attributes":            attributesToStringMap(dp.Attributes()),
			"StartTimeUnix":         dp.StartTimestamp().AsTime(),
			"TimeUnix":              dp.Timestamp().AsTime(),
			"Count":                 dp.Count(),
			"Sum":                   dp.Sum(),
			"BucketCounts":          dp.BucketCounts().AsRaw(),
			"ExplicitBounds":        dp.ExplicitBounds().AsRaw(),
			"Exemplars": map[string]any{
				"FilteredAttributes": exemplars.Attrs,
				"TimeUnix":           exemplars.Times,
				"Value":              exemplars.Values,
				"SpanId":             exemplars.SpanIDs,
				"TraceId":            exemplars.TraceIDs,
			},
			"Flags":                  uint32(dp.Flags()),
			"Min":                    dp.Min(),
			"Max":                    dp.Max(),
			"AggregationTemporality": int32(histogram.AggregationTemporality()),
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Message{Value: b})
		count++
	}
	return out, count, nil
}

// processExpHistogramMetric processes exponential histogram metrics according to ClickHouse exp histogram table structure
func processExpHistogramMetric(m pmetric.Metric, resMap map[string]string, resURL, scopeName, scopeVersion string, scopeMap map[string]string, scopeURL, serviceName string) ([]Message, int, error) {
	var out []Message
	count := 0
	expHistogram := m.ExponentialHistogram()
	for i := 0; i < expHistogram.DataPoints().Len(); i++ {
		dp := expHistogram.DataPoints().At(i)
		exemplars := convertExemplars(dp.Exemplars())
		obj := map[string]any{
			"ResourceAttributes":    resMap,
			"ResourceSchemaUrl":     resURL,
			"ScopeName":             scopeName,
			"ScopeVersion":          scopeVersion,
			"ScopeAttributes":       scopeMap,
			"ScopeDroppedAttrCount": dp.Attributes().Len(), // This is a simplification
			"ScopeSchemaUrl":        scopeURL,
			"ServiceName":           serviceName,
			"MetricName":            m.Name(),
			"MetricDescription":     m.Description(),
			"MetricUnit":            m.Unit(),
			"Attributes":            attributesToStringMap(dp.Attributes()),
			"StartTimeUnix":         dp.StartTimestamp().AsTime(),
			"TimeUnix":              dp.Timestamp().AsTime(),
			"Count":                 dp.Count(),
			"Sum":                   dp.Sum(),
			"Scale":                 dp.Scale(),
			"ZeroCount":             dp.ZeroCount(),
			"PositiveOffset":        dp.Positive().Offset(),
			"PositiveBucketCounts":  dp.Positive().BucketCounts().AsRaw(),
			"NegativeOffset":        dp.Negative().Offset(),
			"NegativeBucketCounts":  dp.Negative().BucketCounts().AsRaw(),
			"Exemplars": map[string]any{
				"FilteredAttributes": exemplars.Attrs,
				"TimeUnix":           exemplars.Times,
				"Value":              exemplars.Values,
				"SpanId":             exemplars.SpanIDs,
				"TraceId":            exemplars.TraceIDs,
			},
			"Flags":                  uint32(dp.Flags()),
			"Min":                    dp.Min(),
			"Max":                    dp.Max(),
			"AggregationTemporality": int32(expHistogram.AggregationTemporality()),
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Message{Value: b})
		count++
	}
	return out, count, nil
}

// processSummaryMetric processes summary metrics according to ClickHouse summary table structure
func processSummaryMetric(m pmetric.Metric, resMap map[string]string, resURL, scopeName, scopeVersion string, scopeMap map[string]string, scopeURL, serviceName string) ([]Message, int, error) {
	var out []Message
	count := 0
	summary := m.Summary()
	for i := 0; i < summary.DataPoints().Len(); i++ {
		dp := summary.DataPoints().At(i)
		quantiles := convertValueAtQuantiles(dp.QuantileValues())
		obj := map[string]any{
			"ResourceAttributes":    resMap,
			"ResourceSchemaUrl":     resURL,
			"ScopeName":             scopeName,
			"ScopeVersion":          scopeVersion,
			"ScopeAttributes":       scopeMap,
			"ScopeDroppedAttrCount": dp.Attributes().Len(), // This is a simplification
			"ScopeSchemaUrl":        scopeURL,
			"ServiceName":           serviceName,
			"MetricName":            m.Name(),
			"MetricDescription":     m.Description(),
			"MetricUnit":            m.Unit(),
			"Attributes":            attributesToStringMap(dp.Attributes()),
			"StartTimeUnix":         dp.StartTimestamp().AsTime(),
			"TimeUnix":              dp.Timestamp().AsTime(),
			"Count":                 dp.Count(),
			"Sum":                   dp.Sum(),
			"ValueAtQuantiles": map[string]any{
				"Quantile": quantiles.Quantiles,
				"Value":    quantiles.Values,
			},
			"Flags": uint32(dp.Flags()),
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Message{Value: b})
		count++
	}
	return out, count, nil
}

// ExemplarData holds converted exemplar data
type ExemplarData struct {
	Attrs    []map[string]string
	Times    []time.Time
	Values   []float64
	SpanIDs  []string
	TraceIDs []string
}

// convertExemplars converts exemplars to the format expected by ClickHouse
func convertExemplars(exemplars pmetric.ExemplarSlice) ExemplarData {
	var attrs []map[string]string
	var times []time.Time
	var values []float64
	var spanIDs []string
	var traceIDs []string

	for i := 0; i < exemplars.Len(); i++ {
		exemplar := exemplars.At(i)
		attrs = append(attrs, attributesToStringMap(exemplar.FilteredAttributes()))
		times = append(times, exemplar.Timestamp().AsTime())
		values = append(values, getExemplarValue(exemplar.IntValue(), exemplar.DoubleValue(), exemplar.ValueType()))

		traceID, spanID := exemplar.TraceID(), exemplar.SpanID()
		traceIDs = append(traceIDs, hex.EncodeToString(traceID[:]))
		spanIDs = append(spanIDs, hex.EncodeToString(spanID[:]))
	}
	return ExemplarData{Attrs: attrs, Times: times, Values: values, SpanIDs: spanIDs, TraceIDs: traceIDs}
}

// QuantileData holds converted quantile data
type QuantileData struct {
	Quantiles []float64
	Values    []float64
}

// convertValueAtQuantiles converts quantile values to the format expected by ClickHouse
func convertValueAtQuantiles(quantiles pmetric.SummaryDataPointValueAtQuantileSlice) QuantileData {
	var qs []float64
	var vs []float64
	for i := 0; i < quantiles.Len(); i++ {
		q := quantiles.At(i)
		qs = append(qs, q.Quantile())
		vs = append(vs, q.Value())
	}
	return QuantileData{Quantiles: qs, Values: vs}
}

// getValue extracts the numeric value from a data point, handling both int and double types
func getValue(intValue int64, floatValue float64, valueType pmetric.NumberDataPointValueType) float64 {
	switch valueType {
	case pmetric.NumberDataPointValueTypeDouble:
		return floatValue
	case pmetric.NumberDataPointValueTypeInt:
		return float64(intValue)
	case pmetric.NumberDataPointValueTypeEmpty:
		return 0.0
	default:
		return 0.0
	}
}

// getExemplarValue extracts the numeric value from an exemplar, handling both int and double types
func getExemplarValue(intValue int64, floatValue float64, valueType pmetric.ExemplarValueType) float64 {
	switch valueType {
	case pmetric.ExemplarValueTypeDouble:
		return floatValue
	case pmetric.ExemplarValueTypeInt:
		return float64(intValue)
	case pmetric.ExemplarValueTypeEmpty:
		return 0.0
	default:
		return 0.0
	}
}
