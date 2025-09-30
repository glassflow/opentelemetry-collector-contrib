package formatter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestLogsToJSON_Golden(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("hello")

	msgs, n, err := LogsToJSON(ld)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(msgs) != 1 {
		t.Fatalf("expected 1 message, got n=%d len=%d", n, len(msgs))
	}
	var got map[string]any
	if err := json.Unmarshal(msgs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["Body"] != "hello" {
		t.Fatalf("expected Body=hello, got %v", got["Body"])
	}
}

func TestTracesToJSON_Golden(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	s := ss.Spans().AppendEmpty()
	s.SetName("op")

	msgs, n, err := TracesToJSON(td)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(msgs) != 1 {
		t.Fatalf("expected 1 message, got n=%d len=%d", n, len(msgs))
	}
	var got map[string]any
	if err := json.Unmarshal(msgs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["SpanName"] != "op" {
		t.Fatalf("expected SpanName=op, got %v", got["SpanName"])
	}
}

func TestMetricsToJSON_Golden(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("requests")
	m.SetEmptyGauge()
	pt := m.Gauge().DataPoints().AppendEmpty()
	pt.SetTimestamp(pcommon.Timestamp(1))

	msgs, n, err := MetricsToJSON(md)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(msgs) != 1 {
		t.Fatalf("expected 1 message, got n=%d len=%d", n, len(msgs))
	}
	var got map[string]any
	if err := json.Unmarshal(msgs[0].Value, &got); err != nil {
		t.Fatal(err)
	}
	if got["MetricName"] != "requests" {
		t.Fatalf("expected MetricName=requests, got %v", got["MetricName"])
	}
}

func TestMetricsToJSON_Gauge_Comprehensive(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	rm.Resource().Attributes().PutStr("host.name", "test-host")
	rm.SetSchemaUrl("resource-schema-url")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0")
	sm.Scope().Attributes().PutStr("scope.attr", "scope-val")
	sm.SetSchemaUrl("scope-schema-url")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test-gauge")
	m.SetDescription("A test gauge metric")
	m.SetUnit("1")
	gauge := m.SetEmptyGauge()
	dp := gauge.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	dp.SetDoubleValue(123.45)
	dp.SetFlags(1)
	dp.Attributes().PutStr("dp.attr", "dp-val")

	// Add an exemplar
	exemplar := dp.Exemplars().AppendEmpty()
	exemplar.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 500*1e6)))
	exemplar.SetDoubleValue(125.0)
	exemplar.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	exemplar.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	exemplar.FilteredAttributes().PutStr("exemplar.attr", "exemplar-val")

	msgs, n, err := MetricsToJSON(md)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)

	var actual map[string]any
	json.Unmarshal(msgs[0].Value, &actual)

	// Verify key fields are present
	assert.Equal(t, "test-gauge", actual["MetricName"])
	assert.Equal(t, "A test gauge metric", actual["MetricDescription"])
	assert.Equal(t, "1", actual["MetricUnit"])
	assert.Equal(t, "test-service", actual["ServiceName"])
	assert.Equal(t, "test-scope", actual["ScopeName"])
	assert.Equal(t, "1.0", actual["ScopeVersion"])
	assert.Equal(t, 123.45, actual["Value"])
	assert.Equal(t, float64(1), actual["Flags"])

	// Verify exemplars structure
	exemplars, ok := actual["Exemplars"].(map[string]any)
	assert.True(t, ok)
	assert.Len(t, exemplars["FilteredAttributes"], 1)
	assert.Len(t, exemplars["TimeUnix"], 1)
	assert.Len(t, exemplars["Value"], 1)
	assert.Len(t, exemplars["SpanId"], 1)
	assert.Len(t, exemplars["TraceId"], 1)
}

func TestMetricsToJSON_Sum_Comprehensive(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test-sum")
	m.SetDescription("A test sum metric")
	m.SetUnit("1")
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	dp.SetIntValue(100)
	dp.SetFlags(1)
	dp.Attributes().PutStr("dp.attr", "dp-val")

	msgs, n, err := MetricsToJSON(md)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)

	var actual map[string]any
	json.Unmarshal(msgs[0].Value, &actual)

	// Verify key fields are present
	assert.Equal(t, "test-sum", actual["MetricName"])
	assert.Equal(t, "A test sum metric", actual["MetricDescription"])
	assert.Equal(t, "1", actual["MetricUnit"])
	assert.Equal(t, "test-service", actual["ServiceName"])
	assert.Equal(t, 100.0, actual["Value"])
	assert.Equal(t, float64(2), actual["AggregationTemporality"])
	assert.Equal(t, true, actual["IsMonotonic"])
}

func TestMetricsToJSON_Histogram_Comprehensive(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test-histogram")
	m.SetDescription("A test histogram metric")
	m.SetUnit("1")
	histogram := m.SetEmptyHistogram()
	histogram.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := histogram.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetMin(1.0)
	dp.SetMax(20.0)
	dp.SetFlags(1)
	dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
	dp.ExplicitBounds().FromRaw([]float64{1.0, 2.0, 3.0})
	dp.Attributes().PutStr("dp.attr", "dp-val")

	msgs, n, err := MetricsToJSON(md)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)

	var actual map[string]any
	json.Unmarshal(msgs[0].Value, &actual)

	// Verify key fields are present
	assert.Equal(t, "test-histogram", actual["MetricName"])
	assert.Equal(t, "A test histogram metric", actual["MetricDescription"])
	assert.Equal(t, "1", actual["MetricUnit"])
	assert.Equal(t, "test-service", actual["ServiceName"])
	assert.Equal(t, float64(10), actual["Count"])
	assert.Equal(t, 100.0, actual["Sum"])
	assert.Equal(t, 1.0, actual["Min"])
	assert.Equal(t, 20.0, actual["Max"])
	assert.Equal(t, float64(2), actual["AggregationTemporality"])

	// Verify bucket data
	bucketCounts, ok := actual["BucketCounts"].([]any)
	assert.True(t, ok)
	assert.Len(t, bucketCounts, 4)
	assert.Equal(t, float64(1), bucketCounts[0])

	explicitBounds, ok := actual["ExplicitBounds"].([]any)
	assert.True(t, ok)
	assert.Len(t, explicitBounds, 3)
	assert.Equal(t, 1.0, explicitBounds[0])
}

func TestMetricsToJSON_Summary_Comprehensive(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test-summary")
	m.SetDescription("A test summary metric")
	m.SetUnit("1")
	summary := m.SetEmptySummary()
	dp := summary.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetFlags(1)
	dp.Attributes().PutStr("dp.attr", "dp-val")

	// Add quantile values
	q1 := dp.QuantileValues().AppendEmpty()
	q1.SetQuantile(0.5)
	q1.SetValue(50.0)
	q2 := dp.QuantileValues().AppendEmpty()
	q2.SetQuantile(0.99)
	q2.SetValue(95.0)

	msgs, n, err := MetricsToJSON(md)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)

	var actual map[string]any
	json.Unmarshal(msgs[0].Value, &actual)

	// Verify key fields are present
	assert.Equal(t, "test-summary", actual["MetricName"])
	assert.Equal(t, "A test summary metric", actual["MetricDescription"])
	assert.Equal(t, "1", actual["MetricUnit"])
	assert.Equal(t, "test-service", actual["ServiceName"])
	assert.Equal(t, float64(10), actual["Count"])
	assert.Equal(t, 100.0, actual["Sum"])

	// Verify quantile data
	quantiles, ok := actual["ValueAtQuantiles"].(map[string]any)
	assert.True(t, ok)

	quantileValues, ok := quantiles["Quantile"].([]any)
	assert.True(t, ok)
	assert.Len(t, quantileValues, 2)
	assert.Equal(t, 0.5, quantileValues[0])
	assert.Equal(t, 0.99, quantileValues[1])

	values, ok := quantiles["Value"].([]any)
	assert.True(t, ok)
	assert.Len(t, values, 2)
	assert.Equal(t, 50.0, values[0])
	assert.Equal(t, 95.0, values[1])
}

func TestMetricsToJSON_ExponentialHistogram_Comprehensive(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")
	sm.Scope().SetVersion("1.0")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test-exp-histogram")
	m.SetDescription("A test exp histogram metric")
	m.SetUnit("1")
	exp := m.SetEmptyExponentialHistogram()
	exp.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := exp.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(1, 0)))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 0)))
	dp.SetCount(10)
	dp.SetSum(100.0)
	dp.SetFlags(1)
	dp.SetScale(3)
	dp.SetZeroCount(2)
	dp.Positive().SetOffset(1)
	dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
	dp.Negative().SetOffset(-2)
	dp.Negative().BucketCounts().FromRaw([]uint64{4, 5})
	dp.SetMin(1.0)
	dp.SetMax(20.0)

	msgs, n, err := MetricsToJSON(md)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)

	var actual map[string]any
	json.Unmarshal(msgs[0].Value, &actual)

	assert.Equal(t, "test-exp-histogram", actual["MetricName"])
	assert.Equal(t, "A test exp histogram metric", actual["MetricDescription"])
	assert.Equal(t, float64(10), actual["Count"])
	assert.Equal(t, 100.0, actual["Sum"])
	assert.Equal(t, float64(3), actual["Scale"])
	assert.Equal(t, float64(2), actual["ZeroCount"])
	assert.Equal(t, float64(1), actual["PositiveOffset"])
	assert.Equal(t, float64(-2), actual["NegativeOffset"])
}
