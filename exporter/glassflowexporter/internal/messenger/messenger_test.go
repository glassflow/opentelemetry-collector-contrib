package messenger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestPartitioning_Traces_ByTraceID(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	sp.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	msgs, n, err := Traces(td, PartitionByTraceID)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)
	assert.Equal(t, []byte("0102030405060708090a0b0c0d0e0f10"), msgs[0].Key)
}

func TestPartitioning_Logs_ByService(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svcA")
	sl := rl.ScopeLogs().AppendEmpty()
	_ = sl.LogRecords().AppendEmpty()

	msgs, n, err := Logs(ld, PartitionByService)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)
	assert.Equal(t, []byte("svcA"), msgs[0].Key)
}

func TestPartitioning_Metrics_ByService(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svcB")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("m")
	m.SetEmptyGauge().DataPoints().AppendEmpty()

	msgs, n, err := Metrics(md, PartitionByService)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Len(t, msgs, 1)
	assert.Equal(t, []byte("svcB"), msgs[0].Key)
}
