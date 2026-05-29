package pipeline

import (
	"testing"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	coreflow "github.com/foden/cdc/internal/core/flow"
	"github.com/foden/cdc/internal/core/ports"
)

var (
	benchBool bool
	benchErr  error
)

func BenchmarkTransformCELFilterPass(b *testing.B) {
	filter, err := coreflow.NewFilter(`after.status == "paid" && schema == "public" && table == "orders"`)
	if err != nil {
		b.Fatal(err)
	}
	event := benchmarkEvent([]byte(postgresDebeziumPayload), constant.OpUpdate, "orders")
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchBool, benchErr = filter.Evaluate(event)
		if benchErr != nil {
			b.Fatal(benchErr)
		}
	}
}

func BenchmarkTransformCELFilterFail(b *testing.B) {
	filter, err := coreflow.NewFilter(`after.status == "cancelled"`)
	if err != nil {
		b.Fatal(err)
	}
	event := benchmarkEvent([]byte(postgresDebeziumPayload), constant.OpUpdate, "orders")
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		benchBool, benchErr = filter.Evaluate(event)
		if benchErr != nil {
			b.Fatal(benchErr)
		}
	}
}

func BenchmarkTransformMappingAfter(b *testing.B) {
	payload := []byte(postgresDebeziumPayload)
	mappings := []ports.ColumnMapping{
		{SourceColumn: "amount", SinkColumn: "total_amount", Enabled: true},
		{SourceColumn: "status", SinkColumn: "payment_status", Enabled: true},
		{SourceColumn: "updated_at", SinkColumn: "updated_at", Enabled: true},
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		benchPayloadBytes, benchErr = coreflow.ApplyColumnMappings(payload, mappings)
		if benchErr != nil {
			b.Fatal(benchErr)
		}
	}
}

func BenchmarkTransformMappingDeleteBefore(b *testing.B) {
	payload := []byte(`{"op":"d","before":{"id":1001,"status":"paid","amount":"123.45"},"after":null,"source":{"schema":"public","table":"orders"}}`)
	mappings := []ports.ColumnMapping{
		{SourceColumn: "amount", SinkColumn: "total_amount", Enabled: true},
		{SourceColumn: "status", SinkColumn: "payment_status", Enabled: true},
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		benchPayloadBytes, benchErr = coreflow.ApplyColumnMappings(payload, mappings)
		if benchErr != nil {
			b.Fatal(benchErr)
		}
	}
}

func benchmarkEvent(payload []byte, op constant.Op, table string) *domain.Event {
	return &domain.Event{
		InstanceID:  "src",
		Schema:      "public",
		Table:       table,
		Op:          op,
		Offset:      "42",
		LSN:         42,
		TimestampMS: 1_779_966_002_000,
		Data:        payload,
		Partition:   0,
	}
}
