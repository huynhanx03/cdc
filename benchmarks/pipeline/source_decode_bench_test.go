package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/foden/cdc/internal/core/domain"
)

var (
	benchPayloadBytes []byte
	benchPayload      domain.DebeziumPayload
	benchPayloadMap   map[string]any
)

func BenchmarkSourceDecodePostgresDebezium(b *testing.B) {
	payload := []byte(postgresDebeziumPayload)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		var decoded domain.DebeziumPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		benchPayload = decoded
	}
}

func BenchmarkSourceDecodeMySQLBinlog(b *testing.B) {
	payload := []byte(mysqlBinlogPayload)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		benchPayloadMap = decoded
	}
}

func BenchmarkSourceDecodeHeavyRow(b *testing.B) {
	payload := []byte(heavyRowPayload)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	for i := 0; i < b.N; i++ {
		var decoded map[string]any
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		benchPayloadMap = decoded
	}
}

const postgresDebeziumPayload = `{
  "op":"u",
  "before":{"id":1001,"status":"pending","amount":"12345678901234567890.1234567890","updated_at":"2026-05-28T10:00:00Z"},
  "after":{"id":1001,"status":"paid","amount":"12345678901234567890.1234567890","updated_at":"2026-05-28T10:00:02Z","metadata":{"channel":"card","risk":false}},
  "source":{"version":"3.0","connector":"postgresql","name":"src","db":"app","schema":"public","table":"orders","lsn":92837465,"txId":991},
  "ts_ms":1779966002000
}`

const mysqlBinlogPayload = `{
  "op":"c",
  "before":null,
  "after":{"id":2002,"email":"buyer@example.com","tier":"gold","created_at":"2026-05-28T10:05:00Z"},
  "source":{"version":"3.0","connector":"mysql","name":"mysql-src","db":"commerce","table":"customers","file":"mysql-bin.000044","pos":928374},
  "ts_ms":1779966300000
}`

const heavyRowPayload = `{
  "op":"u",
  "before":{"id":3003,"amount":"99999999999999999999.9999999999","active":true,"tags":["a","b","c"],"profile":{"locale":"vi-VN","score":91.7}},
  "after":{"id":3003,"amount":"100000000000000000000.0000000000","active":false,"tags":["a","b","d"],"profile":{"locale":"vi-VN","score":93.2},"payload":{"nested":{"json":{"with":"depth"}}}},
  "source":{"version":"3.0","connector":"postgresql","name":"src","db":"app","schema":"public","table":"heavy_rows","lsn":92837466},
  "ts_ms":1779966400000
}`
