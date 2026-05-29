package postgres

import (
	"testing"

	"github.com/foden/cdc/internal/core/ports"
	coreruntime "github.com/foden/cdc/internal/core/runtime"
	"github.com/jackc/pglogrepl"
)

func TestParseOidPreservesNumericPrecision(t *testing.T) {
	p := &PostgresSource{}
	got := p.parseOid([]byte("12345678901234567890.1234567890"), 1700)
	if got != "12345678901234567890.1234567890" {
		t.Fatalf("parseOid(numeric) = %#v, want exact string", got)
	}
}

func TestParseOidFallsBackToStringOnInvalidNumbers(t *testing.T) {
	p := &PostgresSource{}

	tests := []struct {
		name string
		oid  uint32
		val  string
	}{
		{name: "int overflow", oid: 20, val: "9223372036854775808"},
		{name: "invalid int", oid: 23, val: "not-an-int"},
		{name: "invalid float", oid: 701, val: "not-a-float"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := p.parseOid([]byte(tt.val), tt.oid); got != tt.val {
				t.Fatalf("parseOid(%q, %d) = %#v, want %q", tt.val, tt.oid, got, tt.val)
			}
		})
	}
}

func TestDispatchUpdateAndDeleteWithoutOldTupleDoesNotPanic(t *testing.T) {
	reg := coreruntime.NewRegistry()
	if err := reg.RegisterFlow(&ports.FlowConfig{
		FlowID:      "flow-1",
		SourceID:    "source-1",
		SinkID:      "sink-1",
		SourceTable: "public.orders",
		SinkTable:   "public.orders",
	}); err != nil {
		t.Fatalf("RegisterFlow failed: %v", err)
	}

	p := &PostgresSource{
		cfg:             &ports.SourceConfig{InstanceID: "source-1"},
		relations:       make(map[uint32]*pglogrepl.RelationMessage),
		runtimeRegistry: reg,
		taskChan:        make(chan *walTask, 2),
	}
	p.relations[42] = &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "orders",
	}

	assertNotPanics := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s panicked: %v", name, r)
			}
		}()
		fn()
	}

	assertNotPanics("update without old tuple", func() {
		p.dispatchToWorkers(&pglogrepl.UpdateMessage{
			RelationID: 42,
			NewTuple:   &pglogrepl.TupleData{},
		}, 100)
	})

	assertNotPanics("delete without old tuple", func() {
		p.dispatchToWorkers(&pglogrepl.DeleteMessage{
			RelationID: 42,
		}, 101)
	})
}

func TestStandbyFlushPositionDoesNotAdvanceWithoutDurableLSN(t *testing.T) {
	if got := standbyFlushPosition(0, 500); got != 0 {
		t.Fatalf("standbyFlushPosition(0, 500) = %d, want 0", got)
	}
	if got := standbyFlushPosition(250, 500); got != 250 {
		t.Fatalf("standbyFlushPosition(250, 500) = %d, want 250", got)
	}
}
