package postgres

import "testing"

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
