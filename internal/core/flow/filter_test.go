package flow

import (
	"encoding/json"
	"testing"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
)

func TestNewFilter_EmptyExpression(t *testing.T) {
	f, err := NewFilter("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f == nil {
		t.Fatal("expected non-nil filter")
	}
	if f.program != nil {
		t.Error("expected nil CEL program for empty expression")
	}
}

func TestNewFilter_WhitespaceExpression(t *testing.T) {
	f, err := NewFilter("   ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.program != nil {
		t.Error("expected nil CEL program for whitespace-only expression")
	}
}

func TestNewFilter_InvalidSyntax(t *testing.T) {
	cases := []string{
		"== c",
		"!= c",
	}
	for _, expr := range cases {
		_, err := NewFilter(expr)
		if err == nil {
			t.Errorf("expected error for expression %q, got nil", expr)
		}
	}
}

func TestNewFilter_UnsupportedField(t *testing.T) {
	_, err := NewFilter("unknown_field == value")
	if err == nil {
		t.Fatal("expected error for unsupported field")
	}
}

func TestFilter_Expression(t *testing.T) {
	f, _ := NewFilter(`data.op == "c"`)
	if f.Expression() != `data.op == "c"` {
		t.Errorf("Expression() = %q, want %q", f.Expression(), `data.op == "c"`)
	}

	var nilFilter *Filter
	if nilFilter.Expression() != "" {
		t.Error("nil filter Expression() should return empty string")
	}
}

func TestNewFilter_CELExpression(t *testing.T) {
	f, err := NewFilter(`data.status == "active"`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f == nil {
		t.Fatal("expected non-nil filter")
	}
	if f.program == nil {
		t.Error("expected non-nil CEL program")
	}
}

func TestNewFilter_CELInvalidExpression(t *testing.T) {
	_, err := NewFilter(`data.status ++ "active"`)
	if err == nil {
		t.Fatal("expected error for invalid CEL expression")
	}
}

func TestFilter_Evaluate_PassAll(t *testing.T) {
	f, _ := NewFilter("")
	data := []byte(`{"status": "active"}`)
	got, err := f.Evaluate(filterEvent(data))
	if err != nil || !got {
		t.Error("empty filter should pass all events")
	}
}

func TestFilter_Evaluate_NilFilter(t *testing.T) {
	var f *Filter
	data := []byte(`{"status": "active"}`)
	got, err := f.Evaluate(filterEvent(data))
	if err != nil || !got {
		t.Error("nil filter should pass all events")
	}
}

func TestFilter_Evaluate_NilData(t *testing.T) {
	f, _ := NewFilter(`data.status == "active"`)
	got, err := f.Evaluate(nil)
	if err == nil || got {
		t.Error("nil data should not pass filter")
	}
}

func TestFilter_Evaluate_EmptyData(t *testing.T) {
	f, _ := NewFilter(`data.status == "active"`)
	got, err := f.Evaluate(filterEvent([]byte{}))
	if err == nil || got {
		t.Error("empty data should not pass filter")
	}
}

func TestFilter_Evaluate_StringEquality(t *testing.T) {
	f, _ := NewFilter(`data.status == "active"`)

	tests := []struct {
		data string
		want bool
	}{
		{`{"status": "active"}`, true},
		{`{"status": "inactive"}`, false},
		{`{"status": ""}`, false},
		{`{"other_field": "active"}`, false},
	}

	for _, tc := range tests {
		got, err := f.Evaluate(filterEvent([]byte(tc.data)))
		if err != nil {
			t.Fatalf("Evaluate(%s) err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Errorf("Evaluate(%s) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestFilter_Evaluate_NumericComparison(t *testing.T) {
	f, _ := NewFilter(`data.amount > 100`)

	tests := []struct {
		data string
		want bool
	}{
		{`{"amount": 150}`, true},
		{`{"amount": 100}`, false},
		{`{"amount": 50}`, false},
		{`{"amount": 101}`, true},
	}

	for _, tc := range tests {
		got, err := f.Evaluate(filterEvent([]byte(tc.data)))
		if err != nil {
			t.Fatalf("Evaluate(%s) err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Errorf("Evaluate(%s) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestFilter_Evaluate_BooleanExpression(t *testing.T) {
	f, _ := NewFilter(`data.active == true`)

	tests := []struct {
		data string
		want bool
	}{
		{`{"active": true}`, true},
		{`{"active": false}`, false},
	}

	for _, tc := range tests {
		got, err := f.Evaluate(filterEvent([]byte(tc.data)))
		if err != nil {
			t.Fatalf("Evaluate(%s) err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Errorf("Evaluate(%s) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestFilter_Evaluate_ComplexExpression(t *testing.T) {
	f, _ := NewFilter(`data.status == "active" && data.amount > 50`)

	tests := []struct {
		data string
		want bool
	}{
		{`{"status": "active", "amount": 100}`, true},
		{`{"status": "active", "amount": 30}`, false},
		{`{"status": "inactive", "amount": 100}`, false},
		{`{"status": "inactive", "amount": 30}`, false},
	}

	for _, tc := range tests {
		got, err := f.Evaluate(filterEvent([]byte(tc.data)))
		if err != nil {
			t.Fatalf("Evaluate(%s) err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Errorf("Evaluate(%s) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestFilter_Evaluate_InvalidJSON(t *testing.T) {
	f, _ := NewFilter(`data.status == "active"`)
	got, err := f.Evaluate(filterEvent([]byte(`not json`)))
	if err == nil || got {
		t.Error("invalid JSON should not pass filter")
	}
}

func TestFilter_Evaluate_HasField(t *testing.T) {
	f, _ := NewFilter(`has(data.email)`)

	tests := []struct {
		data string
		want bool
	}{
		{`{"email": "test@example.com"}`, true},
		{`{"name": "test"}`, false},
	}

	for _, tc := range tests {
		got, err := f.Evaluate(filterEvent([]byte(tc.data)))
		if err != nil {
			t.Fatalf("Evaluate(%s) err = %v", tc.data, err)
		}
		if got != tc.want {
			t.Errorf("Evaluate(%s) = %v, want %v", tc.data, got, tc.want)
		}
	}
}

func TestFilter_Evaluate_WithEventData(t *testing.T) {
	f, _ := NewFilter(`data.status == "active"`)

	payload := map[string]interface{}{
		"id":     1,
		"status": "active",
		"name":   "test",
	}
	data, _ := json.Marshal(payload)

	got, err := f.Evaluate(filterEvent(data))
	if err != nil || !got {
		t.Error("expected event with active status to pass filter")
	}
}

func TestFilter_Evaluate_EnvelopeVariables(t *testing.T) {
	f, err := NewFilter(`op == "c" && schema == "public" && table == "users" && after.id == 7`)
	if err != nil {
		t.Fatalf("NewFilter err = %v", err)
	}
	got, err := f.Evaluate(&domain.Event{
		Schema: "public",
		Table:  "users",
		Op:     constant.OpCreate,
		Data:   []byte(`{"op":"c","before":null,"after":{"id":7},"source":{"schema":"public","table":"users"}}`),
	})
	if err != nil {
		t.Fatalf("Evaluate err = %v", err)
	}
	if !got {
		t.Fatal("envelope variables did not pass")
	}
}

func filterEvent(data []byte) *domain.Event {
	return &domain.Event{Schema: "public", Table: "users", Op: constant.OpCreate, Data: data}
}
