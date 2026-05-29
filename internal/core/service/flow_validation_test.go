package service

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/foden/cdc/internal/core/ports"
)

func TestValidationFindingsBlockOnlyFatalSeverity(t *testing.T) {
	findings := []ValidationFinding{
		{Code: "WARN_PK", Severity: ValidationSeverityWarning, Message: "missing optional key", Target: "source.table"},
	}
	if HasFatalFindings(findings) {
		t.Fatal("warning finding blocked validation")
	}

	findings = append(findings, ValidationFinding{Code: "BAD_FILTER", Severity: ValidationSeverityFatal, Message: "invalid CEL", Target: "filter"})
	if !HasFatalFindings(findings) {
		t.Fatal("fatal finding did not block validation")
	}
}

func TestDryRunFilterWithDebeziumAfterPayload(t *testing.T) {
	passed, findings := DryRunFilter(`after.status == "paid"`, []byte(`{"op":"c","before":null,"after":{"id":7,"status":"paid"},"source":{"schema":"public","table":"orders"}}`))

	if HasFatalFindings(findings) {
		t.Fatalf("findings = %+v", findings)
	}
	if !passed {
		t.Fatal("filter did not pass paid order")
	}
}

func TestDryRunFilterReportsInvalidCELAsFatal(t *testing.T) {
	passed, findings := DryRunFilter(`after.status ++ "paid"`, []byte(`{"after":{"status":"paid"}}`))

	if passed {
		t.Fatal("invalid filter passed")
	}
	if !HasFatalFindings(findings) {
		t.Fatalf("findings = %+v, want fatal finding", findings)
	}
}

func TestDryRunMappingChangesAfterOnly(t *testing.T) {
	out, findings := DryRunMapping([]byte(`{"op":"u","before":{"amount":10},"after":{"amount":12}}`), []ports.ColumnMapping{
		{SourceColumn: "amount", SinkColumn: "total_amount", Enabled: true},
	})
	if HasFatalFindings(findings) {
		t.Fatalf("findings = %+v", findings)
	}

	var payload struct {
		Before map[string]float64 `json:"before"`
		After  map[string]float64 `json:"after"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("unmarshal mapped payload: %v", err)
	}
	if payload.After["total_amount"] != 12 {
		t.Fatalf("after mapping = %+v", payload.After)
	}
	if payload.Before["amount"] != 10 {
		t.Fatalf("before was mutated: %+v", payload.Before)
	}
}

func TestDryRunMappingUsesBeforeForDelete(t *testing.T) {
	out, findings := DryRunMapping([]byte(`{"op":"d","before":{"amount":10},"after":null}`), []ports.ColumnMapping{
		{SourceColumn: "amount", SinkColumn: "total_amount", Enabled: true},
	})
	if HasFatalFindings(findings) {
		t.Fatalf("findings = %+v", findings)
	}

	var payload struct {
		Before map[string]float64 `json:"before"`
	}
	if err := json.Unmarshal(out, &payload); err != nil {
		t.Fatalf("unmarshal mapped payload: %v", err)
	}
	if payload.Before["total_amount"] != 10 {
		t.Fatalf("delete before mapping = %+v", payload.Before)
	}
}

func TestValidationErrorWrapsFatalFindings(t *testing.T) {
	err := ErrorIfFatal([]ValidationFinding{{Code: "BAD", Severity: ValidationSeverityFatal, Message: "bad"}})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrFlowValidationFatal) {
		t.Fatalf("err = %v, want ErrFlowValidationFatal", err)
	}
}
