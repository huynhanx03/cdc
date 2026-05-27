package domain

import "testing"

func TestCheckpointValidatesSourcePosition(t *testing.T) {
	cp := Checkpoint{FlowID: "flow-1", SourceID: "src-1", Position: "16/B374D848"}
	if cp.FlowID == "" || cp.SourceID == "" || cp.Position == "" {
		t.Fatalf("checkpoint missing required fields: %+v", cp)
	}
}
