//go:build integration

package integration

import "testing"

func TestFlowValidationAPIQualityGate(t *testing.T) {
	t.Skip("pending API implementation: ValidateFlow/PreviewFlowSchema/DryRunFilter/DryRunMapping endpoints are not exposed yet")
}
