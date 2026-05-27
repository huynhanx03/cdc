package drivergrpc

import (
	"fmt"
	"testing"

	cdcerrors "github.com/foden/cdc/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInvalidArgumentIfRequiredMapsValidationError(t *testing.T) {
	err := invalidArgumentIfRequired(fmt.Errorf("%w: invalid filter expression", cdcerrors.ErrValidation))
	if err == nil {
		t.Fatal("expected grpc error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}
