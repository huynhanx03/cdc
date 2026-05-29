package containers

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func cleanupFor(container testcontainers.Container) func(context.Context) error {
	return func(ctx context.Context) error {
		return testcontainers.TerminateContainer(container, testcontainers.StopContext(ctx))
	}
}

func requireContainer(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		return
	}
	t.Skipf("docker-backed test container unavailable: %v", err)
}

func endpoint(host string, port string) string {
	return fmt.Sprintf("%s:%s", host, port)
}
