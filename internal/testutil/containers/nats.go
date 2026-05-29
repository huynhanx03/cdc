package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type RunningNATS struct {
	Host    string
	Port    int
	URL     string
	Cleanup func(context.Context) error
}

func StartNATS(ctx context.Context, t testing.TB) *RunningNATS {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "nats:2.10-alpine",
		Cmd:          []string{"-js"},
		ExposedPorts: []string{"4222/tcp"},
		WaitingFor:   wait.ForListeningPort("4222/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	mappedPort, err := container.MappedPort(ctx, "4222/tcp")
	requireContainer(t, err)

	return &RunningNATS{
		Host:    host,
		Port:    int(mappedPort.Num()),
		URL:     fmt.Sprintf("nats://%s", endpoint(host, mappedPort.Port())),
		Cleanup: cleanupFor(container),
	}
}
