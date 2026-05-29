package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type RunningClickHouse struct {
	Host     string
	Native   int
	HTTP     int
	User     string
	Password string
	Database string
	DSN      string
	Cleanup  func(context.Context) error
}

func StartClickHouse(ctx context.Context, t *testing.T) *RunningClickHouse {
	t.Helper()

	database := "cdc_test"
	req := testcontainers.ContainerRequest{
		Image:        "clickhouse/clickhouse-server:24.8-alpine",
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_DB": database,
		},
		WaitingFor: wait.ForListeningPort("9000/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	nativePort, err := container.MappedPort(ctx, "9000/tcp")
	requireContainer(t, err)
	httpPort, err := container.MappedPort(ctx, "8123/tcp")
	requireContainer(t, err)

	return &RunningClickHouse{
		Host:     host,
		Native:   int(nativePort.Num()),
		HTTP:     int(httpPort.Num()),
		User:     "default",
		Password: "",
		Database: database,
		DSN:      fmt.Sprintf("clickhouse://%s/%s", endpoint(host, nativePort.Port()), database),
		Cleanup:  cleanupFor(container),
	}
}
