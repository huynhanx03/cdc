package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type RunningPostgres struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	DSN      string
	Cleanup  func(context.Context) error
}

func StartPostgres(ctx context.Context, t *testing.T) *RunningPostgres {
	t.Helper()

	user := "cdc"
	password := "cdc"
	database := "cdc_test"
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     user,
			"POSTGRES_PASSWORD": password,
			"POSTGRES_DB":       database,
		},
		Cmd: []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=8",
			"-c", "max_wal_senders=8",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	mappedPort, err := container.MappedPort(ctx, "5432/tcp")
	requireContainer(t, err)

	return &RunningPostgres{
		Host:     host,
		Port:     int(mappedPort.Num()),
		User:     user,
		Password: password,
		Database: database,
		DSN:      fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, endpoint(host, mappedPort.Port()), database),
		Cleanup:  cleanupFor(container),
	}
}
