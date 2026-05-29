package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type RunningMySQL struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	DSN      string
	Cleanup  func(context.Context) error
}

func StartMySQL(ctx context.Context, t *testing.T) *RunningMySQL {
	t.Helper()

	user := "cdc"
	password := "cdc"
	database := "cdc_test"
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.4",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": password,
			"MYSQL_USER":          user,
			"MYSQL_PASSWORD":      password,
			"MYSQL_DATABASE":      database,
		},
		Cmd: []string{
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
		},
		WaitingFor: wait.ForListeningPort("3306/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	mappedPort, err := container.MappedPort(ctx, "3306/tcp")
	requireContainer(t, err)

	return &RunningMySQL{
		Host:     host,
		Port:     int(mappedPort.Num()),
		User:     user,
		Password: password,
		Database: database,
		DSN:      fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", user, password, endpoint(host, mappedPort.Port()), database),
		Cleanup:  cleanupFor(container),
	}
}
