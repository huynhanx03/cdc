package containers

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

type RunningElasticsearch struct {
	Host    string
	Port    int
	URL     string
	Cleanup func(context.Context) error
}

func StartElasticsearch(ctx context.Context, t *testing.T) *RunningElasticsearch {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "docker.elastic.co/elasticsearch/elasticsearch:8.15.5",
		ExposedPorts: []string{"9200/tcp"},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"xpack.security.enabled": "false",
			"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
		},
		WaitingFor: wait.ForHTTP("/").WithPort("9200/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	mappedPort, err := container.MappedPort(ctx, "9200/tcp")
	requireContainer(t, err)

	return &RunningElasticsearch{
		Host:    host,
		Port:    int(mappedPort.Num()),
		URL:     fmt.Sprintf("http://%s", endpoint(host, mappedPort.Port())),
		Cleanup: cleanupFor(container),
	}
}
