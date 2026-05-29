package containers

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const prometheusPort = "9090/tcp"

type RunningPrometheus struct {
	Host    string
	Port    int
	URL     string
	Cleanup func(context.Context) error
}

func StartPrometheus(ctx context.Context, t testing.TB, scrapePort int) *RunningPrometheus {
	t.Helper()

	config := fmt.Sprintf(`global:
  scrape_interval: 1s
  evaluation_interval: 1s
scrape_configs:
  - job_name: cdc-test
    static_configs:
      - targets: ['host.testcontainers.internal:%d']
`, scrapePort)

	req := testcontainers.ContainerRequest{
		Image:           "prom/prometheus:v2.55.1",
		ExposedPorts:    []string{prometheusPort},
		HostAccessPorts: []int{scrapePort},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            bytes.NewReader([]byte(config)),
				ContainerFilePath: "/etc/prometheus/prometheus.yml",
				FileMode:          0o644,
			},
		},
		Cmd: []string{
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.console.libraries=/usr/share/prometheus/console_libraries",
			"--web.console.templates=/usr/share/prometheus/consoles",
		},
		WaitingFor: wait.ForHTTP("/-/ready").WithPort(prometheusPort),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	requireContainer(t, err)

	host, err := container.Host(ctx)
	requireContainer(t, err)
	mappedPort, err := container.MappedPort(ctx, prometheusPort)
	requireContainer(t, err)

	return &RunningPrometheus{
		Host:    host,
		Port:    int(mappedPort.Num()),
		URL:     fmt.Sprintf("http://%s", endpoint(host, mappedPort.Port())),
		Cleanup: cleanupFor(container),
	}
}
