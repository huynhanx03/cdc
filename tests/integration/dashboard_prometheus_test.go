//go:build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	metricquery "github.com/foden/cdc/internal/adapters/driven/metrics"
	"github.com/foden/cdc/internal/testutil/assertcdc"
	testcontainers "github.com/foden/cdc/internal/testutil/containers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestDashboardPrometheusQueryReadsScrapedFlowLatency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	registry := prometheus.NewRegistry()
	flowDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cdc_flow_processing_duration_seconds",
		Help:    "CDC test flow processing latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"flow_id"})
	registry.MustRegister(flowDuration)

	server := httptest.NewServer(promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	defer server.Close()

	scrapePort := serverPort(t, server.URL)
	prometheusContainer := testcontainers.StartPrometheus(ctx, t, scrapePort)
	defer func() { _ = prometheusContainer.Cleanup(context.Background()) }()

	observeCtx, stopObserving := context.WithCancel(ctx)
	defer stopObserving()
	go observeFlowDurations(observeCtx, flowDuration)

	client := metricquery.NewPrometheusClient(prometheusContainer.URL)
	assertcdc.Eventually(t, 45*time.Second, time.Second, func() error {
		p99, err := client.FlowProcessingLatencyP99(ctx, "30s")
		if err != nil {
			return err
		}
		if p99 <= 0 {
			return fmt.Errorf("p99 latency is not available yet")
		}
		return nil
	})
}

func serverPort(t testing.TB, rawURL string) int {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse server url %q: %v", rawURL, err)
	}
	_, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split server host %q: %v", parsed.Host, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse server port %q: %v", portText, err)
	}
	return port
}

func observeFlowDurations(ctx context.Context, histogram *prometheus.HistogramVec) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		histogram.WithLabelValues("flow-prometheus").Observe(0.12)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
