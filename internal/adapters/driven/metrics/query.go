package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultQueryWindow  = "5m"
	defaultQueryTimeout = 1500 * time.Millisecond
	prometheusStatusOK  = "success"
)

type PrometheusClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewPrometheusClient(baseURL string) *PrometheusClient {
	return &PrometheusClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: defaultQueryTimeout,
		},
	}
}

func (c *PrometheusClient) FlowProcessingLatencyP99(ctx context.Context, window string) (float64, error) {
	if c == nil || c.baseURL == "" {
		return 0, nil
	}
	if window == "" {
		window = DefaultQueryWindow
	}
	query := fmt.Sprintf(
		`histogram_quantile(0.99, sum by (le) (rate(cdc_flow_processing_duration_seconds_bucket[%s])))`,
		window,
	)
	value, err := c.queryScalar(ctx, query)
	if err != nil {
		return 0, err
	}
	return value * 1000, nil
}

func (c *PrometheusClient) queryScalar(ctx context.Context, query string) (float64, error) {
	endpoint, err := url.Parse(c.baseURL + "/api/v1/query")
	if err != nil {
		return 0, err
	}
	q := endpoint.Query()
	q.Set("query", query)
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("prometheus query failed: %s", resp.Status)
	}

	var result queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	if result.Status != prometheusStatusOK {
		return 0, fmt.Errorf("prometheus query status: %s", result.Status)
	}
	if result.Data.ResultType != "vector" || len(result.Data.Result) == 0 {
		return 0, nil
	}
	values := result.Data.Result[0].Value
	if len(values) < 2 {
		return 0, nil
	}
	text, ok := values[1].(string)
	if !ok {
		return 0, nil
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value []any `json:"value"`
		} `json:"result"`
	} `json:"data"`
}
