package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSink(constant.SinkTypeWebhook.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg), nil
	})
}

// WebhookSink pushes CDC events to an HTTP endpoint.
type WebhookSink struct {
	cfg    *config.SinkConfig
	client *http.Client
}

// New creates a new WebhookSink instance.
func New(cfg *config.SinkConfig) *WebhookSink {
	return &WebhookSink{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Write sends a single CDC event to the webhook URL.
func (s *WebhookSink) Write(event *models.Event) error {
	if len(s.cfg.URL) == 0 {
		return fmt.Errorf("no webhook URL configured")
	}

	url := s.cfg.URL[0]
	
	// Prepare payload
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event failed: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if s.cfg.APIKey != "" {
		req.Header.Set("X-API-Key", s.cfg.APIKey)
	}

	// Add custom headers
	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned non-2xx status: %d", resp.StatusCode)
	}

	return nil
}

func (s *WebhookSink) Flush() error {
	return nil
}

func (s *WebhookSink) Close() error {
	return nil
}

func (s *WebhookSink) InstanceID() string {
	return s.cfg.InstanceID
}

func (s *WebhookSink) Type() string {
	return constant.SinkTypeWebhook.String()
}

func (s *WebhookSink) Topic() string {
	return s.cfg.Topic
}

func (s *WebhookSink) MaxRetries() int32 {
	return s.cfg.MaxRetries
}
