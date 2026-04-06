package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSource(constant.SourceTypeREST.String(), func(cfg *config.SourceConfig) (interfaces.Source, error) {
		return New(cfg), nil
	})
}

// RESTSource periodically polls an HTTP endpoint for data.
type RESTSource struct {
	cfg    *config.SourceConfig
	client *http.Client
	stopCh chan struct{}
}

// New creates a new RESTSource instance.
func New(cfg *config.SourceConfig) *RESTSource {
	return &RESTSource{
		cfg:    cfg,
		client: &http.Client{Timeout: 30 * time.Second},
		stopCh: make(chan struct{}),
	}
}

// Start begins the polling loop.
func (s *RESTSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error {
	slog.Info("Starting REST source", "url", s.cfg.URL, "interval_ms", s.cfg.PollingIntervalMs)

	go s.pollLoop(pipeline, ackCh)
	return nil
}

func (s *RESTSource) pollLoop(pipeline chan<- *models.Event, ackCh <-chan uint64) {
	ticker := time.NewTicker(time.Duration(s.cfg.PollingIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Initial poll
	s.pollAndSend(pipeline)

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.pollAndSend(pipeline)
		case <-ackCh:
			// Ack requested but we don't have a persistent cursor in this simple version
		}
	}
}

func (s *RESTSource) pollAndSend(pipeline chan<- *models.Event) {
	events, err := s.poll()
	if err != nil {
		slog.Error("REST poll failed", "error", err, "url", s.cfg.URL)
		return
	}
	for _, e := range events {
		pipeline <- e
	}
}

func (s *RESTSource) poll() ([]*models.Event, error) {
	req, err := http.NewRequest(http.MethodGet, s.cfg.URL, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range s.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// Try to decode as array of objects
	var items []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		// Fallback: maybe it's just one object
		return nil, err
	}

	events := make([]*models.Event, 0, len(items))
	now := time.Now().UnixMilli()
	for _, item := range items {
		events = append(events, &models.Event{
			Op:         constant.CreateAction.String(),
			InstanceID: s.cfg.InstanceID,
			Database:   "api",
			Table:      s.cfg.Name,
			After:      item,
			Timestamp:  now,
			Topic:      s.cfg.Topic,
		})
	}
	return events, nil
}

func (s *RESTSource) Stop() error {
	close(s.stopCh)
	return nil
}

func (s *RESTSource) InstanceID() string {
	return s.cfg.InstanceID
}
