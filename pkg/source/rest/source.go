package rest

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

// restTask represents a single item from a polled REST response
type restTask struct {
	item json.RawMessage
	ts   int64
	lsn  uint64
}

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

	// Single worker for strict sequence
	taskChan chan *restTask
	wg       sync.WaitGroup
}

// New creates a new RESTSource instance.
func New(cfg *config.SourceConfig) *RESTSource {
	return &RESTSource{
		cfg:      cfg,
		client:   &http.Client{Timeout: 30 * time.Second},
		stopCh:   make(chan struct{}),
		taskChan: make(chan *restTask, 4096),
	}
}

// Start begins the polling loop.
func (s *RESTSource) Start(pipeline chan<- *models.Event, ackCh <-chan uint64, initialOffset string) error {
	slog.Info("Starting REST source", "url", s.cfg.URL, "interval_ms", s.cfg.PollingIntervalMs)

	// Start exactly ONE worker
	s.wg.Add(1)
	go s.singleOrderedWorker(pipeline)

	go s.pollLoop(ackCh)
	return nil
}

func (s *RESTSource) pollLoop(ackCh <-chan uint64) {
	ticker := time.NewTicker(time.Duration(s.cfg.PollingIntervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Initial poll
	s.pollAndSend()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.pollAndSend()
		case <-ackCh:
			// Ack requested but we don't have a persistent cursor in this simple version
		}
	}
}

func (s *RESTSource) pollAndSend() {
	items, err := s.poll()
	if err != nil {
		slog.Error("REST poll failed", "error", err, "url", s.cfg.URL)
		return
	}

	now := time.Now().UnixMilli()
	for _, item := range items {
		// Mock LSN using current timestamp to maintain rough ordering
		s.taskChan <- &restTask{
			item: item,
			ts:   now,
			lsn:  uint64(now),
		}
	}
}

func (s *RESTSource) singleOrderedWorker(pipeline chan<- *models.Event) {
	defer s.wg.Done()
	for task := range s.taskChan {
		s.processTask(pipeline, task)
	}
}

func (s *RESTSource) processTask(pipeline chan<- *models.Event, t *restTask) {
	payload := models.DebeziumPayload{
		Op:    "c", // Assume create for polling
		After: t.item,
		Source: models.SourceMetadata{
			Version:   "1.0",
			Connector: "rest",
			Name:      s.cfg.InstanceID,
			TsMs:      t.ts,
			Snapshot:  "false",
			DB:        "api",
			Schema:    "public",
			Table:     s.cfg.Name,
			LSN:       t.lsn,
		},
		TimestampMS: t.ts,
	}

	data, _ := sonic.Marshal(payload)

	topic := s.cfg.Topic
	if topic == "" {
		topic = "cdc"
	}

	subject := fmt.Sprintf("%s.%s.%s.%s", topic, s.cfg.InstanceID, "public", s.cfg.Name)
	pipeline <- models.NewEvent(
		topic,
		subject,
		s.cfg.InstanceID,
		"public",
		s.cfg.Name,
		"c",
		t.lsn,
		fmt.Sprintf("%d", t.lsn),
		data,
	)
}

func (s *RESTSource) poll() ([]json.RawMessage, error) {
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
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, err
	}

	return items, nil
}

func (s *RESTSource) Stop() error {
	slog.Info("Stopping REST source", "instance", s.cfg.InstanceID)
	close(s.stopCh)
	close(s.taskChan) // Signal worker to stop
	s.wg.Wait()       // Wait for processing to finish
	return nil
}

func (s *RESTSource) InstanceID() string {
	return s.cfg.InstanceID
}
