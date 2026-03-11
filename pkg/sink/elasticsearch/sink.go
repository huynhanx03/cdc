package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSink(constant.SinkTypeElasticsearch.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg)
	})
}

// ElasticSink writes CDC events to Elasticsearch via the Bulk API.
type ElasticSink struct {
	client *elasticsearch.Client
	cfg    *config.SinkConfig

	mu      sync.Mutex
	buf     bytes.Buffer
	pending int

	flushTicker *time.Ticker
	done        chan struct{}
}

// Bulk Response Parsing
type bulkResponse struct {
	Errors bool                        `json:"errors"`
	Items  []map[string]bulkItemResult `json:"items"`
}

type bulkItemResult struct {
	Index  string         `json:"_index"`
	ID     string         `json:"_id"`
	Status int            `json:"status"`
	Error  *bulkItemError `json:"error,omitempty"`
}

type bulkItemError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

// New creates an ElasticSink — validates the connection on startup.
func New(cfg *config.SinkConfig) (*ElasticSink, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}

	s := &ElasticSink{
		client:      client,
		cfg:         cfg,
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(time.Duration(cfg.FlushInterval) * time.Millisecond),
	}

	go s.flushLoop()
	return s, nil
}

// Write buffers a single CDC event. Auto-flushes when batch_size is reached.
func (s *ElasticSink) Write(event *models.Event) error {
	docMap := event.After
	if event.Op == constant.DeleteAction.String() {
		docMap = event.Before
	}
	if docMap == nil {
		return nil
	}

	index := s.indexName(event.Table)
	docID := extractID(docMap)

	s.mu.Lock()
	defer s.mu.Unlock()

	if event.Op == constant.DeleteAction.String() {
		s.writeDeleteAction(index, docID)
	} else {
		if err := s.writeIndexAction(index, docID, docMap); err != nil {
			return err
		}
	}

	s.pending++
	if s.pending >= s.cfg.BatchSize {
		return s.flushLocked()
	}
	return nil
}

// Flush sends accumulated bulk buffer to Elasticsearch.
func (s *ElasticSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *ElasticSink) flushLocked() error {
	if s.pending == 0 {
		return nil
	}

	data := make([]byte, s.buf.Len())
	copy(data, s.buf.Bytes())
	count := s.pending

	s.buf.Reset()
	s.pending = 0

	maxRetries := s.cfg.MaxRetries
	retryBaseMs := s.cfg.RetryBaseMs

	var res *esapi.Response
	var err error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		req := esapi.BulkRequest{Body: bytes.NewReader(data)}
		res, err = req.Do(context.Background(), s.client)

		if err == nil && !res.IsError() {
			break // Success
		}

		if res != nil {
			// Extract error body if possible for logging
			if res.IsError() && res.Body != nil {
				body, _ := io.ReadAll(res.Body)
				slog.Error("bulk request error", "status", res.StatusCode, "body", string(body))
			}
			if res.Body != nil {
				res.Body.Close()
			}
		}

		if attempt < maxRetries {
			delay := time.Duration(retryBaseMs*(1<<attempt)) * time.Millisecond
			slog.Warn("bulk request failed, retrying", "attempt", attempt+1, "delay", delay, "err", err)
			time.Sleep(delay)
		}
	}

	if err != nil {
		slog.Error("bulk request failed after retries", "err", err, "count", count)
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk response error [%d]", res.StatusCode)
	}

	// Read response body safely after successful request
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read bulk response body: %w", err)
	}

	// Check for item-level errors inside the bulk response
	var bulkRes bulkResponse
	if err := json.Unmarshal(body, &bulkRes); err == nil && bulkRes.Errors {
		for _, item := range bulkRes.Items {
			for action, result := range item {
				if result.Error != nil {
					slog.Error("bulk item error",
						"action", action,
						"index", result.Index,
						"id", result.ID,
						"type", result.Error.Type,
						"reason", result.Error.Reason,
					)
				}
			}
		}
	}

	slog.Info("bulk flush completed", "count", count)
	return nil
}

// flushLoop runs a background goroutine that flushes on a timer.
func (s *ElasticSink) flushLoop() {
	for {
		select {
		case <-s.done:
			return
		case <-s.flushTicker.C:
			if err := s.Flush(); err != nil {
				slog.Error("periodic flush failed", "err", err)
			}
		}
	}
}

// Close flushes remaining events and stops the background flusher.
func (s *ElasticSink) Close() error {
	slog.Info("closing elasticsearch sink")
	s.flushTicker.Stop()
	close(s.done)
	return s.Flush()
}

// newClient builds and pings the ES client.
func newClient(cfg *config.SinkConfig) (*elasticsearch.Client, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.URL,
		Username:  cfg.Username,
		Password:  cfg.Password,
	}
	if cfg.APIKey != "" {
		esCfg.APIKey = cfg.APIKey
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to ping elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("elasticsearch error: %s", res.String())
	}

	slog.Info("elasticsearch sink connected", "addresses", cfg.URL)
	return client, nil
}

// indexName builds the target index/alias name from prefix + table.
// Dots are replaced with underscores (e.g. "public.user_files" → "cdc_public_user_files").
func (s *ElasticSink) indexName(table string) string {
	safe := strings.ReplaceAll(table, ".", "_")
	return fmt.Sprintf("%s%s", s.cfg.IndexPrefix, safe)
}

// extractID tries to pull a document ID from the row data.
func extractID(doc map[string]interface{}) string {
	if v, ok := doc["id"]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

// writeDeleteAction appends a bulk delete line.
func (s *ElasticSink) writeDeleteAction(index, docID string) {
	if docID == "" {
		slog.Warn("delete event without document id, skipping", "index", index)
		return
	}
	meta := fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"}}`, index, docID)
	s.buf.WriteString(meta)
	s.buf.WriteByte('\n')
}

// writeIndexAction appends a bulk index (upsert) line.
func (s *ElasticSink) writeIndexAction(index, docID string, doc map[string]interface{}) error {
	if docID != "" {
		s.buf.WriteString(fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, index, docID))
	} else {
		s.buf.WriteString(fmt.Sprintf(`{"index":{"_index":"%s"}}`, index))
	}
	s.buf.WriteByte('\n')

	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	s.buf.Write(body)
	s.buf.WriteByte('\n')
	return nil
}
