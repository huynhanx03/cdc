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

	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"
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

// Internal structures for parsing Bulk API responses
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

// New creates an ElasticSink and verifies connection.
func New(cfg *config.SinkConfig) (*ElasticSink, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}

	s := &ElasticSink{
		client:      client,
		cfg:         cfg,
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(time.Duration(cfg.FlushIntervalMs) * time.Millisecond),
	}

	go s.flushLoop()
	return s, nil
}

// Write buffers a single event.
func (s *ElasticSink) Write(event *models.Event) error {
	return s.WriteBatch([]*models.Event{event})
}

// WriteBatch processes events using high-performance AST manipulation to ensure mapping compatibility.
func (s *ElasticSink) WriteBatch(events []*models.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		var node ast.Node
		var err error

		// Extract the relevant payload (Debezium 'before' for delete, 'after' for others)
		if event.Op == "d" {
			node, err = sonic.Get(event.Data, "before")
		} else {
			node, err = sonic.Get(event.Data, "after")
		}

		if err != nil || !node.Exists() {
			continue
		}

		// Apply all transformations: Metadata to String, Time to Epoch
		s.sanitizeNode(&node)

		// 2. Render sanitized JSON back to raw bytes
		docBytes, _ := node.MarshalJSON()

		index := s.indexName(event.InstanceID, event.Table)
		docID := extractIDFast(docBytes)

		if event.Op == "d" {
			s.writeDeleteAction(index, docID)
		} else {
			s.writeIndexAction(index, docID, docBytes)
		}
		s.pending++
	}

	if s.pending >= int(s.cfg.BatchSize) {
		return s.flushLocked()
	}
	return nil
}

func (s *ElasticSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *ElasticSink) flushLocked() error {
	if s.pending == 0 {
		return nil
	}

	data := s.buf.Bytes()
	count := s.pending

	var res *esapi.Response
	var err error

	// Execute Bulk Request with Exponential Backoff
	for attempt := 0; attempt <= int(s.cfg.MaxRetries); attempt++ {
		req := esapi.BulkRequest{Body: bytes.NewReader(data)}
		res, err = req.Do(context.Background(), s.client)

		if err == nil && !res.IsError() {
			break
		}

		if res != nil && res.IsError() && res.Body != nil {
			body, _ := io.ReadAll(res.Body)
			slog.Error("Bulk Request Failed", "status", res.StatusCode, "body", string(body))
			res.Body.Close()
		}

		if attempt < int(s.cfg.MaxRetries) {
			delay := time.Duration(s.cfg.RetryBaseMs*(1<<attempt)) * time.Millisecond
			time.Sleep(delay)
		}
	}

	// Reset buffer after attempt
	s.buf.Reset()
	s.pending = 0

	if err != nil {
		return fmt.Errorf("bulk request failed after retries: %w", err)
	}
	defer res.Body.Close()

	// Parse item-level errors
	respBody, _ := io.ReadAll(res.Body)
	var bulkRes bulkResponse
	if err := json.Unmarshal(respBody, &bulkRes); err == nil && bulkRes.Errors {
		for _, item := range bulkRes.Items {
			for action, result := range item {
				if result.Error != nil {
					slog.Error("Bulk Item Error",
						"action", action,
						"id", result.ID,
						"reason", result.Error.Reason,
						"type", result.Error.Type)
				}
			}
		}
		return fmt.Errorf("bulk response contained errors")
	}

	slog.Debug("bulk flush completed", "count", count)
	return nil
}

// flushLoop runs a background goroutine that flushes the buffer periodically.
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

// Close closes the sink.
func (s *ElasticSink) Close() error {
	slog.Info("closing elasticsearch sink")
	s.flushTicker.Stop()
	close(s.done)
	return s.Flush()
}

// Type returns the type of the sink.
func (s *ElasticSink) Type() string {
	return constant.SinkTypeElasticsearch.String()
}

// InstanceID returns the instance ID of the sink.
func (s *ElasticSink) InstanceID() string {
	return s.cfg.InstanceID
}

// MaxRetries returns the maximum number of retries.
func (s *ElasticSink) MaxRetries() int32 {
	return s.cfg.MaxRetries
}

// Topic returns the topic to subscribe to.
func (s *ElasticSink) Topic() string {
	if s.cfg.Topic != "" {
		return s.cfg.Topic
	}
	return "cdc.>"
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
		return nil, err
	}

	res, err := client.Info()
	if err != nil {
		return nil, err
	}
	res.Body.Close()

	return client, nil
}

// indexName builds the target index/alias name from prefix + instance + table.
func (s *ElasticSink) indexName(instanceID, table string) string {
	if s.cfg.IndexMapping != nil {
		if idx, ok := s.cfg.IndexMapping[fmt.Sprintf("%s.%s", instanceID, table)]; ok {
			return idx
		}
		if idx, ok := s.cfg.IndexMapping[table]; ok {
			return idx
		}
	}

	if s.cfg.Index != "" {
		return s.cfg.Index
	}

	safeTable := strings.ReplaceAll(table, ".", "_")
	return fmt.Sprintf("%s%s_%s", s.cfg.IndexPrefix, instanceID, safeTable)
}

// extractIDFast tries to pull a document ID from the raw JSON payload quickly using sonic.Get.
func extractIDFast(doc []byte) string {
	// Candidate keys for primary identifiers
	for _, k := range []string{"id", "ID", "uuid", "uid", "guid"} {
		if node, err := sonic.Get(doc, k); err == nil {
			switch node.TypeSafe() {
			case ast.V_ARRAY: // Handle UUIDs sent as raw byte arrays
				l, _ := node.Len()
				if l == 16 {
					b := make([]byte, 16)
					for i := 0; i < 16; i++ {
						v, _ := node.Index(i).Int64()
						b[i] = byte(v)
					}
					return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
				}
				var b []byte
				for i := 0; i < l; i++ {
					v, _ := node.Index(i).Int64()
					b = append(b, byte(v))
				}
				return fmt.Sprintf("%x", b)
			case ast.V_STRING:
				val, _ := node.String()
				return val
			default:
				val, _ := node.Raw()
				return strings.Trim(val, "\"")
			}
		}
	}
	return ""
}

// writeDeleteAction appends a bulk delete line.
func (s *ElasticSink) writeDeleteAction(index, docID string) {
	if docID == "" {
		return
	}
	s.buf.WriteString(fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"}}`, index, docID))
	s.buf.WriteByte('\n')
}

// writeIndexAction appends a bulk index (upsert) line.
func (s *ElasticSink) writeIndexAction(index, docID string, doc interface{}) {
	if docID != "" {
		s.buf.WriteString(fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`, index, docID))
	} else {
		s.buf.WriteString(fmt.Sprintf(`{"index":{"_index":"%s"}}`, index))
	}
	s.buf.WriteByte('\n')

	switch v := doc.(type) {
	case []byte:
		s.buf.Write(v)
	default:
		b, _ := sonic.Marshal(v)
		s.buf.Write(b)
	}
	s.buf.WriteByte('\n')
}

// Helper to parse various Postgres time formats
func parseFlexTime(val string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999+00",
		"2006-01-02T15:04:05.999999Z",
		time.RFC3339,
		"2006-01-02 15:04:05",
	}

	var lastErr error
	for _, layout := range layouts {
		t, err := time.Parse(layout, val)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

// sanitizeNode cleans up the AST node recursively or by targeting specific patterns.
func (s *ElasticSink) sanitizeNode(node *ast.Node) {
	// 1. Fix 'metadata' object to string for keyword mapping compatibility
	meta := node.Get("metadata")
	if meta.Exists() && meta.TypeSafe() == ast.V_OBJECT {
		raw, _ := meta.Raw()
		if raw == "{}" {
			_, _ = node.Set("metadata", ast.NewString(""))
		} else {
			_, _ = node.Set("metadata", ast.NewString(raw))
		}
	}

	// 2. Scan and Convert ALL potential time fields to Epoch Milliseconds
	// We target fields ending in _at, _time, or specific timestamp keys
	if obj, err := node.Map(); err == nil {
		for key, val := range obj {
			strVal, isStr := val.(string)
			if !isStr {
				continue
			}

			// Optimization: Only check fields that look like timestamps
			if strings.HasSuffix(key, "_at") || strings.HasSuffix(key, "time") || key == "timestamp" {
				if t, err := parseFlexTime(strVal); err == nil {
					// Update the AST node with Epoch Milliseconds
					_, _ = node.Set(key, ast.NewAny(t.UnixMilli()))
				}
			}
		}
	}
}
