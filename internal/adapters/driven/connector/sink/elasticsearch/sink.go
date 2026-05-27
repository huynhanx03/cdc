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

	"github.com/foden/cdc/internal/adapters/driven/registry"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
)

func init() {
	registry.RegisterSink(constant.SinkTypeElasticsearch.String(), func(cfg *ports.SinkConfig) (ports.Sink, error) {
		return New(cfg)
	})
}

// ElasticSink writes CDC events to Elasticsearch via the Bulk API.
type ElasticSink struct {
	client     *elasticsearch.Client
	cfg        *ports.SinkConfig
	bufPool    sync.Pool
	indexCache sync.Map
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
func New(cfg *ports.SinkConfig) (*ElasticSink, error) {
	client, err := newClient(cfg)
	if err != nil {
		return nil, err
	}

	return &ElasticSink{
		client: client,
		cfg:    cfg,
		bufPool: sync.Pool{New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 64*1024))
		}},
	}, nil
}

// WriteBatch writes events to Elasticsearch using the Bulk API.
func (s *ElasticSink) WriteBatch(ctx context.Context, events []*domain.Event) error {
	buf := s.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufPool.Put(buf)

	for _, event := range events {
		node, ok, err := rowNode(event)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		s.sanitizeNode(&node)
		docID := extractIDFromNode(&node)
		docBytes, _ := node.MarshalJSON()
		index := s.indexName(event.InstanceID, event.Table)

		if event.Op == constant.OpDelete {
			writeDeleteAction(buf, index, docID)
		} else {
			writeIndexAction(buf, index, docID, docBytes)
		}
	}

	if buf.Len() == 0 {
		return nil
	}

	data := append([]byte(nil), buf.Bytes()...)
	req := esapi.BulkRequest{Body: bytes.NewReader(data)}
	res, err := req.Do(context.Background(), s.client)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return fmt.Errorf("bulk request failed: status %d, body: %s", res.StatusCode, string(body))
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

	slog.Debug("bulk write completed", "count", len(events))
	return nil
}

// Close is a no-op for the HTTP-based Elasticsearch client.
func (s *ElasticSink) Close() error {
	return nil
}

// InstanceID returns the sink instance ID.
func (s *ElasticSink) InstanceID() string {
	return s.cfg.InstanceID
}

// Type returns the type of the sink.
func (s *ElasticSink) Type() string {
	return constant.SinkTypeElasticsearch.String()
}

// newClient builds and pings the ES client.
func newClient(cfg *ports.SinkConfig) (*elasticsearch.Client, error) {
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

// indexName builds the target index name from prefix + instance + table.
func (s *ElasticSink) indexName(instanceID, table string) string {
	key := instanceID + "." + table
	if cached, ok := s.indexCache.Load(key); ok {
		return cached.(string)
	}
	safeTable := strings.ReplaceAll(table, ".", "_")
	index := s.cfg.IndexPrefix + instanceID + "_" + safeTable
	actual, _ := s.indexCache.LoadOrStore(key, index)
	return actual.(string)
}

func rowNode(event *domain.Event) (ast.Node, bool, error) {
	field := "after"
	if event.Op == constant.OpDelete {
		field = "before"
	}
	node, err := sonic.Get(event.Data, field)
	if err != nil {
		return ast.Node{}, false, err
	}
	if !node.Exists() || node.TypeSafe() == ast.V_NULL {
		return ast.Node{}, false, nil
	}
	return node, true, nil
}

// extractIDFromNode pulls a document ID directly from an already parsed AST node.
func extractIDFromNode(node *ast.Node) string {
	for _, key := range []string{"id", "ID", "uuid", "uid", "guid"} {
		field := node.Get(key)
		if !field.Exists() {
			continue
		}
		switch field.TypeSafe() {
		case ast.V_ARRAY:
			l, _ := field.Len()
			if l == 16 {
				b := make([]byte, 16)
				for i := 0; i < 16; i++ {
					v, _ := field.Index(i).Int64()
					b[i] = byte(v)
				}
				return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
			}
			b := make([]byte, 0, l)
			for i := 0; i < l; i++ {
				v, _ := field.Index(i).Int64()
				b = append(b, byte(v))
			}
			return fmt.Sprintf("%x", b)
		case ast.V_STRING:
			val, _ := field.String()
			return val
		default:
			val, _ := field.Raw()
			return strings.Trim(val, "\"")
		}
	}
	return ""
}

// writeDeleteAction appends a bulk delete line.
func writeDeleteAction(buf *bytes.Buffer, index, docID string) {
	if docID == "" {
		return
	}
	buf.WriteString(`{"delete":{"_index":"`)
	buf.WriteString(index)
	buf.WriteString(`","_id":"`)
	buf.WriteString(docID)
	buf.WriteString(`"}}`)
	buf.WriteByte('\n')
}

// writeIndexAction appends a bulk index (upsert) line.
func writeIndexAction(buf *bytes.Buffer, index, docID string, doc []byte) {
	buf.WriteString(`{"index":{"_index":"`)
	buf.WriteString(index)
	if docID != "" {
		buf.WriteString(`","_id":"`)
		buf.WriteString(docID)
	}
	buf.WriteString(`"}}`)
	buf.WriteByte('\n')
	buf.Write(doc)
	buf.WriteByte('\n')
}

// parseFlexTime parses various Postgres time formats.
func parseFlexTime(val string) (time.Time, error) {
	if strings.ContainsRune(val, 'T') {
		return parseTimeLayouts(val, "2006-01-02T15:04:05.999999Z", time.RFC3339)
	}
	if strings.ContainsRune(val, ' ') {
		return parseTimeLayouts(val,
			"2006-01-02 15:04:05.999999-07",
			"2006-01-02 15:04:05.999999+00",
			"2006-01-02 15:04:05",
		)
	}
	return parseTimeLayouts(val, time.RFC3339)
}

func parseTimeLayouts(val string, layouts ...string) (time.Time, error) {
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

// sanitizeNode cleans up the AST node for ES mapping compatibility.
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

	// 2. Convert time fields to Epoch Milliseconds
	if obj, err := node.Map(); err == nil {
		for key, val := range obj {
			strVal, isStr := val.(string)
			if !isStr {
				continue
			}

			if strings.HasSuffix(key, "_at") || strings.HasSuffix(key, "time") || key == "timestamp" {
				if t, err := parseFlexTime(strVal); err == nil {
					_, _ = node.Set(key, ast.NewAny(t.UnixMilli()))
				}
			}
		}
	}
}
