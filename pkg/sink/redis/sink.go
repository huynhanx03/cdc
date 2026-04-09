package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"

	"github.com/bytedance/sonic/ast"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/constant"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
)

func init() {
	registry.RegisterSink(constant.SinkTypeRedis.String(), func(cfg *config.SinkConfig) (interfaces.Sink, error) {
		return New(cfg)
	})
}

// RedisSink writes CDC events to Redis.
type RedisSink struct {
	client *redis.Client
	cfg    *config.SinkConfig

	mu     sync.Mutex
	events []*models.Event

	flushTicker *time.Ticker
	done        chan struct{}
}

// New creates a RedisSink and verifies connection.
func New(cfg *config.SinkConfig) (*RedisSink, error) {
	addr := cfg.Host
	if cfg.Port > 0 {
		addr = fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	} else if len(cfg.URL) > 0 {
		addr = cfg.URL[0] // Use first URL if provided
	}

	if addr == "" {
		addr = "localhost:6379"
	}

	db, err := strconv.Atoi(cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database: %w", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       db, // Default DB
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	s := &RedisSink{
		client:      client,
		cfg:         cfg,
		done:        make(chan struct{}),
		flushTicker: time.NewTicker(time.Duration(cfg.FlushIntervalMs) * time.Millisecond),
	}

	go s.flushLoop()
	return s, nil
}

// Write buffers a single event.
func (s *RedisSink) Write(event *models.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)

	if len(s.events) >= int(s.cfg.BatchSize) {
		return s.flushLocked()
	}
	return nil
}

// WriteBatch writes a batch of events to the sink.
func (s *RedisSink) WriteBatch(events []*models.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, events...)

	if len(s.events) >= int(s.cfg.BatchSize) {
		return s.flushLocked()
	}
	return nil
}

// Flush forces a sync of the current buffer.
func (s *RedisSink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *RedisSink) flushLocked() error {
	if len(s.events) == 0 {
		return nil
	}

	ctx := context.Background()
	pipe := s.client.Pipeline()

	for _, event := range s.events {
		var data []byte
		var err error

		// Extract Relevant Payload
		var node ast.Node
		if event.Op == "d" {
			node, err = sonic.Get(event.Data, "before")
		} else {
			node, err = sonic.Get(event.Data, "after")
		}

		if err != nil || !node.Exists() {
			continue
		}

		data, err = node.MarshalJSON()
		if err != nil {
			continue
		}

		// Apply transformations/field mapping
		data, err = s.cfg.ApplyFieldMapping(data)
		if err != nil {
			slog.Error("Redis mapping failed", "error", err, "table", event.Table)
			continue
		}

		var fields map[string]interface{}
		if err := sonic.Unmarshal(data, &fields); err != nil {
			slog.Error("Redis unmarshal failed", "error", err)
			continue
		}

		// Build Key using CEL template if available
		key := s.buildKeyWithData(event, fields)

		cmd := strings.ToLower(s.cfg.Redis.Command)
		if cmd == "" {
			cmd = "hset" // default
		}

		if event.Op == "d" {
			if cmd == "sadd" {
				val := s.getValue(fields)
				pipe.SRem(ctx, key, val)
			} else if cmd != "bf_add" { // Bloom filters don't support simple delete
				pipe.Del(ctx, key)
			}
		} else {
			ttl := time.Duration(s.cfg.Redis.TTL) * time.Second

			switch cmd {
			case "set":
				val := s.getValue(fields)
				pipe.Set(ctx, key, val, ttl)
			case "sadd":
				val := s.getValue(fields)
				pipe.SAdd(ctx, key, val)
				if ttl > 0 {
					pipe.Expire(ctx, key, ttl)
				}
			case "bf_add", "bf.add":
				val := s.getValue(fields)
				// BF.ADD is a custom command, not in standard go-redis
				pipe.Do(ctx, "BF.ADD", key, val)
				if ttl > 0 {
					pipe.Expire(ctx, key, ttl)
				}
			case "hset":
				fallthrough
			default:
				pipe.HSet(ctx, key, fields)
				if ttl > 0 {
					pipe.Expire(ctx, key, ttl)
				}
			}
		}
	}

	_, err := pipe.Exec(ctx)

	// Reset buffer
	s.events = s.events[:0]

	if err != nil {
		return fmt.Errorf("failed to execute redis pipeline: %w", err)
	}

	return nil
}

func (s *RedisSink) getValue(fields map[string]interface{}) interface{} {
	vf := s.cfg.Redis.ValueFields
	if len(vf) == 0 || (len(vf) == 1 && vf[0] == "*") {
		// Return whole object as JSON string
		b, _ := sonic.Marshal(fields)
		return string(b)
	}

	if len(vf) == 1 {
		// Return single field raw value
		if val, ok := fields[vf[0]]; ok {
			return val
		}
		return nil
	}

	// Filter and return multiple fields as JSON string
	res := make(map[string]interface{})
	for _, f := range vf {
		if val, ok := fields[f]; ok {
			res[f] = val
		}
	}
	b, _ := sonic.Marshal(res)
	return string(b)
}

func (s *RedisSink) buildKeyWithData(event *models.Event, fields map[string]interface{}) string {
	fallback := s.buildKeyFallback(event, fields)
	return s.cfg.BuildKey(fields, fallback)
}

func (s *RedisSink) buildKeyFallback(event *models.Event, fields map[string]interface{}) string {
	prefix := s.cfg.IndexPrefix
	if prefix == "" {
		prefix = "cdc"
	}

	id := event.Offset
	if extID := extractIDFastFromMap(fields); extID != "" {
		id = extID
	}

	return fmt.Sprintf("%s:%s:%s:%s", prefix, event.InstanceID, event.Table, id)
}

func extractIDFastFromMap(fields map[string]interface{}) string {
	for _, k := range []string{"id", "ID", "uuid", "uid", "guid"} {
		if val, ok := fields[k]; ok {
			return fmt.Sprintf("%v", val)
		}
	}
	return ""
}

// flushLoop runs a background goroutine that flushes the buffer periodically.
func (s *RedisSink) flushLoop() {
	for {
		select {
		case <-s.done:
			return
		case <-s.flushTicker.C:
			if err := s.Flush(); err != nil {
				slog.Error("redis periodic flush failed", "err", err)
			}
		}
	}
}

// Close gracefully flushes remaining events and stops the sink.
func (s *RedisSink) Close() error {
	slog.Info("closing redis sink")
	s.flushTicker.Stop()
	close(s.done)
	err := s.Flush()
	s.client.Close()
	return err
}

// Type returns the sink type.
func (s *RedisSink) Type() string {
	return constant.SinkTypeRedis.String()
}

// InstanceID returns the instance ID of the sink.
func (s *RedisSink) InstanceID() string {
	return s.cfg.InstanceID
}

// Topic returns the NATS topic pattern this sink subscribes to.
func (s *RedisSink) Topic() string {
	if s.cfg.Topic != "" {
		return s.cfg.Topic
	}
	return "cdc.>"
}

// MaxRetries returns the maximum number of delivery attempts.
func (s *RedisSink) MaxRetries() int32 {
	return s.cfg.MaxRetries
}
