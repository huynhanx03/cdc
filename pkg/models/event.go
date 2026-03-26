package models

import (
	"encoding/json"
	"time"
)

// Event represents a CDC event payload
type Event struct {
	// Op is the operation type: "c" (create), "u" (update), "d" (delete)
	Op string `json:"op"`
	// InstanceID identifies the source CDC instance
	InstanceID string `json:"instance_id"`
	// Database name
	Database string `json:"database"`
	// Table name
	Table string `json:"table"`
	// Before state of the row (only for update and delete)
	Before json.RawMessage `json:"before,omitempty"`
	// After state of the row (only for create and update)
	After json.RawMessage `json:"after,omitempty"`
	// Timestamp when the event occurred (in Unix milliseconds)
	Timestamp int64 `json:"timestamp"`
	// LSN tracked for backward compatibility with PostgresSource and old workers
	LSN uint64 `json:"-"`
	// Offset is the generic position in the source log (e.g. LSN or binlog file:pos)
	Offset string `json:"offset,omitempty"`
	// Topic to publish the event to
	Topic string `json:"topic,omitempty"`
}

// NewEvent creates a new CDC event
func NewEvent(topic, op, instanceID, db, table string, before, after json.RawMessage, lsn uint64, offset string) *Event {
	return &Event{
		Topic:      topic,
		Op:         op,
		InstanceID: instanceID,
		Database:   db,
		Table:      table,
		Timestamp:  time.Now().UnixMilli(),
		Before:     before,
		After:      after,
		LSN:        lsn,
		Offset:     offset,
	}
}

// ComponentStats represents success/failure metrics for a source or sink.
type ComponentStats struct {
	SuccessCount uint64 `json:"success_count"`
	FailureCount uint64 `json:"failure_count"`
	LastError    string `json:"last_error"`
}

type MessageStatus int

const (
	MessageStatusSent MessageStatus = iota
	MessageStatusUnsent
	MessageStatusAll
)

type Message struct {
	Sequence  uint64            `json:"sequence"`
	Timestamp int64             `json:"timestamp"`
	Subject   string            `json:"subject"`
	Data      []byte            `json:"data"`
	Headers   map[string]string `json:"headers"`
}
