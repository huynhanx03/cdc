package models

import "time"

// Event represents a CDC event payload
type Event struct {
	// Op is the operation type: "c" (create), "u" (update), "d" (delete)
	Op string `json:"op"`
	// Database name
	Database string `json:"database"`
	// Table name
	Table string `json:"table"`
	// Before state of the row (only for update and delete)
	Before map[string]interface{} `json:"before,omitempty"`
	// After state of the row (only for create and update)
	After map[string]interface{} `json:"after,omitempty"`
	// Timestamp when the event occurred (in Unix milliseconds)
	Timestamp int64 `json:"timestamp"`
	// LSN tracked for reliable offset committing
	LSN uint64 `json:"-"`
}

// NewEvent creates a new CDC event
func NewEvent(op, db, table string, before, after map[string]interface{}, lsn uint64) *Event {
	return &Event{
		Op:        op,
		Database:  db,
		Table:     table,
		Timestamp: time.Now().UnixMilli(),
		Before:    before,
		After:     after,
		LSN:       lsn,
	}
}
