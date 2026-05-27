package domain

import "time"

type Checkpoint struct {
	FlowID      string    `json:"flow_id"`
	SourceID    string    `json:"source_id"`
	Position    string    `json:"position"`
	LastEventID string    `json:"last_event_id,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}
