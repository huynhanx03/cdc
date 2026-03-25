package nats

// StreamStats holds statistics about the JetStream stream
type StreamStats struct {
	Messages      uint64 `json:"messages"`
	Bytes         uint64 `json:"bytes"`
	FirstSeq      uint64 `json:"first_seq"`
	LastSeq       uint64 `json:"last_seq"`
	ConsumerCount int    `json:"consumer_count"`
}
// MessageItem represents a single message from the stream
type MessageItem struct {
	Sequence  uint64            `json:"sequence"`
	Timestamp int64             `json:"timestamp"`
	Subject   string            `json:"subject"`
	Data      []byte            `json:"data"`
	Headers   map[string]string `json:"headers"`
}
