package queue

// Message is a message in the queue. Model Write
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// MessageView is a message in the queue. Model Read
type MessageView struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}
