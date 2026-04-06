package constant

const (
	// NATS Headers
	HeaderLSN        = "cdc-lsn"
	HeaderOffset     = "cdc-offset"
	HeaderInstanceID = "cdc-instance-id"

	// NATS Streams
	StreamDLQ  = "cdc-dlq"
	StreamMain = "cdc-main"

	// NATS Consumer
	ConsumerDLQReprocessor = "dlq-reprocessor"
)
