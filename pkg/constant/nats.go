package constant

const (
	// NATS Headers
	HeaderLSN        = "cdc-lsn"
	HeaderOffset     = "cdc-offset"
	HeaderInstanceID = "cdc-instance-id"
	HeaderSchema     = "cdc-schema"
	HeaderTable      = "cdc-table"
	HeaderOp         = "cdc-op"

	// NATS Streams
	StreamDLQ  = "cdc-dlq"
	StreamMain = "cdc-main"

	// NATS Consumer
	ConsumerDLQReprocessor = "dlq-reprocessor"
)
