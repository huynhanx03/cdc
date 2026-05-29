package response

import (
	"time"

	"github.com/foden/cdc/internal/core/ports"
)

type DLQDuplicateRisk string

const (
	DLQDuplicateRiskNone     DLQDuplicateRisk = "none"
	DLQDuplicateRiskPossible DLQDuplicateRisk = "possible"
	DLQDuplicateRiskHigh     DLQDuplicateRisk = "high"
	DLQDuplicateRiskBlocked  DLQDuplicateRisk = "blocked"
)

type ReprocessDLQResponse struct {
	Count             int32
	ReprocessedDLQIDs []string
	SkippedDLQIDs     []string
	FailedDLQIDs      []string
	DryRun            bool
}

type DLQMessage struct {
	Message         *ports.NATSMessageItem
	Reason          string
	OriginalSubject string
	DLQID           string
	FlowID          string
	SourceID        string
	SinkID          string
	Schema          string
	Table           string
	Op              string
	MsgID           string
	ErrorClass      string
	DeliveryCount   uint64
	RetryCount      uint64
	FailedAt        int64
	DuplicateRisk   DLQDuplicateRisk
	BlockedReason   string
}

type ListDLQMessagesResponse struct {
	Data       []DLQMessage
	Pagination PaginationResponse
	Scan       ScanMetadata
}

type DLQDryRunPreviewItem struct {
	DLQID            string
	OriginalSubject  string
	Reason           string
	DuplicateRisk    DLQDuplicateRisk
	BlockedReason    string
	ReplayTarget     string
	MessageSequence  uint64
	MessageTimestamp time.Time
}

type DLQDryRunResponse struct {
	SelectedCount uint32
	PreviewCount  uint32
	BlockedCount  uint32
	PreviewItems  []DLQDryRunPreviewItem
	ConfirmToken  string
	Warnings      []string
}
