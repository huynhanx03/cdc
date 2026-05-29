package request

type DLQFilter struct {
	OriginalTopic     string
	OriginalPartition string
	SourceID          string
	Schema            string
	Table             string
	Op                string
	ReasonContains    string
	ErrorClass        string
	HeaderKey         string
	HeaderValue       string
	JSONPath          string
	JSONEquals        string
	TextContains      string
}

type DLQDryRunRequest struct {
	SelectedDLQIDs []string
	Filter         DLQFilter
	MaxCount       uint32
}

type ReprocessDLQRequest struct {
	SelectedDLQIDs []string
	Filter         DLQFilter
	ConfirmToken   string
	DryRun         bool
	MaxCount       uint32
}

type ListDLQMessagesRequest struct {
	Page   int
	Limit  int
	Filter DLQFilter
}
