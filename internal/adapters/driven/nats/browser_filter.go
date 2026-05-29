package nats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/ports"
)

// ExplorerMessageFilter contains the server-side predicates needed by a rich
// stream explorer UI. NATS subjects remain the first cut; payload/header filters
// are applied after messages are fetched from JetStream.
type ExplorerMessageFilter ports.NATSMessageFilter

func (f ExplorerMessageFilter) NATSFilterSubject() string {
	switch {
	case f.Topic != "" && f.Partition != "":
		return topicPartitionSubject(f.Topic, f.Partition)
	case f.Topic != "":
		return normalizeSubjectPrefix(f.Topic)
	case f.SubjectPrefix != "":
		return normalizeSubjectPrefix(f.SubjectPrefix)
	case f.Partition != "":
		return f.Partition
	default:
		return ">"
	}
}

func (f ExplorerMessageFilter) HasPostFetchPredicates() bool {
	return f.MinSequence > 0 ||
		f.MaxSequence > 0 ||
		f.FromTimestamp > 0 ||
		f.ToTimestamp > 0 ||
		f.HeaderKey != "" ||
		f.TextContains != "" ||
		f.JSONPath != "" ||
		f.Op != "" ||
		f.SourceID != "" ||
		f.Schema != "" ||
		f.Table != ""
}

func (f ExplorerMessageFilter) Matches(message *MessageItem) bool {
	if message == nil {
		return false
	}
	if f.Topic != "" && !subjectHasPrefix(message.Subject, f.Topic) {
		return false
	}
	if f.SubjectPrefix != "" && !subjectHasPrefix(message.Subject, f.SubjectPrefix) {
		return false
	}
	if f.Partition != "" && !partitionMatches(message, f.Topic, f.Partition) {
		return false
	}
	if f.MinSequence > 0 && message.Sequence < f.MinSequence {
		return false
	}
	if f.MaxSequence > 0 && message.Sequence > f.MaxSequence {
		return false
	}
	if f.FromTimestamp > 0 && message.Timestamp < f.FromTimestamp {
		return false
	}
	if f.ToTimestamp > 0 && message.Timestamp > f.ToTimestamp {
		return false
	}
	if f.HeaderKey != "" {
		got, ok := lookupHeader(message.Headers, f.HeaderKey)
		if !ok {
			return false
		}
		if f.HeaderValue != "" && got != f.HeaderValue {
			return false
		}
	}
	if f.TextContains != "" && !messageContains(message, f.TextContains) {
		return false
	}
	if f.JSONPath != "" {
		value, ok := jsonPathValue(message.Data, f.JSONPath)
		if !ok {
			return false
		}
		if f.JSONEquals != "" && !jsonValueEquals(value, f.JSONEquals) {
			return false
		}
	}
	if f.Op != "" && !metadataEquals(message, constant.HeaderOp, f.Op, "op") {
		return false
	}
	if f.SourceID != "" && !metadataEquals(message, constant.HeaderInstanceID, f.SourceID, "source.id", "source.instance_id", "source.name", "source") {
		return false
	}
	if f.Schema != "" && !metadataEquals(message, constant.HeaderSchema, f.Schema, "source.schema", "schema") {
		return false
	}
	if f.Table != "" && !metadataEquals(message, constant.HeaderTable, f.Table, "source.table", "table") {
		return false
	}
	if f.ReprocessedOnly && lookupHeaderValue(message.Headers, "X-DLQ-Reprocessed-From") == "" {
		return false
	}
	if f.DLQRelatedOnly &&
		lookupHeaderValue(message.Headers, "X-DLQ-Reprocessed-From") == "" &&
		lookupHeaderValue(message.Headers, "X-DLQ-Reason") == "" &&
		!strings.HasPrefix(message.Subject, "dlq.") {
		return false
	}
	return true
}

func normalizeSubjectPrefix(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == ">" {
		return ">"
	}
	if strings.HasSuffix(value, ".>") || strings.HasSuffix(value, ">") {
		return value
	}
	if strings.HasPrefix(value, "cdc.") {
		return strings.TrimSuffix(value, ".") + ".>"
	}
	return fmt.Sprintf("cdc.%s.>", strings.Trim(value, "."))
}

func subjectHasPrefix(subject string, prefix string) bool {
	filter := normalizeSubjectPrefix(prefix)
	if filter == ">" {
		return true
	}
	filter = strings.TrimSuffix(filter, ">")
	filter = strings.TrimSuffix(filter, ".")
	return subject == filter || strings.HasPrefix(subject, filter+".")
}

func partitionMatches(message *MessageItem, topic string, partition string) bool {
	if message == nil {
		return false
	}
	if topic != "" && message.Subject == topicPartitionSubject(topic, partition) {
		return true
	}
	if got, ok := lookupHeader(message.Headers, constant.HeaderPartition); ok && got == partition {
		return true
	}
	parts := strings.Split(message.Subject, ".")
	return len(parts) > 0 && parts[len(parts)-1] == strings.Trim(partition, ".")
}

func lookupHeader(headers map[string]string, key string) (string, bool) {
	if headers == nil {
		return "", false
	}
	if value, ok := headers[key]; ok {
		return value, true
	}
	for k, value := range headers {
		if strings.EqualFold(k, key) {
			return value, true
		}
	}
	return "", false
}

func lookupHeaderValue(headers map[string]string, key string) string {
	value, _ := lookupHeader(headers, key)
	return value
}

func messageContains(message *MessageItem, needle string) bool {
	needle = strings.ToLower(strings.TrimSpace(needle))
	if needle == "" {
		return true
	}
	if strings.Contains(strings.ToLower(message.Subject), needle) {
		return true
	}
	if bytes.Contains(bytes.ToLower(message.Data), []byte(needle)) {
		return true
	}
	for k, v := range message.Headers {
		if strings.Contains(strings.ToLower(k), needle) || strings.Contains(strings.ToLower(v), needle) {
			return true
		}
	}
	return false
}

func metadataEquals(message *MessageItem, headerKey string, expected string, jsonPaths ...string) bool {
	if got, ok := lookupHeader(message.Headers, headerKey); ok {
		return got == expected
	}
	for _, path := range jsonPaths {
		value, ok := jsonPathValue(message.Data, path)
		if ok && jsonValueEquals(value, expected) {
			return true
		}
	}
	return false
}

func jsonPathValue(data []byte, path string) (any, bool) {
	if len(data) == 0 || strings.TrimSpace(path) == "" {
		return nil, false
	}
	var root any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, false
	}
	current := root
	for _, part := range strings.Split(path, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func jsonValueEquals(value any, expected string) bool {
	switch typed := value.(type) {
	case string:
		return typed == expected
	case json.Number:
		return typed.String() == expected
	case bool:
		return fmt.Sprintf("%t", typed) == strings.ToLower(expected)
	case nil:
		return expected == "null"
	default:
		return fmt.Sprint(typed) == expected
	}
}
