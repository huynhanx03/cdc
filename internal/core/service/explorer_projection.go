package service

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
)

func ParseCDCSubject(subject string) response.CDCSubjectParts {
	parts := strings.Split(subject, ".")
	parsed := response.CDCSubjectParts{Topic: subject}
	if len(parts) >= 4 {
		parsed.Topic = strings.Join(parts[:4], ".")
		parsed.SourceID = parts[1]
		parsed.Schema = parts[2]
		parsed.Table = parts[3]
	}
	if len(parts) >= 5 {
		parsed.Partition = parts[4]
	}
	return parsed
}

func ProjectMessageItem(item *ports.NATSMessageItem) response.ProjectedMessageItem {
	if item == nil {
		return response.ProjectedMessageItem{}
	}
	parsed := ParseCDCSubject(item.Subject)
	op := firstNonEmptyString(item.Headers[constant.HeaderOp], jsonStringValue(item.Data, "op"))
	sourceID := firstNonEmptyString(item.Headers[constant.HeaderInstanceID], parsed.SourceID, jsonStringValue(item.Data, "source.id"), jsonStringValue(item.Data, "source.instance_id"))
	schema := firstNonEmptyString(item.Headers[constant.HeaderSchema], parsed.Schema, jsonStringValue(item.Data, "source.schema"), jsonStringValue(item.Data, "schema"))
	table := firstNonEmptyString(item.Headers[constant.HeaderTable], parsed.Table, jsonStringValue(item.Data, "source.table"), jsonStringValue(item.Data, "table"))
	partition := firstNonEmptyString(item.Headers[constant.HeaderPartition], parsed.Partition)
	natsMsgID := firstNonEmptyString(item.Headers["Nats-Msg-Id"], item.Headers["Nats-Msg-ID"])
	reprocessedFrom := item.Headers["X-DLQ-Reprocessed-From"]
	markers := make([]string, 0, 3)
	if op != "" {
		markers = append(markers, op)
	}
	if reprocessedFrom != "" {
		markers = append(markers, "reprocessed")
	}
	isDLQ := strings.HasPrefix(item.Subject, "dlq.") || item.Headers["X-DLQ-Reason"] != ""
	if isDLQ {
		markers = append(markers, "dlq")
	}

	before, after := beforeAfterPayload(item.Data)
	return response.ProjectedMessageItem{
		NATSMessageItem: item,
		Topic:           parsed.Topic,
		SourceID:        sourceID,
		Schema:          schema,
		Table:           table,
		Partition:       partition,
		Op:              op,
		Key:             natsMsgID,
		PayloadSize:     uint64(len(item.Data)),
		HeaderCount:     uint32(len(item.Headers)),
		NATSMsgID:       natsMsgID,
		ReprocessedFrom: reprocessedFrom,
		Markers:         markers,
		IsDLQ:           isDLQ,
		IsReprocessed:   reprocessedFrom != "",
		ChangedFields:   ChangedFields(before, after),
	}
}

func ProjectMessageItems(items []*ports.NATSMessageItem) []response.ProjectedMessageItem {
	result := make([]response.ProjectedMessageItem, 0, len(items))
	for _, item := range items {
		result = append(result, ProjectMessageItem(item))
	}
	return result
}

func ChangedFields(before []byte, after []byte) []string {
	var beforeMap map[string]any
	var afterMap map[string]any
	if len(before) == 0 || len(after) == 0 {
		return nil
	}
	if err := json.Unmarshal(before, &beforeMap); err != nil {
		return nil
	}
	if err := json.Unmarshal(after, &afterMap); err != nil {
		return nil
	}
	keys := make(map[string]struct{}, len(beforeMap)+len(afterMap))
	for key := range beforeMap {
		keys[key] = struct{}{}
	}
	for key := range afterMap {
		keys[key] = struct{}{}
	}
	changed := make([]string, 0)
	for key := range keys {
		left, leftOK := beforeMap[key]
		right, rightOK := afterMap[key]
		if leftOK != rightOK || !reflectJSONEqual(left, right) {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

func beforeAfterPayload(data []byte) ([]byte, []byte) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, nil
	}
	return root["before"], root["after"]
}

func jsonStringValue(data []byte, path string) string {
	value, ok := jsonPathValue(data, path)
	if !ok {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	default:
		return ""
	}
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
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[strings.TrimSpace(part)]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func reflectJSONEqual(left any, right any) bool {
	leftBytes, leftErr := json.Marshal(left)
	rightBytes, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}
