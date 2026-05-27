package flow

import (
	"strings"

	"github.com/bytedance/sonic"
	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/ports"
)

// ApplyColumnMappings transforms a raw event payload according to the column mappings.
// It returns a new JSON payload with the following rules:
//  1. For each enabled mapping: rename source_column key to sink_column key
//  2. For disabled mappings: remove that column from the output
//  3. Columns not in any mapping are passed through unchanged
//
// Uses bytedance/sonic for fast JSON operations.
func ApplyColumnMappings(data []byte, mappings []ports.ColumnMapping) ([]byte, error) {
	var payload map[string]interface{}
	if err := sonic.Unmarshal(data, &payload); err != nil {
		return nil, err
	}

	rowKey := "after"
	if op, _ := payload["op"].(string); constant.Op(op) == constant.OpDelete {
		rowKey = "before"
	}
	if row, ok := payload[rowKey].(map[string]interface{}); ok {
		payload[rowKey] = applyColumnMappingsToRow(row, mappings)
		return sonic.Marshal(payload)
	}

	return sonic.Marshal(applyColumnMappingsToRow(payload, mappings))
}

func applyColumnMappingsToRow(payload map[string]interface{}, mappings []ports.ColumnMapping) map[string]interface{} {
	if payload == nil {
		return nil
	}

	// Build lookup maps for O(1) access
	// enabledMap: source_column -> sink_column (for enabled mappings)
	// disabledSet: source_column -> true (for disabled mappings)
	enabledMap := make(map[string]string, len(mappings))
	disabledSet := make(map[string]bool, len(mappings))
	for _, m := range mappings {
		if m.Enabled {
			enabledMap[m.SourceColumn] = m.SinkColumn
		} else {
			disabledSet[m.SourceColumn] = true
		}
	}

	result := make(map[string]interface{}, len(payload))
	for key, val := range payload {
		// If column is disabled, exclude it
		if disabledSet[key] {
			continue
		}
		// If column has an enabled mapping, rename it
		if sinkCol, ok := enabledMap[key]; ok {
			outputKey := sinkCol
			if outputKey == "" {
				outputKey = key
			}
			result[outputKey] = val
			continue
		}
		// Column not in any mapping: pass through unchanged
		result[key] = val
	}

	return result
}

// AutoGenerateMappings generates column mappings by matching source and sink column
// names case-insensitively. For each source column that has a matching sink column
// (by name, case-insensitive), a ColumnMapping is created with enabled=true.
// Columns that don't match are NOT included in the output.
func AutoGenerateMappings(sourceColumns []ports.ColumnInfo, sinkColumns []ports.ColumnInfo) []ports.ColumnMapping {
	// Build a lookup map: lowercase sink column name -> ColumnInfo
	sinkLookup := make(map[string]ports.ColumnInfo, len(sinkColumns))
	for _, sc := range sinkColumns {
		sinkLookup[strings.ToLower(sc.Name)] = sc
	}

	var mappings []ports.ColumnMapping
	for _, src := range sourceColumns {
		sinkCol, found := sinkLookup[strings.ToLower(src.Name)]
		if !found {
			continue
		}
		mappings = append(mappings, ports.ColumnMapping{
			SourceColumn: src.Name,
			SinkColumn:   sinkCol.Name,
			SourceType:   src.Type,
			SinkType:     sinkCol.Type,
			Enabled:      true,
		})
	}

	return mappings
}
