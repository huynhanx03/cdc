package flow

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/foden/cdc/internal/core/domain"
	"github.com/google/cel-go/cel"
)

// Filter evaluates a filter expression against event data.
// CEL expressions receive a "data" variable which is the event's after/before payload as a map.
// Example expressions: `data.status == "active"`, `data.amount > 100`
// Empty expression always returns true (pass all).
type Filter struct {
	expression string
	program    cel.Program
}

// NewFilter parses a filter expression and returns a compiled Filter.
// An empty expression creates a pass-all filter.
// The expression is compiled as a CEL expression with a "data" variable of type map[string]dyn.
// Returns an error if the expression syntax is invalid.
func NewFilter(expression string) (*Filter, error) {
	expr := strings.TrimSpace(expression)
	if expr == "" {
		return &Filter{expression: ""}, nil
	}

	program, err := compileCEL(expr)
	if err != nil {
		return nil, fmt.Errorf("filter: invalid CEL expression %q: %w", expr, err)
	}

	return &Filter{
		expression: expr,
		program:    program,
	}, nil
}

// compileCEL compiles a CEL expression with a "data" variable of type map[string]dyn.
func compileCEL(expr string) (cel.Program, error) {
	env, err := cel.NewEnv(
		cel.Variable("data", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("before", cel.DynType),
		cel.Variable("after", cel.DynType),
		cel.Variable("op", cel.StringType),
		cel.Variable("source", cel.DynType),
		cel.Variable("schema", cel.StringType),
		cel.Variable("table", cel.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("CEL compile error: %w", issues.Err())
	}

	program, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("CEL program error: %w", err)
	}

	return program, nil
}

// Evaluate evaluates the filter expression against the event's JSON data payload.
// Returns true if the event should pass through, false if it should be skipped.
// A nil filter or empty expression always returns true (pass all).
// If data is nil or empty, and a filter expression is set, returns false.
func (f *Filter) Evaluate(event *domain.Event) (bool, error) {
	if f == nil || f.program == nil {
		return true, nil
	}

	if event == nil || len(event.Data) == 0 {
		return false, fmt.Errorf("filter: event data is empty")
	}

	return f.evaluateCEL(event)
}

// evaluateCEL evaluates the CEL program against JSON data.
func (f *Filter) evaluateCEL(event *domain.Event) (bool, error) {
	// Parse JSON data into a map
	var dataMap map[string]interface{}
	if err := json.Unmarshal(event.Data, &dataMap); err != nil {
		return false, fmt.Errorf("filter: parse event JSON: %w", err)
	}

	// Evaluate the CEL expression
	out, _, err := f.program.Eval(map[string]interface{}{
		"data":   dataMap,
		"before": dataMap["before"],
		"after":  dataMap["after"],
		"op":     eventOp(event, dataMap),
		"source": dataMap["source"],
		"schema": event.Schema,
		"table":  event.Table,
	})
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return false, nil
		}
		return false, fmt.Errorf("filter: CEL eval: %w", err)
	}

	// The result must be a boolean
	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter: expression result is %T, want bool", out.Value())
	}

	return result, nil
}

func eventOp(event *domain.Event, data map[string]interface{}) string {
	if event != nil && event.Op != "" {
		return event.Op.String()
	}
	if op, ok := data["op"].(string); ok {
		return op
	}
	return ""
}

// Expression returns the original filter expression string.
func (f *Filter) Expression() string {
	if f == nil {
		return ""
	}
	return f.expression
}
