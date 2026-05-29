package service

import (
	"errors"
	"fmt"

	"github.com/foden/cdc/internal/core/constant"
	"github.com/foden/cdc/internal/core/domain"
	coreflow "github.com/foden/cdc/internal/core/flow"
	"github.com/foden/cdc/internal/core/ports"
)

type ValidationSeverity string

const (
	ValidationSeverityPass    ValidationSeverity = "PASS"
	ValidationSeverityWarning ValidationSeverity = "WARNING"
	ValidationSeverityFatal   ValidationSeverity = "FATAL"
)

type ValidationFinding struct {
	Code     string             `json:"code"`
	Severity ValidationSeverity `json:"severity"`
	Message  string             `json:"message"`
	Target   string             `json:"target"`
}

var ErrFlowValidationFatal = errors.New("flow validation fatal")

func HasFatalFindings(findings []ValidationFinding) bool {
	for _, finding := range findings {
		if finding.Severity == ValidationSeverityFatal {
			return true
		}
	}
	return false
}

func ErrorIfFatal(findings []ValidationFinding) error {
	if !HasFatalFindings(findings) {
		return nil
	}
	return fmt.Errorf("%w: validation produced fatal findings", ErrFlowValidationFatal)
}

func DryRunFilter(expression string, sample []byte) (bool, []ValidationFinding) {
	filter, err := coreflow.NewFilter(expression)
	if err != nil {
		return false, []ValidationFinding{{
			Code:     "FILTER_COMPILE_ERROR",
			Severity: ValidationSeverityFatal,
			Message:  err.Error(),
			Target:   "filter",
		}}
	}

	passed, err := filter.Evaluate(&domain.Event{Data: sample})
	if err != nil {
		return false, []ValidationFinding{{
			Code:     "FILTER_EVAL_ERROR",
			Severity: ValidationSeverityFatal,
			Message:  err.Error(),
			Target:   "filter",
		}}
	}
	return passed, nil
}

func DryRunMapping(sample []byte, mappings []ports.ColumnMapping) ([]byte, []ValidationFinding) {
	mapped, err := coreflow.ApplyColumnMappings(sample, mappings)
	if err != nil {
		return nil, []ValidationFinding{{
			Code:     "MAPPING_ERROR",
			Severity: ValidationSeverityFatal,
			Message:  err.Error(),
			Target:   "mapping",
		}}
	}
	return mapped, nil
}

func ValidationFindingPass(code string, target string, message string) ValidationFinding {
	return ValidationFinding{Code: code, Severity: ValidationSeverityPass, Message: message, Target: target}
}

func ValidationFindingWarning(code string, target string, message string) ValidationFinding {
	return ValidationFinding{Code: code, Severity: ValidationSeverityWarning, Message: message, Target: target}
}

func ValidationFindingFatal(code string, target string, message string) ValidationFinding {
	return ValidationFinding{Code: code, Severity: ValidationSeverityFatal, Message: message, Target: target}
}

func FlowOpRequiresOldTuple(op constant.Op) bool {
	return op == constant.OpUpdate || op == constant.OpDelete
}
