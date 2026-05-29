package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
)

type DLQService struct {
	natsClient ports.NATSClient
	guard      *DLQReprocessGuard
}

func NewDLQService(natsClient ports.NATSClient) *DLQService {
	return &DLQService{
		natsClient: natsClient,
		guard:      NewDLQReprocessGuard([]byte("cdc-dlq-reprocess"), 5*time.Minute),
	}
}

func (s *DLQService) ListMessages(ctx context.Context, req request.ListDLQMessagesRequest) (response.ListDLQMessagesResponse, error) {
	if s.natsClient == nil {
		return response.ListDLQMessagesResponse{Pagination: pagination(0, req.Page, req.Limit)}, nil
	}
	items, total, err := s.natsClient.ListDLQMessages(ctx, normalizedLimit(req.Limit), normalizedPage(req.Page))
	if err != nil {
		return response.ListDLQMessagesResponse{}, err
	}
	result := make([]response.DLQMessage, 0, len(items))
	for _, item := range items {
		result = append(result, response.DLQMessage{
			Message:         item,
			Reason:          item.Headers["X-DLQ-Reason"],
			OriginalSubject: item.Headers["X-DLQ-Original-Subject"],
			DLQID:           item.Headers["Nats-Msg-Id"],
			DuplicateRisk:   response.DLQDuplicateRiskPossible,
		})
	}
	return response.ListDLQMessagesResponse{
		Data:       result,
		Pagination: pagination(total, req.Page, req.Limit),
		Scan: response.ScanMetadata{
			ScannedCount: total,
			MatchedCount: total,
			MaxScan:      500,
		},
	}, nil
}

func (s *DLQService) PreviewReprocess(ctx context.Context, req request.DLQDryRunRequest) (response.DLQDryRunResponse, error) {
	if s.natsClient == nil {
		return response.DLQDryRunResponse{}, nil
	}
	items, err := s.natsClient.PreviewDLQ(ctx, req.SelectedDLQIDs, dlqFilterToPorts(req.Filter), req.MaxCount)
	if err != nil {
		return response.DLQDryRunResponse{}, err
	}
	preview := make([]response.DLQDryRunPreviewItem, 0, len(items))
	blocked := uint32(0)
	for _, item := range items {
		risk := dlqDuplicateRisk(item.DuplicateRisk)
		if risk == response.DLQDuplicateRiskBlocked {
			blocked++
		}
		preview = append(preview, response.DLQDryRunPreviewItem{
			DLQID:            item.DLQID,
			OriginalSubject:  item.OriginalSubject,
			Reason:           item.Reason,
			DuplicateRisk:    risk,
			BlockedReason:    item.BlockedReason,
			ReplayTarget:     item.ReplayTarget,
			MessageSequence:  item.MessageSequence,
			MessageTimestamp: unixMillisTime(item.MessageTimestamp),
		})
	}
	token, err := s.guard.Issue(DLQReprocessPlan{
		SelectedIDs: req.SelectedDLQIDs,
		Count:       uint32(len(preview)),
		FilterHash:  dlqFilterHash(req.Filter),
	})
	if err != nil {
		return response.DLQDryRunResponse{}, err
	}
	return response.DLQDryRunResponse{
		SelectedCount: uint32(len(req.SelectedDLQIDs)),
		PreviewCount:  uint32(len(preview)),
		BlockedCount:  blocked,
		PreviewItems:  preview,
		ConfirmToken:  token,
	}, nil
}

func (s *DLQService) Reprocess(ctx context.Context, req request.ReprocessDLQRequest) (response.ReprocessDLQResponse, error) {
	if s.natsClient == nil {
		return response.ReprocessDLQResponse{Count: 0}, nil
	}
	if req.DryRun {
		preview, err := s.PreviewReprocess(ctx, request.DLQDryRunRequest{
			SelectedDLQIDs: req.SelectedDLQIDs,
			Filter:         req.Filter,
			MaxCount:       req.MaxCount,
		})
		if err != nil {
			return response.ReprocessDLQResponse{}, err
		}
		return response.ReprocessDLQResponse{Count: int32(preview.PreviewCount), DryRun: true}, nil
	}
	if req.ConfirmToken == "" {
		return response.ReprocessDLQResponse{}, fmt.Errorf("%w: confirm token is required", cdcerrors.ErrValidation)
	}
	plan, err := s.guard.Verify(req.ConfirmToken, time.Now().Unix())
	if err != nil {
		return response.ReprocessDLQResponse{}, fmt.Errorf("%w: %v", cdcerrors.ErrValidation, err)
	}
	if plan.FilterHash != dlqFilterHash(req.Filter) {
		return response.ReprocessDLQResponse{}, fmt.Errorf("%w: confirm token filter does not match request", cdcerrors.ErrValidation)
	}
	if len(req.SelectedDLQIDs) == 0 {
		req.SelectedDLQIDs = plan.SelectedIDs
	}
	result, err := s.natsClient.ReprocessDLQSelected(ctx, req.SelectedDLQIDs, dlqFilterToPorts(req.Filter), req.MaxCount)
	if err != nil {
		return response.ReprocessDLQResponse{}, err
	}
	return response.ReprocessDLQResponse{
		Count:             int32(result.Count),
		ReprocessedDLQIDs: result.ReprocessedDLQIDs,
		SkippedDLQIDs:     result.SkippedDLQIDs,
		FailedDLQIDs:      result.FailedDLQIDs,
	}, nil
}

func dlqFilterToPorts(filter request.DLQFilter) ports.DLQFilter {
	return ports.DLQFilter{
		OriginalTopic:     filter.OriginalTopic,
		OriginalPartition: filter.OriginalPartition,
		SourceID:          filter.SourceID,
		Schema:            filter.Schema,
		Table:             filter.Table,
		Op:                filter.Op,
		ReasonContains:    filter.ReasonContains,
		ErrorClass:        filter.ErrorClass,
		HeaderKey:         filter.HeaderKey,
		HeaderValue:       filter.HeaderValue,
		JSONPath:          filter.JSONPath,
		JSONEquals:        filter.JSONEquals,
		TextContains:      filter.TextContains,
	}
}

func dlqFilterHash(filter request.DLQFilter) string {
	data, _ := json.Marshal(filter)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func dlqDuplicateRisk(value string) response.DLQDuplicateRisk {
	switch value {
	case string(response.DLQDuplicateRiskHigh):
		return response.DLQDuplicateRiskHigh
	case string(response.DLQDuplicateRiskBlocked):
		return response.DLQDuplicateRiskBlocked
	case string(response.DLQDuplicateRiskNone):
		return response.DLQDuplicateRiskNone
	default:
		return response.DLQDuplicateRiskPossible
	}
}
