package drivergrpc

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	replayRiskLow             = "low"
	replayRiskMedium          = "medium"
	replayRiskHigh            = "high"
	highReplayRiskLagMessages = 10_000
)

func (s *CDCService) GetExplorerOverview(ctx context.Context, _ *cdcpb.GetExplorerOverviewRequest) (*cdcpb.GetExplorerOverviewResponse, error) {
	result, err := s.explorerService.Overview(ctx)
	if err != nil {
		slog.Error("GetExplorerOverview failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to load explorer overview: %v", err)
	}
	return &cdcpb.GetExplorerOverviewResponse{
		TopicCount:             result.TopicCount,
		PartitionCount:         result.PartitionCount,
		ConsumerCount:          result.ConsumerCount,
		PendingCount:           result.PendingCount,
		AckPendingCount:        result.AckPendingCount,
		DlqDepth:               result.DLQDepth,
		TopicsNeedingAttention: topicsToProto(result.TopicsNeedingAttention),
		RecentDlq:              dlqSummariesToProto(result.RecentDLQ),
	}, nil
}

func (s *CDCService) ListMessages(ctx context.Context, req *cdcpb.ListMessagesRequest) (*cdcpb.ListMessagesResponse, error) {
	result, err := s.explorerService.Messages(ctx, listMessagesRequestFromProto(req))
	if err != nil {
		slog.Error("ListMessages failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list messages: %v", err)
	}
	return &cdcpb.ListMessagesResponse{
		Data:       messagesToProto(result.Data),
		TotalCount: result.TotalCount,
		Pagination: paginationToProto(result.Pagination),
		Scan:       scanMetadataToProto(result.Scan),
	}, nil
}

func (s *CDCService) ListTopics(ctx context.Context, req *cdcpb.ListTopicsRequest) (*cdcpb.ListTopicsResponse, error) {
	result, err := s.explorerService.Topics(ctx, request.ListTopicsRequest{
		Page:  paginationPage(req.Pagination),
		Limit: paginationLimit(req.Pagination),
	})
	if err != nil {
		slog.Error("ListTopics failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list topics: %v", err)
	}
	return &cdcpb.ListTopicsResponse{Data: topicsToProto(result.Data), Pagination: paginationToProto(result.Pagination)}, nil
}

func (s *CDCService) GetTopicDetail(ctx context.Context, req *cdcpb.GetTopicDetailRequest) (*cdcpb.GetTopicDetailResponse, error) {
	result, err := s.explorerService.TopicDetail(ctx, request.TopicDetailRequest{Topic: req.GetTopic()})
	if err != nil {
		slog.Error("GetTopicDetail failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to get topic detail: %v", err)
	}
	return &cdcpb.GetTopicDetailResponse{
		Summary:    topicSummaryToProto(result.Summary),
		Partitions: partitionsToProto(result.Partitions),
		Scan:       scanMetadataToProto(result.Scan),
	}, nil
}

func (s *CDCService) ListPartitions(ctx context.Context, req *cdcpb.ListPartitionsRequest) (*cdcpb.ListPartitionsResponse, error) {
	result, err := s.explorerService.Partitions(ctx, request.ListPartitionsRequest{
		Topic: req.Topic,
		Page:  paginationPage(req.Pagination),
		Limit: paginationLimit(req.Pagination),
	})
	if err != nil {
		slog.Error("ListPartitions failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list partitions: %v", err)
	}
	return &cdcpb.ListPartitionsResponse{Data: partitionsToProto(result.Data), Pagination: paginationToProto(result.Pagination)}, nil
}

func (s *CDCService) GetPartitionDetail(ctx context.Context, req *cdcpb.GetPartitionDetailRequest) (*cdcpb.GetPartitionDetailResponse, error) {
	result, err := s.explorerService.PartitionDetail(ctx, request.PartitionDetailRequest{
		Topic:     req.GetTopic(),
		Partition: req.GetPartition(),
	})
	if err != nil {
		slog.Error("GetPartitionDetail failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to get partition detail: %v", err)
	}
	return &cdcpb.GetPartitionDetailResponse{
		Summary:        partitionSummaryToProto(result.Summary),
		RecentMessages: messagesToProto(result.RecentMessages),
		Checkpoints:    checkpointsToProto(result.Checkpoints),
		Scan:           scanMetadataToProto(result.Scan),
	}, nil
}

func (s *CDCService) ListPartitionMessages(ctx context.Context, req *cdcpb.ListPartitionMessagesRequest) (*cdcpb.ListPartitionMessagesResponse, error) {
	result, err := s.explorerService.Messages(ctx, listPartitionMessagesRequestFromProto(req))
	if err != nil {
		slog.Error("ListPartitionMessages failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list partition messages: %v", err)
	}
	return &cdcpb.ListPartitionMessagesResponse{
		Data:       messagesToProto(result.Data),
		TotalCount: result.TotalCount,
		Pagination: paginationToProto(result.Pagination),
		Scan:       scanMetadataToProto(result.Scan),
	}, nil
}

func (s *CDCService) GetMessageDetail(ctx context.Context, req *cdcpb.GetMessageDetailRequest) (*cdcpb.GetMessageDetailResponse, error) {
	result, err := s.explorerService.MessageDetail(ctx, request.MessageDetailRequest{
		Topic:     req.GetTopic(),
		Partition: req.GetPartition(),
		Sequence:  req.GetSequence(),
	})
	if err != nil {
		slog.Error("GetMessageDetail failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to get message detail: %v", err)
	}
	return &cdcpb.GetMessageDetailResponse{
		Item:          messageToProto(result.Item),
		Before:        result.Before,
		After:         result.After,
		ChangedFields: result.ChangedFields,
		Checkpoint:    checkpointToProto(result.Checkpoint),
	}, nil
}

func (s *CDCService) GetConsumerInfo(ctx context.Context, req *cdcpb.GetConsumerInfoRequest) (*cdcpb.GetConsumerInfoResponse, error) {
	result, err := s.explorerService.Consumers(ctx, request.ListConsumersRequest{Page: 1, Limit: 500})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get consumer info: %v", err)
	}
	for _, consumer := range result.Data {
		if consumer.Name == req.ConsumerName {
			return &cdcpb.GetConsumerInfoResponse{
				AckFloor:     consumer.AckFloorStreamSeq,
				PendingCount: consumer.NumPending,
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "consumer %q not found", req.ConsumerName)
}

func (s *CDCService) ListConsumers(ctx context.Context, req *cdcpb.ListConsumersRequest) (*cdcpb.ListConsumersResponse, error) {
	result, err := s.explorerService.Consumers(ctx, request.ListConsumersRequest{
		Page:  paginationPage(req.Pagination),
		Limit: paginationLimit(req.Pagination),
	})
	if err != nil {
		slog.Error("ListConsumers failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list consumers: %v", err)
	}
	consumers := make([]*cdcpb.ConsumerSummary, 0, len(result.Data))
	for _, consumer := range result.Data {
		consumers = append(consumers, consumerSummaryToProto(consumer))
	}
	return &cdcpb.ListConsumersResponse{Data: consumers, Pagination: paginationToProto(result.Pagination)}, nil
}

func (s *CDCService) GetConsumerDetail(ctx context.Context, req *cdcpb.GetConsumerDetailRequest) (*cdcpb.GetConsumerDetailResponse, error) {
	result, err := s.explorerService.ConsumerDetail(ctx, request.ConsumerDetailRequest{ConsumerName: req.GetConsumerName()})
	if err != nil {
		slog.Error("GetConsumerDetail failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to get consumer detail: %v", err)
	}
	return &cdcpb.GetConsumerDetailResponse{
		Summary:        consumerSummaryToProto(result.Summary),
		Topics:         topicsToProto(result.Topics),
		Partitions:     partitionsToProto(result.Partitions),
		RecentMessages: messagesToProto(result.RecentMessages),
		Scan:           scanMetadataToProto(result.Scan),
	}, nil
}

func (s *CDCService) ListDLQMessages(ctx context.Context, req *cdcpb.ListDLQMessagesRequest) (*cdcpb.ListDLQMessagesResponse, error) {
	result, err := s.dlqService.ListMessages(ctx, request.ListDLQMessagesRequest{
		Page:   paginationPage(req.Pagination),
		Limit:  paginationLimit(req.Pagination),
		Filter: dlqFilterFromProto(req.Filter),
	})
	if err != nil {
		slog.Error("ListDLQMessages failed", "err", err)
		return nil, status.Errorf(codes.Internal, "failed to list DLQ messages: %v", err)
	}
	messages := make([]*cdcpb.DLQMessage, 0, len(result.Data))
	for _, message := range result.Data {
		messages = append(messages, &cdcpb.DLQMessage{
			Sequence:        message.Message.Sequence,
			Timestamp:       message.Message.Timestamp,
			Subject:         message.Message.Subject,
			Data:            message.Message.Data,
			Headers:         message.Message.Headers,
			Reason:          message.Reason,
			OriginalSubject: message.OriginalSubject,
			DlqId:           message.DLQID,
			FlowId:          message.FlowID,
			SourceId:        message.SourceID,
			SinkId:          message.SinkID,
			Schema:          message.Schema,
			Table:           message.Table,
			Op:              message.Op,
			MsgId:           message.MsgID,
			ErrorClass:      message.ErrorClass,
			DeliveryCount:   message.DeliveryCount,
			RetryCount:      message.RetryCount,
			FailedAt:        message.FailedAt,
			DuplicateRisk:   dlqRiskToProto(message.DuplicateRisk),
			BlockedReason:   message.BlockedReason,
		})
	}
	return &cdcpb.ListDLQMessagesResponse{Data: messages, Pagination: paginationToProto(result.Pagination), Scan: scanMetadataToProto(result.Scan)}, nil
}

func protoMessageStatus(status cdcpb.MessageStatus) domain.MessageStatus {
	switch status {
	case cdcpb.MessageStatus_MESSAGE_STATUS_SENT:
		return domain.MessageStatusSent
	case cdcpb.MessageStatus_MESSAGE_STATUS_UNSENT:
		return domain.MessageStatusUnsent
	default:
		return domain.MessageStatusAll
	}
}

func paginationPage(p *cdcpb.OffsetPaginationRequest) int {
	if p == nil || p.Page == 0 {
		return 1
	}
	return int(p.Page)
}

func paginationLimit(p *cdcpb.OffsetPaginationRequest) int {
	if p == nil || p.Limit == 0 {
		return 25
	}
	return int(p.Limit)
}

func paginationToProto(p response.PaginationResponse) *cdcpb.OffsetPaginationResponse {
	return &cdcpb.OffsetPaginationResponse{
		TotalRows: p.TotalRows,
		Page:      uint32(p.Page),
		Limit:     uint32(p.Limit),
		HasNext:   p.HasNext,
		HasPrev:   p.HasPrev,
	}
}

func messagesToProto(messages []response.ProjectedMessageItem) []*cdcpb.MessageItem {
	result := make([]*cdcpb.MessageItem, 0, len(messages))
	for _, message := range messages {
		result = append(result, messageToProto(message))
	}
	return result
}

func messageToProto(message response.ProjectedMessageItem) *cdcpb.MessageItem {
	if message.NATSMessageItem == nil {
		return nil
	}
	return &cdcpb.MessageItem{
		Sequence:        message.Sequence,
		Timestamp:       strconv.FormatInt(message.Timestamp, 10),
		Subject:         message.Subject,
		Data:            message.Data,
		Headers:         message.Headers,
		Op:              message.Op,
		SourceId:        message.SourceID,
		Schema:          message.Schema,
		Table:           message.Table,
		Partition:       message.Partition,
		Key:             message.Key,
		PayloadSize:     message.PayloadSize,
		HeaderCount:     message.HeaderCount,
		NatsMsgId:       message.NATSMsgID,
		ReprocessedFrom: message.ReprocessedFrom,
		Markers:         message.Markers,
	}
}

func topicSummaryToProto(topic response.TopicSummary) *cdcpb.TopicSummary {
	return &cdcpb.TopicSummary{
		Name:            topic.Name,
		MessageCount:    topic.MessageCount,
		PartitionCount:  topic.PartitionCount,
		ConsumerCount:   topic.ConsumerCount,
		DlqCount:        topic.DLQCount,
		PendingCount:    topic.PendingCount,
		AckPendingCount: topic.AckPendingCount,
		FirstSequence:   topic.FirstSequence,
		LatestSequence:  topic.LatestSequence,
		LatestEventAt:   timeToProtoString(topic.LatestEventAt),
		Health:          explorerHealthToProto(topic.Health),
		Partial:         topic.Partial,
	}
}

func topicsToProto(topics []response.TopicSummary) []*cdcpb.TopicSummary {
	result := make([]*cdcpb.TopicSummary, 0, len(topics))
	for _, topic := range topics {
		result = append(result, topicSummaryToProto(topic))
	}
	return result
}

func partitionSummaryToProto(partition response.PartitionSummary) *cdcpb.PartitionSummary {
	return &cdcpb.PartitionSummary{
		Id:              partition.ID,
		MessageCount:    partition.MessageCount,
		Topic:           partition.Topic,
		PendingCount:    partition.PendingCount,
		AckPendingCount: partition.AckPendingCount,
		FirstSequence:   partition.FirstSequence,
		LatestSequence:  partition.LatestSequence,
		LatestEventAt:   timeToProtoString(partition.LatestEventAt),
		Health:          explorerHealthToProto(partition.Health),
		Partial:         partition.Partial,
	}
}

func partitionsToProto(partitions []response.PartitionSummary) []*cdcpb.PartitionSummary {
	result := make([]*cdcpb.PartitionSummary, 0, len(partitions))
	for _, partition := range partitions {
		result = append(result, partitionSummaryToProto(partition))
	}
	return result
}

func consumerSummaryToProto(consumer ports.NATSConsumerSummary) *cdcpb.ConsumerSummary {
	lag := consumer.NumPending
	if consumer.DeliveredStreamSeq > consumer.AckFloorStreamSeq {
		lag += consumer.DeliveredStreamSeq - consumer.AckFloorStreamSeq
	}
	replayRisk := replayRiskLow
	if lag > highReplayRiskLagMessages {
		replayRisk = replayRiskHigh
	} else if lag > 0 {
		replayRisk = replayRiskMedium
	}
	return &cdcpb.ConsumerSummary{
		Name:               consumer.Name,
		FilterSubjects:     consumer.FilterSubjects,
		NumPending:         consumer.NumPending,
		NumAckPending:      consumer.NumAckPending,
		DeliveredStreamSeq: consumer.DeliveredStreamSeq,
		AckFloorStreamSeq:  consumer.AckFloorStreamSeq,
		LagMessages:        lag,
		ReplayRisk:         replayRisk,
	}
}

func checkpointToProto(checkpoint response.CheckpointContext) *cdcpb.CheckpointContext {
	if checkpoint.ConsumerName == "" {
		return nil
	}
	return &cdcpb.CheckpointContext{
		ConsumerName:       checkpoint.ConsumerName,
		DeliveredStreamSeq: checkpoint.DeliveredStreamSeq,
		AckFloorStreamSeq:  checkpoint.AckFloorStreamSeq,
		NumPending:         checkpoint.NumPending,
		NumAckPending:      checkpoint.NumAckPending,
		LagMessages:        checkpoint.LagMessages,
		LastDeliveredAt:    timeToProtoString(checkpoint.LastDeliveredAt),
		LastAckAt:          timeToProtoString(checkpoint.LastAckAt),
	}
}

func checkpointsToProto(checkpoints []response.CheckpointContext) []*cdcpb.CheckpointContext {
	result := make([]*cdcpb.CheckpointContext, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		result = append(result, checkpointToProto(checkpoint))
	}
	return result
}

func scanMetadataToProto(scan response.ScanMetadata) *cdcpb.ExplorerScanMetadata {
	return &cdcpb.ExplorerScanMetadata{
		Partial:      scan.Partial,
		ScanLimitHit: scan.ScanLimitHit,
		ScannedCount: scan.ScannedCount,
		MatchedCount: scan.MatchedCount,
		MaxScan:      scan.MaxScan,
	}
}

func dlqSummariesToProto(messages []response.DLQMessageSummary) []*cdcpb.DLQMessageSummary {
	result := make([]*cdcpb.DLQMessageSummary, 0, len(messages))
	for _, message := range messages {
		result = append(result, &cdcpb.DLQMessageSummary{
			DlqId:           message.DLQID,
			OriginalSubject: message.OriginalSubject,
			Reason:          message.Reason,
			ErrorClass:      message.ErrorClass,
			Timestamp:       timeToProtoString(message.Timestamp),
		})
	}
	return result
}

func dlqFilterFromProto(filter *cdcpb.DLQFilter) request.DLQFilter {
	if filter == nil {
		return request.DLQFilter{}
	}
	return request.DLQFilter{
		OriginalTopic:     filter.GetOriginalTopic(),
		OriginalPartition: filter.GetOriginalPartition(),
		SourceID:          filter.GetSourceId(),
		Schema:            filter.GetSchema(),
		Table:             filter.GetTable(),
		Op:                filter.GetOp(),
		ReasonContains:    filter.GetReasonContains(),
		ErrorClass:        filter.GetErrorClass(),
		HeaderKey:         filter.GetHeaderKey(),
		HeaderValue:       filter.GetHeaderValue(),
		JSONPath:          filter.GetJsonPath(),
		JSONEquals:        filter.GetJsonEquals(),
		TextContains:      filter.GetTextContains(),
	}
}

func dlqRiskToProto(risk response.DLQDuplicateRisk) cdcpb.DLQDuplicateRisk {
	switch risk {
	case response.DLQDuplicateRiskNone:
		return cdcpb.DLQDuplicateRisk_DLQ_DUPLICATE_RISK_NONE
	case response.DLQDuplicateRiskHigh:
		return cdcpb.DLQDuplicateRisk_DLQ_DUPLICATE_RISK_HIGH
	case response.DLQDuplicateRiskBlocked:
		return cdcpb.DLQDuplicateRisk_DLQ_DUPLICATE_RISK_BLOCKED
	case response.DLQDuplicateRiskPossible:
		return cdcpb.DLQDuplicateRisk_DLQ_DUPLICATE_RISK_POSSIBLE
	default:
		return cdcpb.DLQDuplicateRisk_DLQ_DUPLICATE_RISK_UNSPECIFIED
	}
}

func dlqDryRunPreviewToProto(items []response.DLQDryRunPreviewItem) []*cdcpb.DLQDryRunPreviewItem {
	result := make([]*cdcpb.DLQDryRunPreviewItem, 0, len(items))
	for _, item := range items {
		result = append(result, &cdcpb.DLQDryRunPreviewItem{
			DlqId:            item.DLQID,
			OriginalSubject:  item.OriginalSubject,
			Reason:           item.Reason,
			DuplicateRisk:    dlqRiskToProto(item.DuplicateRisk),
			BlockedReason:    item.BlockedReason,
			ReplayTarget:     item.ReplayTarget,
			MessageSequence:  item.MessageSequence,
			MessageTimestamp: timeToProtoString(item.MessageTimestamp),
		})
	}
	return result
}

func explorerHealthToProto(status response.ExplorerHealthStatus) cdcpb.ExplorerHealthStatus {
	switch status {
	case response.ExplorerHealthHealthy:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_HEALTHY
	case response.ExplorerHealthIdle:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_IDLE
	case response.ExplorerHealthLagging:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_LAGGING
	case response.ExplorerHealthStale:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_STALE
	case response.ExplorerHealthDLQ:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_DLQ
	default:
		return cdcpb.ExplorerHealthStatus_EXPLORER_HEALTH_STATUS_UNSPECIFIED
	}
}

func protoExplorerSort(sort cdcpb.ExplorerSort) string {
	switch sort {
	case cdcpb.ExplorerSort_EXPLORER_SORT_OLDEST_FIRST:
		return "oldest"
	default:
		return "newest"
	}
}

func listMessagesRequestFromProto(req *cdcpb.ListMessagesRequest) request.ListMessagesRequest {
	return request.ListMessagesRequest{
		Status:          protoMessageStatus(req.GetStatus()),
		SourceID:        req.GetSourceId(),
		Topic:           req.GetTopic(),
		Partition:       req.GetPartition(),
		Schema:          req.GetSchema(),
		Table:           req.GetTable(),
		Op:              req.GetOp(),
		SequenceMin:     req.GetSequenceMin(),
		SequenceMax:     req.GetSequenceMax(),
		TimestampFrom:   req.GetTimestampFrom(),
		TimestampTo:     req.GetTimestampTo(),
		HeaderKey:       req.GetHeaderKey(),
		HeaderValue:     req.GetHeaderValue(),
		JSONPath:        req.GetJsonPath(),
		JSONEquals:      req.GetJsonEquals(),
		TextContains:    req.GetTextContains(),
		ReprocessedOnly: req.GetReprocessedOnly(),
		DLQRelatedOnly:  req.GetDlqRelatedOnly(),
		Sort:            protoExplorerSort(req.GetSort()),
		Page:            paginationPage(req.GetPagination()),
		Limit:           paginationLimit(req.GetPagination()),
	}
}

func listPartitionMessagesRequestFromProto(req *cdcpb.ListPartitionMessagesRequest) request.ListMessagesRequest {
	return request.ListMessagesRequest{
		Status:          protoMessageStatus(req.GetStatus()),
		SourceID:        req.GetSourceId(),
		Topic:           req.GetTopic(),
		Partition:       req.GetPartition(),
		Schema:          req.GetSchema(),
		Table:           req.GetTable(),
		Op:              req.GetOp(),
		SequenceMin:     req.GetSequenceMin(),
		SequenceMax:     req.GetSequenceMax(),
		TimestampFrom:   req.GetTimestampFrom(),
		TimestampTo:     req.GetTimestampTo(),
		HeaderKey:       req.GetHeaderKey(),
		HeaderValue:     req.GetHeaderValue(),
		JSONPath:        req.GetJsonPath(),
		JSONEquals:      req.GetJsonEquals(),
		TextContains:    req.GetTextContains(),
		ReprocessedOnly: req.GetReprocessedOnly(),
		DLQRelatedOnly:  req.GetDlqRelatedOnly(),
		Sort:            protoExplorerSort(req.GetSort()),
		Page:            paginationPage(req.GetPagination()),
		Limit:           paginationLimit(req.GetPagination()),
	}
}

func timeToProtoString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}
