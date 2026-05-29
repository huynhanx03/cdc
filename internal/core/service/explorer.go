package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/dto/response"
	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
)

type ExplorerService struct {
	natsClient ports.NATSClient
}

func NewExplorerService(natsClient ports.NATSClient) *ExplorerService {
	return &ExplorerService{natsClient: natsClient}
}

func (s *ExplorerService) Messages(ctx context.Context, req request.ListMessagesRequest) (response.ListMessagesResponse, error) {
	if s.natsClient == nil {
		return response.ListMessagesResponse{Pagination: pagination(0, req.Page, req.Limit)}, nil
	}
	limit := normalizedLimit(req.Limit)
	page := normalizedPage(req.Page)
	items, total, err := s.natsClient.ListMessagesWithFilter(ctx, req.Status, limit, page, ports.NATSMessageFilter{
		Topic:           req.Topic,
		Partition:       req.Partition,
		MinSequence:     req.SequenceMin,
		MaxSequence:     req.SequenceMax,
		FromTimestamp:   req.TimestampFrom,
		ToTimestamp:     req.TimestampTo,
		HeaderKey:       req.HeaderKey,
		HeaderValue:     req.HeaderValue,
		TextContains:    req.TextContains,
		JSONPath:        req.JSONPath,
		JSONEquals:      req.JSONEquals,
		Op:              req.Op,
		SourceID:        req.SourceID,
		Schema:          req.Schema,
		Table:           req.Table,
		ReprocessedOnly: req.ReprocessedOnly,
		DLQRelatedOnly:  req.DLQRelatedOnly,
		Sort:            req.Sort,
	})
	if err != nil {
		return response.ListMessagesResponse{}, err
	}
	return response.ListMessagesResponse{
		Data:       ProjectMessageItems(items),
		TotalCount: total,
		Pagination: pagination(total, req.Page, req.Limit),
		Scan: response.ScanMetadata{
			ScannedCount: total,
			MatchedCount: total,
			MaxScan:      500,
		},
	}, nil
}

func (s *ExplorerService) Topics(ctx context.Context, req request.ListTopicsRequest) (response.ListTopicsResponse, error) {
	if s.natsClient == nil {
		return response.ListTopicsResponse{Pagination: pagination(0, req.Page, req.Limit)}, nil
	}
	topics, total, err := s.natsClient.ListTopics(ctx, normalizedLimit(req.Limit), normalizedPage(req.Page))
	if err != nil {
		return response.ListTopicsResponse{}, err
	}
	result := make([]response.TopicSummary, 0, len(topics))
	for _, topic := range topics {
		partitions, _, partitionsErr := s.natsClient.ListPartitions(ctx, topic, 500, 1)
		partitionCount := int32(0)
		if partitionsErr == nil {
			partitionCount = int32(len(partitions))
		}
		messages, messagesErr := s.Messages(ctx, request.ListMessagesRequest{Topic: topic, Page: 1, Limit: 1, Sort: "newest"})
		messageCount := uint64(0)
		latestSequence := uint64(0)
		if messagesErr == nil {
			messageCount = messages.TotalCount
			if len(messages.Data) > 0 {
				latestSequence = messages.Data[0].Sequence
			}
		}
		result = append(result, response.TopicSummary{
			Name:           topic,
			PartitionCount: partitionCount,
			MessageCount:   messageCount,
			LatestSequence: latestSequence,
			Health:         response.ExplorerHealthIdle,
		})
	}
	return response.ListTopicsResponse{Data: result, Pagination: pagination(total, req.Page, req.Limit)}, nil
}

func (s *ExplorerService) Partitions(ctx context.Context, req request.ListPartitionsRequest) (response.ListPartitionsResponse, error) {
	if s.natsClient == nil {
		return response.ListPartitionsResponse{Pagination: pagination(0, req.Page, req.Limit)}, nil
	}
	partitions, total, err := s.natsClient.ListPartitions(ctx, req.Topic, normalizedLimit(req.Limit), normalizedPage(req.Page))
	if err != nil {
		return response.ListPartitionsResponse{}, err
	}
	result := make([]response.PartitionSummary, 0, len(partitions))
	for _, partition := range partitions {
		messages, messagesErr := s.Messages(ctx, request.ListMessagesRequest{Topic: req.Topic, Partition: partition, Page: 1, Limit: 1, Sort: "newest"})
		messageCount := uint64(0)
		latestSequence := uint64(0)
		if messagesErr == nil {
			messageCount = messages.TotalCount
			if len(messages.Data) > 0 {
				latestSequence = messages.Data[0].Sequence
			}
		}
		result = append(result, response.PartitionSummary{
			ID:             partition,
			Topic:          req.Topic,
			MessageCount:   messageCount,
			LatestSequence: latestSequence,
			Health:         response.ExplorerHealthIdle,
		})
	}
	return response.ListPartitionsResponse{Data: result, Pagination: pagination(total, req.Page, req.Limit)}, nil
}

func (s *ExplorerService) Consumers(ctx context.Context, req request.ListConsumersRequest) (response.ListConsumersResponse, error) {
	if s.natsClient == nil {
		return response.ListConsumersResponse{Pagination: pagination(0, req.Page, req.Limit)}, nil
	}
	consumers, total, err := s.natsClient.ListConsumers(ctx, normalizedLimit(req.Limit), normalizedPage(req.Page))
	if err != nil {
		return response.ListConsumersResponse{}, err
	}
	return response.ListConsumersResponse{Data: consumers, Pagination: pagination(total, req.Page, req.Limit)}, nil
}

func (s *ExplorerService) Overview(ctx context.Context) (response.ExplorerOverviewResponse, error) {
	if s.natsClient == nil {
		return response.ExplorerOverviewResponse{}, nil
	}
	topics, err := s.Topics(ctx, request.ListTopicsRequest{Page: 1, Limit: 500})
	if err != nil {
		return response.ExplorerOverviewResponse{}, err
	}
	consumers, _, err := s.natsClient.ListConsumers(ctx, 500, 1)
	if err != nil {
		return response.ExplorerOverviewResponse{}, err
	}
	dlqMessages, dlqDepth, err := s.natsClient.ListDLQMessages(ctx, 10, 1)
	if err != nil {
		dlqDepth = 0
	}
	var partitionCount uint64
	for _, topic := range topics.Data {
		partitionCount += uint64(topic.PartitionCount)
	}
	var pending uint64
	var ackPending uint64
	for _, consumer := range consumers {
		pending += consumer.NumPending
		ackPending += consumer.NumAckPending
	}
	return response.ExplorerOverviewResponse{
		TopicCount:             uint64(len(topics.Data)),
		PartitionCount:         partitionCount,
		ConsumerCount:          uint64(len(consumers)),
		PendingCount:           pending,
		AckPendingCount:        ackPending,
		DLQDepth:               dlqDepth,
		TopicsNeedingAttention: topics.Data,
		RecentDLQ:              dlqSummaries(dlqMessages),
	}, nil
}

func (s *ExplorerService) TopicDetail(ctx context.Context, req request.TopicDetailRequest) (response.TopicDetailResponse, error) {
	if strings.TrimSpace(req.Topic) == "" {
		return response.TopicDetailResponse{}, fmt.Errorf("%w: topic is required", cdcerrors.ErrValidation)
	}
	partitions, err := s.Partitions(ctx, request.ListPartitionsRequest{Topic: req.Topic, Page: 1, Limit: 500})
	if err != nil {
		return response.TopicDetailResponse{}, err
	}
	summary := response.TopicSummary{
		Name:           req.Topic,
		PartitionCount: int32(len(partitions.Data)),
		Health:         response.ExplorerHealthHealthy,
	}
	for _, partition := range partitions.Data {
		summary.MessageCount += partition.MessageCount
		summary.PendingCount += partition.PendingCount
		summary.AckPendingCount += partition.AckPendingCount
	}
	return response.TopicDetailResponse{
		Summary:    summary,
		Partitions: partitions.Data,
		Scan: response.ScanMetadata{
			ScannedCount: uint64(len(partitions.Data)),
			MatchedCount: uint64(len(partitions.Data)),
			MaxScan:      500,
		},
	}, nil
}

func (s *ExplorerService) PartitionDetail(ctx context.Context, req request.PartitionDetailRequest) (response.PartitionDetailResponse, error) {
	if strings.TrimSpace(req.Topic) == "" || strings.TrimSpace(req.Partition) == "" {
		return response.PartitionDetailResponse{}, fmt.Errorf("%w: topic and partition are required", cdcerrors.ErrValidation)
	}
	if s.natsClient == nil {
		return response.PartitionDetailResponse{}, nil
	}
	messages, err := s.Messages(ctx, request.ListMessagesRequest{
		Topic:     req.Topic,
		Partition: req.Partition,
		Page:      1,
		Limit:     50,
	})
	if err != nil {
		return response.PartitionDetailResponse{}, err
	}
	summary := response.PartitionSummary{
		ID:           req.Partition,
		Topic:        req.Topic,
		MessageCount: messages.TotalCount,
		Health:       response.ExplorerHealthHealthy,
	}
	checkpoints, err := s.checkpoints(ctx, req.Topic)
	if err != nil {
		return response.PartitionDetailResponse{}, err
	}
	return response.PartitionDetailResponse{
		Summary:        summary,
		RecentMessages: messages.Data,
		Checkpoints:    checkpoints,
		Scan:           messages.Scan,
	}, nil
}

func (s *ExplorerService) MessageDetail(ctx context.Context, req request.MessageDetailRequest) (response.MessageDetailResponse, error) {
	if req.Sequence == 0 {
		return response.MessageDetailResponse{}, fmt.Errorf("%w: sequence is required", cdcerrors.ErrValidation)
	}
	if s.natsClient == nil {
		return response.MessageDetailResponse{}, fmt.Errorf("%w: message %d", cdcerrors.ErrNotFound, req.Sequence)
	}
	messages, err := s.Messages(ctx, request.ListMessagesRequest{
		Topic:       req.Topic,
		Partition:   req.Partition,
		SequenceMin: req.Sequence,
		SequenceMax: req.Sequence,
		Page:        1,
		Limit:       1,
	})
	if err != nil {
		return response.MessageDetailResponse{}, err
	}
	if len(messages.Data) == 0 {
		return response.MessageDetailResponse{}, fmt.Errorf("%w: message %d", cdcerrors.ErrNotFound, req.Sequence)
	}
	item := messages.Data[0]
	before, after := beforeAfterPayload(item.Data)
	return response.MessageDetailResponse{
		Item:          item,
		Before:        before,
		After:         after,
		ChangedFields: item.ChangedFields,
	}, nil
}

func (s *ExplorerService) ConsumerDetail(ctx context.Context, req request.ConsumerDetailRequest) (response.ConsumerDetailResponse, error) {
	if strings.TrimSpace(req.ConsumerName) == "" {
		return response.ConsumerDetailResponse{}, fmt.Errorf("%w: consumer_name is required", cdcerrors.ErrValidation)
	}
	if s.natsClient == nil {
		return response.ConsumerDetailResponse{}, fmt.Errorf("%w: consumer %s", cdcerrors.ErrNotFound, req.ConsumerName)
	}
	consumers, err := s.Consumers(ctx, request.ListConsumersRequest{Page: 1, Limit: 500})
	if err != nil {
		return response.ConsumerDetailResponse{}, err
	}
	var summary ports.NATSConsumerSummary
	for _, consumer := range consumers.Data {
		if consumer.Name == req.ConsumerName {
			summary = consumer
			break
		}
	}
	if summary.Name == "" {
		return response.ConsumerDetailResponse{}, fmt.Errorf("%w: consumer %s", cdcerrors.ErrNotFound, req.ConsumerName)
	}
	topics, err := s.Topics(ctx, request.ListTopicsRequest{Page: 1, Limit: 500})
	if err != nil {
		return response.ConsumerDetailResponse{}, err
	}
	recent, err := s.Messages(ctx, request.ListMessagesRequest{Page: 1, Limit: 25})
	if err != nil {
		return response.ConsumerDetailResponse{}, err
	}
	return response.ConsumerDetailResponse{
		Summary:        summary,
		Topics:         topics.Data,
		RecentMessages: recent.Data,
		Scan:           recent.Scan,
	}, nil
}

func topicFromSubject(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) < 4 {
		return subject
	}
	return strings.Join(parts[:4], ".")
}

func (s *ExplorerService) checkpoints(ctx context.Context, topic string) ([]response.CheckpointContext, error) {
	consumers, _, err := s.natsClient.ListConsumers(ctx, 500, 1)
	if err != nil {
		return nil, err
	}
	result := make([]response.CheckpointContext, 0, len(consumers))
	for _, consumer := range consumers {
		if topic != "" && !consumerMatchesTopic(consumer, topic) {
			continue
		}
		lag := consumer.NumPending
		if consumer.DeliveredStreamSeq > consumer.AckFloorStreamSeq {
			lag += consumer.DeliveredStreamSeq - consumer.AckFloorStreamSeq
		}
		result = append(result, response.CheckpointContext{
			ConsumerName:       consumer.Name,
			DeliveredStreamSeq: consumer.DeliveredStreamSeq,
			AckFloorStreamSeq:  consumer.AckFloorStreamSeq,
			NumPending:         consumer.NumPending,
			NumAckPending:      consumer.NumAckPending,
			LagMessages:        lag,
		})
	}
	return result, nil
}

func consumerMatchesTopic(consumer ports.NATSConsumerSummary, topic string) bool {
	if len(consumer.FilterSubjects) == 0 {
		return true
	}
	for _, filter := range consumer.FilterSubjects {
		prefix := strings.TrimSuffix(strings.TrimSuffix(filter, ">"), ".")
		if prefix == "" || prefix == topic || strings.HasPrefix(topic, prefix) || strings.HasPrefix(prefix, topic) {
			return true
		}
	}
	return false
}

func dlqSummaries(messages []*ports.NATSMessageItem) []response.DLQMessageSummary {
	result := make([]response.DLQMessageSummary, 0, len(messages))
	for _, message := range messages {
		result = append(result, response.DLQMessageSummary{
			DLQID:           firstNonEmptyString(message.Headers["Nats-Msg-Id"], message.Headers["X-DLQ-ID"]),
			OriginalSubject: firstNonEmptyString(message.Headers["X-DLQ-Original-Subject"], strings.TrimPrefix(message.Subject, "dlq.")),
			Reason:          message.Headers["X-DLQ-Reason"],
			Timestamp:       unixMillisTime(message.Timestamp),
		})
	}
	return result
}

func unixMillisTime(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}

func normalizedLimit(limit int) int {
	if limit <= 0 {
		return 25
	}
	if limit > 500 {
		return 500
	}
	return limit
}

func normalizedPage(page int) int {
	if page <= 0 {
		return 1
	}
	return page
}

func pagination(total uint64, page int, limit int) response.PaginationResponse {
	page = normalizedPage(page)
	limit = normalizedLimit(limit)
	start := uint64((page - 1) * limit)
	return response.PaginationResponse{
		TotalRows: total,
		Page:      int32(page),
		Limit:     int32(limit),
		HasPrev:   page > 1,
		HasNext:   start+uint64(limit) < total,
	}
}
