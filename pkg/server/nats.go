package server

import (
	"context"
	"strconv"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/utils"
)

func (s *GRPCService) GetStats(_ context.Context, _ *cdcpb.GetStatsRequest) (*cdcpb.GetStatsResponse, error) {
	srcStats, snkStats := s.engine.GetStats()

	resp := &cdcpb.GetStatsResponse{
		SourceStats: make(map[string]*cdcpb.ComponentStats),
		SinkStats:   make(map[string]*cdcpb.ComponentStats),
	}

	for k, v := range srcStats {
		resp.SourceStats[k] = &cdcpb.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	for k, v := range snkStats {
		resp.SinkStats[k] = &cdcpb.ComponentStats{
			SuccessCount: v.SuccessCount,
			FailureCount: v.FailureCount,
			LastError:    v.LastError,
		}
	}

	return resp, nil
}
func (s *GRPCService) ListMessages(ctx context.Context, req *cdcpb.ListMessagesRequest) (*cdcpb.ListMessagesResponse, error) {
	limit, page, sort := s.getPaginationParams(req.Pagination)
	messages, totalCount, err := s.engine.ListMessages(ctx, req.Status, int(limit), int(page), req.GetTopic(), req.GetPartition())
	if err != nil {
		return nil, err
	}

	pbMessages := make([]*cdcpb.MessageItem, len(messages))
	for i, msg := range messages {
		pbMessages[i] = &cdcpb.MessageItem{
			Sequence:  msg.Sequence,
			Timestamp: strconv.FormatInt(msg.Timestamp, 10),
			Subject:   msg.Subject,
			Data:      msg.Data,
			Headers:   msg.Headers,
		}
	}

	return &cdcpb.ListMessagesResponse{
		Data:       pbMessages,
		TotalCount: totalCount,
		Pagination: s.getPaginationResponse(totalCount, limit, page, sort),
	}, nil
}

func (s *GRPCService) GetConsumerInfo(ctx context.Context, req *cdcpb.GetConsumerInfoRequest) (*cdcpb.GetConsumerInfoResponse, error) {
	consumerName := req.GetConsumerName()
	if consumerName == "" {
		consumerName = "pipeline-worker"
	}
	ackFloor, pendingCount, err := s.engine.GetConsumerInfo(ctx, consumerName)
	if err != nil {
		return nil, err
	}

	return &cdcpb.GetConsumerInfoResponse{
		AckFloor:     ackFloor,
		PendingCount: pendingCount,
	}, nil
}

func (s *GRPCService) ListTopics(ctx context.Context, req *cdcpb.ListTopicsRequest) (*cdcpb.ListTopicsResponse, error) {
	limit, page, sort := s.getPaginationParams(req.Pagination)
	topics, total, err := s.engine.ListTopics(ctx, int(limit), int(page))
	if err != nil {
		return nil, err
	}

	summaries := make([]*cdcpb.TopicSummary, len(topics))
	for i, t := range topics {
		summaries[i] = &cdcpb.TopicSummary{
			Name:           t,
			MessageCount:   0,
			PartitionCount: 0,
		}
	}

	return &cdcpb.ListTopicsResponse{
		Data:       summaries,
		Pagination: s.getPaginationResponse(total, limit, page, sort),
	}, nil
}

func (s *GRPCService) ListPartitions(ctx context.Context, req *cdcpb.ListPartitionsRequest) (*cdcpb.ListPartitionsResponse, error) {
	limit, page, sort := s.getPaginationParams(req.Pagination)
	partitions, total, err := s.engine.ListPartitions(ctx, req.GetTopic(), int(limit), int(page))
	if err != nil {
		return nil, err
	}
	return &cdcpb.ListPartitionsResponse{
		Data: utils.Map(partitions, func(p string, _ int) *cdcpb.PartitionSummary {
			return toPartitionProto(p, req.GetTopic())
		}),
		Pagination: s.getPaginationResponse(total, limit, page, sort),
	}, nil
}

func (s *GRPCService) ReprocessDLQ(ctx context.Context, _ *cdcpb.ReprocessDLQRequest) (*cdcpb.ReprocessDLQResponse, error) {
	count, err := s.engine.ReprocessDLQ(ctx)
	if err != nil {
		return nil, err
	}
	return &cdcpb.ReprocessDLQResponse{
		Count: int32(count),
	}, nil
}


func (s *GRPCService) getPaginationParams(req *cdcpb.OffsetPaginationRequest) (uint32, uint32, []*cdcpb.Sort) {
	limit := uint32(20)
	page := uint32(1)
	var sort []*cdcpb.Sort

	if req != nil {
		if req.Limit > 0 {
			if req.Limit > 100 { // Max limit validation
				limit = 100
			} else {
				limit = req.Limit
			}
		}
		if req.Page > 0 {
			page = req.Page
		}
		if len(req.Sort) > 0 {
			sort = req.Sort
		}
	}
	return limit, page, sort
}

func (s *GRPCService) getPaginationResponse(total uint64, limit uint32, page uint32, sort []*cdcpb.Sort) *cdcpb.OffsetPaginationResponse {
	totalPages := uint64(0)
	if limit > 0 {
		totalPages = (total + uint64(limit) - 1) / uint64(limit)
	}

	hasNext := uint64(page) < totalPages
	hasPrev := page > 1

	return &cdcpb.OffsetPaginationResponse{
		TotalRows: total,
		Limit:     limit,
		Page:      page,
		HasNext:   hasNext,
		HasPrev:   hasPrev,
		Sort:      sort,
	}
}
