package drivergrpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/ports"
)

func (s *CDCService) CreateFlow(ctx context.Context, req *cdcpb.CreateFlowRequest) (*cdcpb.CreateFlowResponse, error) {
	cfg := &ports.FlowConfig{
		Name:        req.Name,
		SourceID:    req.SourceId,
		SinkID:      req.SinkId,
		SourceTable: req.SourceTable,
		SinkTable:   req.SinkTable,
	}

	if req.Options != nil {
		cfg.Options = &ports.FlowOptions{
			BatchSize:        req.Options.BatchSize,
			FlushIntervalMs:  req.Options.FlushIntervalMs,
			FilterExpression: req.Options.FilterExpression,
			PartitionCount:   int(req.Options.PartitionCount),
		}
	}

	if len(req.ColumnMappings) > 0 {
		cfg.ColumnMappings = make([]ports.ColumnMapping, len(req.ColumnMappings))
		for i, cm := range req.ColumnMappings {
			cfg.ColumnMappings[i] = ports.ColumnMapping{
				SourceColumn: cm.SourceColumn,
				SinkColumn:   cm.SinkColumn,
				SourceType:   cm.SourceType,
				SinkType:     cm.SinkType,
				Enabled:      cm.Enabled,
			}
		}
	}

	result, err := s.flowService.Create(ctx, request.CreateFlowRequest{Flow: cfg})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create flow: %v", err)
	}

	return &cdcpb.CreateFlowResponse{
		FlowId: result.FlowID,
		Status: flowStatusToProto(result.Status),
	}, nil
}

func (s *CDCService) GetFlow(ctx context.Context, req *cdcpb.GetFlowRequest) (*cdcpb.GetFlowResponse, error) {
	result, err := s.flowService.Get(ctx, request.GetFlowRequest{FlowID: req.FlowId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "flow not found: %v", err)
	}

	return &cdcpb.GetFlowResponse{
		Flow: flowConfigToProto(result.Flow),
	}, nil
}

func (s *CDCService) ListFlows(ctx context.Context, req *cdcpb.ListFlowsRequest) (*cdcpb.ListFlowsResponse, error) {
	result, err := s.flowService.List(ctx, request.ListFlowsRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list flows: %v", err)
	}

	var pbFlows []*cdcpb.FlowConfig
	for _, f := range result.Flows {
		pbFlows = append(pbFlows, flowConfigToProto(f))
	}

	return &cdcpb.ListFlowsResponse{Flows: pbFlows}, nil
}

func (s *CDCService) UpdateFlow(ctx context.Context, req *cdcpb.UpdateFlowRequest) (*cdcpb.UpdateFlowResponse, error) {
	cfg := &ports.FlowConfig{
		FlowID:      req.FlowId,
		Name:        req.Name,
		SourceTable: req.SourceTable,
		SinkTable:   req.SinkTable,
	}

	if req.Options != nil {
		cfg.Options = &ports.FlowOptions{
			BatchSize:        req.Options.BatchSize,
			FlushIntervalMs:  req.Options.FlushIntervalMs,
			FilterExpression: req.Options.FilterExpression,
			PartitionCount:   int(req.Options.PartitionCount),
		}
	}

	if len(req.ColumnMappings) > 0 {
		cfg.ColumnMappings = make([]ports.ColumnMapping, len(req.ColumnMappings))
		for i, cm := range req.ColumnMappings {
			cfg.ColumnMappings[i] = ports.ColumnMapping{
				SourceColumn: cm.SourceColumn,
				SinkColumn:   cm.SinkColumn,
				SourceType:   cm.SourceType,
				SinkType:     cm.SinkType,
				Enabled:      cm.Enabled,
			}
		}
	}

	result, err := s.flowService.Update(ctx, request.UpdateFlowRequest{Flow: cfg})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update flow: %v", err)
	}

	return &cdcpb.UpdateFlowResponse{
		Flow: flowConfigToProto(result.Flow),
	}, nil
}

func (s *CDCService) DeleteFlow(ctx context.Context, req *cdcpb.DeleteFlowRequest) (*cdcpb.DeleteFlowResponse, error) {
	result, err := s.flowService.Delete(ctx, request.DeleteFlowRequest{FlowID: req.FlowId})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete flow: %v", err)
	}
	return &cdcpb.DeleteFlowResponse{Success: result.Success}, nil
}

func (s *CDCService) PauseFlow(ctx context.Context, req *cdcpb.PauseFlowRequest) (*cdcpb.PauseFlowResponse, error) {
	result, err := s.flowService.Pause(ctx, request.PauseFlowRequest{FlowID: req.FlowId})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to pause flow: %v", err)
	}
	return &cdcpb.PauseFlowResponse{
		Status: flowStatusToProto(result.Status),
	}, nil
}

func (s *CDCService) ResumeFlow(ctx context.Context, req *cdcpb.ResumeFlowRequest) (*cdcpb.ResumeFlowResponse, error) {
	result, err := s.flowService.Resume(ctx, request.ResumeFlowRequest{FlowID: req.FlowId})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "failed to resume flow: %v", err)
	}
	return &cdcpb.ResumeFlowResponse{
		Status: flowStatusToProto(result.Status),
	}, nil
}

func (s *CDCService) GetFlowStats(ctx context.Context, req *cdcpb.GetFlowStatsRequest) (*cdcpb.GetFlowStatsResponse, error) {
	result, err := s.flowService.Stats(ctx, request.GetFlowStatsRequest{FlowID: req.FlowId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "flow stats not found: %v", err)
	}
	stats := result.Stats

	return &cdcpb.GetFlowStatsResponse{
		EventsPerSecond:      stats.EventsPerSecond,
		ReplicationLagMs:     stats.ReplicationLagMs,
		TotalEventsProcessed: stats.TotalEventsProcessed,
		RunningWorkers:       stats.RunningWorkers,
		PoolCapacity:         stats.PoolCapacity,
		WorkerUtilization:    stats.WorkerUtilization,
		FailureCount:         stats.FailureCount,
		DlqCount:             stats.DLQCount,
		FilteredCount:        stats.FilteredCount,
		LastError:            stats.LastError,
	}, nil
}
