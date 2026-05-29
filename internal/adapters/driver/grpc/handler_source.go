package drivergrpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/internal/core/dto/request"
)

func (s *CDCService) CreateSource(ctx context.Context, req *cdcpb.CreateSourceRequest) (*cdcpb.CreateSourceResponse, error) {
	result, err := s.sourceService.Create(ctx, request.CreateSourceRequest{Source: protoToSourceConfig(req.Source)})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist source: %v", err)
	}

	return &cdcpb.CreateSourceResponse{
		InstanceId: result.InstanceID,
	}, nil
}

func (s *CDCService) GetSource(ctx context.Context, req *cdcpb.GetSourceRequest) (*cdcpb.GetSourceResponse, error) {
	result, err := s.sourceService.Get(ctx, request.GetSourceRequest{InstanceID: req.InstanceId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "source not found: %v", err)
	}

	return &cdcpb.GetSourceResponse{
		Source: sourceConfigToProto(result.Source),
	}, nil
}

func (s *CDCService) ListSources(ctx context.Context, req *cdcpb.ListSourcesRequest) (*cdcpb.ListSourcesResponse, error) {
	result, err := s.sourceService.List(ctx, request.ListSourcesRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list sources: %v", err)
	}

	var pbSources []*cdcpb.SourceConfig
	for _, src := range result.Sources {
		pbSources = append(pbSources, sourceConfigToProto(src))
	}

	return &cdcpb.ListSourcesResponse{Sources: pbSources}, nil
}

func (s *CDCService) DeleteSource(ctx context.Context, req *cdcpb.DeleteSourceRequest) (*cdcpb.DeleteSourceResponse, error) {
	result, err := s.sourceService.Delete(ctx, request.DeleteSourceRequest{InstanceID: req.InstanceId})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete source: %v", err)
	}
	return &cdcpb.DeleteSourceResponse{Success: result.Success}, nil
}

func (s *CDCService) TestSourceConnection(ctx context.Context, req *cdcpb.TestSourceConnectionRequest) (*cdcpb.TestSourceConnectionResponse, error) {
	result, err := s.sourceService.TestConnection(ctx, request.TestSourceConnectionRequest{
		Source: protoToSourceConfig(req.Source),
	})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}

	return &cdcpb.TestSourceConnectionResponse{
		Success:   result.Success,
		Message:   result.Message,
		LatencyMs: result.LatencyMs,
	}, nil
}
