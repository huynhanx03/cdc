package drivergrpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/internal/core/dto/request"
)

func (s *CDCService) CreateSink(ctx context.Context, req *cdcpb.CreateSinkRequest) (*cdcpb.CreateSinkResponse, error) {
	result, err := s.sinkService.Create(ctx, request.CreateSinkRequest{Sink: protoToSinkConfig(req.Sink)})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist sink: %v", err)
	}

	return &cdcpb.CreateSinkResponse{
		InstanceId: result.InstanceID,
	}, nil
}

func (s *CDCService) GetSink(ctx context.Context, req *cdcpb.GetSinkRequest) (*cdcpb.GetSinkResponse, error) {
	result, err := s.sinkService.Get(ctx, request.GetSinkRequest{InstanceID: req.InstanceId})
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "sink not found: %v", err)
	}

	return &cdcpb.GetSinkResponse{
		Sink: sinkConfigToProto(result.Sink),
	}, nil
}

func (s *CDCService) ListSinks(ctx context.Context, req *cdcpb.ListSinksRequest) (*cdcpb.ListSinksResponse, error) {
	result, err := s.sinkService.List(ctx, request.ListSinksRequest{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list sinks: %v", err)
	}

	var pbSinks []*cdcpb.SinkConfig
	for _, snk := range result.Sinks {
		pbSinks = append(pbSinks, sinkConfigToProto(snk))
	}

	return &cdcpb.ListSinksResponse{Sinks: pbSinks}, nil
}

func (s *CDCService) DeleteSink(ctx context.Context, req *cdcpb.DeleteSinkRequest) (*cdcpb.DeleteSinkResponse, error) {
	result, err := s.sinkService.Delete(ctx, request.DeleteSinkRequest{InstanceID: req.InstanceId})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete sink: %v", err)
	}
	return &cdcpb.DeleteSinkResponse{Success: result.Success}, nil
}

func (s *CDCService) TestSinkConnection(ctx context.Context, req *cdcpb.TestSinkConnectionRequest) (*cdcpb.TestSinkConnectionResponse, error) {
	result, err := s.sinkService.TestConnection(ctx, request.TestSinkConnectionRequest{
		Sink: protoToSinkConfig(req.Sink),
	})
	if grpcErr := invalidArgumentIfRequired(err); grpcErr != nil {
		return nil, grpcErr
	}

	return &cdcpb.TestSinkConnectionResponse{
		Success:   result.Success,
		Message:   result.Message,
		LatencyMs: result.LatencyMs,
	}, nil
}
