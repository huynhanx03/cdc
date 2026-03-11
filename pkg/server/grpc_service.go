package server

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/registry"
	"github.com/foden/cdc/pkg/wal"
	"github.com/foden/cdc/version"
)

type GRPCService struct {
	cdcpb.UnimplementedCDCServiceServer
	appCfg *config.Config

	manager   *wal.Manager[*models.Event]
	startTime time.Time
}

func NewGRPCService(appCfg *config.Config, manager *wal.Manager[*models.Event]) *GRPCService {
	return &GRPCService{
		appCfg:    appCfg,
		manager:   manager,
		startTime: time.Now(),
	}
}

func (s *GRPCService) ListTopics(_ context.Context, _ *cdcpb.ListTopicsRequest) (*cdcpb.ListTopicsResponse, error) {

	ids := s.manager.GetPartitionIDs()

	topicMap := make(map[string]*cdcpb.TopicSummary)

	for _, id := range ids {

		msgs, _ := s.manager.InspectRaw(id, 1)
		topicName := "default"

		if len(msgs) > 0 && len(msgs[0].Key) > 0 {
			topicName = string(msgs[0].Key)
		}

		ts, ok := topicMap[topicName]
		if !ok {
			ts = &cdcpb.TopicSummary{Name: topicName}
			topicMap[topicName] = ts
		}

		ts.PartitionCount++
	}

	resp := &cdcpb.ListTopicsResponse{}
	for _, t := range topicMap {
		resp.Topics = append(resp.Topics, t)
	}

	sort.Slice(resp.Topics, func(i, j int) bool {
		return resp.Topics[i].Name < resp.Topics[j].Name
	})

	return resp, nil
}

func (s *GRPCService) GetTopic(_ context.Context, req *cdcpb.GetTopicRequest) (*cdcpb.TopicDetail, error) {

	ids := s.manager.GetPartitionIDs()
	sort.Ints(ids)

	detail := &cdcpb.TopicDetail{
		Name: req.Name,
	}

	for _, id := range ids {

		stats := s.partitionSummary(id)
		detail.Partitions = append(detail.Partitions, stats)
	}

	detail.PartitionCount = int32(len(detail.Partitions))

	return detail, nil
}

func (s *GRPCService) ListPartitions(_ context.Context, _ *cdcpb.ListPartitionsRequest) (*cdcpb.ListPartitionsResponse, error) {

	ids := s.manager.GetPartitionIDs()
	sort.Ints(ids)

	resp := &cdcpb.ListPartitionsResponse{}

	for _, id := range ids {
		resp.Partitions = append(resp.Partitions, s.partitionSummary(id))
	}

	return resp, nil
}

func (s *GRPCService) GetPartition(_ context.Context, req *cdcpb.GetPartitionRequest) (*cdcpb.PartitionDetail, error) {

	ps := s.partitionSummary(int(req.Id))

	detail := &cdcpb.PartitionDetail{
		Id:             ps.Id,
		SizeBytes:      ps.SizeBytes,
		SegmentCount:   ps.SegmentCount,
		EarliestOffset: ps.EarliestOffset,
		LatestOffset:   ps.LatestOffset,
	}

	return detail, nil
}

func (s *GRPCService) GetMessages(_ context.Context, req *cdcpb.GetMessagesRequest) (*cdcpb.GetMessagesResponse, error) {

	limit := int(req.Limit)
	if limit <= 0 || limit > 500 {
		limit = 20
	}

	var allMsgs []wal.WalMessage[*models.Event]

	if req.PartitionId == nil {
		// Global view: fetch from all partitions and sort by timestamp
		ids := s.manager.GetPartitionIDs()
		for _, id := range ids {
			msgs, _ := s.manager.InspectRaw(id, limit)
			allMsgs = append(allMsgs, msgs...)
		}

		// Sort by timestamp descending (newest first)
		sort.Slice(allMsgs, func(i, j int) bool {
			return allMsgs[i].Timestamp.After(allMsgs[j].Timestamp)
		})

		// Trim to limit
		if len(allMsgs) > limit {
			allMsgs = allMsgs[:limit]
		}
	} else {
		// Single partition view
		partID := int(*req.PartitionId)
		msgs, err := s.manager.InspectRaw(partID, limit)
		if err != nil {
			return nil, err
		}
		allMsgs = msgs
	}

	resp := &cdcpb.GetMessagesResponse{}

	for _, m := range allMsgs {
		val, _ := json.Marshal(m.Item)

		resp.Messages = append(resp.Messages, &cdcpb.MessageItem{
			Offset:    m.Offset,
			Timestamp: m.Timestamp.UnixMilli(),
			Key:       string(m.Key),
			Value:     val,
		})
	}

	return resp, nil
}

func (s *GRPCService) GetStats(_ context.Context, _ *cdcpb.GetStatsRequest) (*cdcpb.StatsResponse, error) {

	st := s.manager.GetTotalStats()
	ids := s.manager.GetPartitionIDs()

	return &cdcpb.StatsResponse{
		TotalEnqueued:  st.TotalEnqueued,
		TotalDequeued:  st.TotalDequeued,
		Pending:        st.Pending,
		SegmentsCount:  int32(st.SegmentsCount),
		TotalSizeMb:    st.TotalSizeMB,
		PartitionCount: int32(len(ids)),
	}, nil
}

func (s *GRPCService) HealthCheck(_ context.Context, _ *cdcpb.HealthCheckRequest) (*cdcpb.HealthCheckResponse, error) {

	return &cdcpb.HealthCheckResponse{
		Status:  "ok",
		Version: version.Version,
		Uptime:  int64(time.Since(s.startTime).Seconds()),
	}, nil
}

func (s *GRPCService) GetConfig(_ context.Context, _ *cdcpb.GetConfigRequest) (*cdcpb.GetConfigResponse, error) {

	resp := &cdcpb.GetConfigResponse{
		AvailableSources: registry.SourceNames(),
		AvailableSinks:   registry.SinkNames(),
		Config: &cdcpb.AppConfig{
			Name:    s.appCfg.Name,
			LogMode: s.appCfg.LogMode,
			Source: &cdcpb.SourceConfig{
				Type:     s.appCfg.Source.Type,
				Host:     s.appCfg.Source.Host,
				Port:     int32(s.appCfg.Source.Port),
				Database: s.appCfg.Source.Database,
				Tables:   s.appCfg.Source.Tables,
			},
		},
	}

	for _, sc := range s.appCfg.Sinks {
		resp.Config.Sinks = append(resp.Config.Sinks, &cdcpb.SinkConfig{
			Type:        sc.Type,
			Url:         sc.URL,
			IndexPrefix: sc.IndexPrefix,
		})
	}

	return resp, nil
}

func (s *GRPCService) partitionSummary(id int) *cdcpb.PartitionSummary {

	q, err := s.manager.GetPartition(id)
	if err != nil {
		return &cdcpb.PartitionSummary{Id: int32(id)}
	}

	stats := q.GetStats()

	return &cdcpb.PartitionSummary{
		Id:           int32(id),
		SizeBytes:    stats.TotalSizeMB * 1024 * 1024,
		SegmentCount: int32(stats.SegmentsCount),
		LatestOffset: stats.TotalEnqueued,
	}
}
