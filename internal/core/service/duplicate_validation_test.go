package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/dto/request"
	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
)

type serviceTestStore struct {
	sources map[string]*ports.SourceConfig
	sinks   map[string]*ports.SinkConfig
	flows   map[string]*ports.FlowConfig
}

func newServiceTestStore() *serviceTestStore {
	return &serviceTestStore{
		sources: make(map[string]*ports.SourceConfig),
		sinks:   make(map[string]*ports.SinkConfig),
		flows:   make(map[string]*ports.FlowConfig),
	}
}

func (s *serviceTestStore) PutSource(_ context.Context, cfg *ports.SourceConfig) error {
	s.sources[cfg.InstanceID] = cfg
	return nil
}
func (s *serviceTestStore) GetSource(_ context.Context, id string) (*ports.SourceConfig, error) {
	source := s.sources[id]
	if source == nil {
		return nil, fmt.Errorf("%w: source %q", cdcerrors.ErrNotFound, id)
	}
	return source, nil
}
func (s *serviceTestStore) DeleteSource(_ context.Context, id string) error {
	if s.sources[id] == nil {
		return fmt.Errorf("%w: source %q", cdcerrors.ErrNotFound, id)
	}
	delete(s.sources, id)
	return nil
}
func (s *serviceTestStore) ListSources(_ context.Context) ([]*ports.SourceConfig, error) {
	result := make([]*ports.SourceConfig, 0, len(s.sources))
	for _, source := range s.sources {
		result = append(result, source)
	}
	return result, nil
}
func (s *serviceTestStore) PutSink(_ context.Context, cfg *ports.SinkConfig) error {
	s.sinks[cfg.InstanceID] = cfg
	return nil
}
func (s *serviceTestStore) GetSink(_ context.Context, id string) (*ports.SinkConfig, error) {
	sink := s.sinks[id]
	if sink == nil {
		return nil, fmt.Errorf("%w: sink %q", cdcerrors.ErrNotFound, id)
	}
	return sink, nil
}
func (s *serviceTestStore) DeleteSink(_ context.Context, id string) error {
	if s.sinks[id] == nil {
		return fmt.Errorf("%w: sink %q", cdcerrors.ErrNotFound, id)
	}
	delete(s.sinks, id)
	return nil
}
func (s *serviceTestStore) ListSinks(_ context.Context) ([]*ports.SinkConfig, error) {
	result := make([]*ports.SinkConfig, 0, len(s.sinks))
	for _, sink := range s.sinks {
		result = append(result, sink)
	}
	return result, nil
}
func (s *serviceTestStore) PutFlow(_ context.Context, cfg *ports.FlowConfig) error {
	s.flows[cfg.FlowID] = cfg
	return nil
}
func (s *serviceTestStore) GetFlow(_ context.Context, id string) (*ports.FlowConfig, error) {
	flow := s.flows[id]
	if flow == nil {
		return nil, fmt.Errorf("%w: flow %q", cdcerrors.ErrNotFound, id)
	}
	return flow, nil
}
func (s *serviceTestStore) DeleteFlow(_ context.Context, id string) error {
	if s.flows[id] == nil {
		return fmt.Errorf("%w: flow %q", cdcerrors.ErrNotFound, id)
	}
	delete(s.flows, id)
	return nil
}
func (s *serviceTestStore) ListFlows(_ context.Context) ([]*ports.FlowConfig, error) {
	result := make([]*ports.FlowConfig, 0, len(s.flows))
	for _, flow := range s.flows {
		result = append(result, flow)
	}
	return result, nil
}
func (s *serviceTestStore) SaveCheckpoint(context.Context, *domain.Checkpoint) error {
	return nil
}
func (s *serviceTestStore) GetCheckpoint(context.Context, string) (*domain.Checkpoint, error) {
	return nil, nil
}
func (s *serviceTestStore) SaveSourceOffset(context.Context, string, string) error {
	return nil
}
func (s *serviceTestStore) GetSourceOffset(context.Context, string) (string, error) {
	return "", nil
}

func TestSourceCreateRejectsDuplicateInstanceID(t *testing.T) {
	store := newServiceTestStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres", Host: "a", Port: 5432, Database: "db"}
	svc := NewSourceService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSourceRequest{Source: &ports.SourceConfig{
		InstanceID: "src-1", Type: "postgres", Host: "b", Port: 5432, Database: "other",
	}})

	if err == nil || !strings.Contains(err.Error(), "source instance_id") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceGetMissingReturnsTypedNotFound(t *testing.T) {
	svc := NewSourceService(newServiceTestStore(), nil)

	_, err := svc.Get(context.Background(), request.GetSourceRequest{InstanceID: "missing"})

	if !errors.Is(err, cdcerrors.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSinkDeleteMissingReturnsTypedNotFound(t *testing.T) {
	svc := NewSinkService(newServiceTestStore(), nil)

	_, err := svc.Delete(context.Background(), request.DeleteSinkRequest{InstanceID: "missing"})

	if !errors.Is(err, cdcerrors.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestSourceCreateRejectsDuplicateName(t *testing.T) {
	store := newServiceTestStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Name: "Primary DB", Type: "postgres", Host: "a", Port: 5432, Database: "db"}
	svc := NewSourceService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSourceRequest{Source: &ports.SourceConfig{
		Name: " primary db ", Type: "postgres", Host: "b", Port: 5432, Database: "other",
	}})

	if err == nil || !strings.Contains(err.Error(), "source name") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourceCreateRejectsDuplicateEndpoint(t *testing.T) {
	store := newServiceTestStore()
	store.sources["src-1"] = &ports.SourceConfig{InstanceID: "src-1", Type: "postgres", Host: "DB.local", Port: 5432, Username: "cdc", Database: "app"}
	svc := NewSourceService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSourceRequest{Source: &ports.SourceConfig{
		Type: "POSTGRES", Host: " db.local ", Port: 5432, Username: "cdc", Database: "app",
	}})

	if err == nil || !strings.Contains(err.Error(), "source endpoint") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkCreateRejectsDuplicateInstanceID(t *testing.T) {
	store := newServiceTestStore()
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "postgres", Host: "a", Port: 5432, Database: "db"}
	svc := NewSinkService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSinkRequest{Sink: &ports.SinkConfig{
		InstanceID: "sink-1", Type: "postgres", Host: "b", Port: 5432, Database: "other",
	}})

	if err == nil || !strings.Contains(err.Error(), "sink instance_id") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkCreateRejectsDuplicateName(t *testing.T) {
	store := newServiceTestStore()
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Name: "Warehouse", Type: "clickhouse", Host: "a", Port: 9000, Database: "db"}
	svc := NewSinkService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSinkRequest{Sink: &ports.SinkConfig{
		Name: "warehouse", Type: "clickhouse", Host: "b", Port: 9000, Database: "other",
	}})

	if err == nil || !strings.Contains(err.Error(), "sink name") {
		t.Fatalf("err = %v", err)
	}
}

func TestSinkCreateRejectsDuplicateEndpoint(t *testing.T) {
	store := newServiceTestStore()
	store.sinks["sink-1"] = &ports.SinkConfig{InstanceID: "sink-1", Type: "elasticsearch", URL: []string{"https://es-1:9200", "https://es-2:9200"}, IndexPrefix: "cdc"}
	svc := NewSinkService(store, nil)

	_, err := svc.Create(context.Background(), request.CreateSinkRequest{Sink: &ports.SinkConfig{
		Type: "elasticsearch", URL: []string{" https://es-2:9200 ", "https://es-1:9200"}, IndexPrefix: "CDC",
	}})

	if err == nil || !strings.Contains(err.Error(), "sink endpoint") {
		t.Fatalf("err = %v", err)
	}
}
