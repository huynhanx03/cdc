package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/foden/cdc/internal/core/domain"
	"github.com/foden/cdc/internal/core/ports"
	cdcerrors "github.com/foden/cdc/pkg/errors"
	"github.com/nats-io/nats.go/jetstream"
)

// Compile-time check that NATSKVStore implements ports.Store.
var _ ports.Store = (*NATSKVStore)(nil)

// NATSKVStore implements ports.Store using NATS JetStream KV.
type NATSKVStore struct {
	js     jetstream.JetStream
	bucket jetstream.KeyValue
}

// NewNATSKVStore creates a new NATSKVStore, creating or binding to the CDC_STATE bucket.
func NewNATSKVStore(ctx context.Context, js jetstream.JetStream) (*NATSKVStore, error) {
	bucket, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: BucketName,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: create/get KV bucket %q: %w", BucketName, err)
	}
	return &NATSKVStore{js: js, bucket: bucket}, nil
}

// --- Source CRUD ---

func (s *NATSKVStore) PutSource(ctx context.Context, cfg *ports.SourceConfig) error {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("storage: marshal source %q: %w", cfg.InstanceID, err)
	}
	key := PrefixSources + cfg.InstanceID
	if _, err := s.bucket.Put(ctx, key, data); err != nil {
		return fmt.Errorf("storage: put source %q: %w", cfg.InstanceID, err)
	}
	return nil
}

func (s *NATSKVStore) GetSource(ctx context.Context, instanceID string) (*ports.SourceConfig, error) {
	key := PrefixSources + instanceID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: storage source %q", cdcerrors.ErrNotFound, instanceID)
		}
		return nil, fmt.Errorf("storage: get source %q: %w", instanceID, err)
	}
	var cfg ports.SourceConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, fmt.Errorf("storage: unmarshal source %q: %w", instanceID, err)
	}
	return &cfg, nil
}

func (s *NATSKVStore) DeleteSource(ctx context.Context, instanceID string) error {
	key := PrefixSources + instanceID
	if err := s.bucket.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("%w: storage source %q", cdcerrors.ErrNotFound, instanceID)
		}
		return fmt.Errorf("storage: delete source %q: %w", instanceID, err)
	}
	return nil
}

func (s *NATSKVStore) ListSources(ctx context.Context) ([]*ports.SourceConfig, error) {
	keys, err := s.bucket.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("storage: list source keys: %w", err)
	}

	sources := make([]*ports.SourceConfig, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, PrefixSources) {
			continue
		}
		entry, err := s.bucket.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("storage: get source key %q: %w", key, err)
		}
		var cfg ports.SourceConfig
		if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
			return nil, fmt.Errorf("storage: unmarshal source key %q: %w", key, err)
		}
		sources = append(sources, &cfg)
	}
	return sources, nil
}

// --- Sink CRUD ---

func (s *NATSKVStore) PutSink(ctx context.Context, cfg *ports.SinkConfig) error {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("storage: marshal sink %q: %w", cfg.InstanceID, err)
	}
	key := PrefixSinks + cfg.InstanceID
	if _, err := s.bucket.Put(ctx, key, data); err != nil {
		return fmt.Errorf("storage: put sink %q: %w", cfg.InstanceID, err)
	}
	return nil
}

func (s *NATSKVStore) GetSink(ctx context.Context, instanceID string) (*ports.SinkConfig, error) {
	key := PrefixSinks + instanceID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: storage sink %q", cdcerrors.ErrNotFound, instanceID)
		}
		return nil, fmt.Errorf("storage: get sink %q: %w", instanceID, err)
	}
	var cfg ports.SinkConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, fmt.Errorf("storage: unmarshal sink %q: %w", instanceID, err)
	}
	return &cfg, nil
}

func (s *NATSKVStore) DeleteSink(ctx context.Context, instanceID string) error {
	key := PrefixSinks + instanceID
	if err := s.bucket.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("%w: storage sink %q", cdcerrors.ErrNotFound, instanceID)
		}
		return fmt.Errorf("storage: delete sink %q: %w", instanceID, err)
	}
	return nil
}

func (s *NATSKVStore) ListSinks(ctx context.Context) ([]*ports.SinkConfig, error) {
	keys, err := s.bucket.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("storage: list sink keys: %w", err)
	}

	sinks := make([]*ports.SinkConfig, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, PrefixSinks) {
			continue
		}
		entry, err := s.bucket.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("storage: get sink key %q: %w", key, err)
		}
		var cfg ports.SinkConfig
		if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
			return nil, fmt.Errorf("storage: unmarshal sink key %q: %w", key, err)
		}
		sinks = append(sinks, &cfg)
	}
	return sinks, nil
}

// --- Flow CRUD ---

func (s *NATSKVStore) PutFlow(ctx context.Context, cfg *ports.FlowConfig) error {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("storage: marshal flow %q: %w", cfg.FlowID, err)
	}
	key := PrefixFlows + cfg.FlowID
	if _, err := s.bucket.Put(ctx, key, data); err != nil {
		return fmt.Errorf("storage: put flow %q: %w", cfg.FlowID, err)
	}
	return nil
}

func (s *NATSKVStore) GetFlow(ctx context.Context, flowID string) (*ports.FlowConfig, error) {
	key := PrefixFlows + flowID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: storage flow %q", cdcerrors.ErrNotFound, flowID)
		}
		return nil, fmt.Errorf("storage: get flow %q: %w", flowID, err)
	}
	var cfg ports.FlowConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, fmt.Errorf("storage: unmarshal flow %q: %w", flowID, err)
	}
	return &cfg, nil
}

func (s *NATSKVStore) DeleteFlow(ctx context.Context, flowID string) error {
	key := PrefixFlows + flowID
	if err := s.bucket.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("%w: storage flow %q", cdcerrors.ErrNotFound, flowID)
		}
		return fmt.Errorf("storage: delete flow %q: %w", flowID, err)
	}
	return nil
}

func (s *NATSKVStore) ListFlows(ctx context.Context) ([]*ports.FlowConfig, error) {
	keys, err := s.bucket.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("storage: list flow keys: %w", err)
	}

	flows := make([]*ports.FlowConfig, 0, len(keys))
	for _, key := range keys {
		if !strings.HasPrefix(key, PrefixFlows) {
			continue
		}
		entry, err := s.bucket.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("storage: get flow key %q: %w", key, err)
		}
		var cfg ports.FlowConfig
		if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
			return nil, fmt.Errorf("storage: unmarshal flow key %q: %w", key, err)
		}
		flows = append(flows, &cfg)
	}
	return flows, nil
}

// --- Checkpoints and source offsets ---

func (s *NATSKVStore) SaveCheckpoint(ctx context.Context, checkpoint *domain.Checkpoint) error {
	if checkpoint == nil {
		return fmt.Errorf("storage: checkpoint is nil")
	}
	if checkpoint.FlowID == "" {
		return fmt.Errorf("storage: checkpoint flow_id is required")
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}
	data, err := sonic.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("storage: marshal checkpoint for flow %q: %w", checkpoint.FlowID, err)
	}
	key, err := CheckpointKey(checkpoint)
	if err != nil {
		return fmt.Errorf("storage: checkpoint key for flow %q: %w", checkpoint.FlowID, err)
	}
	if _, err := s.bucket.Put(ctx, key, data); err != nil {
		return fmt.Errorf("storage: save checkpoint for flow %q: %w", checkpoint.FlowID, err)
	}
	return nil
}

func (s *NATSKVStore) GetCheckpoint(ctx context.Context, flowID string) (*domain.Checkpoint, error) {
	entry, err := s.latestCheckpointEntry(ctx, flowID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("storage: get checkpoint for flow %q: %w", flowID, err)
	}
	var checkpoint domain.Checkpoint
	if err := sonic.Unmarshal(entry.Value(), &checkpoint); err != nil {
		return nil, fmt.Errorf("storage: unmarshal checkpoint for flow %q: %w", flowID, err)
	}
	return &checkpoint, nil
}

func (s *NATSKVStore) latestCheckpointEntry(ctx context.Context, flowID string) (jetstream.KeyValueEntry, error) {
	keys, err := s.bucket.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, fmt.Errorf("storage: list checkpoint keys: %w", err)
	}

	prefix := PrefixCheckpoints + flowID + "."
	var latest jetstream.KeyValueEntry
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := s.bucket.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("storage: get checkpoint key %q: %w", key, err)
		}
		if latest == nil || entry.Created().After(latest.Created()) {
			latest = entry
		}
	}
	if latest != nil {
		return latest, nil
	}

	return s.bucket.Get(ctx, LegacyCheckpointKey(flowID))
}

func (s *NATSKVStore) SaveSourceOffset(ctx context.Context, sourceID string, offset string) error {
	key := PrefixSourceOffsets + sourceID
	if _, err := s.bucket.Put(ctx, key, []byte(offset)); err != nil {
		return fmt.Errorf("storage: save source offset for source %q: %w", sourceID, err)
	}
	return nil
}

func (s *NATSKVStore) GetSourceOffset(ctx context.Context, sourceID string) (string, error) {
	key := PrefixSourceOffsets + sourceID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("storage: get source offset for source %q: %w", sourceID, err)
	}
	return string(entry.Value()), nil
}

// --- Revision-based operations for optimistic concurrency control ---

// PutSourceWithRevision persists a source config only if the current revision matches.
// Use revision 0 for initial creation (key must not exist).
func (s *NATSKVStore) PutSourceWithRevision(ctx context.Context, cfg *ports.SourceConfig, revision uint64) (uint64, error) {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("storage: marshal source %q: %w", cfg.InstanceID, err)
	}
	key := PrefixSources + cfg.InstanceID
	newRevision, err := s.putWithRevision(ctx, key, data, revision)
	if err != nil {
		return 0, fmt.Errorf("storage: put source %q with revision: %w", cfg.InstanceID, err)
	}
	return newRevision, nil
}

// PutSinkWithRevision persists a sink config only if the current revision matches.
func (s *NATSKVStore) PutSinkWithRevision(ctx context.Context, cfg *ports.SinkConfig, revision uint64) (uint64, error) {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("storage: marshal sink %q: %w", cfg.InstanceID, err)
	}
	key := PrefixSinks + cfg.InstanceID
	newRevision, err := s.putWithRevision(ctx, key, data, revision)
	if err != nil {
		return 0, fmt.Errorf("storage: put sink %q with revision: %w", cfg.InstanceID, err)
	}
	return newRevision, nil
}

// PutFlowWithRevision persists a flow config only if the current revision matches.
func (s *NATSKVStore) PutFlowWithRevision(ctx context.Context, cfg *ports.FlowConfig, revision uint64) (uint64, error) {
	data, err := sonic.Marshal(cfg)
	if err != nil {
		return 0, fmt.Errorf("storage: marshal flow %q: %w", cfg.FlowID, err)
	}
	key := PrefixFlows + cfg.FlowID
	newRevision, err := s.putWithRevision(ctx, key, data, revision)
	if err != nil {
		return 0, fmt.Errorf("storage: put flow %q with revision: %w", cfg.FlowID, err)
	}
	return newRevision, nil
}

// GetSourceWithRevision returns the source config along with its current revision.
func (s *NATSKVStore) GetSourceWithRevision(ctx context.Context, instanceID string) (*ports.SourceConfig, uint64, error) {
	key := PrefixSources + instanceID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, fmt.Errorf("%w: storage source %q", cdcerrors.ErrNotFound, instanceID)
		}
		return nil, 0, fmt.Errorf("storage: get source %q: %w", instanceID, err)
	}
	var cfg ports.SourceConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, 0, fmt.Errorf("storage: unmarshal source %q: %w", instanceID, err)
	}
	return &cfg, entry.Revision(), nil
}

// GetSinkWithRevision returns the sink config along with its current revision.
func (s *NATSKVStore) GetSinkWithRevision(ctx context.Context, instanceID string) (*ports.SinkConfig, uint64, error) {
	key := PrefixSinks + instanceID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, fmt.Errorf("%w: storage sink %q", cdcerrors.ErrNotFound, instanceID)
		}
		return nil, 0, fmt.Errorf("storage: get sink %q: %w", instanceID, err)
	}
	var cfg ports.SinkConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, 0, fmt.Errorf("storage: unmarshal sink %q: %w", instanceID, err)
	}
	return &cfg, entry.Revision(), nil
}

// GetFlowWithRevision returns the flow config along with its current revision.
func (s *NATSKVStore) GetFlowWithRevision(ctx context.Context, flowID string) (*ports.FlowConfig, uint64, error) {
	key := PrefixFlows + flowID
	entry, err := s.bucket.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, 0, fmt.Errorf("%w: storage flow %q", cdcerrors.ErrNotFound, flowID)
		}
		return nil, 0, fmt.Errorf("storage: get flow %q: %w", flowID, err)
	}
	var cfg ports.FlowConfig
	if err := sonic.Unmarshal(entry.Value(), &cfg); err != nil {
		return nil, 0, fmt.Errorf("storage: unmarshal flow %q: %w", flowID, err)
	}
	return &cfg, entry.Revision(), nil
}

// putWithRevision performs a conditional put using NATS KV revision tracking.
// If revision is 0, it uses Create (key must not exist).
// Otherwise, it uses Update with the expected revision for optimistic concurrency.
func (s *NATSKVStore) putWithRevision(ctx context.Context, key string, data []byte, revision uint64) (uint64, error) {
	if revision == 0 {
		rev, err := s.bucket.Create(ctx, key, data)
		if err != nil {
			return 0, err
		}
		return rev, nil
	}
	rev, err := s.bucket.Update(ctx, key, data, revision)
	if err != nil {
		return 0, err
	}
	return rev, nil
}
