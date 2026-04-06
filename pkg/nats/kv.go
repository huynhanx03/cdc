package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultKVBucket is the name of the KeyValue bucket for persistent state (e.g., offsets)
	DefaultKVBucket = "CDC_STATE"
	// ConfigKVBucket is the name of the KeyValue bucket for persistent configuration
	ConfigKVBucket = "CDC_CONFIG"
	// ConfigKey is the key for the active application configuration in the KV store
	ConfigKey = "ACTIVE_CONFIG"
)

// SaveOffset persists the offset for a given instance ID.
// The offset can be a string, uint64, or a complex struct.
func (c *Client) SaveOffset(ctx context.Context, instanceID string, offset interface{}) error {
	kv, err := c.getKV(ctx, DefaultKVBucket)
	if err != nil {
		return fmt.Errorf("failed to access state KV: %w", err)
	}

	if err := kvPut(ctx, kv, instanceID, offset); err != nil {
		return fmt.Errorf("failed to save offset for %s: %w", instanceID, err)
	}

	return nil
}

// GetOffset retrieves the persisted offset for a given instance ID.
// It returns a pointer to T (nil if not found) to handle various offset types.
func (c *Client) GetOffset(ctx context.Context, instanceID string) (*string, error) {
	kv, err := c.getKV(ctx, DefaultKVBucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access state KV: %w", err)
	}

	// Using generic kvGet to fetch the string offset
	return kvGet[string](ctx, kv, instanceID)
}

// SaveConfig persists the active configuration into the NATS KV store.
func (c *Client) SaveConfig(ctx context.Context, cfg interface{}) error {
	kv, err := c.getKV(ctx, ConfigKVBucket)
	if err != nil {
		return fmt.Errorf("failed to access config KV: %w", err)
	}

	if err := kvPut(ctx, kv, ConfigKey, cfg); err != nil {
		return fmt.Errorf("failed to save active config: %w", err)
	}

	return nil
}

// GetConfig retrieves the configuration and unmarshals it into the target pointer.
// Returns (true, nil) if found, (false, nil) if missing, or (false, error) on failure.
func (c *Client) GetConfig(ctx context.Context, target interface{}) (bool, error) {
	kv, err := c.getKV(ctx, ConfigKVBucket)
	if err != nil {
		// If bucket doesn't exist yet, config is considered missing
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to access config KV: %w", err)
	}

	return kvGetInto(ctx, kv, ConfigKey, target)
}

// --- Generic Helper Functions ---

// kvGet retrieves a value by key and unmarshals it into a new instance of T.
func kvGet[T any](ctx context.Context, kv jetstream.KeyValue, key string) (*T, error) {
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, nil // Return nil pointer to signify "Not Found"
		}
		return nil, err
	}

	var res T
	if err := json.Unmarshal(entry.Value(), &res); err != nil {
		return nil, fmt.Errorf("failed to unmarshal KV value for key %s: %w", key, err)
	}
	return &res, nil
}

// kvGetInto unmarshals the value for a key directly into the provided target pointer.
func kvGetInto(ctx context.Context, kv jetstream.KeyValue, key string, target any) (bool, error) {
	entry, err := kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	if err := json.Unmarshal(entry.Value(), target); err != nil {
		return false, fmt.Errorf("failed to decode KV value for key %s: %w", key, err)
	}
	return true, nil
}

// kvPut marshals the given value to JSON and stores it in the KV bucket.
func kvPut[T any](ctx context.Context, kv jetstream.KeyValue, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to encode value for KV key %s: %w", key, err)
	}

	_, err = kv.Put(ctx, key, data)
	return err
}
