package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// DefaultKVBucket is the name of the KeyValue bucket for persistent state (offsets)
	DefaultKVBucket = "CDC_STATE"
	// ConfigKVBucket is the name of the KeyValue bucket for persistent configuration
	ConfigKVBucket = "CDC_CONFIG"
	// ConfigKey is the key for the application configuration in the KV store
	ConfigKey = "ACTIVE_CONFIG"
)

// SaveOffset persists the offset for a given instance ID in the NATS KeyValue store.
func (c *Client) SaveOffset(ctx context.Context, instanceID string, offset interface{}) error {
	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: DefaultKVBucket,
	})
	if err != nil {
		return fmt.Errorf("failed to get/create KV bucket: %w", err)
	}

	data, err := json.Marshal(offset)
	if err != nil {
		return fmt.Errorf("failed to marshal offset: %w", err)
	}

	_, err = kv.Put(ctx, instanceID, data)
	if err != nil {
		return fmt.Errorf("failed to put offset into KV: %w", err)
	}

	return nil
}

// GetOffset retrieves the persisted offset for a given instance ID as a string.
func (c *Client) GetOffset(ctx context.Context, instanceID string) (string, error) {
	kv, err := c.js.KeyValue(ctx, DefaultKVBucket)
	if err != nil {
		return "", fmt.Errorf("failed to get KV bucket: %w", err)
	}

	entry, err := kv.Get(ctx, instanceID)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return "", nil // No offset found
		}
		return "", fmt.Errorf("failed to get offset from KV: %w", err)
	}

	var offset string
	if err := json.Unmarshal(entry.Value(), &offset); err != nil {
		// Fallback: try returning original bytes as string if not valid JSON string
		return string(entry.Value()), nil
	}

	return offset, nil
}

// SaveConfig persists the active configuration into NATS KV.
func (c *Client) SaveConfig(ctx context.Context, cfg interface{}) error {
	kv, err := c.js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: ConfigKVBucket,
	})
	if err != nil {
		return fmt.Errorf("failed to get/create Config KV bucket: %w", err)
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	_, err = kv.Put(ctx, ConfigKey, data)
	if err != nil {
		return fmt.Errorf("failed to put config into KV: %w", err)
	}

	return nil
}

// GetConfig retrieves the persisted configuration from NATS KV.
// It returns nil if no configuration is found.
func (c *Client) GetConfig(ctx context.Context, target interface{}) (bool, error) {
	kv, err := c.js.KeyValue(ctx, ConfigKVBucket)
	if err != nil {
		if err == jetstream.ErrBucketNotFound || err == jetstream.ErrStreamNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get Config KV bucket: %w", err)
	}

	entry, err := kv.Get(ctx, ConfigKey)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get config from KV: %w", err)
	}

	if err := json.Unmarshal(entry.Value(), target); err != nil {
		return false, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return true, nil
}
