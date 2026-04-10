package pool

import (
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
)

// TypeDecoder decodes a PostgreSQL column value for a specific OID.
type TypeDecoder struct {
	oid uint32
}

// Decode converts PostgreSQL text-format data into a Go value using the type map.
func (d *TypeDecoder) Decode(typeMap *pgtype.Map, data []byte) (interface{}, error) {
	if dt, ok := typeMap.TypeForOID(d.oid); ok {
		return dt.Codec.DecodeValue(typeMap, d.oid, pgtype.TextFormatCode, data)
	}
	return string(data), nil
}

// DecoderCache caches TypeDecoders by OID to avoid repeated reflection lookups.
// Uses double-checked locking: fast path (RLock) for cache hits, slow path (Lock)
// with double-check for cache misses.
type DecoderCache struct {
	cache map[uint32]*TypeDecoder
	mu    sync.RWMutex
}

// NewDecoderCache creates a new decoder cache pre-sized for common PostgreSQL types.
func NewDecoderCache() *DecoderCache {
	return &DecoderCache{
		cache: make(map[uint32]*TypeDecoder, 50),
	}
}

// Get retrieves or creates a decoder for the given OID.
func (c *DecoderCache) Get(oid uint32) *TypeDecoder {
	// Fast path: read lock for cache hit
	c.mu.RLock()
	decoder, exists := c.cache[oid]
	c.mu.RUnlock()

	if exists {
		return decoder
	}

	// Slow path: write lock for cache miss
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check after acquiring write lock
	if decoder, exists := c.cache[oid]; exists {
		return decoder
	}

	decoder = &TypeDecoder{oid: oid}
	c.cache[oid] = decoder
	return decoder
}

// Size returns the number of cached decoders.
func (c *DecoderCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}
