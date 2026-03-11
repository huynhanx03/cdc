package wal

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foden/cdc/pkg/queue"
)

type WalMessage[T any] struct {
	Offset    uint64
	Timestamp time.Time
	Key       []byte
	Item      T
}

type Manager[T any] struct {
	mu sync.RWMutex

	dir            string
	maxSegmentSize int64
	retentionHours int

	partitions map[int]*queue.Queue

	nextPartition uint64
}

// OpenManager initializes a new WAL manager.
func OpenManager[T any](dir string, maxSegmentSize int64, retentionHours int) (*Manager[T], error) {

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create manager dir: %w", err)
	}

	m := &Manager[T]{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		retentionHours: retentionHours,
		partitions:     make(map[int]*queue.Queue),
	}

	entries, err := os.ReadDir(dir)
	if err == nil {

		for _, entry := range entries {

			if !entry.IsDir() {
				continue
			}

			var id int

			if n, _ := fmt.Sscanf(entry.Name(), "partition_%d", &id); n != 1 {
				continue
			}

			partDir := filepath.Join(dir, entry.Name())

			q, err := queue.OpenQueue(partDir, maxSegmentSize)
			if err != nil {

				slog.Error(
					"failed to open partition queue",
					"partition", id,
					"err", err,
				)

				continue
			}

			m.partitions[id] = q
		}
	}

	return m, nil
}

func (m *Manager[T]) GetPartition(id int) (*queue.Queue, error) {

	m.mu.RLock()
	q, ok := m.partitions[id]
	m.mu.RUnlock()

	if ok {
		return q, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if q, ok := m.partitions[id]; ok {
		return q, nil
	}

	partDir := filepath.Join(
		m.dir,
		fmt.Sprintf("partition_%d", id),
	)

	newQ, err := queue.OpenQueue(partDir, m.maxSegmentSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition %d: %w", id, err)
	}

	m.partitions[id] = newQ

	return newQ, nil
}

func (m *Manager[T]) partitionCount() int {

	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.partitions)
}

func (m *Manager[T]) selectPartition(key []byte) int {

	n := m.partitionCount()

	if n == 0 {
		return 0
	}

	if len(key) > 0 {

		h := fnv.New32a()
		h.Write(key)

		return int(h.Sum32()) % n
	}

	id := atomic.AddUint64(&m.nextPartition, 1)

	return int(id % uint64(n))
}

func (m *Manager[T]) Enqueue(key []byte, item T) error {

	partitionID := m.selectPartition(key)

	q, err := m.GetPartition(partitionID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	return q.Enqueue(key, data)
}

func (m *Manager[T]) DequeueBatch(partitionID int, batchSize int) ([]WalMessage[T], error) {

	q, err := m.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}

	rawMessages, err := q.DequeueBatch(batchSize)
	if err == queue.ErrQueueClosed {
		return nil, queue.ErrQueueClosed
	}

	if err != nil {
		return nil, err
	}

	items := make([]WalMessage[T], 0, len(rawMessages))

	for _, msg := range rawMessages {

		var decoded T

		if err := json.Unmarshal(msg.Value, &decoded); err != nil {

			slog.Error(
				"failed to unmarshal item",
				"partition", partitionID,
				"err", err,
			)

			continue
		}

		items = append(items, WalMessage[T]{
			Offset:    msg.Offset,
			Timestamp: time.Unix(0, msg.Timestamp),
			Key:       msg.Key,
			Item:      decoded,
		})
	}

	return items, nil
}

func (m *Manager[T]) Commit(partitionID int) error {

	m.mu.RLock()
	q, ok := m.partitions[partitionID]
	m.mu.RUnlock()

	if !ok {
		return nil
	}

	return q.Commit()
}

func (m *Manager[T]) Close() error {

	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	for _, q := range m.partitions {

		if err := q.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("multiple errors closing partitions: %v", errs)
	}

	return nil
}

func (m *Manager[T]) GetPartitionIDs() []int {

	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]int, 0, len(m.partitions))

	for id := range m.partitions {
		ids = append(ids, id)
	}

	return ids
}

func (m *Manager[T]) InspectRaw(partitionID int, limit int) ([]WalMessage[T], error) {

	q, err := m.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}

	rawMessages, err := q.InspectRaw(limit)
	if err != nil {
		return nil, err
	}

	items := make([]WalMessage[T], 0, len(rawMessages))

	for _, msg := range rawMessages {

		var decoded T

		if err := json.Unmarshal(msg.Value, &decoded); err == nil {

			items = append(items, WalMessage[T]{
				Offset:    msg.Offset,
				Timestamp: time.Unix(0, msg.Timestamp),
				Key:       msg.Key,
				Item:      decoded,
			})
		}
	}

	return items, nil
}

func (m *Manager[T]) GetTotalStats() queue.QueueStats {

	m.mu.RLock()
	defer m.mu.RUnlock()

	var total queue.QueueStats

	for _, q := range m.partitions {

		s := q.GetStats()

		total.TotalEnqueued += s.TotalEnqueued
		total.TotalDequeued += s.TotalDequeued
		total.Pending += s.Pending
		total.SegmentsCount += s.SegmentsCount
		total.TotalSizeMB += s.TotalSizeMB
	}

	return total
}

func hashPartition(table, key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(table))
	h.Write([]byte(key))
	// Prevent negative hash
	val := int(h.Sum32())
	if val < 0 {
		val = -val
	}
	return val % numPartitions
}
