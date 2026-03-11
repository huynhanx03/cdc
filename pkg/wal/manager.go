package wal

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/foden/cdc/pkg/queue"
)

// Manager handles a collection of partitioned queues.
type Manager[T any] struct {
	mu             sync.RWMutex
	dir            string
	maxSegmentSize int64
	retentionHours int
	partitions     map[int]*queue.Queue
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

	// Discover existing partitions
	entries, err := os.ReadDir(dir)
	if err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				var id int
				if n, _ := fmt.Sscanf(entry.Name(), "partition_%d", &id); n == 1 {
					partDir := filepath.Join(dir, entry.Name())
					q, err := queue.OpenQueue(partDir, maxSegmentSize, retentionHours)
					if err != nil {
						slog.Error("failed to open partition queue", "partition", id, "err", err)
						continue
					}
					m.partitions[id] = q
				}
			}
		}
	}

	return m, nil
}

// GetPartition lazily creates or retrieves a partition queue.
func (m *Manager[T]) GetPartition(id int) (*queue.Queue, error) {
	m.mu.RLock()
	q, ok := m.partitions[id]
	m.mu.RUnlock()
	if ok {
		return q, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check logging after acquire lock
	if q, ok := m.partitions[id]; ok {
		return q, nil
	}

	partDir := filepath.Join(m.dir, fmt.Sprintf("partition_%d", id))
	newQ, err := queue.OpenQueue(partDir, m.maxSegmentSize, m.retentionHours)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition %d: %w", id, err)
	}

	m.partitions[id] = newQ
	return newQ, nil
}

// Enqueue serializes and adds an item to the requested partition.
func (m *Manager[T]) Enqueue(partitionID int, item T) error {
	q, err := m.GetPartition(partitionID)
	if err != nil {
		return err
	}

	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	return q.Enqueue(data)
}

// DequeueBatch reads a batch from the specific partition.
func (m *Manager[T]) DequeueBatch(partitionID int, batchSize int) ([]T, error) {
	q, err := m.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}

	rawData, err := q.DequeueBatch(batchSize)
	if err == queue.ErrQueueClosed {
		return nil, queue.ErrQueueClosed
	}
	if err != nil {
		return nil, err
	}

	var items []T
	for _, item := range rawData {
		var decoded T
		if err := json.Unmarshal(item, &decoded); err != nil {
			slog.Error("Failed to unmarshal item from partition", "partition", partitionID, "err", err)
			continue
		}
		items = append(items, decoded)
	}

	return items, nil
}

// Commit removes fully read segments for a partition.
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
		return fmt.Errorf("multiple errors closing manager partitions: %v", errs)
	}
	return nil
}

// GetPartitionIDs returns a list of all current partitions.
func (m *Manager[T]) GetPartitionIDs() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []int
	for id := range m.partitions {
		ids = append(ids, id)
	}
	return ids
}

// InspectRaw is maintained for debugging in UI, grabs last N from a given partition.
func (m *Manager[T]) InspectRaw(partitionID int, limit int) ([]T, error) {
	q, err := m.GetPartition(partitionID)
	if err != nil {
		return nil, err
	}

	rawData, err := q.InspectRaw(limit)
	if err != nil {
		return nil, err
	}

	var items []T
	for _, item := range rawData {
		var decoded T
		if err := json.Unmarshal(item, &decoded); err == nil {
			items = append(items, decoded)
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
