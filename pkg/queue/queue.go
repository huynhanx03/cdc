package queue

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	ErrQueueClosed = errors.New("queue is closed")
)

// Queue represents a persistent, append-only message queue for raw bytes.
type Queue struct {
	mu             sync.Mutex
	dir            string
	maxSegmentSize int64
	retentionHours int
	segments       []*Segment
	activeSegment  *Segment

	// read pointer
	readSegIdx int
	readOffset int64

	closed bool

	// Stats
	totalEnqueued uint64
	totalDequeued uint64
}

// OpenQueue opens or creates a raw queue at the given directory.
func OpenQueue(dir string, maxSegmentSize int64, retentionHours int) (*Queue, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create queue directory %s: %w", dir, err)
	}

	q := &Queue{
		dir:            dir,
		maxSegmentSize: maxSegmentSize,
		retentionHours: retentionHours,
	}

	if err := q.loadSegments(); err != nil {
		return nil, fmt.Errorf("failed to load queue segments: %w", err)
	}

	return q, nil
}

func (q *Queue) loadSegments() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return err
	}

	var segmentPaths []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".log" {
			segmentPaths = append(segmentPaths, filepath.Join(q.dir, entry.Name()))
		}
	}
	sort.Strings(segmentPaths)

	for _, path := range segmentPaths {
		seg, err := OpenSegment(path)
		if err != nil {
			return fmt.Errorf("failed to open segment %s: %w", path, err)
		}
		q.segments = append(q.segments, seg)
	}

	if len(q.segments) == 0 {
		if err := q.rotateSegment(0); err != nil {
			return fmt.Errorf("failed to create initial rotating segment: %w", err)
		}
	} else {
		q.activeSegment = q.segments[len(q.segments)-1]
	}

	return nil
}

func (q *Queue) rotateSegment(id int64) error {
	filename := fmt.Sprintf("%020d.log", id)
	path := filepath.Join(q.dir, filename)

	seg, err := OpenSegment(path)
	if err != nil {
		return fmt.Errorf("failed to create new segment %s: %w", path, err)
	}

	if q.activeSegment != nil {
		if err := q.activeSegment.Close(); err != nil {
			slog.Error("Failed to close previous active segment", "err", err, "path", q.activeSegment.Path())
		}
	}

	q.segments = append(q.segments, seg)
	q.activeSegment = seg
	return nil
}

// Enqueue adds raw data bytes to the queue.
func (q *Queue) Enqueue(data []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	// Rotate if needed
	if q.activeSegment.Size()+int64(len(data)+4) > q.maxSegmentSize {
		nextID := extractSegmentID(q.activeSegment.Path()) + 1
		if err := q.rotateSegment(nextID); err != nil {
			return fmt.Errorf("failed to rotate segment: %w", err)
		}
	}

	if _, err := q.activeSegment.Append(data); err != nil {
		return fmt.Errorf("failed to append to segment: %w", err)
	}

	q.totalEnqueued++
	return nil
}

// DequeueBatch reads up to batchSize entries of raw byte data.
// It will not block. It returns the raw bytes read and nil error if successful.
func (q *Queue) DequeueBatch(batchSize int) ([][]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	var batch [][]byte

	for len(batch) < batchSize {
		if q.readSegIdx >= len(q.segments) {
			// No more active segments
			break
		}

		seg := q.segments[q.readSegIdx]
		data, nextOffset, err := seg.ReadAt(q.readOffset)

		if err == io.EOF {
			// End of this segment.
			// Move to next segment if it's not the last one
			if q.readSegIdx < len(q.segments)-1 {
				q.readSegIdx++
				q.readOffset = 0
				continue
			} else {
				// Waiting for more data in the active segment
				break
			}
		} else if err != nil {
			slog.Error("Failed to read from queue segment", "err", err, "segment", seg.Path())
			return batch, fmt.Errorf("read error on segment: %w", err)
		}

		q.readOffset = nextOffset
		q.totalDequeued++
		batch = append(batch, data)
	}

	return batch, nil
}

// Commit removes completely read segments.
// With retention active, it only deletes segments that have been fully read up to readSegIdx AND exceed the retention period.
func (q *Queue) Commit() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if q.retentionHours <= 0 {
		// No retention enforcement: delete fully-read segments immediately
		if q.readSegIdx > 0 {
			oldSegments := q.segments[:q.readSegIdx]
			for _, seg := range oldSegments {
				if err := seg.Remove(); err != nil {
					slog.Error("Failed to remove old queue segment", "err", err, "path", seg.Path())
				} else {
					slog.Info("Garbage collected old queue segment", "path", seg.Path())
				}
			}
			// Keep remaining
			q.segments = q.segments[q.readSegIdx:]
			q.readSegIdx = 0
		}
		return nil
	}

	// Wait for retention to expire before garbage collecting read segments
	cutoff := time.Now().Add(-time.Duration(q.retentionHours) * time.Hour)
	var newSegments []*Segment
	removedCount := 0

	for i, seg := range q.segments {
		// Retain active unread segments
		if i == len(q.segments)-1 || i >= q.readSegIdx {
			newSegments = append(newSegments, seg)
			continue
		}

		stat, err := os.Stat(seg.Path())
		if err == nil && stat.ModTime().Before(cutoff) {
			if err := seg.Remove(); err != nil {
				slog.Error("Failed to remove old queue segment", "err", err, "path", seg.Path())
				newSegments = append(newSegments, seg)
			} else {
				slog.Info("Garbage collected old queue segment by retention", "path", seg.Path())
				removedCount++
			}
		} else {
			newSegments = append(newSegments, seg)
		}
	}

	q.segments = newSegments
	q.readSegIdx -= removedCount
	if q.readSegIdx < 0 {
		q.readSegIdx = 0
		q.readOffset = 0
	}

	return nil
}

// Close closes the queue and its segments.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true

	var errs []error
	for _, seg := range q.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing segments: %v", errs)
	}

	return nil
}

// QueueStats contains metrics for the queue.
type QueueStats struct {
	TotalEnqueued uint64 `json:"total_enqueued"`
	TotalDequeued uint64 `json:"total_dequeued"`
	Pending       uint64 `json:"pending"`
	SegmentsCount int    `json:"segments_count"`
	TotalSizeMB   int64  `json:"total_size_mb"`
}

// GetStats returns current statistics of the Queue.
func (q *Queue) GetStats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	var totalSize int64
	for _, seg := range q.segments {
		totalSize += seg.Size()
	}

	return QueueStats{
		TotalEnqueued: q.totalEnqueued,
		TotalDequeued: q.totalDequeued,
		Pending:       q.totalEnqueued - q.totalDequeued,
		SegmentsCount: len(q.segments),
		TotalSizeMB:   totalSize / (1024 * 1024), // MB
	}
}

// InspectRaw returns the last `limit` entires from the ends of the active queue for debugging.
func (q *Queue) InspectRaw(limit int) ([][]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	seg := q.activeSegment
	if seg == nil {
		return nil, nil
	}

	var batch [][]byte
	var offset int64 = 0

	for {
		data, nextOffset, err := seg.ReadAt(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return batch, err // Best effort
		}

		batch = append(batch, data)
		if len(batch) > limit {
			batch = batch[1:] // keep last limit elements
		}
		offset = nextOffset
	}

	return batch, nil
}
