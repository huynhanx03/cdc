package queue

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrQueueClosed = fmt.Errorf("queue is closed")
)

type Queue struct {
	mu sync.Mutex

	segments []*Segment
	active   *Segment

	dir string

	nextOffset     uint64
	readPos        int64
	maxSegmentSize int64

	readSeg int
	closed  bool
}

func OpenQueue(dir string, segmentSize int64) (*Queue, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		dir:            dir,
		maxSegmentSize: segmentSize,
	}

	err = q.rotate(0)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (q *Queue) rotate(baseOffset uint64) error {
	path := filepath.Join(
		q.dir,
		fmt.Sprintf("%020d.log", baseOffset),
	)

	seg, err := OpenSegment(path, baseOffset, q.maxSegmentSize)
	if err != nil {
		return err
	}

	q.segments = append(q.segments, seg)
	q.active = seg

	return nil
}

func (q *Queue) Enqueue(key, value []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	msg := &Message{
		Offset:    q.nextOffset,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}

	if _, err := q.active.Append(msg); err == ErrSegmentFull {
		err = q.rotate(q.nextOffset)
		if err != nil {
			return err
		}

		if _, err = q.active.Append(msg); err != nil {
			return err
		}

	} else if err != nil {
		return err
	}
	q.nextOffset++

	return nil
}

func (q *Queue) DequeueBatch(n int) ([]MessageView, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	var out []MessageView

	for len(out) < n {

		if q.readSeg >= len(q.segments) {
			break
		}

		seg := q.segments[q.readSeg]
		msg, next, err := seg.ReadAt(q.readPos)

		if err != nil {
			if err == io.EOF && q.readSeg == len(q.segments)-1 {
				// Reached end of active segment, break and wait for more data.
				break
			}
			q.readSeg++
			q.readPos = 0
			continue
		}

		q.readPos = next
		out = append(out, *msg)
	}

	return out, nil
}

func (q *Queue) Commit() error {
	// Not implemented in new version
	return nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
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

type QueueStats struct {
	TotalEnqueued uint64 `json:"total_enqueued"`
	TotalDequeued uint64 `json:"total_dequeued"`
	Pending       uint64 `json:"pending"`
	SegmentsCount int    `json:"segments_count"`
	TotalSizeMB   int64  `json:"total_size_mb"`
}

func (q *Queue) GetStats() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	var totalSize int64
	for _, seg := range q.segments {
		totalSize += seg.Size()
	}

	return QueueStats{
		TotalEnqueued: q.nextOffset,
		SegmentsCount: len(q.segments),
		TotalSizeMB:   totalSize / (1024 * 1024),
	}
}

func (q *Queue) InspectRaw(limit int) ([]MessageView, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if limit <= 0 {
		limit = 50
	}

	var out []MessageView

	for si := 0; si < len(q.segments) && len(out) < limit; si++ {
		seg := q.segments[si]
		var pos int64

		for len(out) < limit {
			msg, next, err := seg.ReadAt(pos)
			if err != nil {
				break // EOF or corruption — move to next segment
			}
			out = append(out, *msg)
			pos = next
		}
	}

	return out, nil
}
