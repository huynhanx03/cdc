package fastqueue

import (
	"time"
)

type Queue struct {
	ring      *Ring
	log       *DiskLog
	batchSize int
}

func NewQueue(size uint64, logPath string) (*Queue, error) {

	r := NewRing(size)
	log, err := NewDiskLog(logPath)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		ring:      r,
		log:       log,
		batchSize: 64,
	}

	go q.flushLoop()

	return q, nil
}

func (q *Queue) Enqueue(data []byte) bool {

	e := Entry{
		Timestamp: time.Now().UnixNano(),
		Data:      data,
	}

	return q.ring.Push(e)
}

func (q *Queue) Dequeue() ([]byte, bool) {
	e, ok := q.ring.Pop()
	if !ok {
		return nil, false
	}

	return e.Data, true
}

func (q *Queue) flushLoop() {

	batch := make([]Entry, 0, q.batchSize)

	for {

		e, ok := q.ring.Pop()

		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}

		batch = append(batch, e)

		if len(batch) >= q.batchSize {

			q.log.AppendBatch(batch)

			batch = batch[:0]
		}
	}
}
