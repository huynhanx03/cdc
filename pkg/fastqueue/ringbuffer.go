package fastqueue

import (
	"sync/atomic"
)

type Ring struct {
	buffer []Entry
	size   uint64
	mask   uint64

	write uint64
	read  uint64
}

func NewRing(size uint64) *Ring {
	if size&(size-1) != 0 {
		panic("size must be power of 2")
	}

	return &Ring{
		buffer: make([]Entry, size),
		size:   size,
		mask:   size - 1,
	}
}

func (r *Ring) Push(e Entry) bool {
	pos := atomic.AddUint64(&r.write, 1) - 1

	if pos-atomic.LoadUint64(&r.read) >= r.size {
		return false
	}

	r.buffer[pos&r.mask] = e
	return true
}

func (r *Ring) Pop() (Entry, bool) {

	if atomic.LoadUint64(&r.read) >= atomic.LoadUint64(&r.write) {
		return Entry{}, false
	}

	pos := r.read
	e := r.buffer[pos&r.mask]
	r.read++

	return e, true
}
