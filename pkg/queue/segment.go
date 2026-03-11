package queue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrClosed = errors.New("segment is closed")
)

// Segment represents a single append-only file on disk.
type Segment struct {
	mu     sync.RWMutex
	file   *os.File
	path   string
	size   int64
	closed bool
}

// OpenSegment opens or creates a segment file.
func OpenSegment(path string) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment %s: %w", path, err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &Segment{
		file: file,
		path: path,
		size: stat.Size(),
	}, nil
}

// Append writes data to the end of the segment.
func (s *Segment) Append(data []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}

	writeOffset := s.size

	// Write size header (4 bytes)
	sizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(len(data)))

	// Write length
	if _, err := s.file.Write(sizeBuf); err != nil {
		return 0, err
	}

	// Write payload
	if _, err := s.file.Write(data); err != nil {
		return 0, err
	}

	writtenSize := int64(4 + len(data))
	s.size += writtenSize

	return writeOffset, nil
}

// ReadAt reads a single message from the given offset.
func (s *Segment) ReadAt(offset int64) ([]byte, int64, error) {
	s.mu.RLock()
	staleSize := s.size
	s.mu.RUnlock()

	if offset >= staleSize {
		return nil, offset, io.EOF
	}

	// Read 4 bytes of length
	lenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(lenBuf, offset); err != nil {
		if err == io.EOF && offset < staleSize {
			return nil, offset, io.ErrUnexpectedEOF
		}
		return nil, offset, err
	}

	length := binary.LittleEndian.Uint32(lenBuf)

	// Read data
	data := make([]byte, length)
	if _, err := s.file.ReadAt(data, offset+4); err != nil {
		if err == io.EOF {
			return nil, offset, io.ErrUnexpectedEOF
		}
		return nil, offset, err
	}

	nextOffset := offset + int64(4+length)
	return data, nextOffset, nil
}

// Size returns the current size of the segment.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Close closes the file.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	s.file.Sync()
	return s.file.Close()
}

func (s *Segment) Path() string {
	return s.path
}

// Remove closes and deletes the file.
func (s *Segment) Remove() error {
	s.Close()
	return os.Remove(s.path)
}

// extractSegmentID extracts the segment ID from the file path.
func extractSegmentID(path string) int64 {
	var id int64
	base := filepath.Base(path)
	fmt.Sscanf(base, "%020d.log", &id)
	return id
}
