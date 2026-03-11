package queue

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
)

const (
	_defaultIndexInterval = 4096
)

type Segment struct {
	mu sync.RWMutex

	logFile       *os.File
	indexFile     *os.File
	timeIndexFile *os.File

	mmap []byte

	baseOffset uint64
	lastOffset uint64

	writePos int64

	lastIndexedPos int64

	maxSize int64
}

func OpenSegment(path string, baseOffset uint64, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	err = file.Truncate(maxSize)
	if err != nil {
		return nil, err
	}

	mmap, err := syscall.Mmap(
		int(file.Fd()),
		0,
		int(maxSize),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	)

	if err != nil {
		return nil, err
	}

	indexFile, _ := os.OpenFile(path+".index", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	timeFile, _ := os.OpenFile(path+".timeindex", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	return &Segment{
		logFile:       file,
		mmap:          mmap,
		indexFile:     indexFile,
		timeIndexFile: timeFile,
		baseOffset:    baseOffset,
		lastOffset:    baseOffset,
		maxSize:       maxSize,
	}, nil
}

// Append
func (s *Segment) Append(msg *Message) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyLen := len(msg.Key)
	valLen := len(msg.Value)

	payloadSize := 8 + 8 + 4 + keyLen + 4 + valLen
	total := 4 + 4 + payloadSize

	if s.writePos+int64(total) > s.maxSize {
		return 0, ErrSegmentFull
	}

	start := s.writePos
	buf := s.mmap[start : start+int64(total)]
	binary.LittleEndian.PutUint32(buf[0:4], uint32(payloadSize+4))
	payload := buf[8:]
	binary.LittleEndian.PutUint64(payload[0:8], msg.Offset)
	binary.LittleEndian.PutUint64(payload[8:16], uint64(msg.Timestamp))
	binary.LittleEndian.PutUint32(payload[16:20], uint32(keyLen))
	copy(payload[20:], msg.Key)
	pos := 20 + keyLen
	binary.LittleEndian.PutUint32(payload[pos:pos+4], uint32(valLen))
	copy(payload[pos+4:], msg.Value)
	crc := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(buf[4:8], crc)
	s.writePos += int64(total)
	s.lastOffset = msg.Offset

	if start-s.lastIndexedPos > _defaultIndexInterval {
		s.writeIndex(msg.Offset, start)
		s.writeTimeIndex(msg.Timestamp, start)
		s.lastIndexedPos = start
	}

	return start, nil
}

// writeIndex writes an index entry.
func (s *Segment) writeIndex(offset uint64, pos int64) {
	var buf [8]byte
	rel := uint32(offset - s.baseOffset)
	binary.LittleEndian.PutUint32(buf[0:4], rel)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(pos))
	s.indexFile.Write(buf[:])
}

// writeTimeIndex writes a time index entry.
func (s *Segment) writeTimeIndex(ts int64, pos int64) {
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(ts))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(pos))
	s.timeIndexFile.Write(buf[:])
}

// ReadAt reads a message at a given position.
func (s *Segment) ReadAt(pos int64) (*MessageView, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if pos >= s.writePos {
		return nil, pos, io.EOF
	}

	length := binary.LittleEndian.Uint32(s.mmap[pos : pos+4])
	buf := s.mmap[pos+4 : pos+4+int64(length)]
	crcStored := binary.LittleEndian.Uint32(buf[0:4])
	payload := buf[4:]

	if crcStored != crc32.ChecksumIEEE(payload) {
		return nil, pos, ErrCRC
	}

	offset := binary.LittleEndian.Uint64(payload[0:8])
	ts := int64(binary.LittleEndian.Uint64(payload[8:16]))
	keyLen := binary.LittleEndian.Uint32(payload[16:20])
	key := payload[20 : 20+keyLen]
	valPos := 20 + keyLen
	valLen := binary.LittleEndian.Uint32(payload[valPos : valPos+4])
	val := payload[valPos+4 : valPos+4+valLen]
	next := pos + 4 + int64(length)

	return &MessageView{
		Offset:    offset,
		Timestamp: ts,
		Key:       key,
		Value:     val,
	}, next, nil
}

// Size returns the size of the segment.
func (s *Segment) Size() int64 {
	return s.writePos
}

func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	syscall.Munmap(s.mmap)
	if s.logFile != nil {
		s.logFile.Close()
	}
	if s.indexFile != nil {
		s.indexFile.Close()
	}
	if s.timeIndexFile != nil {
		s.timeIndexFile.Close()
	}
	return nil
}
