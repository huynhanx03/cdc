package fastqueue

import (
	"encoding/binary"
	"os"
)

type DiskLog struct {
	file *os.File
}

func NewDiskLog(path string) (*DiskLog, error) {
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)

	if err != nil {
		return nil, err
	}

	return &DiskLog{file: f}, nil
}

func (l *DiskLog) AppendBatch(batch []Entry) error {

	for _, e := range batch {

		var header [12]byte
		binary.LittleEndian.PutUint64(header[0:8], uint64(e.Timestamp))
		binary.LittleEndian.PutUint32(header[8:12], uint32(len(e.Data)))
		if _, err := l.file.Write(header[:]); err != nil {
			return err
		}

		if _, err := l.file.Write(e.Data); err != nil {
			return err
		}
	}

	return nil
}
