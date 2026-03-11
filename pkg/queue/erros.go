package queue

import "errors"

var (
	ErrSegmentFull = errors.New("segment full")
	ErrCRC         = errors.New("crc mismatch")
)
