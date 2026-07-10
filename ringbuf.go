package gocache

import (
	"errors"
	"io"
)

var ErrOutOfRange = errors.New("out of range")

// RingBuf stores a bounded byte stream in a fixed-size buffer.
type RingBuf struct {
	begin int64
	end   int64
	data  []byte
	index int
}

func NewRingBuf(size int, begin int64) (rb RingBuf) {
	rb.data = make([]byte, size)
	rb.Reset(begin)
	return rb
}

// Reset discards the current stream and starts at begin.
func (rb *RingBuf) Reset(begin int64) {
	rb.begin = begin
	rb.end = begin
	rb.index = 0
}

func (rb *RingBuf) Size() int64 {
	return int64(len(rb.data))
}

func (rb *RingBuf) Begin() int64 {
	return rb.begin
}

func (rb *RingBuf) End() int64 {
	return rb.end
}

// Write appends at most one buffer's worth of data to the stream.
func (rb *RingBuf) Write(p []byte) (n int, err error) {
	if len(p) > len(rb.data) {
		return 0, ErrOutOfRange
	}
	for n < len(p) {
		written := copy(rb.data[rb.index:], p[n:])
		rb.end += int64(written)
		n += written
		rb.index += written
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	}
	if rb.end-rb.begin > int64(len(rb.data)) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return n, nil
}

// ReadAt reads from the stream offset into p.
func (rb *RingBuf) ReadAt(p []byte, off int64) (n int, err error) {
	if off < rb.begin || off > rb.end {
		return 0, ErrOutOfRange
	}
	available := int(rb.end - off)
	if available < len(p) {
		err = io.EOF
	}
	if len(p) > available {
		p = p[:available]
	}
	if len(p) == 0 {
		return 0, err
	}
	readOff := int(off-rb.begin) + rb.index
	if rb.end-rb.begin < int64(len(rb.data)) {
		readOff = int(off - rb.begin)
	}
	if readOff >= len(rb.data) {
		readOff -= len(rb.data)
	}
	n = copy(p, rb.data[readOff:])
	if n < len(p) {
		n += copy(p[n:], rb.data[:len(p)-n])
	}
	return n, err
}

// WriteAt replaces bytes already present in the stream.
func (rb *RingBuf) WriteAt(p []byte, off int64) (n int, err error) {
	if off < rb.begin || off+int64(len(p)) > rb.end {
		return 0, ErrOutOfRange
	}
	writeOff := int(off-rb.begin) + rb.index
	if rb.end-rb.begin < int64(len(rb.data)) {
		writeOff = int(off - rb.begin)
	}
	if writeOff >= len(rb.data) {
		writeOff -= len(rb.data)
	}
	n = copy(rb.data[writeOff:], p)
	if n < len(p) {
		n += copy(rb.data[:], p[n:])
	}
	return n, nil
}
