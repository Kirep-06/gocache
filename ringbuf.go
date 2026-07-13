package gocache

import (
	"bytes"
	"errors"
	"fmt"
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

// Dump returns a copy of the underlying buffer.
func (rb *RingBuf) Dump() []byte {
	dump := make([]byte, len(rb.data))
	copy(dump, rb.data)
	return dump
}

func (rb *RingBuf) String() string {
	return fmt.Sprintf("[size:%v, start:%v, end:%v, index:%v]", len(rb.data), rb.begin, rb.end, rb.index)
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
	readOff := rb.getDataOff(off)
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
	writeOff := rb.getDataOff(off)
	n = copy(rb.data[writeOff:], p)
	if n < len(p) {
		n += copy(rb.data[:], p[n:])
	}
	return n, nil
}

func (rb *RingBuf) getDataOff(off int64) int {
	dataOff := int(off - rb.begin)
	if rb.end-rb.begin == int64(len(rb.data)) {
		dataOff += rb.index
	}
	if dataOff >= len(rb.data) {
		dataOff -= len(rb.data)
	}
	return dataOff
}

// Slice returns a view into the stream, copying only when it wraps.
func (rb *RingBuf) Slice(off, length int64) ([]byte, error) {
	if off < rb.begin || off > rb.end || length < 0 || length > rb.end-off {
		return nil, ErrOutOfRange
	}
	readOff := rb.getDataOff(off)
	readEnd := readOff + int(length)
	if readEnd <= len(rb.data) {
		return rb.data[readOff:readEnd:readEnd], nil
	}
	buf := make([]byte, length)
	n := copy(buf, rb.data[readOff:])
	copy(buf[n:], rb.data[:readEnd-len(rb.data)])
	return buf, nil
}

// EqualAt compares p with bytes already present at the stream offset.
func (rb *RingBuf) EqualAt(p []byte, off int64) bool {
	if off < rb.begin || off > rb.end || int64(len(p)) > rb.end-off {
		return false
	}
	readOff := rb.getDataOff(off)
	firstLen := len(rb.data) - readOff
	if len(p) <= firstLen {
		return bytes.Equal(p, rb.data[readOff:readOff+len(p)])
	}
	return bytes.Equal(p[:firstLen], rb.data[readOff:]) &&
		bytes.Equal(p[firstLen:], rb.data[:len(p)-firstLen])
}

// Skip advances the stream without writing bytes.
func (rb *RingBuf) Skip(length int64) {
	rb.end += length
	rb.index += int(length)
	for rb.index >= len(rb.data) {
		rb.index -= len(rb.data)
	}
	if rb.end-rb.begin > int64(len(rb.data)) {
		rb.begin = rb.end - int64(len(rb.data))
	}
}

// Resize changes capacity, discarding the oldest bytes if necessary.
func (rb *RingBuf) Resize(newSize int) {
	if len(rb.data) == newSize {
		return
	}
	newData := make([]byte, newSize)
	var offset int
	if rb.end-rb.begin == int64(len(rb.data)) {
		offset = rb.index
	}
	if int(rb.end-rb.begin) > newSize {
		discard := int(rb.end-rb.begin) - newSize
		offset = (offset + discard) % len(rb.data)
		rb.begin = rb.end - int64(newSize)
	}
	n := copy(newData, rb.data[offset:])
	if n < newSize {
		copy(newData[n:], rb.data[:offset])
	}
	rb.data = newData
	rb.index = 0
}

// Evacuate moves an existing entry to the stream end before it is overwritten.
func (rb *RingBuf) Evacuate(off int64, length int) (newOff int64) {
	if off < rb.begin || off+int64(length) > rb.end {
		return -1
	}
	readOff := rb.getDataOff(off)
	if readOff == rb.index {
		rb.index += length
		if rb.index >= len(rb.data) {
			rb.index -= len(rb.data)
		}
	} else if readOff < rb.index {
		n := copy(rb.data[rb.index:], rb.data[readOff:readOff+length])
		rb.index += n
		if rb.index == len(rb.data) {
			rb.index = copy(rb.data, rb.data[readOff+n:readOff+length])
		}
	} else {
		readEnd := readOff + length
		if readEnd <= len(rb.data) {
			rb.index += copy(rb.data[rb.index:], rb.data[readOff:readEnd])
		} else {
			n := copy(rb.data[rb.index:], rb.data[readOff:])
			rb.index += n
			tail := length - n
			n = copy(rb.data[rb.index:], rb.data[:tail])
			rb.index += n
			if rb.index == len(rb.data) {
				rb.index = copy(rb.data, rb.data[n:tail])
			}
		}
	}
	newOff = rb.end
	rb.end += int64(length)
	if rb.begin < rb.end-int64(len(rb.data)) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return newOff
}
