package gocache

import (
	"io"
	"testing"
)

func TestRingBufInitializeAndReset(t *testing.T) {
	rb := NewRingBuf(16, 7)
	if rb.Size() != 16 || rb.Begin() != 7 || rb.End() != 7 {
		t.Fatalf("initial state: size=%d begin=%d end=%d", rb.Size(), rb.Begin(), rb.End())
	}

	rb.begin, rb.end, rb.index = 8, 10, 3
	rb.Reset(20)
	if rb.Begin() != 20 || rb.End() != 20 || rb.index != 0 {
		t.Fatalf("reset state: begin=%d end=%d index=%d", rb.Begin(), rb.End(), rb.index)
	}
}

func TestRingBufWriteWrapAndOverwrite(t *testing.T) {
	rb := NewRingBuf(4, 10)
	if n, err := rb.Write([]byte("abc")); n != 3 || err != nil {
		t.Fatalf("first write: n=%d err=%v", n, err)
	}
	if n, err := rb.Write([]byte("def")); n != 3 || err != nil {
		t.Fatalf("wrapped write: n=%d err=%v", n, err)
	}
	if rb.Begin() != 12 || rb.End() != 16 || rb.index != 2 || string(rb.data) != "efcd" {
		t.Fatalf("wrapped state: begin=%d end=%d index=%d data=%q", rb.Begin(), rb.End(), rb.index, rb.data)
	}
	if n, err := rb.Write([]byte("12345")); n != 0 || err != ErrOutOfRange {
		t.Fatalf("oversized write: n=%d err=%v", n, err)
	}
}

func TestRingBufReadAt(t *testing.T) {
	rb := NewRingBuf(4, 10)
	rb.Write([]byte("abc"))
	rb.Write([]byte("def"))
	buf := make([]byte, 4)
	if n, err := rb.ReadAt(buf, 12); n != 4 || err != nil || string(buf) != "cdef" {
		t.Fatalf("wrapped read: n=%d err=%v data=%q", n, err, buf)
	}
	if n, err := rb.ReadAt(buf, 14); n != 2 || err != io.EOF || string(buf[:n]) != "ef" {
		t.Fatalf("partial read: n=%d err=%v data=%q", n, err, buf[:n])
	}
	if n, err := rb.ReadAt(buf, 11); n != 0 || err != ErrOutOfRange {
		t.Fatalf("out of range read: n=%d err=%v", n, err)
	}
}

func TestRingBufWriteAt(t *testing.T) {
	rb := NewRingBuf(4, 0)
	rb.Write([]byte("abc"))
	rb.Write([]byte("def"))
	if n, err := rb.WriteAt([]byte("XY"), 14); n != 0 || err != ErrOutOfRange {
		t.Fatalf("out of range write: n=%d err=%v", n, err)
	}
	if n, err := rb.WriteAt([]byte("XY"), 4); n != 2 || err != nil {
		t.Fatalf("write at: n=%d err=%v", n, err)
	}
	buf := make([]byte, 4)
	if n, err := rb.ReadAt(buf, 2); n != 4 || err != nil || string(buf) != "cdXY" {
		t.Fatalf("updated data: n=%d err=%v data=%q", n, err, buf)
	}
	if rb.Begin() != 2 || rb.End() != 6 {
		t.Fatalf("offsets changed: begin=%d end=%d", rb.Begin(), rb.End())
	}
}

func TestRingBufSlice(t *testing.T) {
	rb := NewRingBuf(4, 0)
	rb.Write([]byte("abc"))
	view, err := rb.Slice(1, 2)
	if err != nil || string(view) != "bc" {
		t.Fatalf("contiguous slice: data=%q err=%v", view, err)
	}
	view[0] = 'X'
	if rb.data[1] != 'X' {
		t.Fatal("contiguous slice should share storage")
	}
	rb.Write([]byte("def"))
	wrapped, err := rb.Slice(2, 4)
	if err != nil || string(wrapped) != "cdef" {
		t.Fatalf("wrapped slice: data=%q err=%v", wrapped, err)
	}
	wrapped[0] = 'Y'
	if rb.data[2] == 'Y' {
		t.Fatal("wrapped slice should be copied")
	}
	if _, err := rb.Slice(1, 1); err != ErrOutOfRange {
		t.Fatalf("expired slice: err=%v", err)
	}
}

func TestRingBufEqualAt(t *testing.T) {
	rb := NewRingBuf(4, 0)
	rb.Write([]byte("abc"))
	rb.Write([]byte("def"))
	if !rb.EqualAt([]byte("cdef"), 2) || !rb.EqualAt([]byte("cd"), 2) {
		t.Fatal("existing bytes should compare equal")
	}
	if rb.EqualAt([]byte("cdeX"), 2) || rb.EqualAt([]byte("abc"), 0) || rb.EqualAt([]byte("defg"), 3) {
		t.Fatal("different or out-of-range bytes should not compare equal")
	}
}

func TestRingBufInspection(t *testing.T) {
	rb := NewRingBuf(4, 10)
	rb.Write([]byte("ab"))
	dump := rb.Dump()
	dump[0] = 'X'
	if rb.data[0] != 'a' {
		t.Fatal("Dump should return a copy")
	}
	if got := rb.String(); got != "[size:4, start:10, end:12, index:2]" {
		t.Fatalf("String()=%q", got)
	}
}

func TestRingBufSkip(t *testing.T) {
	rb := NewRingBuf(4, 0)
	rb.Write([]byte("ab"))
	before := rb.Dump()
	rb.Skip(5)
	if rb.Begin() != 3 || rb.End() != 7 || rb.index != 3 {
		t.Fatalf("skip state: begin=%d end=%d index=%d", rb.Begin(), rb.End(), rb.index)
	}
	if string(rb.Dump()) != string(before) {
		t.Fatal("Skip should not write data")
	}
}
