package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReopenReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	for _, p := range payloads {
		if err := w.Append(p); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.Size(); got == 0 {
		t.Fatal("size should be non-zero")
	}
	// The active segment is not replayed.
	if segs, _ := w.ClosedSegments(); len(segs) != 0 {
		t.Fatalf("active segment should not be closed, got %d", len(segs))
	}
	_ = w.Close()

	// Reopen: the previous segment is now closed and replayable.
	w2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	segs, err := w2.ClosedSegments()
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("want 1 closed segment, got %d", len(segs))
	}
	recs, err := ReadSegment(segs[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(payloads) {
		t.Fatalf("want %d records, got %d", len(payloads), len(recs))
	}
	for i := range payloads {
		if string(recs[i]) != string(payloads[i]) {
			t.Fatalf("record %d mismatch: %q", i, recs[i])
		}
	}
	if err := w2.RemoveSegment(segs[0]); err != nil {
		t.Fatal(err)
	}
	if w2.Size() != 0 {
		t.Fatalf("size after remove = %d", w2.Size())
	}
}

func TestCorruptTrailingRecordDiscarded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal-00000000000000000001.log")
	var buf []byte
	good := []byte("good")
	frame := make([]byte, 8+len(good))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(good)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(good))
	copy(frame[8:], good)
	buf = append(buf, frame...)
	// A truncated second frame.
	buf = append(buf, 0, 0, 0, 9, 0, 0, 0, 0, 'x')
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err := ReadSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || string(recs[0]) != "good" {
		t.Fatalf("want only the good record, got %v", recs)
	}
}

func TestCRCMismatchStops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal-00000000000000000001.log")
	payload := []byte("data")
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], 12345) // wrong crc
	copy(frame[8:], payload)
	if err := os.WriteFile(path, frame, 0o644); err != nil {
		t.Fatal(err)
	}
	recs, _ := ReadSegment(path)
	if len(recs) != 0 {
		t.Fatalf("corrupt record must be dropped, got %d", len(recs))
	}
}

func TestFull(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 40)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("0123456789")); err != ErrFull {
		t.Fatalf("want ErrFull, got %v", err)
	}
}
