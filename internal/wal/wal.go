package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// ErrFull is returned when appending would exceed the configured size budget.
var ErrFull = errors.New("wal full")

const (
	headerSize  = 8 // 4-byte length + 4-byte crc32
	segmentSize = 32 << 20
)

// WAL is an append-only, CRC-checked, fsync-per-record log split into segments.
// A record is [uint32 length][uint32 crc32(payload)][payload]. On restart an
// incomplete or corrupt trailing record is discarded; it was never acknowledged.
type WAL struct {
	dir      string
	maxBytes int64

	mu      sync.Mutex
	file    *os.File
	seq     uint64
	curSize int64
	total   int64
	active  string
}

func Open(dir string, maxBytes int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, maxBytes: maxBytes}
	segs, err := w.segments()
	if err != nil {
		return nil, err
	}
	for _, s := range segs {
		fi, err := os.Stat(s)
		if err != nil {
			continue
		}
		w.total += fi.Size()
		base := filepath.Base(s)
		numStr := strings.TrimSuffix(strings.TrimPrefix(base, "wal-"), ".log")
		if n, err := strconv.ParseUint(numStr, 10, 64); err == nil && n >= w.seq {
			w.seq = n + 1
		}
	}
	return w, nil
}

func (w *WAL) segments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 4 && name[:4] == "wal-" && filepath.Ext(name) == ".log" {
			out = append(out, filepath.Join(w.dir, name))
		}
	}
	sort.Strings(out)
	return out, nil
}

// Append durably writes one record. It returns ErrFull if the budget is used.
func (w *WAL) Append(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	frameLen := int64(headerSize + len(payload))
	if w.total+frameLen > w.maxBytes {
		return ErrFull
	}
	if w.file == nil || w.curSize >= segmentSize {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	buf := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerSize:], payload)
	n, err := w.file.Write(buf)
	w.total += int64(n)
	w.curSize += int64(n)
	if err != nil {
		_ = w.sealLocked()
		return err
	}
	if err := w.file.Sync(); err != nil {
		_ = w.sealLocked()
		return err
	}
	return nil
}

func (w *WAL) rotateLocked() error {
	if err := w.sealLocked(); err != nil {
		return err
	}
	name := filepath.Join(w.dir, fmt.Sprintf("wal-%020d.log", w.seq))
	w.seq++
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	dir, err := os.Open(w.dir)
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return err
	}
	w.file = f
	w.active = name
	w.curSize = 0
	return nil
}

// Size returns the total bytes currently stored.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}

// Seal makes the active segment available for replay without creating an
// empty file. The next append opens a new segment.
func (w *WAL) Seal() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sealLocked()
}

func (w *WAL) sealLocked() error {
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.active = ""
	w.curSize = 0
	return err
}

// ClosedSegments returns segments that are complete and safe to replay, oldest
// first. The active segment is excluded.
func (w *WAL) ClosedSegments() ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	segs, err := w.segments()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(segs))
	for _, s := range segs {
		if s != w.active {
			out = append(out, s)
		}
	}
	return out, nil
}

// ReadSegment returns all complete, CRC-valid records in a segment.
func ReadSegment(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out [][]byte
	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		n := binary.BigEndian.Uint32(hdr[0:4])
		sum := binary.BigEndian.Uint32(hdr[4:8])
		if n == 0 || n > 16<<20 {
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			break
		}
		if crc32.ChecksumIEEE(payload) != sum {
			break
		}
		out = append(out, payload)
	}
	return out, nil
}

// RemoveSegment deletes a fully replayed segment and updates accounting.
func (w *WAL) RemoveSegment(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	w.mu.Lock()
	w.total -= fi.Size()
	if w.total < 0 {
		w.total = 0
	}
	w.mu.Unlock()
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}
