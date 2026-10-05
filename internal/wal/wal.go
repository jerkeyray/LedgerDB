package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Magic                      = "LDBW"
	Version             uint16 = 2
	headerSize                 = 16
	MaxRecordBytes      int64  = 1 << 20
	DefaultSegmentBytes int64  = 64 << 20
)

var ErrCorrupt = errors.New("wal: corrupt record")

type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
	Stat() (fs.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

type FS interface {
	OpenFile(string, int, fs.FileMode) (File, error)
	ReadDir(string) ([]fs.DirEntry, error)
	Remove(string) error
}

type Record struct {
	Type        string          `json:"type"`
	LSN         uint64          `json:"lsn"`
	CommittedAt int64           `json:"committed_at"`
	Key         string          `json:"key"`
	Fingerprint [32]byte        `json:"fingerprint"`
	Request     json.RawMessage `json:"request"`
	Result      json.RawMessage `json:"result"`
	State       json.RawMessage `json:"state,omitempty"`
}

func Encode(record Record) ([]byte, error) {
	body, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > MaxRecordBytes {
		return nil, fmt.Errorf("wal: record too large")
	}
	buf := make([]byte, headerSize+len(body))
	copy(buf[:4], Magic)
	binary.LittleEndian.PutUint16(buf[4:6], Version)
	binary.LittleEndian.PutUint16(buf[6:8], 0)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(body)))
	binary.LittleEndian.PutUint32(buf[12:16], crc32.ChecksumIEEE(body))
	copy(buf[headerSize:], body)
	return buf, nil
}

func Decode(data []byte) (Record, error) {
	var record Record
	if len(data) < headerSize || string(data[:4]) != Magic || !supportedVersion(binary.LittleEndian.Uint16(data[4:6])) || binary.LittleEndian.Uint16(data[6:8]) != 0 {
		return record, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint32(data[8:12]))
	if n < 0 || int64(n) > MaxRecordBytes || len(data) != headerSize+n {
		return record, ErrCorrupt
	}
	body := data[headerSize:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(data[12:16]) {
		return record, ErrCorrupt
	}
	if err := json.Unmarshal(body, &record); err != nil {
		return record, ErrCorrupt
	}
	if record.LSN == 0 || record.Type == "" || record.Key == "" {
		return record, ErrCorrupt
	}
	return record, nil
}

func supportedVersion(v uint16) bool { return v == 1 || v == Version }

type Writer struct {
	mu           sync.Mutex
	fs           FS
	dir          string
	segmentBytes int64
	file         File
	segment      uint64
	size         int64
	syncCount    uint64
	syncDuration time.Duration
}

func Open(filesystem FS, dir string, segmentBytes int64) (*Writer, error) {
	if segmentBytes <= 0 {
		segmentBytes = DefaultSegmentBytes
	}
	segments, err := segmentNames(filesystem, dir)
	if err != nil {
		return nil, err
	}
	var segment uint64 = 1
	if len(segments) > 0 {
		segment = segments[len(segments)-1].number
	}
	path := filepath.Join(dir, segmentName(segment))
	f, err := filesystem.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syncDirectory(filesystem, dir); err != nil {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{fs: filesystem, dir: dir, segmentBytes: segmentBytes, file: f, segment: segment, size: info.Size()}, nil
}

func (w *Writer) Append(record Record) error { return w.AppendBatch([]Record{record}) }

// AppendBatch encodes before writing, retaining independent record boundaries.
// Every touched segment is synchronized before success; a crash may retain a prefix.
func (w *Writer) AppendBatch(records []Record) error {
	encoded := make([][]byte, len(records))
	for i, record := range records {
		var err error
		encoded[i], err = Encode(record)
		if err != nil {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	dirty := false
	for _, data := range encoded {
		if w.size > 0 && w.size+int64(len(data)) > w.segmentBytes {
			if dirty {
				if err := w.sync(); err != nil {
					return err
				}
				dirty = false
			}
			if err := w.rotate(); err != nil {
				return err
			}
		}
		for len(data) > 0 {
			n, err := w.file.Write(data)
			if n > 0 {
				w.size += int64(n)
				data = data[n:]
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
		}
		dirty = true
	}
	if dirty {
		return w.sync()
	}
	return nil
}
func (w *Writer) sync() error {
	start := time.Now()
	err := w.file.Sync()
	w.syncCount++
	w.syncDuration += time.Since(start)
	return err
}
func (w *Writer) SyncStats() (uint64, time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncCount, w.syncDuration
}

func (w *Writer) rotate() error {
	if err := w.file.Close(); err != nil {
		return err
	}
	w.segment++
	f, err := w.fs.OpenFile(filepath.Join(w.dir, segmentName(w.segment)), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if err := syncDirectory(w.fs, w.dir); err != nil {
		_ = f.Close()
		return err
	}
	w.file, w.size = f, 0
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *Writer) CompactThrough(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	segments, err := segmentNames(w.fs, w.dir)
	if err != nil {
		return err
	}
	removed := false
	for _, segment := range segments {
		if segment.number == w.segment {
			continue
		}
		maxLSN, err := scanSegment(w.fs, filepath.Join(w.dir, segment.name), nil, false)
		if err != nil {
			return err
		}
		if maxLSN <= lsn {
			if err := w.fs.Remove(filepath.Join(w.dir, segment.name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			removed = true
		}
	}
	if removed {
		return syncDirectory(w.fs, w.dir)
	}
	return nil
}

func Replay(filesystem FS, dir string, afterLSN uint64, apply func(Record) error) (uint64, error) {
	segments, err := segmentNames(filesystem, dir)
	if err != nil {
		return 0, err
	}
	maxLSN := afterLSN
	for index, segment := range segments {
		segmentMax, err := scanSegment(filesystem, filepath.Join(dir, segment.name), func(record Record) error {
			if record.LSN <= afterLSN {
				return nil
			}
			if record.LSN <= maxLSN {
				return fmt.Errorf("%w: non-monotonic LSN", ErrCorrupt)
			}
			if err := apply(record); err != nil {
				return err
			}
			maxLSN = record.LSN
			return nil
		}, index == len(segments)-1)
		if err != nil {
			return 0, err
		}
		if segmentMax > maxLSN {
			maxLSN = segmentMax
		}
	}
	return maxLSN, nil
}

func scanSegment(filesystem FS, path string, apply func(Record) error, repairTail bool) (uint64, error) {
	flags := os.O_RDONLY
	if repairTail {
		flags = os.O_RDWR
	}
	f, err := filesystem.OpenFile(path, flags, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	var offset int64
	var maxLSN uint64
	for offset < size {
		remaining := size - offset
		if remaining < headerSize {
			if repairTail {
				if err := f.Truncate(offset); err != nil {
					return 0, err
				}
				if err := f.Sync(); err != nil {
					return 0, err
				}
			} else {
				return 0, fmt.Errorf("%w: incomplete record at %s offset %d", ErrCorrupt, path, offset)
			}
			break
		}
		header := make([]byte, headerSize)
		if _, err := f.ReadAt(header, offset); err != nil {
			return 0, err
		}
		if string(header[:4]) != Magic || !supportedVersion(binary.LittleEndian.Uint16(header[4:6])) || binary.LittleEndian.Uint16(header[6:8]) != 0 {
			return 0, fmt.Errorf("%w: invalid header at %s offset %d", ErrCorrupt, path, offset)
		}
		bodyLen := int64(binary.LittleEndian.Uint32(header[8:12]))
		if bodyLen > MaxRecordBytes {
			return 0, fmt.Errorf("%w: oversized record at %s offset %d", ErrCorrupt, path, offset)
		}
		total := int64(headerSize) + bodyLen
		if bodyLen < 0 || total > remaining {
			if repairTail {
				if err := f.Truncate(offset); err != nil {
					return 0, err
				}
				if err := f.Sync(); err != nil {
					return 0, err
				}
			} else {
				return 0, fmt.Errorf("%w: incomplete record at %s offset %d", ErrCorrupt, path, offset)
			}
			break
		}
		data := make([]byte, total)
		if _, err := f.ReadAt(data, offset); err != nil {
			return 0, err
		}
		record, decodeErr := Decode(data)
		if decodeErr != nil {
			return 0, fmt.Errorf("%w at %s offset %d", ErrCorrupt, path, offset)
		}
		if record.LSN > maxLSN {
			maxLSN = record.LSN
		}
		if apply != nil {
			if err := apply(record); err != nil {
				return 0, err
			}
		}
		offset += total
	}
	return maxLSN, nil
}

type segmentInfo struct {
	name   string
	number uint64
}

func segmentNames(filesystem FS, dir string) ([]segmentInfo, error) {
	entries, err := filesystem.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segments []segmentInfo
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "wal-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "wal-"), ".log"), 10, 64)
		if err != nil || name != segmentName(n) {
			return nil, fmt.Errorf("%w: noncanonical WAL segment %q", ErrCorrupt, name)
		}
		segments = append(segments, segmentInfo{name, n})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].number < segments[j].number })
	return segments, nil
}

func segmentName(n uint64) string { return fmt.Sprintf("wal-%020d.log", n) }

func syncDirectory(filesystem FS, dir string) error {
	directory, err := filesystem.OpenFile(dir, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// CorruptFinalCRC is used by internal tests and fault-injection tooling.
func CorruptFinalCRC(encoded []byte) []byte {
	copyOf := bytes.Clone(encoded)
	if len(copyOf) >= headerSize {
		copyOf[12] ^= 0xff
	}
	return copyOf
}
