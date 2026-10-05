package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestEncodeDecodeAndCRC(t *testing.T) {
	record := Record{Type: "transfer", LSN: 7, CommittedAt: 42, Key: "key", Request: json.RawMessage(`{"amount":1}`), Result: json.RawMessage(`{"ok":true}`)}
	encoded, err := Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Type != record.Type || decoded.LSN != record.LSN || decoded.Key != record.Key {
		t.Fatalf("decoded record changed: %#v", decoded)
	}
	corrupted := CorruptFinalCRC(encoded)
	if _, err := Decode(corrupted); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption, got %v", err)
	}
}

func TestDecodeRejectsTruncation(t *testing.T) {
	encoded, err := Encode(Record{Type: "create_account", LSN: 1, Key: "key", Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []int{0, 4, 15, len(encoded) - 1} {
		if _, err := Decode(encoded[:length]); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("length %d: expected corrupt, got %v", length, err)
		}
	}
}

type testFS struct{}

func (testFS) OpenFile(path string, flags int, mode fs.FileMode) (File, error) {
	return os.OpenFile(path, flags, mode)
}
func (testFS) ReadDir(path string) ([]fs.DirEntry, error) { return os.ReadDir(path) }
func (testFS) Remove(path string) error                   { return os.Remove(path) }
func TestBatchRotationAndEncodingFailure(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(testFS{}, dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	records := []Record{{Type: "transfer", LSN: 1, Key: "one", Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)}, {Type: "transfer", LSN: 2, Key: "two", Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)}, {Type: "transfer", LSN: 3, Key: "three", Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)}}
	invalid := records[1]
	invalid.Request = json.RawMessage(`{`)
	if err = w.AppendBatch([]Record{records[0], invalid}); err == nil {
		t.Fatal("invalid batch accepted")
	}
	info, err := os.Stat(filepath.Join(dir, segmentName(1)))
	if err != nil || info.Size() != 0 {
		t.Fatal("wrote before encoding complete", info, err)
	}
	if err = w.AppendBatch(records); err != nil {
		t.Fatal(err)
	}
	count, _ := w.SyncStats()
	if count != 3 {
		t.Fatalf("expected one sync per touched segment, got %d", count)
	}
	var replayed []uint64
	lsn, err := Replay(testFS{}, dir, 0, func(r Record) error { replayed = append(replayed, r.LSN); return nil })
	if err != nil || lsn != 3 || len(replayed) != 3 {
		t.Fatal(lsn, replayed, err)
	}
}
func TestTornBatchRetainsCompletePrefix(t *testing.T) {
	dir := t.TempDir()
	var data []byte
	for i := range 3 {
		b, e := Encode(Record{Type: "transfer", LSN: uint64(i + 1), Key: fmt.Sprint(i), Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)})
		if e != nil {
			t.Fatal(e)
		}
		if i == 2 {
			b = b[:len(b)-3]
		}
		data = append(data, b...)
	}
	path := filepath.Join(dir, segmentName(1))
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	seen := 0
	lsn, e := Replay(testFS{}, dir, 0, func(r Record) error { seen++; return nil })
	if e != nil || lsn != 2 || seen != 2 {
		t.Fatal(lsn, seen, e)
	}
	seen = 0
	lsn, e = Replay(testFS{}, dir, 0, func(r Record) error { seen++; return nil })
	if e != nil || lsn != 2 || seen != 2 {
		t.Fatal("repair did not persist", lsn, seen, e)
	}
}
func TestVersionTwoRejectsLegacyWriterAssumption(t *testing.T) {
	encoded, e := Encode(Record{Type: "transfer", LSN: 1, Key: "key", Request: json.RawMessage(`{}`), Result: json.RawMessage(`{}`)})
	if e != nil {
		t.Fatal(e)
	}
	if binary.LittleEndian.Uint16(encoded[4:6]) == 1 {
		t.Fatal("legacy reader would silently accept new format")
	}
	binary.LittleEndian.PutUint16(encoded[4:6], 1)
	if _, e = Decode(encoded); e != nil {
		t.Fatal("legacy version unsupported", e)
	}
	binary.LittleEndian.PutUint16(encoded[4:6], 3)
	if _, e = Decode(encoded); !errors.Is(e, ErrCorrupt) {
		t.Fatal("future version accepted", e)
	}
}
