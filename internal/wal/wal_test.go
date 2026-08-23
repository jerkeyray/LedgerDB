package wal

import (
	"encoding/json"
	"errors"
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
