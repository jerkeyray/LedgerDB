package ledgerdb

import (
	"bytes"
	"context"
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
	"time"
)

const (
	checkpointMagic = "LDBC"
	manifestName    = "MANIFEST"
)

type checkpointData struct {
	Version     uint16                      `json:"version"`
	BaseLSN     uint64                      `json:"base_lsn"`
	Holds       []Hold                      `json:"holds,omitempty"`
	Accounts    []accountState              `json:"accounts"`
	Idempotency map[string]idempotencyEntry `json:"idempotency"`
}

type manifestData struct {
	Version    uint16 `json:"version"`
	Checkpoint string `json:"checkpoint"`
	BaseLSN    uint64 `json:"base_lsn"`
}

// Checkpoint publishes a fuzzy state snapshot and compacts covered WAL segments.
func (db *DB) Checkpoint(ctx context.Context) (retErr error) {
	var baseLSN uint64
	db.metrics.checkpointMu.Lock()
	db.metrics.checkpointAttempts++
	db.metrics.checkpointMu.Unlock()
	defer func() {
		db.metrics.checkpointMu.Lock()
		defer db.metrics.checkpointMu.Unlock()
		if retErr != nil {
			db.metrics.checkpointError = retErr.Error()
		} else {
			db.metrics.checkpointSuccesses++
			db.metrics.checkpointLSN = baseLSN
			db.metrics.checkpointTime = time.Now()
			db.metrics.checkpointError = ""
		}
	}()
	if err := checkContext(ctx); err != nil {
		return err
	}
	db.checkpointMu.Lock()
	defer db.checkpointMu.Unlock()
	if err := db.usable(); err != nil {
		return err
	}

	db.commitMu.Lock()
	baseLSN = db.nextLSN
	db.commitMu.Unlock()

	snapshot := checkpointData{Version: 2, BaseLSN: baseLSN, Idempotency: make(map[string]idempotencyEntry)}
	for i := range db.accounts {
		if err := checkContext(ctx); err != nil {
			return err
		}
		shard := &db.accounts[i]
		shard.RLock()
		for _, account := range shard.items {
			snapshot.Accounts = append(snapshot.Accounts, account)
		}
		shard.RUnlock()
	}
	db.holdsMu.RLock()
	for _, hold := range db.holds {
		snapshot.Holds = append(snapshot.Holds, hold)
	}
	db.holdsMu.RUnlock()
	sort.Slice(snapshot.Holds, func(i, j int) bool { return snapshot.Holds[i].ID < snapshot.Holds[j].ID })
	for i := range db.idem {
		if err := checkContext(ctx); err != nil {
			return err
		}
		shard := &db.idem[i]
		shard.Lock()
		for key, entry := range shard.items {
			if entry.State == idemCommitted && db.expired(entry.CommittedAt) {
				delete(shard.items, key)
				continue
			}
			if entry.State != idemCommitted {
				continue
			}
			snapshot.Idempotency[key] = idempotencyEntry{State: idemCommitted, Fingerprint: entry.Fingerprint, Type: entry.Type, Result: append(json.RawMessage(nil), entry.Result...), LSN: entry.LSN, CommittedAt: entry.CommittedAt}
		}
		shard.Unlock()
	}
	sort.Slice(snapshot.Accounts, func(i, j int) bool { return snapshot.Accounts[i].ID < snapshot.Accounts[j].ID })

	encoded, err := encodeEnvelope(checkpointMagic, snapshot)
	if err != nil {
		return err
	}
	finalPath := checkpointPath(db.dir, baseLSN)
	if err := db.publishFile(finalPath, encoded); err != nil {
		return fmt.Errorf("ledgerdb: publish checkpoint: %w", err)
	}
	manifest := manifestData{Version: 2, Checkpoint: filepath.Base(finalPath), BaseLSN: baseLSN}
	manifestBytes, err := encodeEnvelope("LDBM", manifest)
	if err != nil {
		return err
	}
	if err := db.publishFile(filepath.Join(db.dir, manifestName), manifestBytes); err != nil {
		return fmt.Errorf("ledgerdb: publish manifest: %w", err)
	}

	if err := db.writer.CompactThrough(baseLSN); err != nil {
		return fmt.Errorf("ledgerdb: compact WAL: %w", err)
	}
	db.removeOldCheckpoints(filepath.Base(finalPath))
	return nil
}

func (db *DB) loadCheckpoint() (uint64, error) {
	manifestBytes, err := db.fs.ReadFile(filepath.Join(db.dir, manifestName))
	if err == nil {
		var manifest manifestData
		if decodeEnvelope(manifestBytes, "LDBM", &manifest) != nil || (manifest.Version != 1 && manifest.Version != 2) || filepath.Base(manifest.Checkpoint) != manifest.Checkpoint {
			return 0, fmt.Errorf("%w: invalid manifest", ErrCorrupt)
		}
		data, readErr := db.fs.ReadFile(filepath.Join(db.dir, manifest.Checkpoint))
		if readErr != nil {
			return 0, fmt.Errorf("%w: manifest checkpoint: %v", ErrCorrupt, readErr)
		}
		var snapshot checkpointData
		if decodeEnvelope(data, checkpointMagic, &snapshot) != nil || (snapshot.Version != 1 && snapshot.Version != 2) || snapshot.BaseLSN != manifest.BaseLSN {
			return 0, fmt.Errorf("%w: invalid manifest checkpoint", ErrCorrupt)
		}
		db.installCheckpoint(snapshot)
		return snapshot.BaseLSN, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return 0, err
	}

	// Without a manifest, any checkpoint is an orphan published before the
	// manifest step. WAL compaction cannot yet have occurred, so using the newest
	// valid orphan is safe and replay remains complete.
	entries, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return 0, err
	}
	var discovered []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "checkpoint-") || !strings.HasSuffix(entry.Name(), ".dat") {
			continue
		}
		discovered = append(discovered, entry.Name())
	}
	sort.Slice(discovered, func(i, j int) bool { return checkpointNumber(discovered[i]) > checkpointNumber(discovered[j]) })
	for _, name := range discovered {
		data, readErr := db.fs.ReadFile(filepath.Join(db.dir, name))
		if readErr != nil {
			continue
		}
		var snapshot checkpointData
		if decodeEnvelope(data, checkpointMagic, &snapshot) != nil || (snapshot.Version != 1 && snapshot.Version != 2) {
			continue
		}
		db.installCheckpoint(snapshot)
		return snapshot.BaseLSN, nil
	}
	return 0, nil
}

func (db *DB) installCheckpoint(snapshot checkpointData) {
	for _, hold := range snapshot.Holds {
		db.holds[hold.ID] = hold
	}
	for _, account := range snapshot.Accounts {
		db.accounts[shardIndex(account.ID)].items[account.ID] = account
	}
	for key, entry := range snapshot.Idempotency {
		if db.expired(entry.CommittedAt) {
			continue
		}
		copyOf := entry
		copyOf.State = idemCommitted
		copyOf.Result = append(json.RawMessage(nil), entry.Result...)
		db.idem[shardIndex(key)].items[key] = &copyOf
	}
}

func encodeEnvelope(magic string, value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(magic) != 4 {
		return nil, fmt.Errorf("ledgerdb: invalid envelope magic")
	}
	buffer := bytes.NewBuffer(make([]byte, 0, 12+len(body)))
	buffer.WriteString(magic)
	_ = binary.Write(buffer, binary.LittleEndian, uint32(len(body)))
	_ = binary.Write(buffer, binary.LittleEndian, crc32.ChecksumIEEE(body))
	buffer.Write(body)
	return buffer.Bytes(), nil
}

func decodeEnvelope(data []byte, magic string, value any) error {
	if len(data) < 12 || string(data[:4]) != magic {
		return ErrCorrupt
	}
	length := int(binary.LittleEndian.Uint32(data[4:8]))
	if length < 0 || len(data) != 12+length {
		return ErrCorrupt
	}
	body := data[12:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(data[8:12]) {
		return ErrCorrupt
	}
	if err := json.Unmarshal(body, value); err != nil {
		return ErrCorrupt
	}
	return nil
}

func (db *DB) publishFile(finalPath string, data []byte) error {
	tmpPath := finalPath + ".tmp"
	f, err := db.fs.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = db.fs.Remove(tmpPath)
		}
	}()
	remaining := data
	for len(remaining) > 0 {
		n, writeErr := f.Write(remaining)
		if n > 0 {
			remaining = remaining[n:]
		}
		if writeErr != nil {
			_ = f.Close()
			return writeErr
		}
		if n == 0 {
			_ = f.Close()
			return io.ErrShortWrite
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := db.fs.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if err := db.syncDirectory(); err != nil {
		return err
	}
	ok = true
	return nil
}

func (db *DB) syncDirectory() error {
	directory, err := db.fs.OpenFile(db.dir, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (db *DB) removeOldCheckpoints(current string) {
	entries, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if name != current && strings.HasPrefix(name, "checkpoint-") && strings.HasSuffix(name, ".dat") {
			_ = db.fs.Remove(filepath.Join(db.dir, name))
		}
	}
}

func checkpointNumber(name string) uint64 {
	value := strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), ".dat")
	n, _ := strconv.ParseUint(value, 10, 64)
	return n
}
