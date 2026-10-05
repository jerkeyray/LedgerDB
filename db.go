package ledgerdb

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jerkeyray/ledgerdb/internal/wal"
)

const shardCount = 64

type accountState struct {
	ID       string `json:"id"`
	Currency string `json:"currency"`
	Balance  int64  `json:"balance"`
	Reserved int64  `json:"reserved,omitempty"`
	LSN      uint64 `json:"lsn"`
}

type accountShard struct {
	sync.RWMutex
	items map[string]accountState
}

type idempotencyState string

const (
	idemInFlight  idempotencyState = "in_flight"
	idemCommitted idempotencyState = "committed"
)

type idempotencyEntry struct {
	State       idempotencyState `json:"-"`
	Fingerprint [32]byte         `json:"fingerprint"`
	Type        string           `json:"type"`
	Result      json.RawMessage  `json:"result"`
	LSN         uint64           `json:"lsn"`
	CommittedAt int64            `json:"committed_at"`
	done        chan struct{}
}

type idempotencyShard struct {
	sync.Mutex
	items map[string]*idempotencyEntry
}

type DB struct {
	dir       string
	fs        FileSystem
	clock     func() time.Time
	retention time.Duration

	accounts [shardCount]accountShard
	idem     [shardCount]idempotencyShard

	commitMu       sync.Mutex
	checkpointMu   sync.Mutex
	writer         *wal.Writer
	directoryLock  Lock
	nextLSN        uint64
	closed         atomic.Bool
	poisoned       atomic.Bool
	pruneCursor    atomic.Uint32
	holdsMu        sync.RWMutex
	holds          map[string]Hold
	queue          chan *mutation
	admissionMu    sync.RWMutex
	closing        bool
	stop           chan struct{}
	workerDone     chan struct{}
	checkpointDone chan struct{}
	checkpointWake chan struct{}
	closeOnce      sync.Once
	closeErr       error
	options        Options
	metrics        dbMetrics
}

// Open creates or recovers a database and exclusively locks its directory.
func Open(path string, options Options) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("ledgerdb: empty database path")
	}
	filesystem := options.FileSystem
	if filesystem == nil {
		filesystem = NewOSFileSystem()
	}
	retention := options.IdempotencyRetention
	if retention == 0 {
		retention = DefaultIdempotencyRetention
	}
	if retention < 0 {
		return nil, fmt.Errorf("ledgerdb: idempotency retention must be nonnegative")
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	if err := durableMkdirAll(filesystem, path); err != nil {
		return nil, err
	}
	lockingFilesystem, ok := filesystem.(LockingFileSystem)
	if !ok {
		return nil, ErrLockUnsupported
	}
	directoryLock, err := lockingFilesystem.Lock(filepath.Join(path, "LOCK"))
	if err != nil {
		return nil, err
	}
	failOpen := func(openErr error) (*DB, error) {
		return nil, errors.Join(openErr, directoryLock.Close())
	}
	if err := cleanupTemporaryFiles(filesystem, path); err != nil {
		return failOpen(err)
	}

	if options.GroupCommitMaxBatch < 0 || options.GroupCommitDelay < 0 || options.CheckpointInterval < 0 {
		return failOpen(fmt.Errorf("ledgerdb: negative scheduling option"))
	}
	if options.GroupCommitMaxBatch == 0 {
		options.GroupCommitMaxBatch = 1
	}
	if options.GroupCommitMaxBatch > 1024 {
		return failOpen(fmt.Errorf("ledgerdb: batch size exceeds queue capacity"))
	}
	recoveryStart := time.Now()
	db := &DB{dir: path, fs: filesystem, clock: clock, retention: retention, directoryLock: directoryLock,
		holds: make(map[string]Hold), queue: make(chan *mutation, 1024), stop: make(chan struct{}),
		workerDone: make(chan struct{}), checkpointDone: make(chan struct{}), checkpointWake: make(chan struct{}, 1), options: options}
	for i := range db.accounts {
		db.accounts[i].items = make(map[string]accountState)
	}
	for i := range db.idem {
		db.idem[i].items = make(map[string]*idempotencyEntry)
	}

	replayFrom, err := db.loadCheckpoint()
	if err != nil {
		return failOpen(err)
	}
	walFS := walFSAdapter{filesystem}
	maxLSN, err := wal.Replay(walFS, path, replayFrom, db.applyRecoveryRecord)
	if err != nil {
		if errors.Is(err, wal.ErrCorrupt) {
			return failOpen(fmt.Errorf("%w: %w", ErrCorrupt, err))
		}
		return failOpen(err)
	}
	db.nextLSN = maxLSN
	if err := db.validateRecoveredState(); err != nil {
		return failOpen(err)
	}
	db.metrics.lsn.Store(maxLSN)
	db.metrics.checkpointLSN = replayFrom
	db.metrics.recoveryNanos.Store(int64(time.Since(recoveryStart)))
	writer, err := wal.Open(walFS, path, options.WALSegmentBytes)
	if err != nil {
		return failOpen(err)
	}
	db.writer = writer
	go db.commitLoop()
	go db.checkpointLoop()
	return db, nil
}

type walFSAdapter struct{ FileSystem }

func (adapter walFSAdapter) OpenFile(path string, flag int, mode fs.FileMode) (wal.File, error) {
	return adapter.FileSystem.OpenFile(path, flag, mode)
}

// CreateAccount durably creates an account with safe retries.
func (db *DB) CreateAccount(ctx context.Context, key string, request CreateAccountRequest) (CreateAccountResult, error) {
	var result CreateAccountResult
	err := db.submit(ctx, key, "create_account", request, &result)
	return result, err
}

// Transfer atomically moves available funds between two accounts.
func (db *DB) Transfer(ctx context.Context, key string, request TransferRequest) (TransferResult, error) {
	var result TransferResult
	err := db.submit(ctx, key, "transfer", request, &result)
	return result, err
}

// GetAccount returns the latest in-memory snapshot of an account.
func (db *DB) GetAccount(ctx context.Context, id string) (Account, error) {
	if err := checkContext(ctx); err != nil {
		return Account{}, err
	}
	if db.closed.Load() {
		return Account{}, ErrClosed
	}
	if err := validateAccountID(id); err != nil {
		return Account{}, err
	}
	shard := &db.accounts[shardIndex(id)]
	shard.RLock()
	state, ok := shard.items[id]
	shard.RUnlock()
	if !ok {
		return Account{}, ErrAccountNotFound
	}
	return state.public(), nil
}

// Close closes the WAL and releases the exclusive directory lock.
func (db *DB) Close() error {
	db.closeOnce.Do(func() {
		db.admissionMu.Lock()
		db.closing = true
		close(db.stop)
		db.admissionMu.Unlock()
		<-db.checkpointDone
		<-db.workerDone
		db.checkpointMu.Lock()
		defer db.checkpointMu.Unlock()
		db.commitMu.Lock()
		defer db.commitMu.Unlock()
		db.closed.Store(true)
		db.closeErr = errors.Join(db.writer.Close(), db.directoryLock.Close())
	})
	return db.closeErr
}

func (db *DB) usable() error {
	if db.closed.Load() {
		return ErrClosed
	}
	if db.poisoned.Load() {
		return ErrPoisoned
	}
	return nil
}

func (db *DB) reserve(ctx context.Context, key string, fingerprint [32]byte) (*idempotencyEntry, bool, error) {
	shard := &db.idem[shardIndex(key)]
	for {
		shard.Lock()
		entry, ok := shard.items[key]
		if ok && entry.State == idemCommitted && db.expired(entry.CommittedAt) {
			delete(shard.items, key)
			entry, ok = nil, false
		}
		if !ok {
			shard.items[key] = &idempotencyEntry{State: idemInFlight, Fingerprint: fingerprint, done: make(chan struct{})}
			shard.Unlock()
			return nil, true, nil
		}
		if entry.Fingerprint != fingerprint {
			shard.Unlock()
			return nil, false, ErrIdempotencyConflict
		}
		if entry.State == idemCommitted {
			db.metrics.duplicates.Add(1)
			copyOf := *entry
			copyOf.Result = append(json.RawMessage(nil), entry.Result...)
			shard.Unlock()
			return &copyOf, false, nil
		}
		done := entry.done
		shard.Unlock()
		var contextDone <-chan struct{}
		if ctx != nil {
			contextDone = ctx.Done()
		}
		select {
		case <-contextDone:
			return nil, false, ctx.Err()
		case <-done:
			if err := db.usable(); err != nil {
				return nil, false, err
			}
		}
	}
}

func (db *DB) commitReservation(key string, record wal.Record) {
	shard := &db.idem[shardIndex(key)]
	shard.Lock()
	entry := shard.items[key]
	entry.State, entry.Type, entry.Result = idemCommitted, record.Type, append(json.RawMessage(nil), record.Result...)
	entry.LSN, entry.CommittedAt = record.LSN, record.CommittedAt
	close(entry.done)
	entry.done = nil
	shard.Unlock()
}

func (db *DB) abortReservation(key string) {
	shard := &db.idem[shardIndex(key)]
	shard.Lock()
	if entry, ok := shard.items[key]; ok && entry.State == idemInFlight {
		delete(shard.items, key)
		close(entry.done)
	}
	shard.Unlock()
}

func (db *DB) expired(committedAt int64) bool {
	return db.clock().Sub(time.Unix(0, committedAt)) >= db.retention
}

func (db *DB) pruneOneIdempotencyShard() {
	index := int((db.pruneCursor.Add(1) - 1) % shardCount)
	shard := &db.idem[index]
	shard.Lock()
	for key, entry := range shard.items {
		if entry.State == idemCommitted && db.expired(entry.CommittedAt) {
			delete(shard.items, key)
		}
	}
	shard.Unlock()
}

func (db *DB) applyRecoveryRecord(record wal.Record) error {
	if len(record.State) != 0 {
		switch record.Type {
		case "create_account", "transfer", "reserve", "settle", "cancel":
		default:
			return fmt.Errorf("%w: unknown record type", wal.ErrCorrupt)
		}
		var state stateUpdates
		if json.Unmarshal(record.State, &state) != nil {
			return fmt.Errorf("%w: state payload", wal.ErrCorrupt)
		}
		for _, account := range state.Accounts {
			if account.LSN != record.LSN {
				return fmt.Errorf("%w: account LSN", wal.ErrCorrupt)
			}
			index := shardIndex(account.ID)
			current, exists := db.accounts[index].items[account.ID]
			if !exists || current.LSN < record.LSN {
				db.accounts[index].items[account.ID] = account
			}
		}
		for _, hold := range state.Holds {
			if hold.LSN != record.LSN {
				return fmt.Errorf("%w: hold LSN", wal.ErrCorrupt)
			}
			current, exists := db.holds[hold.ID]
			if !exists || current.LSN < record.LSN {
				db.holds[hold.ID] = hold
			}
		}
	} else {
		switch record.Type {
		case "create_account":
			var result CreateAccountResult
			if err := json.Unmarshal(record.Result, &result); err != nil {
				return fmt.Errorf("%w: create result", wal.ErrCorrupt)
			}
			index := shardIndex(result.Account.ID)
			current, exists := db.accounts[index].items[result.Account.ID]
			if !exists || current.LSN < record.LSN {
				db.accounts[index].items[result.Account.ID] = accountState{ID: result.Account.ID, Currency: result.Account.Currency, Balance: result.Account.Balance, LSN: record.LSN}
			}
		case "transfer":
			var request TransferRequest
			var result TransferResult
			if json.Unmarshal(record.Request, &request) != nil || json.Unmarshal(record.Result, &result) != nil {
				return fmt.Errorf("%w: transfer payload", wal.ErrCorrupt)
			}
			fromIndex, toIndex := shardIndex(request.FromAccount), shardIndex(request.ToAccount)
			from, fromOK := db.accounts[fromIndex].items[request.FromAccount]
			to, toOK := db.accounts[toIndex].items[request.ToAccount]
			if !fromOK || !toOK {
				return fmt.Errorf("%w: transfer references missing account", wal.ErrCorrupt)
			}
			if from.LSN < record.LSN {
				from.Balance, from.LSN = result.FromBalance, record.LSN
				db.accounts[fromIndex].items[from.ID] = from
			}
			if to.LSN < record.LSN {
				to.Balance, to.LSN = result.ToBalance, record.LSN
				db.accounts[toIndex].items[to.ID] = to
			}
		default:
			return fmt.Errorf("%w: unknown record type %q", wal.ErrCorrupt, record.Type)
		}
	}
	if !db.expired(record.CommittedAt) {
		index := shardIndex(record.Key)
		current := db.idem[index].items[record.Key]
		if current == nil || current.LSN < record.LSN {
			db.idem[index].items[record.Key] = &idempotencyEntry{State: idemCommitted, Fingerprint: record.Fingerprint, Type: record.Type, Result: append(json.RawMessage(nil), record.Result...), LSN: record.LSN, CommittedAt: record.CommittedAt}
		}
	}
	return nil
}

func fingerprint(operation string, request any) ([32]byte, []byte, error) {
	data, err := json.Marshal(request)
	if err != nil {
		return [32]byte{}, nil, err
	}
	hash := sha256.New()
	hash.Write([]byte(operation))
	hash.Write([]byte{0})
	hash.Write(data)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result, data, nil
}

func shardIndex(value string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return int(h.Sum32() % shardCount)
}

func validateKey(key string) error {
	if len(key) == 0 || len(key) > 256 || !utf8.ValidString(key) {
		return ErrInvalidIdempotencyKey
	}
	return nil
}

func validateAccountID(id string) error {
	if len(id) == 0 || len(id) > 128 || !utf8.ValidString(id) {
		return ErrInvalidAccountID
	}
	return nil
}

func validateCurrency(currency string) error {
	if len(currency) != 3 {
		return ErrInvalidCurrency
	}
	for i := range 3 {
		if currency[i] < 'A' || currency[i] > 'Z' {
			return ErrInvalidCurrency
		}
	}
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (state accountState) public() Account {
	return Account{ID: state.ID, Currency: state.Currency, Balance: state.Balance, Reserved: state.Reserved, Available: state.Balance - state.Reserved, LSN: state.LSN}
}

func checkpointPath(dir string, baseLSN uint64) string {
	return filepath.Join(dir, fmt.Sprintf("checkpoint-%020d.dat", baseLSN))
}
