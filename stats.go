package ledgerdb

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stats is an operational snapshot. Counts and durations are process-local except
// CommittedLSN, RetainedKeys, and hold counts, which reflect recovered/live state.
// Fields sampled from different locks are not a transactional account snapshot.
type Stats struct {
	CommittedLSN        uint64
	Mutations           uint64
	Batches             uint64
	WALSyncs            uint64
	WALSyncDuration     time.Duration
	QueueDepth          int
	DuplicateRetries    uint64
	RetainedKeys        int
	ActiveHolds         int
	TerminalHolds       int
	Poisoned            bool
	RecoveryDuration    time.Duration
	CheckpointAttempts  uint64
	CheckpointSuccesses uint64
	LastCheckpointLSN   uint64
	LastCheckpointTime  time.Time
	LastCheckpointError string
}
type dbMetrics struct {
	lsn, mutations, batches, duplicates     atomic.Uint64
	recoveryNanos                           atomic.Int64
	checkpointMu                            sync.Mutex
	checkpointAttempts, checkpointSuccesses uint64
	checkpointLSN                           uint64
	checkpointTime                          time.Time
	checkpointError                         string
}

func (db *DB) Stats() Stats {
	s := Stats{CommittedLSN: db.metrics.lsn.Load(), Mutations: db.metrics.mutations.Load(), Batches: db.metrics.batches.Load(), QueueDepth: len(db.queue), DuplicateRetries: db.metrics.duplicates.Load(), Poisoned: db.poisoned.Load(), RecoveryDuration: time.Duration(db.metrics.recoveryNanos.Load())}
	if db.writer != nil {
		s.WALSyncs, s.WALSyncDuration = db.writer.SyncStats()
	}
	for i := range db.idem {
		shard := &db.idem[i]
		shard.Lock()
		for _, e := range shard.items {
			if e.State == idemCommitted && !db.expired(e.CommittedAt) {
				s.RetainedKeys++
			}
		}
		shard.Unlock()
	}
	db.holdsMu.RLock()
	for _, h := range db.holds {
		if h.Status == HoldReserved {
			s.ActiveHolds++
		} else {
			s.TerminalHolds++
		}
	}
	db.holdsMu.RUnlock()
	db.metrics.checkpointMu.Lock()
	s.CheckpointAttempts = db.metrics.checkpointAttempts
	s.CheckpointSuccesses = db.metrics.checkpointSuccesses
	s.LastCheckpointLSN = db.metrics.checkpointLSN
	s.LastCheckpointTime = db.metrics.checkpointTime
	s.LastCheckpointError = db.metrics.checkpointError
	db.metrics.checkpointMu.Unlock()
	return s
}
func (db *DB) checkpointLoop() {
	defer close(db.checkpointDone)
	var ticks <-chan time.Time
	var timer *time.Ticker
	if db.options.CheckpointInterval > 0 {
		timer = time.NewTicker(db.options.CheckpointInterval)
		ticks = timer.C
		defer timer.Stop()
	}
	var lastAttempt uint64
	for {
		select {
		case <-db.stop:
			return
		case <-ticks:
		case <-db.checkpointWake:
			if db.options.CheckpointOperations == 0 || db.metrics.mutations.Load()-lastAttempt < db.options.CheckpointOperations {
				continue
			}
		}
		select {
		case <-db.stop:
			return
		default:
		}
		lastAttempt = db.metrics.mutations.Load()
		if !db.poisoned.Load() {
			_ = db.Checkpoint(nil)
		}
	}
}
