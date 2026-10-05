package ledgerdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jerkeyray/ledgerdb/internal/wal"
	"math"
	"sort"
	"time"
)

type mutation struct {
	ctx          context.Context
	key, kind    string
	request      any
	fingerprint  [32]byte
	requestBytes json.RawMessage
	done         chan outcome
}
type outcome struct {
	result json.RawMessage
	err    error
}
type stateUpdates struct {
	Accounts []accountState `json:"accounts"`
	Holds    []Hold         `json:"holds,omitempty"`
}
type stagedState struct {
	db       *DB
	accounts map[string]accountState
	holds    map[string]Hold
	updates  stateUpdates
}

func (s *stagedState) account(id string) (accountState, bool) {
	if a, ok := s.accounts[id]; ok {
		return a, true
	}
	a, ok := s.db.accounts[shardIndex(id)].items[id]
	return a, ok
}
func (s *stagedState) hold(id string) (Hold, bool) {
	if h, ok := s.holds[id]; ok {
		return h, true
	}
	h, ok := s.db.holds[id]
	return h, ok
}
func (s *stagedState) putAccount(a accountState) {
	s.accounts[a.ID] = a
	s.updates.Accounts = append(s.updates.Accounts, a)
}
func (s *stagedState) putHold(h Hold) {
	s.holds[h.ID] = h
	s.updates.Holds = append(s.updates.Holds, h)
}

func (db *DB) submit(ctx context.Context, key, kind string, request any, result any) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := db.usable(); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if err := validateMutation(request); err != nil {
		return err
	}
	fp, data, err := fingerprint(kind, request)
	if err != nil {
		return err
	}
	existing, owner, err := db.reserve(ctx, key, fp)
	if err != nil {
		return err
	}
	if !owner {
		if existing.Type != kind {
			return ErrIdempotencyConflict
		}
		return json.Unmarshal(existing.Result, result)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m := &mutation{ctx: ctx, key: key, kind: kind, request: request, fingerprint: fp, requestBytes: data, done: make(chan outcome, 1)}
	// Admission and shutdown share a lock so no work is queued after the drain starts.
	db.admissionMu.RLock()
	if db.closing {
		db.admissionMu.RUnlock()
		db.abortReservation(key)
		return ErrClosed
	}
	select {
	case db.queue <- m:
		db.admissionMu.RUnlock()
	case <-ctx.Done():
		db.admissionMu.RUnlock()
		db.abortReservation(key)
		return ctx.Err()
	case <-db.stop:
		db.admissionMu.RUnlock()
		db.abortReservation(key)
		return ErrClosed
	}
	// The coordinator checks cancellation before processing; afterwards it owns the outcome.
	out := <-m.done
	if out.err != nil {
		return out.err
	}
	return json.Unmarshal(out.result, result)
}
func validateHoldID(id string) error {
	if validateAccountID(id) != nil {
		return ErrInvalidHoldID
	}
	return nil
}
func validateMutation(request any) error {
	switch r := request.(type) {
	case CreateAccountRequest:
		if err := validateAccountID(r.ID); err != nil {
			return err
		}
		if err := validateCurrency(r.Currency); err != nil {
			return err
		}
		if r.OpeningBalance < 0 {
			return ErrInvalidOpeningBalance
		}
	case TransferRequest:
		return validatePair(r.FromAccount, r.ToAccount, r.Amount)
	case ReserveRequest:
		if err := validateHoldID(r.HoldID); err != nil {
			return err
		}
		return validatePair(r.FromAccount, r.ToAccount, r.Amount)
	case SettleRequest:
		if err := validateHoldID(r.HoldID); err != nil {
			return err
		}
		if r.Amount < 0 {
			return ErrInvalidAmount
		}
	case CancelRequest:
		return validateHoldID(r.HoldID)
	}
	return nil
}
func validatePair(from, to string, amount int64) error {
	if err := validateAccountID(from); err != nil {
		return err
	}
	if err := validateAccountID(to); err != nil {
		return err
	}
	if from == to {
		return ErrSameAccount
	}
	if amount <= 0 {
		return ErrInvalidAmount
	}
	return nil
}
func (db *DB) commitLoop() {
	defer close(db.workerDone)
	for {
		var first *mutation
		select {
		case first = <-db.queue:
		case <-db.stop:
			for {
				select {
				case m := <-db.queue:
					db.processBatch([]*mutation{m})
				default:
					return
				}
			}
		}
		batch := []*mutation{first}
		if db.options.GroupCommitMaxBatch > 1 {
			timer := time.NewTimer(db.options.GroupCommitDelay)
		collect:
			for len(batch) < db.options.GroupCommitMaxBatch {
				// Drain already queued work even when the configured delay is zero.
				select {
				case m := <-db.queue:
					batch = append(batch, m)
					continue
				default:
				}
				select {
				case m := <-db.queue:
					batch = append(batch, m)
				case <-timer.C:
					break collect
				case <-db.stop:
					break collect
				}
			}
			timer.Stop()
		}
		db.processBatch(batch)
	}
}
func (db *DB) processBatch(batch []*mutation) {
	// Resolve immutable hold account IDs before acquiring account locks.
	indices := map[int]bool{}
	db.holdsMu.RLock()
	for _, m := range batch {
		switch r := m.request.(type) {
		case CreateAccountRequest:
			indices[shardIndex(r.ID)] = true
		case TransferRequest:
			indices[shardIndex(r.FromAccount)] = true
			indices[shardIndex(r.ToAccount)] = true
		case ReserveRequest:
			indices[shardIndex(r.FromAccount)] = true
			indices[shardIndex(r.ToAccount)] = true
		case SettleRequest:
			if h, ok := db.holds[r.HoldID]; ok {
				indices[shardIndex(h.Request.FromAccount)] = true
				indices[shardIndex(h.Request.ToAccount)] = true
			}
		case CancelRequest:
			if h, ok := db.holds[r.HoldID]; ok {
				indices[shardIndex(h.Request.FromAccount)] = true
				indices[shardIndex(h.Request.ToAccount)] = true
			}
		}
	}
	db.holdsMu.RUnlock()
	// A reserve earlier in this batch already contributes the shards for a later settle.
	ordered := make([]int, 0, len(indices))
	for i := range indices {
		ordered = append(ordered, i)
	}
	sort.Ints(ordered)
	for _, i := range ordered {
		db.accounts[i].Lock()
	}
	db.holdsMu.Lock()
	db.commitMu.Lock()
	defer func() {
		db.commitMu.Unlock()
		db.holdsMu.Unlock()
		for i := len(ordered) - 1; i >= 0; i-- {
			db.accounts[ordered[i]].Unlock()
		}
	}()
	state := &stagedState{db: db, accounts: map[string]accountState{}, holds: map[string]Hold{}}
	outcomes := make([]outcome, len(batch))
	records := []wal.Record{}
	next := db.nextLSN
	for i, m := range batch {
		if err := db.usable(); err != nil {
			outcomes[i].err = err
			continue
		}
		if err := checkContext(m.ctx); err != nil {
			outcomes[i].err = err
			continue
		}
		if next == math.MaxUint64 {
			outcomes[i].err = ErrLSNExhausted
			continue
		}
		state.updates = stateUpdates{}
		result, changed, err := state.evaluate(m.request, next+1)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		data, err := json.Marshal(result)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		outcomes[i].result = data
		if !changed {
			// Retain this new key as an alias to the original response, without
			// applying financial state again. Its WAL LSN differs from the result LSN.
			db.metrics.duplicates.Add(1)
		}
		next++
		updates, _ := json.Marshal(state.updates)
		records = append(records, wal.Record{Type: m.kind, LSN: next, CommittedAt: db.clock().UnixNano(), Key: m.key, Fingerprint: m.fingerprint, Request: m.requestBytes, Result: data, State: updates})
	}
	if len(records) > 0 {
		if err := db.writer.AppendBatch(records); err != nil {
			db.poisoned.Store(true)
			for i := range outcomes {
				outcomes[i] = outcome{err: fmt.Errorf("%w: WAL batch: %w", ErrPoisoned, err)}
			}
		} else {
			for _, a := range state.accounts {
				db.accounts[shardIndex(a.ID)].items[a.ID] = a
			}
			for _, h := range state.holds {
				db.holds[h.ID] = h
			}
			db.nextLSN = next
			db.metrics.lsn.Store(next)
			db.metrics.mutations.Add(uint64(len(records)))
			db.metrics.batches.Add(1)
			for _, record := range records {
				db.commitReservation(record.Key, record)
			}
			if db.options.CheckpointOperations > 0 {
				select {
				case db.checkpointWake <- struct{}{}:
				default:
				}
			}
		}
	}
	if len(records) > 0 && !db.poisoned.Load() {
		db.pruneOneIdempotencyShard()
	}
	for i, m := range batch {
		// Committed reservations have already been published; all other owners release theirs.
		db.abortReservation(m.key)
		m.done <- outcomes[i]
	}
}
func (s *stagedState) evaluate(request any, lsn uint64) (any, bool, error) {
	switch r := request.(type) {
	case CreateAccountRequest:
		if _, ok := s.account(r.ID); ok {
			return nil, false, ErrAccountExists
		}
		a := accountState{ID: r.ID, Currency: r.Currency, Balance: r.OpeningBalance, LSN: lsn}
		s.putAccount(a)
		return CreateAccountResult{Account: a.public(), CommitLSN: lsn}, true, nil
	case TransferRequest:
		from, to, err := s.pair(r.FromAccount, r.ToAccount)
		if err != nil {
			return nil, false, err
		}
		if from.Balance-from.Reserved < r.Amount {
			return nil, false, ErrInsufficientFunds
		}
		if to.Balance > math.MaxInt64-r.Amount {
			return nil, false, ErrBalanceOverflow
		}
		from.Balance -= r.Amount
		to.Balance += r.Amount
		from.LSN = lsn
		to.LSN = lsn
		s.putAccount(from)
		s.putAccount(to)
		return TransferResult{FromBalance: from.Balance, ToBalance: to.Balance, CommitLSN: lsn}, true, nil
	case ReserveRequest:
		if _, ok := s.hold(r.HoldID); ok {
			return nil, false, ErrHoldExists
		}
		from, to, err := s.pair(r.FromAccount, r.ToAccount)
		if err != nil {
			return nil, false, err
		}
		if from.Balance-from.Reserved < r.Amount {
			return nil, false, ErrInsufficientFunds
		}
		from.Reserved += r.Amount
		from.LSN = lsn
		s.putAccount(from)
		h := Hold{ID: r.HoldID, Request: r, Status: HoldReserved, LSN: lsn}
		s.putHold(h)
		return paymentResult(h, from, to, lsn), true, nil
	case SettleRequest:
		return s.resolve(r.HoldID, r.Amount, false, lsn)
	case CancelRequest:
		return s.resolve(r.HoldID, 0, true, lsn)
	}
	return nil, false, errors.New("ledgerdb: unsupported mutation")
}
func (s *stagedState) pair(fromID, toID string) (accountState, accountState, error) {
	from, ok := s.account(fromID)
	if !ok {
		return from, accountState{}, ErrAccountNotFound
	}
	to, ok := s.account(toID)
	if !ok {
		return from, to, ErrAccountNotFound
	}
	if from.Currency != to.Currency {
		return from, to, ErrCurrencyMismatch
	}
	return from, to, nil
}
func (s *stagedState) resolve(id string, amount int64, cancel bool, lsn uint64) (any, bool, error) {
	h, ok := s.hold(id)
	if !ok {
		return nil, false, ErrHoldNotFound
	}
	if h.Status != HoldReserved {
		if (cancel && h.Status == HoldCancelled) || (!cancel && h.Status == HoldSettled && h.SettledAmount == amount) {
			return *h.TerminalResult, false, nil
		}
		return nil, false, ErrHoldConflict
	}
	if amount > h.Request.Amount {
		return nil, false, ErrInvalidAmount
	}
	from, to, err := s.pair(h.Request.FromAccount, h.Request.ToAccount)
	if err != nil {
		return nil, false, err
	}
	if to.Balance > math.MaxInt64-amount {
		return nil, false, ErrBalanceOverflow
	}
	from.Reserved -= h.Request.Amount
	from.Balance -= amount
	from.LSN = lsn
	s.putAccount(from)
	h.Status = HoldSettled
	h.SettledAmount = amount
	h.LSN = lsn
	if cancel {
		h.Status = HoldCancelled
	} else {
		to.Balance += amount
		to.LSN = lsn
		s.putAccount(to)
	}
	result := paymentResult(h, from, to, lsn)
	h.TerminalResult = &result
	s.putHold(h)
	return result, true, nil
}
func paymentResult(h Hold, from, to accountState, lsn uint64) PaymentResult {
	return PaymentResult{HoldID: h.ID, Status: h.Status, SettledAmount: h.SettledAmount, FromBalance: from.Balance, FromReserved: from.Reserved, ToBalance: to.Balance, CommitLSN: lsn}
}
func (db *DB) Reserve(ctx context.Context, key string, r ReserveRequest) (PaymentResult, error) {
	var result PaymentResult
	err := db.submit(ctx, key, "reserve", r, &result)
	return result, err
}
func (db *DB) Settle(ctx context.Context, key string, r SettleRequest) (PaymentResult, error) {
	var result PaymentResult
	err := db.submit(ctx, key, "settle", r, &result)
	return result, err
}
func (db *DB) Cancel(ctx context.Context, key string, r CancelRequest) (PaymentResult, error) {
	var result PaymentResult
	err := db.submit(ctx, key, "cancel", r, &result)
	return result, err
}
func (db *DB) GetHold(ctx context.Context, id string) (Hold, error) {
	if err := checkContext(ctx); err != nil {
		return Hold{}, err
	}
	if db.closed.Load() {
		return Hold{}, ErrClosed
	}
	if err := validateHoldID(id); err != nil {
		return Hold{}, err
	}
	db.holdsMu.RLock()
	defer db.holdsMu.RUnlock()
	h, ok := db.holds[id]
	if !ok {
		return Hold{}, ErrHoldNotFound
	}
	if h.TerminalResult != nil {
		copyOf := *h.TerminalResult
		h.TerminalResult = &copyOf
	}
	return h, nil
}
func (db *DB) validateRecoveredState() error {
	reserved := map[string]int64{}
	for _, h := range db.holds {
		if validateMutation(h.Request) != nil || h.ID != h.Request.HoldID || (h.LSN == 0 || h.LSN > db.nextLSN) {
			return fmt.Errorf("%w: invalid hold", ErrCorrupt)
		}
		from, ok := db.accounts[shardIndex(h.Request.FromAccount)].items[h.Request.FromAccount]
		if !ok {
			return fmt.Errorf("%w: hold source", ErrCorrupt)
		}
		to, ok := db.accounts[shardIndex(h.Request.ToAccount)].items[h.Request.ToAccount]
		if !ok || from.Currency != to.Currency {
			return fmt.Errorf("%w: hold destination", ErrCorrupt)
		}
		switch h.Status {
		case HoldReserved:
			if h.TerminalResult != nil || h.SettledAmount != 0 || reserved[from.ID] > math.MaxInt64-h.Request.Amount {
				return fmt.Errorf("%w: reservation", ErrCorrupt)
			}
			reserved[from.ID] += h.Request.Amount
		case HoldSettled, HoldCancelled:
			if h.TerminalResult == nil || h.SettledAmount < 0 || h.SettledAmount > h.Request.Amount || h.TerminalResult.HoldID != h.ID || h.TerminalResult.Status != h.Status || h.TerminalResult.SettledAmount != h.SettledAmount || h.TerminalResult.CommitLSN != h.LSN || (h.Status == HoldCancelled && h.SettledAmount != 0) {
				return fmt.Errorf("%w: terminal hold", ErrCorrupt)
			}
		default:
			return fmt.Errorf("%w: hold status", ErrCorrupt)
		}
	}
	for i := range db.accounts {
		for _, a := range db.accounts[i].items {
			if validateAccountID(a.ID) != nil || validateCurrency(a.Currency) != nil || a.LSN == 0 || a.LSN > db.nextLSN || a.Balance < 0 || a.Reserved < 0 || a.Reserved > a.Balance || a.Reserved != reserved[a.ID] {
				return fmt.Errorf("%w: account reservation invariant", ErrCorrupt)
			}
		}
	}
	return nil
}
