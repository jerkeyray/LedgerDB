package ledgerdb_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jerkeyray/ledgerdb"
	"github.com/jerkeyray/ledgerdb/internal/failpoint"
	"github.com/jerkeyray/ledgerdb/internal/wal"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func paymentDB(t *testing.T, opts ledgerdb.Options) (*ledgerdb.DB, string) {
	t.Helper()
	dir := t.TempDir()
	db := openDB(t, dir, opts)
	t.Cleanup(func() { _ = db.Close() })
	createAccount(t, db, "create-a", "a", "INR", 1000)
	createAccount(t, db, "create-b", "b", "INR", 0)
	return db, dir
}
func hold(t *testing.T, db *ledgerdb.DB, id string, amount int64) ledgerdb.PaymentResult {
	t.Helper()
	r, e := db.Reserve(context.Background(), "reserve:"+id, ledgerdb.ReserveRequest{HoldID: id, FromAccount: "a", ToAccount: "b", Amount: amount})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestPaymentLifecycleAndCheckpoint(t *testing.T) {
	for _, amount := range []int64{0, 180, 500} {
		t.Run(fmt.Sprint(amount), func(t *testing.T) {
			db, dir := paymentDB(t, ledgerdb.Options{WALSegmentBytes: 1000})
			first := hold(t, db, "h", 500)
			again, e := db.Reserve(context.Background(), "reserve:h", ledgerdb.ReserveRequest{HoldID: "h", FromAccount: "a", ToAccount: "b", Amount: 500})
			if e != nil || again != first {
				t.Fatalf("reserve retry %v %v", again, e)
			}
			a, _ := db.GetAccount(nil, "a")
			if a.Balance != 1000 || a.Available != 500 || a.Reserved != 500 {
				t.Fatal(a)
			}
			_, e = db.Transfer(nil, "overspend", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 501})
			if !errors.Is(e, ledgerdb.ErrInsufficientFunds) {
				t.Fatal(e)
			}
			original, e := db.Settle(nil, "settle:h", ledgerdb.SettleRequest{HoldID: "h", Amount: amount})
			if e != nil {
				t.Fatal(e)
			}
			copyOf, e := db.Settle(nil, "alias:h", ledgerdb.SettleRequest{HoldID: "h", Amount: amount})
			if e != nil || copyOf != original {
				t.Fatalf("terminal alias %v %v", copyOf, e)
			}
			_, e = db.Settle(nil, "alias:h", ledgerdb.SettleRequest{HoldID: "h", Amount: amount + 1})
			if !errors.Is(e, ledgerdb.ErrIdempotencyConflict) {
				t.Fatalf("alias conflict %v", e)
			}
			_, e = db.Cancel(nil, "cancel:h", ledgerdb.CancelRequest{HoldID: "h"})
			if !errors.Is(e, ledgerdb.ErrHoldConflict) {
				t.Fatal(e)
			}
			if e = db.Checkpoint(nil); e != nil {
				t.Fatal(e)
			}
			if e = db.Close(); e != nil {
				t.Fatal(e)
			}
			db = openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			a, _ = db.GetAccount(nil, "a")
			b, _ := db.GetAccount(nil, "b")
			if a.Reserved != 0 || a.Available != 1000-amount || a.Balance+b.Balance != 1000 {
				t.Fatalf("recovery a=%v b=%v", a, b)
			}
			recovered, e := db.Settle(nil, "settle:h", ledgerdb.SettleRequest{HoldID: "h", Amount: amount})
			if e != nil || recovered != original {
				t.Fatalf("recovered retry %v %v", recovered, e)
			}
			h, e := db.GetHold(nil, "h")
			if e != nil {
				t.Fatal(e)
			}
			h.TerminalResult.FromBalance = -99
			unchanged, _ := db.GetHold(nil, "h")
			if unchanged.TerminalResult.FromBalance == -99 {
				t.Fatal("mutable public hold")
			}
		})
	}
}
func TestPaymentExpiryAndCancellation(t *testing.T) {
	var nanos atomic.Int64
	nanos.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, nanos.Load()) }
	db, dir := paymentDB(t, ledgerdb.Options{IdempotencyRetention: time.Hour, Clock: clock})
	hold(t, db, "h", 400)
	hold(t, db, "other", 200)
	original, e := db.Cancel(nil, "cancel:h", ledgerdb.CancelRequest{HoldID: "h"})
	if e != nil {
		t.Fatal(e)
	}
	nanos.Add(int64(2 * time.Hour))
	repeated, e := db.Cancel(nil, "cancel:h", ledgerdb.CancelRequest{HoldID: "h"})
	if e != nil || repeated != original {
		t.Fatalf("expired retry %v %v", repeated, e)
	}
	_, e = db.Reserve(nil, "new-reservation", ledgerdb.ReserveRequest{HoldID: "h", FromAccount: "a", ToAccount: "b", Amount: 400})
	if !errors.Is(e, ledgerdb.ErrHoldExists) {
		t.Fatal(e)
	}
	_, e = db.Settle(nil, "settle-cancelled", ledgerdb.SettleRequest{HoldID: "h", Amount: 1})
	if !errors.Is(e, ledgerdb.ErrHoldConflict) {
		t.Fatal(e)
	}
	if e = db.Checkpoint(nil); e != nil {
		t.Fatal(e)
	}
	db.Close()
	db = openDB(t, dir, ledgerdb.Options{IdempotencyRetention: time.Hour, Clock: clock})
	defer db.Close()
	a, _ := db.GetAccount(nil, "a")
	if a.Reserved != 200 || a.Available != 800 {
		t.Fatal(a)
	}
	s := db.Stats()
	if s.ActiveHolds != 1 || s.TerminalHolds != 1 {
		t.Fatal(s)
	}
}
func TestPaymentValidation(t *testing.T) {
	db, _ := paymentDB(t, ledgerdb.Options{})
	createAccount(t, db, "create-usd", "usd", "USD", 0)
	createAccount(t, db, "create-full", "full", "INR", math.MaxInt64)
	for _, tc := range []struct {
		r    ledgerdb.ReserveRequest
		want error
	}{
		{ledgerdb.ReserveRequest{HoldID: "", FromAccount: "a", ToAccount: "b", Amount: 1}, ledgerdb.ErrInvalidHoldID},
		{ledgerdb.ReserveRequest{HoldID: "x", FromAccount: "a", ToAccount: "b", Amount: 0}, ledgerdb.ErrInvalidAmount},
		{ledgerdb.ReserveRequest{HoldID: "x", FromAccount: "a", ToAccount: "a", Amount: 1}, ledgerdb.ErrSameAccount},
		{ledgerdb.ReserveRequest{HoldID: "x", FromAccount: "missing", ToAccount: "b", Amount: 1}, ledgerdb.ErrAccountNotFound},
		{ledgerdb.ReserveRequest{HoldID: "x", FromAccount: "a", ToAccount: "usd", Amount: 1}, ledgerdb.ErrCurrencyMismatch},
		{ledgerdb.ReserveRequest{HoldID: "x", FromAccount: "a", ToAccount: "b", Amount: 1001}, ledgerdb.ErrInsufficientFunds},
	} {
		_, e := db.Reserve(nil, "validate", tc.r)
		if !errors.Is(e, tc.want) {
			t.Fatalf("%v: %v", tc.r, e)
		}
	}
	hold(t, db, "h", 500)
	for _, a := range []int64{-1, 501} {
		_, e := db.Settle(nil, "invalid-amount", ledgerdb.SettleRequest{HoldID: "h", Amount: a})
		if !errors.Is(e, ledgerdb.ErrInvalidAmount) {
			t.Fatal(e)
		}
	}
	_, e := db.Settle(nil, "missing", ledgerdb.SettleRequest{HoldID: "missing", Amount: 1})
	if !errors.Is(e, ledgerdb.ErrHoldNotFound) {
		t.Fatal(e)
	}
	_, e = db.Reserve(nil, "reserve-full", ledgerdb.ReserveRequest{HoldID: "overflow", FromAccount: "a", ToAccount: "full", Amount: 100})
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Settle(nil, "overflow", ledgerdb.SettleRequest{HoldID: "overflow", Amount: 1})
	if !errors.Is(e, ledgerdb.ErrBalanceOverflow) {
		t.Fatal(e)
	}
	a, _ := db.GetAccount(nil, "a")
	if a.Reserved != 600 {
		t.Fatal("failed settlement altered reservation", a)
	}
}
func TestGroupCommitSharedFundsAndRaces(t *testing.T) {
	db, _ := paymentDB(t, ledgerdb.Options{GroupCommitMaxBatch: 64, GroupCommitDelay: 10 * time.Millisecond})
	before := db.Stats()
	var wg sync.WaitGroup
	start := make(chan struct{})
	var wins atomic.Int64
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, e := db.Reserve(nil, fmt.Sprint("reserve", i), ledgerdb.ReserveRequest{HoldID: fmt.Sprint("h", i), FromAccount: "a", ToAccount: "b", Amount: 100})
			if e == nil {
				wins.Add(1)
			} else if !errors.Is(e, ledgerdb.ErrInsufficientFunds) {
				t.Error(e)
			}
		}()
	}
	close(start)
	wg.Wait()
	if wins.Load() != 10 {
		t.Fatalf("overspend: %d successful holds", wins.Load())
	}
	a, _ := db.GetAccount(nil, "a")
	if a.Reserved != 1000 || a.Available != 0 {
		t.Fatal(a)
	}
	after := db.Stats()
	if after.WALSyncs-before.WALSyncs >= uint64(wins.Load()) {
		t.Fatal("grouped operations did not share syncs", after)
	}
	// Resolve each surviving hold with a real settle/cancel race.
	for i := range 32 {
		h, e := db.GetHold(nil, fmt.Sprint("h", i))
		if errors.Is(e, ledgerdb.ErrHoldNotFound) {
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		results := make(chan error, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, e := db.Settle(nil, "settle:"+h.ID, ledgerdb.SettleRequest{HoldID: h.ID, Amount: 40})
			results <- e
		}()
		go func() {
			defer wg.Done()
			_, e := db.Cancel(nil, "cancel:"+h.ID, ledgerdb.CancelRequest{HoldID: h.ID})
			results <- e
		}()
		wg.Wait()
		close(results)
		win, conflict := 0, 0
		for e := range results {
			if e == nil {
				win++
			} else if errors.Is(e, ledgerdb.ErrHoldConflict) {
				conflict++
			} else {
				t.Fatal(e)
			}
		}
		if win != 1 || conflict != 1 {
			t.Fatal(win, conflict)
		}
	}
	a, _ = db.GetAccount(nil, "a")
	b, _ := db.GetAccount(nil, "b")
	if a.Reserved != 0 || a.Balance+b.Balance != 1000 {
		t.Fatal(a, b)
	}
}
func TestLifecycleConcurrentDuplicates(t *testing.T) {
	db, _ := paymentDB(t, ledgerdb.Options{GroupCommitMaxBatch: 64, GroupCommitDelay: time.Millisecond})
	hold(t, db, "h", 500)
	var wg sync.WaitGroup
	results := make(chan ledgerdb.PaymentResult, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := db.Settle(nil, "same", ledgerdb.SettleRequest{HoldID: "h", Amount: 180})
			if e != nil {
				t.Error(e)
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	var original ledgerdb.PaymentResult
	for r := range results {
		if original.CommitLSN == 0 {
			original = r
		} else if original != r {
			t.Fatal("unstable result")
		}
	}
	assertBalance(t, db, "a", 820)
	assertBalance(t, db, "b", 180)
}
func TestCancellationBeforeAndDuringCommit(t *testing.T) {
	t.Run("before-processing", func(t *testing.T) {
		db, _ := paymentDB(t, ledgerdb.Options{GroupCommitMaxBatch: 64, GroupCommitDelay: 50 * time.Millisecond})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		_, e := db.Transfer(ctx, "cancelled", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
		if !errors.Is(e, context.DeadlineExceeded) {
			t.Fatal(e)
		}
		assertBalance(t, db, "a", 1000)
		if _, e = db.Transfer(nil, "cancelled", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1}); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("after-processing", func(t *testing.T) {
		faults := failpoint.New(nil)
		db, _ := paymentDB(t, ledgerdb.Options{FileSystem: faults})
		started, release := make(chan struct{}), make(chan struct{})
		faults.Add(failpoint.Failure{Op: failpoint.OpSync, At: faults.Count(failpoint.OpSync) + 1, After: true, Partial: -1, Crash: func() { close(started); <-release }})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, e := db.Transfer(ctx, "in-progress", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
			done <- e
		}()
		<-started
		cancel()
		select {
		case e := <-done:
			t.Fatalf("returned before commit publication: %v", e)
		case <-time.After(10 * time.Millisecond):
		}
		close(release)
		if e := <-done; e != nil {
			t.Fatal(e)
		}
		assertBalance(t, db, "a", 999)
	})
}
func TestGroupFailureRecovery(t *testing.T) {
	for _, op := range []failpoint.Operation{failpoint.OpWrite, failpoint.OpSync} {
		t.Run(string(op), func(t *testing.T) {
			faults := failpoint.New(nil)
			db, dir := paymentDB(t, ledgerdb.Options{FileSystem: faults, GroupCommitMaxBatch: 64, GroupCommitDelay: 20 * time.Millisecond})
			hold(t, db, "h0", 200)
			hold(t, db, "h1", 200)
			faults.Add(failpoint.Failure{Op: op, At: faults.Count(op) + 1, Partial: 17, Err: errors.New("injected I/O")})
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range 2 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, e := db.Settle(nil, fmt.Sprint("settle", i), ledgerdb.SettleRequest{HoldID: fmt.Sprint("h", i), Amount: 100})
					if !errors.Is(e, ledgerdb.ErrPoisoned) {
						t.Error(e)
					}
				}()
			}
			close(start)
			wg.Wait()
			if !db.Stats().Poisoned {
				t.Fatal("not poisoned")
			}
			a, _ := db.GetAccount(nil, "a")
			if a.Balance != 1000 || a.Reserved != 400 {
				t.Fatal("published uncertain batch", a)
			}
			db.Close()
			db = openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			for i := range 2 {
				if _, e := db.Settle(nil, fmt.Sprint("settle", i), ledgerdb.SettleRequest{HoldID: fmt.Sprint("h", i), Amount: 100}); e != nil {
					t.Fatal(e)
				}
			}
			assertBalance(t, db, "a", 800)
			assertBalance(t, db, "b", 200)
		})
	}
}
func TestPaymentProcessCrashMatrix(t *testing.T) {
	for _, phase := range []string{"write-before", "write-after", "sync-before", "sync-after"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestPaymentCrashHelper$")
			cmd.Env = append(os.Environ(), "LEDGERDB_PAYMENT_CRASH=1", "LEDGERDB_PAYMENT_DIR="+dir, "LEDGERDB_PAYMENT_PHASE="+phase)
			output, e := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(e, &exit) || exit.ExitCode() != 91 {
				t.Fatalf("%v %s", e, output)
			}
			db := openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			for i := range 2 {
				h, e := db.GetHold(nil, fmt.Sprint("h", i))
				if e != nil {
					t.Fatal(e)
				}
				if phase == "sync-after" && h.Status != ledgerdb.HoldSettled {
					t.Fatal("synced settlement missing", h)
				}
				if _, e = db.Settle(nil, fmt.Sprint("settle", i), ledgerdb.SettleRequest{HoldID: h.ID, Amount: 100}); e != nil {
					t.Fatal(e)
				}
			}
			assertBalance(t, db, "a", 800)
			assertBalance(t, db, "b", 200)
		})
	}
}
func TestPaymentCrashHelper(t *testing.T) {
	if os.Getenv("LEDGERDB_PAYMENT_CRASH") != "1" {
		return
	}
	faults := failpoint.New(nil)
	db := openDB(t, os.Getenv("LEDGERDB_PAYMENT_DIR"), ledgerdb.Options{FileSystem: faults, GroupCommitMaxBatch: 64, GroupCommitDelay: 100 * time.Millisecond})
	createAccount(t, db, "create-a", "a", "INR", 1000)
	createAccount(t, db, "create-b", "b", "INR", 0)
	hold(t, db, "h0", 200)
	hold(t, db, "h1", 200)
	op := failpoint.OpWrite
	after := false
	switch os.Getenv("LEDGERDB_PAYMENT_PHASE") {
	case "write-before":
	case "write-after":
		after = true
	case "sync-before":
		op = failpoint.OpSync
	case "sync-after":
		op = failpoint.OpSync
		after = true
	default:
		t.Fatal("unknown phase")
	}
	faults.Add(failpoint.Failure{Op: op, At: faults.Count(op) + 1, After: after, Partial: -1, Crash: func() { os.Exit(91) }})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _ = db.Settle(nil, fmt.Sprint("settle", i), ledgerdb.SettleRequest{HoldID: fmt.Sprint("h", i), Amount: 100})
		}()
	}
	close(start)
	wg.Wait()
	os.Exit(92)
}
func TestAutomaticCheckpointsAndShutdown(t *testing.T) {
	db, _ := paymentDB(t, ledgerdb.Options{GroupCommitMaxBatch: 64, GroupCommitDelay: time.Millisecond, CheckpointOperations: 2, CheckpointInterval: 10 * time.Millisecond})
	hold(t, db, "h", 500)
	deadline := time.Now().Add(2 * time.Second)
	for db.Stats().CheckpointSuccesses == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.Stats().CheckpointSuccesses == 0 {
		t.Fatal("automatic checkpoint missing")
	}
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := db.Transfer(nil, fmt.Sprint("close", i), ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
			if e != nil && !errors.Is(e, ledgerdb.ErrClosed) {
				t.Error(e)
			}
		}()
	}
	if e := db.Close(); e != nil {
		t.Fatal(e)
	}
	wg.Wait()
	if e := db.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestLegacyWALAndCheckpointLoad(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			dir := t.TempDir()
			db := openDB(t, dir, ledgerdb.Options{})
			db.Close()
			req := ledgerdb.CreateAccountRequest{ID: "legacy", Currency: "INR", OpeningBalance: 100}
			res := ledgerdb.CreateAccountResult{Account: ledgerdb.Account{ID: "legacy", Currency: "INR", Balance: 100, LSN: 1}, CommitLSN: 1}
			rb, _ := json.Marshal(req)
			result, _ := json.Marshal(res)
			encoded, e := wal.Encode(wal.Record{Type: "create_account", LSN: 1, Key: "legacy-create", Request: rb, Result: result, CommittedAt: time.Now().UnixNano()})
			if e != nil {
				t.Fatal(e)
			}
			binary.LittleEndian.PutUint16(encoded[4:6], 1)
			if e = os.WriteFile(filepath.Join(dir, "wal-00000000000000000001.log"), encoded, 0600); e != nil {
				t.Fatal(e)
			}
			db = openDB(t, dir, ledgerdb.Options{})
			if checkpoint {
				if e = db.Checkpoint(nil); e != nil {
					t.Fatal(e)
				}
				db.Close()
				downgradeCheckpoint(t, dir)
				db = openDB(t, dir, ledgerdb.Options{})
			}
			defer db.Close()
			a, e := db.GetAccount(nil, "legacy")
			if e != nil || a.Reserved != 0 || a.Available != 100 {
				t.Fatal(a, e)
			}
			_, e = db.CreateAccount(nil, "new", ledgerdb.CreateAccountRequest{ID: "new", Currency: "INR"})
			if e != nil {
				t.Fatal(e)
			}
			raw, e := os.ReadFile(filepath.Join(dir, "wal-00000000000000000001.log"))
			if e != nil {
				t.Fatal(e)
			}
			offset := 16 + int(binary.LittleEndian.Uint32(raw[8:12]))
			if binary.LittleEndian.Uint16(raw[offset+4:offset+6]) != 2 {
				t.Fatal("new writes did not upgrade format")
			}
		})
	}
}
func downgradeCheckpoint(t *testing.T, dir string) {
	t.Helper()
	paths, e := filepath.Glob(filepath.Join(dir, "checkpoint-*.dat"))
	if e != nil || len(paths) != 1 {
		t.Fatal(paths, e)
	}
	for _, path := range []string{paths[0], filepath.Join(dir, "MANIFEST")} {
		raw, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		var payload map[string]any
		if e = json.Unmarshal(raw[12:], &payload); e != nil {
			t.Fatal(e)
		}
		payload["version"] = 1
		body, e := json.Marshal(payload)
		if e != nil {
			t.Fatal(e)
		}
		upgraded := append(append([]byte(nil), raw[:12]...), body...)
		binary.LittleEndian.PutUint32(upgraded[4:8], uint32(len(body)))
		binary.LittleEndian.PutUint32(upgraded[8:12], crc32.ChecksumIEEE(body))
		if e = os.WriteFile(path, upgraded, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
func TestAutomaticCheckpointFailureRetries(t *testing.T) {
	faults := failpoint.New(nil)
	db, _ := paymentDB(t, ledgerdb.Options{FileSystem: faults, CheckpointOperations: 3})
	faults.Add(failpoint.Failure{Op: failpoint.OpRename, At: faults.Count(failpoint.OpRename) + 1, Partial: -1, Err: errors.New("checkpoint rename failed")})
	transfer := func(key string) {
		t.Helper()
		if _, e := db.Transfer(nil, key, ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1}); e != nil {
			t.Fatal(e)
		}
	}
	transfer("trigger-failure")
	deadline := time.Now().Add(2 * time.Second)
	for db.Stats().LastCheckpointError == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := db.Stats(); s.LastCheckpointError == "" || s.Poisoned {
		t.Fatal(s)
	}
	for i := range 3 {
		transfer(fmt.Sprint("retry", i))
	}
	deadline = time.Now().Add(2 * time.Second)
	for db.Stats().CheckpointSuccesses == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s := db.Stats(); s.CheckpointSuccesses == 0 || s.LastCheckpointError != "" {
		t.Fatal(s)
	}
}
func TestLifecycleFuzzyCheckpointUnderLoad(t *testing.T) {
	db, dir := paymentDB(t, ledgerdb.Options{GroupCommitMaxBatch: 16, GroupCommitDelay: time.Millisecond, WALSegmentBytes: 1500})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range 6 {
				id := fmt.Sprintf("h-%d-%d", worker, i)
				if _, e := db.Reserve(nil, "reserve:"+id, ledgerdb.ReserveRequest{HoldID: id, FromAccount: "a", ToAccount: "b", Amount: 10}); e != nil {
					t.Error(e)
					return
				}
				if i%2 == 0 {
					if _, e := db.Settle(nil, "settle:"+id, ledgerdb.SettleRequest{HoldID: id, Amount: 4}); e != nil {
						t.Error(e)
					}
				} else {
					if _, e := db.Cancel(nil, "cancel:"+id, ledgerdb.CancelRequest{HoldID: id}); e != nil {
						t.Error(e)
					}
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 10 {
			if e := db.Checkpoint(nil); e != nil {
				t.Error(e)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	if e := db.Checkpoint(nil); e != nil {
		t.Fatal(e)
	}
	db.Close()
	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	a, _ := db.GetAccount(nil, "a")
	b, _ := db.GetAccount(nil, "b")
	if a.Reserved != 0 || a.Available != 952 || b.Balance != 48 || db.Stats().TerminalHolds != 24 {
		t.Fatal(a, b, db.Stats())
	}
}
func TestRecoveredReservationInvariantsFailClosed(t *testing.T) {
	for _, field := range []string{"reserved", "lsn"} {
		t.Run(field, func(t *testing.T) {
			db, dir := paymentDB(t, ledgerdb.Options{})
			hold(t, db, "h", 400)
			if e := db.Checkpoint(nil); e != nil {
				t.Fatal(e)
			}
			db.Close()
			paths, e := filepath.Glob(filepath.Join(dir, "checkpoint-*.dat"))
			if e != nil || len(paths) != 1 {
				t.Fatal(paths, e)
			}
			raw, e := os.ReadFile(paths[0])
			if e != nil {
				t.Fatal(e)
			}
			var payload map[string]any
			if e = json.Unmarshal(raw[12:], &payload); e != nil {
				t.Fatal(e)
			}
			accounts := payload["accounts"].([]any)
			for _, item := range accounts {
				a := item.(map[string]any)
				if a["id"] == "a" {
					if field == "reserved" {
						a[field] = 399
					} else {
						a[field] = 999
					}
				}
			}
			body, e := json.Marshal(payload)
			if e != nil {
				t.Fatal(e)
			}
			updated := append(append([]byte(nil), raw[:12]...), body...)
			binary.LittleEndian.PutUint32(updated[4:8], uint32(len(body)))
			binary.LittleEndian.PutUint32(updated[8:12], crc32.ChecksumIEEE(body))
			if e = os.WriteFile(paths[0], updated, 0600); e != nil {
				t.Fatal(e)
			}
			recovered, e := ledgerdb.Open(dir, ledgerdb.Options{})
			if recovered != nil {
				recovered.Close()
			}
			if !errors.Is(e, ledgerdb.ErrCorrupt) {
				t.Fatalf("accepted inconsistent recovered %s: %v", field, e)
			}
		})
	}
}
