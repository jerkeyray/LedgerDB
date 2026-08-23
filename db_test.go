package ledgerdb_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jerkeyray/ledgerdb"
	"github.com/jerkeyray/ledgerdb/internal/failpoint"
	"github.com/jerkeyray/ledgerdb/internal/wal"
)

func TestCreateTransferIdempotencyAndRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, ledgerdb.Options{})

	a := createAccount(t, db, "create-a", "a", "USD", 100)
	b := createAccount(t, db, "create-b", "b", "USD", 5)
	if a.CommitLSN >= b.CommitLSN {
		t.Fatalf("LSNs are not monotonic: %d >= %d", a.CommitLSN, b.CommitLSN)
	}

	request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 30}
	first, err := db.Transfer(context.Background(), "transfer-1", request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Transfer(context.Background(), "transfer-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent result changed: %#v != %#v", first, second)
	}
	_, err = db.Transfer(context.Background(), "transfer-1", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 31})
	if !errors.Is(err, ledgerdb.ErrIdempotencyConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	if err := db.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	assertBalance(t, db, "a", 70)
	assertBalance(t, db, "b", 35)
	replayed, err := db.Transfer(context.Background(), "transfer-1", request)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != first {
		t.Fatalf("recovered result changed: %#v != %#v", replayed, first)
	}
}

func TestOpenExcludesConcurrentWriter(t *testing.T) {
	dir := t.TempDir()
	first := openDB(t, dir, ledgerdb.Options{})
	second, err := ledgerdb.Open(dir, ledgerdb.Options{})
	if !errors.Is(err, ledgerdb.ErrAlreadyOpen) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("expected ErrAlreadyOpen, got %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openDB(t, dir, ledgerdb.Options{})
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDurablyCreatesNestedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "ledger")
	db := openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	createAccount(t, db, "create-a", "a", "USD", 1)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("database directory: %v, %v", info, err)
	}
}

func TestConcurrentDuplicateExecutesOnce(t *testing.T) {
	db := openDB(t, t.TempDir(), ledgerdb.Options{})
	defer db.Close()
	createAccount(t, db, "create-a", "a", "USD", 100)
	createAccount(t, db, "create-b", "b", "USD", 0)

	const goroutines = 32
	results := make(chan ledgerdb.TransferResult, goroutines)
	errorsCh := make(chan error, goroutines)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := db.Transfer(context.Background(), "same-key", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
			results <- result
			errorsCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var expected ledgerdb.TransferResult
	for result := range results {
		if expected.CommitLSN == 0 {
			expected = result
		}
		if result != expected {
			t.Fatalf("results differ: %#v != %#v", result, expected)
		}
	}
	assertBalance(t, db, "a", 99)
	assertBalance(t, db, "b", 1)
}

func TestTransferValidation(t *testing.T) {
	db := openDB(t, t.TempDir(), ledgerdb.Options{})
	defer db.Close()
	createAccount(t, db, "create-usd-a", "usd-a", "USD", 10)
	createAccount(t, db, "create-usd-b", "usd-b", "USD", 0)
	createAccount(t, db, "create-eur", "eur", "EUR", 0)
	createAccount(t, db, "create-max", "max", "USD", int64(^uint64(0)>>1))

	tests := []struct {
		name    string
		request ledgerdb.TransferRequest
		want    error
	}{
		{"zero amount", ledgerdb.TransferRequest{"usd-a", "usd-b", 0}, ledgerdb.ErrInvalidAmount},
		{"negative amount", ledgerdb.TransferRequest{"usd-a", "usd-b", -1}, ledgerdb.ErrInvalidAmount},
		{"same account", ledgerdb.TransferRequest{"usd-a", "usd-a", 1}, ledgerdb.ErrSameAccount},
		{"missing account", ledgerdb.TransferRequest{"usd-a", "missing", 1}, ledgerdb.ErrAccountNotFound},
		{"currency mismatch", ledgerdb.TransferRequest{"usd-a", "eur", 1}, ledgerdb.ErrCurrencyMismatch},
		{"insufficient funds", ledgerdb.TransferRequest{"usd-a", "usd-b", 11}, ledgerdb.ErrInsufficientFunds},
		{"overflow", ledgerdb.TransferRequest{"usd-a", "max", 1}, ledgerdb.ErrBalanceOverflow},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := db.Transfer(context.Background(), fmt.Sprintf("validation-%d", index), test.request)
			if !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
		})
	}
	_, err := db.CreateAccount(context.Background(), "bad-currency", ledgerdb.CreateAccountRequest{ID: "bad", Currency: "usd"})
	if !errors.Is(err, ledgerdb.ErrInvalidCurrency) {
		t.Fatalf("expected invalid currency, got %v", err)
	}
	_, err = db.CreateAccount(context.Background(), "negative-opening", ledgerdb.CreateAccountRequest{ID: "bad", Currency: "USD", OpeningBalance: -1})
	if !errors.Is(err, ledgerdb.ErrInvalidOpeningBalance) {
		t.Fatalf("expected invalid opening balance, got %v", err)
	}
	_, err = db.CreateAccount(context.Background(), "invalid-id", ledgerdb.CreateAccountRequest{ID: string([]byte{0xff}), Currency: "USD"})
	if !errors.Is(err, ledgerdb.ErrInvalidAccountID) {
		t.Fatalf("expected invalid account ID, got %v", err)
	}
	_, err = db.CreateAccount(context.Background(), string([]byte{0xff}), ledgerdb.CreateAccountRequest{ID: "valid", Currency: "USD"})
	if !errors.Is(err, ledgerdb.ErrInvalidIdempotencyKey) {
		t.Fatalf("expected invalid key, got %v", err)
	}
}

func TestIdempotencyLeaseExpiry(t *testing.T) {
	now := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	db := openDB(t, t.TempDir(), ledgerdb.Options{IdempotencyRetention: time.Hour, Clock: func() time.Time { return now }})
	defer db.Close()
	createAccount(t, db, "create-a", "a", "USD", 10)
	createAccount(t, db, "create-b", "b", "USD", 0)
	request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1}
	first, err := db.Transfer(context.Background(), "leased-key", request)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Minute)
	withinLease, err := db.Transfer(context.Background(), "leased-key", request)
	if err != nil {
		t.Fatal(err)
	}
	if withinLease != first {
		t.Fatal("request re-executed within lease")
	}
	now = now.Add(2 * time.Minute)
	afterLease, err := db.Transfer(context.Background(), "leased-key", request)
	if err != nil {
		t.Fatal(err)
	}
	if afterLease.CommitLSN == first.CommitLSN {
		t.Fatal("expired request did not execute again")
	}
	assertBalance(t, db, "a", 8)
}

func TestSyncFailurePoisonsMutationsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	faults := failpoint.New(nil)
	db := openDB(t, dir, ledgerdb.Options{FileSystem: faults})
	createAccount(t, db, "create-a", "a", "USD", 100)
	createAccount(t, db, "create-b", "b", "USD", 0)
	faults.Add(failpoint.Failure{Op: failpoint.OpSync, At: faults.Count(failpoint.OpSync) + 1, Partial: -1, Err: syscall.EIO})
	request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 10}
	_, err := db.Transfer(context.Background(), "uncertain", request)
	if !errors.Is(err, ledgerdb.ErrPoisoned) {
		t.Fatalf("expected poisoned error, got %v", err)
	}
	assertBalance(t, db, "a", 100)
	_, err = db.Transfer(context.Background(), "later", request)
	if !errors.Is(err, ledgerdb.ErrPoisoned) {
		t.Fatalf("expected later mutation rejection, got %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	if _, err := db.Transfer(context.Background(), "uncertain", request); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, db, "a", 90)
	assertBalance(t, db, "b", 10)
}

func TestPartialFinalRecordIsTruncatedOnRecovery(t *testing.T) {
	dir := t.TempDir()
	faults := failpoint.New(nil)
	db := openDB(t, dir, ledgerdb.Options{FileSystem: faults})
	createAccount(t, db, "create-a", "a", "USD", 100)
	createAccount(t, db, "create-b", "b", "USD", 0)
	faults.Add(failpoint.Failure{Op: failpoint.OpWrite, At: faults.Count(failpoint.OpWrite) + 1, Partial: 11, Err: io.ErrUnexpectedEOF})
	request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 10}
	_, err := db.Transfer(context.Background(), "torn", request)
	if !errors.Is(err, ledgerdb.ErrPoisoned) {
		t.Fatalf("expected poison, got %v", err)
	}
	_ = db.Close()

	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	assertBalance(t, db, "a", 100)
	if _, err := db.Transfer(context.Background(), "torn", request); err != nil {
		t.Fatal(err)
	}
	assertBalance(t, db, "a", 90)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	assertBalance(t, db, "a", 90)
}

func TestTornFinalRecordBoundedMatrix(t *testing.T) {
	for _, partial := range []int{0, 1, 4, 15, 16, 31, 127} {
		t.Run(fmt.Sprintf("bytes-%d", partial), func(t *testing.T) {
			dir := t.TempDir()
			faults := failpoint.New(nil)
			db := openDB(t, dir, ledgerdb.Options{FileSystem: faults})
			createAccount(t, db, "create-a", "a", "USD", 10)
			createAccount(t, db, "create-b", "b", "USD", 0)
			faults.Add(failpoint.Failure{Op: failpoint.OpWrite, At: faults.Count(failpoint.OpWrite) + 1, Partial: partial, Err: io.ErrUnexpectedEOF})
			_, err := db.Transfer(context.Background(), "torn", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
			if !errors.Is(err, ledgerdb.ErrPoisoned) {
				t.Fatalf("expected poison, got %v", err)
			}
			_ = db.Close()
			db = openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			assertBalance(t, db, "a", 10)
		})
	}
}

func TestProcessCrashMatrix(t *testing.T) {
	for _, phase := range []string{"write-before", "write-after", "sync-before", "sync-after"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestProcessCrashHelper$")
			command.Env = append(os.Environ(), "LEDGERDB_CRASH_HELPER=1", "LEDGERDB_CRASH_DIR="+dir, "LEDGERDB_CRASH_PHASE="+phase)
			err := command.Run()
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) || exitError.ExitCode() != 91 {
				t.Fatalf("child did not crash at %s: %v", phase, err)
			}
			db := openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1}
			if _, err := db.Transfer(context.Background(), "crashing-transfer", request); err != nil {
				t.Fatal(err)
			}
			assertBalance(t, db, "a", 9)
			assertBalance(t, db, "b", 1)
		})
	}
}

func TestProcessCrashHelper(t *testing.T) {
	if os.Getenv("LEDGERDB_CRASH_HELPER") != "1" {
		return
	}
	dir := os.Getenv("LEDGERDB_CRASH_DIR")
	phase := os.Getenv("LEDGERDB_CRASH_PHASE")
	faults := failpoint.New(nil)
	db := openDB(t, dir, ledgerdb.Options{FileSystem: faults})
	createAccount(t, db, "create-a", "a", "USD", 10)
	createAccount(t, db, "create-b", "b", "USD", 0)
	operation := failpoint.OpWrite
	after := false
	switch phase {
	case "write-before":
	case "write-after":
		after = true
	case "sync-before":
		operation = failpoint.OpSync
	case "sync-after":
		operation, after = failpoint.OpSync, true
	default:
		t.Fatalf("unknown crash phase %q", phase)
	}
	faults.Add(failpoint.Failure{Op: operation, At: faults.Count(operation) + 1, After: after, Partial: -1, Crash: func() { os.Exit(91) }})
	_, _ = db.Transfer(context.Background(), "crashing-transfer", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
	os.Exit(92)
}

func TestRecoveryAppliesDurableRecordNotAppliedToMemory(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, ledgerdb.Options{})
	createAccount(t, db, "create-a", "a", "USD", 100)
	createAccount(t, db, "create-b", "b", "USD", 0)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	request := ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 10}
	requestBytes, _ := json.Marshal(request)
	result := ledgerdb.TransferResult{FromBalance: 90, ToBalance: 10, CommitLSN: 3}
	resultBytes, _ := json.Marshal(result)
	hash := sha256.New()
	hash.Write([]byte("transfer"))
	hash.Write([]byte{0})
	hash.Write(requestBytes)
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	writer, err := wal.Open(walTestFS{}, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(wal.Record{Type: "transfer", LSN: 3, CommittedAt: time.Now().UnixNano(), Key: "durable", Fingerprint: fingerprint, Request: requestBytes, Result: resultBytes}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	db = openDB(t, dir, ledgerdb.Options{})
	defer db.Close()
	assertBalance(t, db, "a", 90)
	assertBalance(t, db, "b", 10)
	retry, err := db.Transfer(context.Background(), "durable", request)
	if err != nil {
		t.Fatal(err)
	}
	if retry != result {
		t.Fatalf("stored response changed: %#v != %#v", retry, result)
	}
}

func TestCheckpointFailuresDoNotPoisonMutations(t *testing.T) {
	tests := []struct {
		name string
		op   failpoint.Operation
		err  error
	}{
		{"write", failpoint.OpWrite, io.ErrShortWrite},
		{"sync", failpoint.OpSync, syscall.EIO},
		{"rename", failpoint.OpRename, syscall.EIO},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			faults := failpoint.New(nil)
			db := openDB(t, dir, ledgerdb.Options{FileSystem: faults})
			createAccount(t, db, "create-a", "a", "USD", 10)
			createAccount(t, db, "create-b", "b", "USD", 0)
			failure := failpoint.Failure{Op: test.op, At: faults.Count(test.op) + 1, Partial: -1, Err: test.err}
			faults.Add(failure)
			if err := db.Checkpoint(context.Background()); err == nil {
				t.Fatal("expected checkpoint failure")
			}
			if _, err := db.Transfer(context.Background(), "after-failed-checkpoint", ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1}); err != nil {
				t.Fatalf("mutation after checkpoint failure: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openDB(t, dir, ledgerdb.Options{})
			defer db.Close()
			assertBalance(t, db, "a", 9)
		})
	}
}

func TestMidLogCorruptionIsRejected(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, ledgerdb.Options{})
	createAccount(t, db, "create-a", "a", "USD", 1)
	createAccount(t, db, "create-b", "b", "USD", 0)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	paths, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("WAL path: %v, %v", paths, err)
	}
	file, err := os.OpenFile(paths[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0}, 12); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = ledgerdb.Open(dir, ledgerdb.Options{})
	if !errors.Is(err, ledgerdb.ErrCorrupt) {
		t.Fatalf("expected corruption, got %v", err)
	}
}

func TestCompleteCorruptFinalRecordIsRejected(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir, ledgerdb.Options{})
	createAccount(t, db, "create-a", "a", "USD", 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("WAL path: %v, %v", paths, err)
	}
	file, err := os.OpenFile(paths[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	last := make([]byte, 1)
	if _, err := file.ReadAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	last[0] ^= 0xff
	if _, err := file.WriteAt(last, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = ledgerdb.Open(dir, ledgerdb.Options{})
	if !errors.Is(err, ledgerdb.ErrCorrupt) {
		t.Fatalf("expected corruption, got %v", err)
	}
}

func TestCheckpointCompactsSegmentsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	options := ledgerdb.Options{WALSegmentBytes: 1}
	db := openDB(t, dir, options)
	createAccount(t, db, "create-a", "a", "USD", 25)
	createAccount(t, db, "create-b", "b", "USD", 0)
	for i := range 10 {
		_, err := db.Transfer(context.Background(), fmt.Sprintf("transfer-%d", i), ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("expected one active WAL segment, found %d", len(paths))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir, options)
	defer db.Close()
	assertBalance(t, db, "a", 15)
	assertBalance(t, db, "b", 10)
}

func TestCorruptPublishedCheckpointIsRejected(t *testing.T) {
	for _, target := range []string{"MANIFEST", "checkpoint"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			db := openDB(t, dir, ledgerdb.Options{WALSegmentBytes: 1})
			createAccount(t, db, "create-a", "a", "USD", 10)
			createAccount(t, db, "create-b", "b", "USD", 0)
			if err := db.Checkpoint(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(dir, "MANIFEST")
			if target == "checkpoint" {
				paths, err := filepath.Glob(filepath.Join(dir, "checkpoint-*.dat"))
				if err != nil || len(paths) != 1 {
					t.Fatalf("checkpoint path: %v, %v", paths, err)
				}
				path = paths[0]
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.WriteAt([]byte{0xff}, 8); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			_, err = ledgerdb.Open(dir, ledgerdb.Options{})
			if !errors.Is(err, ledgerdb.ErrCorrupt) {
				t.Fatalf("expected corruption, got %v", err)
			}
		})
	}
}

func TestConcurrentTransfersPreserveTotal(t *testing.T) {
	db := openDB(t, t.TempDir(), ledgerdb.Options{})
	defer db.Close()
	const accounts = 8
	for i := range accounts {
		createAccount(t, db, fmt.Sprintf("create-%d", i), fmt.Sprintf("a-%d", i), "USD", 100)
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 20 {
				from := (worker + iteration) % accounts
				to := (from + 1) % accounts
				key := fmt.Sprintf("worker-%d-%d", worker, iteration)
				_, err := db.Transfer(context.Background(), key, ledgerdb.TransferRequest{FromAccount: fmt.Sprintf("a-%d", from), ToAccount: fmt.Sprintf("a-%d", to), Amount: 1})
				if err != nil {
					t.Errorf("transfer: %v", err)
					return
				}
			}
		}()
	}
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint(context.Background()) }()
	wg.Wait()
	if err := <-checkpointDone; err != nil {
		t.Fatal(err)
	}
	var total int64
	for i := range accounts {
		account, err := db.GetAccount(context.Background(), fmt.Sprintf("a-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if account.Balance < 0 {
			t.Fatalf("negative balance: %#v", account)
		}
		total += account.Balance
	}
	if total != accounts*100 {
		t.Fatalf("total changed: %d", total)
	}
}

func openDB(t *testing.T, dir string, options ledgerdb.Options) *ledgerdb.DB {
	t.Helper()
	db, err := ledgerdb.Open(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func createAccount(t *testing.T, db *ledgerdb.DB, key, id, currency string, balance int64) ledgerdb.CreateAccountResult {
	t.Helper()
	result, err := db.CreateAccount(context.Background(), key, ledgerdb.CreateAccountRequest{ID: id, Currency: currency, OpeningBalance: balance})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertBalance(t *testing.T, db *ledgerdb.DB, id string, want int64) {
	t.Helper()
	account, err := db.GetAccount(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if account.Balance != want {
		t.Fatalf("account %s balance = %d, want %d", id, account.Balance, want)
	}
}

type walTestFS struct{}

func (walTestFS) OpenFile(path string, flag int, mode os.FileMode) (wal.File, error) {
	return os.OpenFile(path, flag, mode)
}
func (walTestFS) ReadDir(path string) ([]os.DirEntry, error) { return os.ReadDir(path) }
func (walTestFS) Remove(path string) error                   { return os.Remove(path) }
