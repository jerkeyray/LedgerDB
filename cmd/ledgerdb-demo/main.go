// Command ledgerdb-demo demonstrates real process-crash recovery for metered payments.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/jerkeyray/ledgerdb"
	"github.com/jerkeyray/ledgerdb/internal/failpoint"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

var ctx = context.Background()

func options() ledgerdb.Options {
	return ledgerdb.Options{GroupCommitMaxBatch: 64, GroupCommitDelay: time.Millisecond, CheckpointInterval: 30 * time.Second, CheckpointOperations: 1000, IdempotencyRetention: 24 * time.Hour}
}
func main() {
	dir := flag.String("dir", "", "existing output parent; a fresh child directory is always created")
	worker := flag.String("worker", "", "internal subprocess scene")
	data := flag.String("data", "", "internal database directory")
	flag.Parse()
	if *worker != "" {
		if err := scene(*worker, *data); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	root, err := os.MkdirTemp(*dir, "ledgerdb-demo-")
	if err != nil {
		fatal(err)
	}
	fmt.Printf("LedgerDB: crash-safe metered payments\nEvidence: %s\nAmounts use INR minor units. This is a process-crash demonstration.\n", root)
	executable, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	for _, name := range []string{"reserve", "crash", "recover", "race", "checkpoint", "verify"} {
		fmt.Printf("\n--- %s ---\n", name)
		cmd := exec.Command(executable, "-worker", name, "-data", filepath.Join(root, "data"))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if name == "crash" {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 91 {
				fatal(fmt.Errorf("expected crash exit 91, got %v; evidence %s", err, root))
			}
			fmt.Println("PASS: child exited after successful WAL sync, before publication/response")
		} else if err != nil {
			fatal(fmt.Errorf("scene %s: %w; evidence %s", name, err, root))
		}
	}
	fmt.Printf("\nPASS: all lifecycle, retry, conservation, race, and restart checks passed.\nEvidence retained: %s\n", root)
}
func scene(name, dir string) error {
	opts := options()
	var faults *failpoint.FS
	if name == "crash" {
		faults = failpoint.New(nil)
		opts.FileSystem = faults
	}
	db, err := ledgerdb.Open(dir, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	switch name {
	case "reserve":
		for _, r := range []ledgerdb.CreateAccountRequest{{ID: "driver", Currency: "INR", OpeningBalance: 100000}, {ID: "operator", Currency: "INR"}, {ID: "shop", Currency: "INR"}} {
			if _, err = db.CreateAccount(ctx, "create:"+r.ID, r); err != nil {
				return err
			}
		}
		if _, err = db.Reserve(ctx, "reserve:charging-1", ledgerdb.ReserveRequest{HoldID: "charging-1", FromAccount: "driver", ToAccount: "operator", Amount: 50000}); err != nil {
			return err
		}
		if err = check(db, "charging-1", 50000, 50000, 0); err != nil {
			return err
		}
		_, err = db.Transfer(ctx, "overspend", ledgerdb.TransferRequest{FromAccount: "driver", ToAccount: "shop", Amount: 60000})
		if !errors.Is(err, ledgerdb.ErrInsufficientFunds) {
			return fmt.Errorf("overspend was not rejected: %v", err)
		}
		fmt.Println("PASS: unrelated ₹600 spend rejected because only ₹500 is available")
	case "crash":
		fmt.Println("Charging finished: ₹180 actual bill; arming crash after next WAL sync")
		faults.Add(failpoint.Failure{Op: failpoint.OpSync, At: faults.Count(failpoint.OpSync) + 1, After: true, Partial: -1, Crash: func() { os.Exit(91) }})
		_, err = db.Settle(ctx, "settle:charging-1", ledgerdb.SettleRequest{HoldID: "charging-1", Amount: 18000})
		if err != nil {
			return err
		}
		return errors.New("crash injection did not fire")
	case "recover":
		h, err := db.GetHold(ctx, "charging-1")
		if err != nil {
			return err
		}
		if h.Status != ledgerdb.HoldSettled {
			return errors.New("durable settlement did not recover")
		}
		a, err := db.Settle(ctx, "settle:charging-1", ledgerdb.SettleRequest{HoldID: "charging-1", Amount: 18000})
		if err != nil {
			return err
		}
		b, err := db.Settle(ctx, "a-different-key", ledgerdb.SettleRequest{HoldID: "charging-1", Amount: 18000})
		if err != nil {
			return err
		}
		if a != b {
			return errors.New("retry changed original response")
		}
		if err = check(db, "charging-1", 82000, 0, 18000); err != nil {
			return err
		}
		fmt.Println("PASS: same-key and different-key retries returned the original settlement LSN; released ₹320")
	case "race":
		if _, err = db.Reserve(ctx, "reserve:charging-2", ledgerdb.ReserveRequest{HoldID: "charging-2", FromAccount: "driver", ToAccount: "operator", Amount: 10000}); err != nil {
			return err
		}
		var wg sync.WaitGroup
		results := make(chan error, 2)
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, e := db.Settle(ctx, "settle:charging-2", ledgerdb.SettleRequest{HoldID: "charging-2", Amount: 4000})
			results <- e
		}()
		go func() {
			defer wg.Done()
			<-start
			_, e := db.Cancel(ctx, "cancel:charging-2", ledgerdb.CancelRequest{HoldID: "charging-2"})
			results <- e
		}()
		close(start)
		wg.Wait()
		close(results)
		wins, conflicts := 0, 0
		for e := range results {
			if e == nil {
				wins++
			} else if errors.Is(e, ledgerdb.ErrHoldConflict) {
				conflicts++
			} else {
				return e
			}
		}
		if wins != 1 || conflicts != 1 {
			return fmt.Errorf("race results: %d winners, %d conflicts", wins, conflicts)
		}
		h, err := db.GetHold(ctx, "charging-2")
		if err != nil {
			return err
		}
		available, operator := int64(82000), int64(18000)
		if h.Status == ledgerdb.HoldSettled {
			available -= 4000
			operator += 4000
		}
		if err = check(db, "charging-2", available, 0, operator); err != nil {
			return err
		}
		fmt.Println("PASS: exactly one of settlement/cancellation won")
	case "checkpoint":
		if err = db.Checkpoint(ctx); err != nil {
			return err
		}
		s := db.Stats()
		fmt.Printf("PASS: checkpoint published at LSN %d; terminal holds=%d\n", s.LastCheckpointLSN, s.TerminalHolds)
	case "verify":
		h, err := db.GetHold(ctx, "charging-1")
		if err != nil {
			return err
		}
		if h.Status != ledgerdb.HoldSettled || h.SettledAmount != 18000 {
			return errors.New("first hold changed after checkpoint")
		}
		h, err = db.GetHold(ctx, "charging-2")
		if err != nil {
			return err
		}
		available, operator := int64(82000), int64(18000)
		if h.Status == ledgerdb.HoldSettled {
			available -= 4000
			operator += 4000
		} else if h.Status != ledgerdb.HoldCancelled {
			return errors.New("race hold not terminal")
		}
		if err = check(db, "charging-2", available, 0, operator); err != nil {
			return err
		}
		s := db.Stats()
		if s.TerminalHolds != 2 || s.ActiveHolds != 0 {
			return errors.New("recovered hold counts disagree")
		}
		fmt.Printf("PASS: checkpoint reopen; committed LSN=%d recovery=%s retained keys=%d\n", s.CommittedLSN, s.RecoveryDuration, s.RetainedKeys)
	default:
		return errors.New("unknown scene")
	}
	return nil
}
func check(db *ledgerdb.DB, id string, available, reserved, operator int64) error {
	driver, err := db.GetAccount(ctx, "driver")
	if err != nil {
		return err
	}
	op, err := db.GetAccount(ctx, "operator")
	if err != nil {
		return err
	}
	shop, err := db.GetAccount(ctx, "shop")
	if err != nil {
		return err
	}
	h, err := db.GetHold(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("driver available=₹%.2f reserved=₹%.2f | operator=₹%.2f | hold=%s | LSN=%d\n", float64(driver.Available)/100, float64(driver.Reserved)/100, float64(op.Balance)/100, h.Status, h.LSN)
	if driver.Available != available || driver.Reserved != reserved || op.Balance != operator || driver.Balance+op.Balance+shop.Balance != 100000 || driver.Available+driver.Reserved != driver.Balance {
		return errors.New("balance/conservation invariant failed")
	}
	fmt.Println("PASS: expected balances and ₹1,000 total funds conserved")
	return nil
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "ledgerdb-demo:", err); os.Exit(1) }
