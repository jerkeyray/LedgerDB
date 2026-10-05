package ledgerdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// Stall the coordinator by holding the commit barrier, then fill the public
// admission queue. This tests cancellation both before and after queue admission.
func TestQueueBackpressureCancellationAndClose(t *testing.T) {
	db, e := Open(t.TempDir(), Options{})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.CreateAccount(nil, "a", CreateAccountRequest{ID: "a", Currency: "INR", OpeningBalance: 2000}); e != nil {
		t.Fatal(e)
	}
	if _, e = db.CreateAccount(nil, "b", CreateAccountRequest{ID: "b", Currency: "INR"}); e != nil {
		t.Fatal(e)
	}
	db.commitMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 1050 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := db.Transfer(ctx, fmt.Sprint("q", i), TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
			if !errors.Is(e, context.Canceled) && !errors.Is(e, ErrClosed) {
				t.Errorf("queue outcome: %v", e)
			}
		}()
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(db.queue) < cap(db.queue) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	full := len(db.queue) == cap(db.queue)
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	db.commitMu.Unlock()
	wg.Wait()
	if e := <-closed; e != nil {
		t.Fatal(e)
	}
	if !full {
		t.Fatal("queue never filled")
	}
}
