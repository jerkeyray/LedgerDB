package ledgerdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestExpiredIdempotencyEntriesAreReclaimed(t *testing.T) {
	now := time.Date(2026, 8, 23, 0, 0, 0, 0, time.UTC)
	db, err := Open(t.TempDir(), Options{IdempotencyRetention: time.Hour, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = db.CreateAccount(context.Background(), "old-create-a", CreateAccountRequest{ID: "a", Currency: "USD", OpeningBalance: 1_000})
	_, _ = db.CreateAccount(context.Background(), "old-create-b", CreateAccountRequest{ID: "b", Currency: "USD"})
	for i := range 64 {
		_, err := db.Transfer(context.Background(), fmt.Sprintf("old-%d", i), TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(2 * time.Hour)
	for i := range shardCount {
		_, err := db.Transfer(context.Background(), fmt.Sprintf("new-%d", i), TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range db.idem {
		db.idem[i].Lock()
		for key := range db.idem[i].items {
			if strings.HasPrefix(key, "old-") {
				t.Errorf("expired key %q was retained", key)
			}
		}
		db.idem[i].Unlock()
	}
}
