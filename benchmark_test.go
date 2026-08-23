package ledgerdb_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jerkeyray/ledgerdb"
)

func BenchmarkTransfers(b *testing.B) {
	workloads := []struct {
		name string
	}{
		{"disjoint"},
		{"uniform"},
		{"hot-account"},
	}
	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			accounts := 64
			if workload.name == "disjoint" {
				accounts = 2 * runtime.GOMAXPROCS(0)
			}
			db, err := ledgerdb.Open(b.TempDir(), ledgerdb.Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			for i := range accounts {
				_, err := db.CreateAccount(context.Background(), fmt.Sprintf("create-%d", i), ledgerdb.CreateAccountRequest{ID: fmt.Sprintf("a-%d", i), Currency: "USD", OpeningBalance: int64(b.N + 1)})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			var globalSequence atomic.Uint64
			var workerSequence atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				worker := int(workerSequence.Add(1) - 1)
				random := rand.New(rand.NewPCG(uint64(worker+1), uint64(worker+17)))
				for pb.Next() {
					sequence := int(globalSequence.Add(1) - 1)
					var from, to int
					switch workload.name {
					case "disjoint":
						from = (2 * worker) % accounts
						to = from + 1
					case "uniform":
						from = random.IntN(accounts)
						to = random.IntN(accounts - 1)
						if to >= from {
							to++
						}
					case "hot-account":
						from = 0
						to = 1 + random.IntN(accounts-1)
					}
					key := fmt.Sprintf("bench-%d-%d", from, sequence)
					_, err := db.Transfer(context.Background(), key, ledgerdb.TransferRequest{FromAccount: fmt.Sprintf("a-%d", from), ToAccount: fmt.Sprintf("a-%d", to), Amount: 1})
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkRecovery(b *testing.B) {
	for _, records := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("records-%d", records), func(b *testing.B) {
			dir := b.TempDir()
			db, err := ledgerdb.Open(dir, ledgerdb.Options{})
			if err != nil {
				b.Fatal(err)
			}
			for i := range records {
				_, err := db.CreateAccount(context.Background(), fmt.Sprintf("create-%d", i), ledgerdb.CreateAccountRequest{ID: fmt.Sprintf("a-%d", i), Currency: "USD", OpeningBalance: 1})
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				db, err = ledgerdb.Open(dir, ledgerdb.Options{})
				if err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCheckpoint(b *testing.B) {
	db, err := ledgerdb.Open(b.TempDir(), ledgerdb.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	for i := range 1_000 {
		_, err := db.CreateAccount(context.Background(), fmt.Sprintf("create-%d", i), ledgerdb.CreateAccountRequest{ID: fmt.Sprintf("a-%d", i), Currency: "USD", OpeningBalance: 1})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for range b.N {
		if err := db.Checkpoint(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIdempotencyIndexGrowth(b *testing.B) {
	db, err := ledgerdb.Open(b.TempDir(), ledgerdb.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	_, _ = db.CreateAccount(context.Background(), "create-a", ledgerdb.CreateAccountRequest{ID: "a", Currency: "USD", OpeningBalance: int64(b.N + 1)})
	_, _ = db.CreateAccount(context.Background(), "create-b", ledgerdb.CreateAccountRequest{ID: "b", Currency: "USD"})
	b.ReportAllocs()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	b.ResetTimer()
	for i := range b.N {
		_, err := db.Transfer(context.Background(), fmt.Sprintf("idempotency-%d", i), ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc && b.N > 0 {
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(b.N), "live-B/key")
	}
}

func BenchmarkTransferCheckpointImpact(b *testing.B) {
	for _, checkpoints := range []bool{false, true} {
		name := "without-checkpoints"
		if checkpoints {
			name = "with-checkpoints"
		}
		b.Run(name, func(b *testing.B) {
			db, err := ledgerdb.Open(b.TempDir(), ledgerdb.Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			_, _ = db.CreateAccount(context.Background(), "create-a", ledgerdb.CreateAccountRequest{ID: "a", Currency: "USD", OpeningBalance: int64(b.N + 1)})
			_, _ = db.CreateAccount(context.Background(), "create-b", ledgerdb.CreateAccountRequest{ID: "b", Currency: "USD"})
			stop := make(chan struct{})
			errorsCh := make(chan error, 1)
			var wait sync.WaitGroup
			if checkpoints {
				wait.Add(1)
				go func() {
					defer wait.Done()
					for {
						select {
						case <-stop:
							return
						default:
							if err := db.Checkpoint(context.Background()); err != nil {
								errorsCh <- err
								return
							}
						}
					}
				}()
			}
			b.ResetTimer()
			for i := range b.N {
				_, err := db.Transfer(context.Background(), fmt.Sprintf("impact-%d", i), ledgerdb.TransferRequest{FromAccount: "a", ToAccount: "b", Amount: 1})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			close(stop)
			wait.Wait()
			select {
			case err := <-errorsCh:
				b.Fatal(err)
			default:
			}
		})
	}
}
