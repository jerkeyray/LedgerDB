// Command ledgerdb-bench runs reproducible transfer workloads and emits JSON
// with throughput and latency percentiles.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jerkeyray/ledgerdb"
)

type result struct {
	Workload        string  `json:"workload"`
	Operations      int     `json:"operations"`
	Workers         int     `json:"workers"`
	Accounts        int     `json:"accounts"`
	ElapsedSeconds  float64 `json:"elapsed_seconds"`
	OperationsSec   float64 `json:"operations_per_second"`
	P50Microseconds float64 `json:"p50_microseconds"`
	P95Microseconds float64 `json:"p95_microseconds"`
	P99Microseconds float64 `json:"p99_microseconds"`
}

func main() {
	operations := flag.Int("operations", 10_000, "number of transfers per workload")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "concurrent workers")
	accounts := flag.Int("accounts", 64, "accounts for uniform and hot workloads")
	workload := flag.String("workload", "all", "disjoint, uniform, hot-account, or all")
	dir := flag.String("dir", "", "parent directory for benchmark databases; defaults to a temporary directory")
	flag.Parse()
	if *operations <= 0 || *workers <= 0 || *accounts < 2 {
		fatalf("operations and workers must be positive; accounts must be at least two")
	}
	root := *dir
	cleanup := func() {}
	if root == "" {
		var err error
		root, err = os.MkdirTemp("", "ledgerdb-bench-")
		if err != nil {
			fatalf("create temp directory: %v", err)
		}
		cleanup = func() { _ = os.RemoveAll(root) }
	}
	defer cleanup()
	workloads := []string{*workload}
	if *workload == "all" {
		workloads = []string{"disjoint", "uniform", "hot-account"}
	}
	encoder := json.NewEncoder(os.Stdout)
	for _, name := range workloads {
		if name != "disjoint" && name != "uniform" && name != "hot-account" {
			fatalf("unknown workload %q", name)
		}
		measurement, err := run(context.Background(), filepath.Join(root, name), name, *operations, *workers, *accounts)
		if err != nil {
			fatalf("%s: %v", name, err)
		}
		if err := encoder.Encode(measurement); err != nil {
			fatalf("encode result: %v", err)
		}
	}
}

func run(ctx context.Context, dir, workload string, operations, workers, accounts int) (result, error) {
	if workload == "disjoint" && accounts < workers*2 {
		accounts = workers * 2
	}
	db, err := ledgerdb.Open(dir, ledgerdb.Options{})
	if err != nil {
		return result{}, err
	}
	defer db.Close()
	for i := range accounts {
		_, err := db.CreateAccount(ctx, fmt.Sprintf("create-%d", i), ledgerdb.CreateAccountRequest{ID: fmt.Sprintf("a-%d", i), Currency: "USD", OpeningBalance: int64(operations + 1)})
		if err != nil {
			return result{}, err
		}
	}
	durations := make([]time.Duration, operations)
	var next atomic.Uint64
	errorsCh := make(chan error, workers)
	start := time.Now()
	var wait sync.WaitGroup
	for worker := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			random := rand.New(rand.NewPCG(uint64(worker+1), uint64(worker+17)))
			for {
				sequence := int(next.Add(1) - 1)
				if sequence >= operations {
					return
				}
				from, to := accountsFor(workload, worker, accounts, random)
				operationStart := time.Now()
				_, err := db.Transfer(ctx, fmt.Sprintf("transfer-%d", sequence), ledgerdb.TransferRequest{FromAccount: fmt.Sprintf("a-%d", from), ToAccount: fmt.Sprintf("a-%d", to), Amount: 1})
				durations[sequence] = time.Since(operationStart)
				if err != nil {
					errorsCh <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	elapsed := time.Since(start)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			return result{}, err
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return result{
		Workload: workload, Operations: operations, Workers: workers, Accounts: accounts,
		ElapsedSeconds: elapsed.Seconds(), OperationsSec: float64(operations) / elapsed.Seconds(),
		P50Microseconds: percentile(durations, 0.50), P95Microseconds: percentile(durations, 0.95), P99Microseconds: percentile(durations, 0.99),
	}, nil
}

func accountsFor(workload string, worker, accounts int, random *rand.Rand) (int, int) {
	switch workload {
	case "disjoint":
		from := (worker * 2) % accounts
		return from, from + 1
	case "hot-account":
		return 0, 1 + random.IntN(accounts-1)
	default:
		from := random.IntN(accounts)
		to := random.IntN(accounts - 1)
		if to >= from {
			to++
		}
		return from, to
	}
}

func percentile(values []time.Duration, quantile float64) float64 {
	index := int(float64(len(values)-1) * quantile)
	return float64(values[index]) / float64(time.Microsecond)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ledgerdb-bench: "+format+"\n", args...)
	os.Exit(1)
}
