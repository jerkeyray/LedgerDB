<p align="center">
  <picture>
    <source srcset="docs/brand/logo-dark.svg" media="(prefers-color-scheme: dark)">
    <source srcset="docs/brand/logo-light.svg" media="(prefers-color-scheme: light)">
    <img src="docs/brand/logo-light.svg" alt="LedgerDB" width="390">
  </picture>
</p>

<p align="center">
  <strong>Crash-safe payment lifecycles, embedded in Go.</strong>
</p>

<p align="center">
  Reserve · Settle · Cancel · Durable retries · Optional group commit
</p>

<p align="center">
  <a href="#quickstart">Quickstart</a> ·
  <a href="docs/README.md">Documentation</a> ·
  <a href="DESIGN.md">Storage design</a> ·
  <a href="docs/evaluation.md">Evaluation</a>
</p>

<p align="center">
  <img alt="Go 1.24+" src="https://img.shields.io/badge/Go-1.24%2B-00ADD8?style=flat-square&logo=go&logoColor=white">
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-0F766E?style=flat-square">
  <img alt="Status: beta" src="https://img.shields.io/badge/status-beta-F59E0B?style=flat-square">
</p>

---

## Show the failure, then show the guarantee

```bash
go run ./cmd/ledgerdb-demo
```

The terminal demo reserves ₹500 for campus EV charging, bills ₹180, and kills a
real child process immediately after settlement's WAL sync. Recovery restores
₹820 available and ₹180 paid. Same-key and different-key retries return the
original settlement result. A second session races settlement against
cancellation; exactly one outcome wins. Every scene checks balances and funds
conservation. Evidence files remain in a fresh temporary directory.

See [the review walkthrough](docs/review-demo.md) for commands, talking points,
and the [payment lifecycle guide](docs/payments.md) for the API.

## One durable record. One committed operation.

That is the core idea. LedgerDB stores the balance/hold mutation and its idempotency
completion result in the **same checksummed WAL record**. Once that record is
synchronized, the transfer exists; before it is synchronized, account state is
untouched.

This one-record boundary gives LedgerDB its useful properties:

- **Safe retries** return the original result instead of moving money twice.
- **Atomic transfers** update both accounts or neither account.
- **Redo-only recovery** replays durable records without an undo pass.
- **Deterministic failure handling** poisons the writer after uncertain I/O and
  resolves the outcome by retrying the same key after restart.
- **Simple deployment** keeps the ledger in your Go process—no database server,
  network hop, or distributed control plane.

## What is included

- Thread-safe account creation, balance reads, and two-account transfers.
- Durable payment holds, partial/zero settlement, cancellation, and hold reads.
- Total, reserved, and available balances; transfers cannot spend reserved funds.
- Opt-in group commit with ordered validation and shared WAL synchronization.
- Optional automatic checkpoints and exported operational statistics.
- Signed 64-bit minor units and three-letter currency identifiers.
- Caller-supplied idempotency keys with concurrent retry rendezvous.
- Configurable key leases with automatic in-memory reclamation.
- Segmented, versioned WAL records with CRC validation and full sync.
- Fuzzy checkpoints with atomic manifest publication and WAL compaction.
- Exclusive directory locking to prevent concurrent-writer corruption.
- Fail-closed recovery for complete corrupt records and repair of incomplete tails.
- Fault-injecting storage, subprocess crash tests, race tests, and workload tools.

## Install

```bash
go get github.com/jerkeyray/ledgerdb
```

LedgerDB is a single Go module with no third-party runtime dependencies. It is
currently beta software; pin a version before using it in a long-lived service.

## Quickstart

```go
package main

import (
	"context"
	"log"

	"github.com/jerkeyray/ledgerdb"
)

func main() {
	db, err := ledgerdb.Open("./ledger-data", ledgerdb.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	_, err = db.CreateAccount(ctx, "account:wallet", ledgerdb.CreateAccountRequest{
		ID: "wallet", Currency: "USD", OpeningBalance: 10_000,
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.CreateAccount(ctx, "account:merchant", ledgerdb.CreateAccountRequest{
		ID: "merchant", Currency: "USD",
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Transfer(ctx, "payment:2026-0001", ledgerdb.TransferRequest{
		FromAccount: "wallet",
		ToAccount:   "merchant",
		Amount:      2_500,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("committed at LSN %d; wallet=%d merchant=%d",
		result.CommitLSN, result.FromBalance, result.ToBalance)
}
```

Retry `payment:2026-0001` with the same request at any point during the
retention window. LedgerDB returns the stored result without applying the
transfer again. Reusing it for a different request returns
`ErrIdempotencyConflict`.

## Commit model

```text
reserve key → queue → stage/validate batch → append WAL → sync
                                                    │
                                                    ▼
                                  apply balances/holds → publish results
```

The commit coordinator assigns a monotonic LSN per operation and holds the commit barrier until account/hold state
and the idempotency result are visible. If WAL write or sync returns an error,
the database rejects further mutations with `ErrPoisoned`. Close it, reopen it,
and retry the same key to discover the durable outcome safely.

## Guarantees at a glance

| Property | Current behavior |
| --- | --- |
| Transfer atomicity | Both balances change, or neither changes |
| Retry behavior | Exactly once within the configured key lease |
| Successful return | WAL record passed `File.Sync` |
| Recovery | Checkpoint load followed by ordered redo |
| Torn tail | Physically incomplete final record is truncated |
| Complete corruption | Open fails with `ErrCorrupt` |
| Writer ownership | One process; enforced with an exclusive directory lock |
| Cross-currency transfer | Rejected |
| Overdraft | Rejected, including spending reserved funds |
| Payment lifecycle | A hold settles or cancels once; matching terminal retries return the original result |
| Group commit | Opt-in; acknowledgements wait for all touched WAL segments to sync |
| Automatic checkpoints | Opt-in interval and/or committed-operation threshold |

## Documentation

- [Getting started](docs/getting-started.md) — installation, account setup,
  transfers, retries, and shutdown.
- [Core concepts](docs/concepts.md) — accounts, LSNs, idempotency leases, and
  the one-record transaction model.
- [API guide](docs/api.md) — options, methods, result types, and errors.
- [Durability and recovery](docs/durability.md) — WAL ordering, checkpoints,
  corruption handling, and the failure model.
- [Operations](docs/operations.md) — directory ownership, checkpointing,
  backups, monitoring, and filesystem requirements.
- [Storage design](DESIGN.md) — implementation-level architecture.
- [Evaluation protocol](docs/evaluation.md) — crash checks and reproducible
  workloads.

The complete documentation index is [docs/README.md](docs/README.md).

## Verification

```bash
go test ./...
go test -race ./...
go vet ./...
```

Run transfer latency workloads and emit JSON percentiles:

```bash
go run ./cmd/ledgerdb-bench -mode compare -trials 3 -operations 1000 -workers 16
```

Run Go benchmarks for throughput, recovery scaling, checkpoint impact, and
retained-key memory:

```bash
go test -run '^$' -bench . -benchmem ./...
```

## Scope and status

LedgerDB is a single-node, embedded ledger. Each operation is one durable
account creation, transfer, reservation, settlement, or cancellation record.
Grouped operations can recover as a prefix; a batch is not one atomic transaction.
Terminal holds are retained indefinitely to prevent repeated financial effects,
so their memory and checkpoint footprint grows with payment history.
New writes use storage format v2; the reader accepts v1 and v2. Downgrading to
an older binary after v2 writes is unsupported. Opening balances introduce funds from outside the ledger; use an
explicit source account when strict double-entry conservation is required.

Durability ultimately depends on the kernel, filesystem, mount options, device
firmware, and hardware honoring synchronization requests. The test suite covers
process crashes and injected storage failures, but production claims must be
validated on the exact Linux storage stack being deployed.

## License

[MIT](LICENSE)
