# Review walkthrough: crash-safe payment lifecycles

## Start with the concrete problem

“Before charging an EV, we need to reserve a spending limit. After charging, we
bill actual usage and release the remainder. If the backend dies after committing
payment but before responding, a retry must recover the outcome without charging
again. Settlement and cancellation must also never both succeed.”

The example uses one campus Go backend with prepaid internal INR wallets. It
models money accounting, not a charging protocol, external payment gateway, or
multi-controller offline settlement.

## Run the demonstration

From the repository root:

```bash
go run ./cmd/ledgerdb-demo
```

For presentation, build first so the live demonstration needs no compilation:

```bash
go build -o /tmp/ledgerdb-demo ./cmd/ledgerdb-demo
/tmp/ledgerdb-demo
```

The command creates a fresh directory and runs scenes in separate subprocesses.
It preserves evidence on both success and failure. `-dir /existing/parent` creates
a fresh child in that parent and never deletes existing data.

1. **Reserve:** driver starts with ₹1,000. Reserving ₹500 leaves ₹500 available
   and ₹500 reserved. An unrelated ₹600 spend is rejected.
2. **Crash:** bill actual usage at ₹180. The process exits with code 91 immediately
   after successful WAL sync, before in-memory publication or returning a result.
3. **Recover/retry:** reopen the directory. Settlement has survived. Driver has
   ₹820 available, zero reserved; operator has ₹180. Same-key and different-key
   retries return the original result/LSN, and ₹320 of unused reservation is released.
4. **Race:** a second hold reserves ₹100. Settlement for ₹40 races cancellation.
   Either outcome may win; exactly one succeeds and the other conflicts. The
   printed balances are checked against the winning outcome.
5. **Checkpoint/reopen:** checkpoint the state, restart again, and verify both
   terminal holds and total funds. A final `PASS` means every assertion succeeded.

All scenes conserve ₹1,000 total funds. Opening balances seed the demonstration;
account creation itself is not a strict double-entry funding transaction.

## Explain what makes the engine interesting

“The engine stores the financial state change, payment lifecycle transition,
and retry result in one durable record. Recovery only redoes complete records.
We also retain the terminal payment outcome independently of expiring request
keys, so changing the retry key cannot settle a payment twice.”

The feature is not an industry-first claim: pending transfers and payment holds
exist in other systems. The contribution here is their implementation inside a
small embedded Go engine with an explicit recovery boundary and executable
failure demonstration. A conventional transactional database can also couple
payment state and balances; LedgerDB packages this as a purpose-built API.

## Show the measured performance improvement

```bash
go run ./cmd/ledgerdb-bench -mode compare -trials 3 -operations 1000 -workers 16
```

Group commit uses a 64-operation cap and a 1 ms collection window. The following
are medians of three fresh-database trials, each with 1,000 measured transfers,
16 workers, and 64 accounts. Account setup is excluded from timings and sync counts.

| Workload | Individual transfers/s | Group transfers/s | Ratio | Individual p99 ms | Group p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Disjoint | 289 | 2,771 | 9.59× | 67.68 | 6.39 |
| Uniform | 293 | 2,959 | 10.11× | 64.00 | 6.39 |
| Hot account | 289 | 2,842 | 9.83× | 65.29 | 6.15 |

Each individual trial performed 1,000 WAL syncs; every grouped trial performed
63, averaging 15.87 operations per batch. Workers submit one request at a time,
so 16 concurrent workers limit observed batches to approximately 16 despite the
64-operation cap. Grouping reduces barrier frequency; it does not eliminate
synchronization. An isolated request can incur the extra collection delay.

[Raw results](benchmarks/2026-10-05-group-commit.jsonl) include p50/p95/p99,
sync duration, batch size, and trial configuration. Measured on 2026-10-05 from
an uncommitted working tree based on `6835bae`: Go 1.26.1, darwin/arm64, macOS
26.1 (25B78), Darwin 25.1.0, Apple M5 (10 CPUs), 16 GiB RAM, APFS on
`/System/Volumes/Data`, internal Apple Fabric SSD. Device firmware and mount
options were not separately qualified. These are local short-run measurements,
not a production throughput guarantee or a cross-platform comparison.

## Improvements and remaining limits

Implemented: reserve/settle/cancel with partial or zero settlement, available-fund
checks, terminal retry protection, opt-in group commit, operational statistics,
automatic checkpoint triggers, v1 read compatibility, and v2 fail-closed versions.

Remaining: single-node ownership, no replication or online backup, indefinite
hold-history growth, no automatic hold expiry, no external gateway integration,
and no hardware power-loss qualification. Batches contain independently durable
operations and may recover as a prefix; they are not all-or-nothing transactions.

Correctness, race, and static-analysis commands:

```bash
go test ./...
go test -race ./...
go vet ./...
```
