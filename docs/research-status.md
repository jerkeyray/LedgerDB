# LedgerDB: research context, problem statement, status, and benchmarks

> Historical baseline: the measurements and status tables below describe
> revision `6835bae`, not the current implementation. The current engine adds
> payment holds, optional group commit, automatic checkpoints, statistics, and
> storage v2. See [review-demo.md](review-demo.md) for new measurements and limits.

This document is the project-level research record. It states the problem
LedgerDB attacks, positions the work against prior art, reports what is
actually implemented and verified today, and publishes measured benchmark
results with the disclosure required to interpret them.

Companion documents: [Storage design](../DESIGN.md) for implementation-level
detail, [Durability and recovery](durability.md) for the failure model, and
[Evaluation protocol](evaluation.md) for the commands that reproduce the
measurements below.

- Revision measured: `6835bae` (`main`, clean working tree)
- Measurement date: 2026-09-25
- Status: beta; single-node; v1 storage format

---

## 1. Problem description

### 1.1 The failure boundary

A ledger operation carries two inseparable obligations. It must change account
state **exactly once**, and it must retain enough evidence to answer a repeated
request **consistently**. Both obligations are easy to honor while nothing goes
wrong, and both break at the same boundary: the moment between the caller
issuing a request and the caller learning the outcome.

Three concrete failure shapes define the problem:

| Failure | Consequence when handled naively |
| --- | --- |
| Process dies after memory is updated but before the log record is durable | Acknowledged funds vanish on restart |
| Client times out after the engine committed, then retries | The same transfer is applied twice |
| `fsync` returns an error, leaving durability genuinely unknown | Either data loss or a double-apply, depending on which way the caller guesses |

The third is the sharpest. An I/O error on the durability barrier does not tell
the application whether the record reached stable storage. Worse, on Linux the
dirty page is marked clean after a failed `fsync`, so a naive retry returns
success while the data was never written. Any system that treats that second
success as proof of durability has voided its central guarantee.

### 1.2 Why the conventional layering is the wrong fix

The standard industry response is to bolt an idempotency table onto an
application sitting above a general-purpose database: the application writes a
"request seen" row, performs the balance mutation, then writes a "request
completed" row with the response. This is the pattern Helland describes as
developers repeatedly reimplementing plumbing that the layer below should have
provided, and patching the resulting anomalies into the application
incrementally as surprising bugs surface.

If completion tracking and balance mutation are written in separate transactions,
they can diverge after a crash. A general-purpose database can already solve this
with an ordinary local transaction containing both objects; a distributed
transaction is needed only when separate transactional systems are involved.
LedgerDB's contribution is to make this coupling a built-in embedded API and
single-record recovery boundary, reducing application protocol work. It does not
claim that conventional transactional databases cannot enforce the same atomicity.

### 1.3 Research question

> If the storage engine owns both the balance mutation and the idempotency
> completion record, and commits them as a single log record, what does that buy
> — and what does it cost?

LedgerDB's answer, stated as a falsifiable claim:

**Claim.** Collapsing the state mutation and its idempotency completion record
into one checksummed, synchronously-durable WAL record yields redo-only
recovery (no undo pass, no analysis pass) and a well-defined answer to
uncertain I/O, at the cost of bounding a transaction to exactly one log record
and serializing commits at one durability barrier.

Sections 6 and 7 quantify that cost.

---

## 2. Research context and positioning

### 2.1 Prior art

| Work | What it establishes | Relationship to LedgerDB |
| --- | --- | --- |
| **RIFL** (Lee et al., SOSP 2015) | Exactly-once RPC as a reusable layer; decomposes it into RPC identification, completion-record durability, retry rendezvous, and lease-based GC | Closest prior art. RIFL sits *above* the storage system and **requires** it to supply durable completion-record storage as a primitive |
| **ARIES** (Mohan et al., TODS 1992) | No-force/steal WAL; LSN per record and per page; three-pass analysis/redo/undo recovery | The recovery baseline LedgerDB simplifies away from |
| **Helland** (ACM Queue 2012) | Idempotence as an architectural concern repeatedly and badly reimplemented in applications | Framing citation for §1.2 |
| **CrashMonkey / ACE / B3** (Mohan et al., OSDI 2018) | Bounded black-box crash testing; exhaustive exploration of a bounded crash space against an automatic oracle | Methodological template for the crash suite in §4.3 |
| **ALICE / BOB** (Pillai et al., OSDI 2014) | Application crash-consistency depends on filesystem-specific *persistence properties* that vary widely | Source of the threats-to-validity position in §7.3 |
| **Rebello et al.** (USENIX ATC 2020) | Across ext4/XFS/btrfs, failed `fsync` marks pages clean; no studied application handled `fsync` failure sufficiently | Directly determines LedgerDB's poisoning policy |
| **TigerBeetle** | Purpose-built financial OLTP database; single-threaded by design; consensus-replicated; batching amortizes per-transfer cost | Positioning foil: different point in the design space |

### 2.2 The contribution delta, stated precisely

LedgerDB does **not** claim that engine-level idempotency is novel. RIFL solved
exactly-once semantics, including lease-based garbage collection, ten years
earlier. The defensible narrower claim is about *layer placement and its
consequence*:

| Dimension | RIFL | LedgerDB |
| --- | --- | --- |
| Layer | Above the storage system | Inside it — owns the log |
| Completion record | A durable object the storage system must provide | The same bytes as the state mutation |
| Atomicity mechanism | Storage system's own transaction | Single log append + one barrier |
| Recovery consequence | Storage system's recovery protocol unchanged | Undo and analysis passes become unnecessary |

Against ARIES the delta is equally specific. ARIES needs undo because it is
*steal*: an uncommitted transaction can have dirty pages on disk. LedgerDB keeps account state in memory and does not persist uncommitted
account pages. It is **force-at-commit for the log record**, and a transaction is exactly one record. An uncommitted transaction
therefore has no record and cannot have mutated any state, because state is
published only after the barrier. Undo has nothing to operate on.

This is a trade, not a free lunch. It is paid for in §3.2's scope restriction
and §6.1's throughput ceiling.

### 2.3 Position relative to TigerBeetle

TigerBeetle is a replicated cluster with a fixed double-entry schema, optimized
for batch throughput, single-threaded by design, and it explicitly considered
and rejected embedding in a single application process because replication and
consensus were still required for durability and availability.

LedgerDB occupies the opposite corner: a single-node embeddable library where
the contribution is the atomicity of *idempotency-plus-mutation*, not
throughput via batching. The embedded case remains worth studying precisely
because it is where the layering problem of §1.2 is most visible and where no
consensus layer can paper over it. The honest cost of this position is
quantified in §6.1: without batching or group commit, every transfer pays a
full durability barrier.

---

## 3. System under evaluation

### 3.1 The commit protocol

```text
validate → reserve key → lock account shards (ascending) → re-validate
        → acquire sequencer → assign LSN → append one record → SYNC
                                                                │
                                          ── durability barrier ─┤
                                                                ▼
                                     publish balances → publish result → return
```

The sequencer (`commitMu`) is held through publication, which makes a
checkpoint's captured base LSN a true barrier: every mutation at or below it is
present in both the account state and the idempotency index before checkpoint
copying begins.

One WAL record contains: type, LSN, commit timestamp, idempotency key, SHA-256
request fingerprint, serialized request, and serialized result — behind a
16-byte envelope (magic `LDBW`, version, flags, payload length, CRC32).

Measured on-disk record sizes at this revision:

| Record type | Bytes per record |
| --- | ---: |
| `create_account` | 381 |
| `transfer` | 346 |

### 3.2 Scope restriction (the price of the claim)

A v1 transaction is **exactly one** account creation or **exactly one**
transfer between two same-currency accounts. Amounts are signed 64-bit minor
units; currency codes are three uppercase ASCII letters. Excluded from v1:
currency conversion, replication, multi-process coordination, arbitrary
multi-account transactions, and strict double-entry conservation (opening
balances introduce funds from outside the ledger).

---

## 4. Implementation status

### 4.1 Module inventory

| Component | Location | Lines | Responsibility |
| --- | --- | ---: | --- |
| Database core | `db.go` | 598 | API, validation, sharding, locking, idempotency, commit sequencing, replay application |
| Checkpoint layer | `checkpoint.go` | 280 | Snapshot envelopes, manifest discovery, publish ordering, installation, compaction |
| WAL package | `internal/wal/wal.go` | 373 | Encoding, CRC validation, segmentation, append/sync, replay, tail repair, compaction |
| Fault injection | `internal/failpoint/fs.go` | 270 | Deterministic failures across 12 filesystem operations |
| Filesystem boundary | `fs.go` | 60 | Portable storage interface |
| Public contracts | `types.go`, `errors.go` | 91 | Requests, results, options, 20 stable sentinel errors |
| Locking | `lock_unix.go`, `lock_other.go` | 45 | Exclusive advisory directory lock |
| Benchmark runner | `cmd/ledgerdb-bench/main.go` | 156 | Latency-percentile workloads, JSON output |
| Tests | `db_test.go`, `db_internal_test.go`, `benchmark_test.go`, `wal_test.go` | 930 | Correctness, crash behavior, concurrency, performance |
| **Total** | | **2,888** | Single Go module, zero third-party runtime dependencies |

### 4.2 Feature status

| Capability | Status | Notes |
| --- | --- | --- |
| Atomic two-account transfer | Implemented | Both balances at one commit LSN, or neither |
| Durable idempotency with stored result | Implemented | SHA-256 fingerprint; conflicting fingerprint returns `ErrIdempotencyConflict` |
| Concurrent retry rendezvous | Implemented | Duplicate waits on the owner's channel; holds no locks while waiting |
| Lease-based key GC | Implemented | 30-day default; incremental pruning per commit, eager pruning at checkpoint |
| Redo-only recovery | Implemented | Checkpoint install + ordered replay; per-object LSN prevents double-apply |
| Torn-tail repair | Implemented | Physically incomplete final record truncated and synced |
| Fail-closed corruption | Implemented | Complete record with bad header/CRC/version/LSN → `ErrCorrupt`, even at the tail |
| `fsync`-failure policy | Implemented | Poison the handle; never retry sync in-process |
| Fuzzy checkpoints | Implemented | Temp-write → sync → rename → dir-sync; WAL compaction only after manifest is durable |
| Exclusive writer enforcement | Implemented | Advisory `LOCK`; second opener gets `ErrAlreadyOpen` |
| Group commit / batching | **Not implemented** | Primary cause of the §6.1 throughput ceiling |
| Automatic checkpoint scheduling | **Not implemented** | Manual `Checkpoint(ctx)` only |
| Metrics / observability | **Not implemented** | No exported counters; applications must instrument externally |
| Online backup | **Not implemented** | Requires stop-and-copy |
| Latent-sector-error / silent-corruption handling | **Not implemented** | CRC32 detects corruption; it does not repair, and is not cryptographic |
| Power-loss qualification | **Not performed** | Only process-level crashes are tested; see §7.3 |

### 4.3 Verification status

All results from revision `6835bae` on the environment in §5.1.

| Check | Command | Result |
| --- | --- | --- |
| Static analysis | `go vet ./...` | Clean |
| Correctness | `go test ./...` | **PASS** — ledgerdb 3.15 s, internal/wal 0.75 s |
| Race detection | `go test -race ./...` | **PASS** — ledgerdb 3.47 s, internal/wal 2.29 s |
| Test count | `go test -v ./...` | 44 cases (21 top-level + 23 subtests), 0 failures |

The crash suite follows the B3 bounded-exploration model: a bounded crash space
with an automatic oracle (balance and idempotency invariants checked after
every recovery). Coverage at this revision:

| Crash / fault class | Test | Bound explored |
| --- | --- | --- |
| Torn final record | `TestTornFinalRecordBoundedMatrix` | 7 partial-write offsets: 0, 1, 4, 15, 16, 31, 127 bytes |
| Process crash around the barrier | `TestProcessCrashMatrix` | 4 phases: before/after write, before/after sync — real subprocess `os.Exit` |
| `fsync` returns `EIO` | `TestSyncFailurePoisonsMutationsAndRecovers` | Poison, reopen, retry-same-key resolution |
| Durable record not yet in memory | `TestRecoveryAppliesDurableRecordNotAppliedToMemory` | Crash in the publication window |
| Checkpoint I/O failure | `TestCheckpointFailuresDoNotPoisonMutations` | 3 ops: write, sync, rename — mutations must survive |
| Mid-log corruption | `TestMidLogCorruptionIsRejected` | Must fail closed, not silently truncate |
| Complete corrupt tail record | `TestCompleteCorruptFinalRecordIsRejected` | Must fail closed |
| Corrupt published checkpoint | `TestCorruptPublishedCheckpointIsRejected` | Manifest/checkpoint envelope validation |
| WAL compaction + recovery | `TestCheckpointCompactsSegmentsAndRecovers` | Recovery across a compacted generation |
| Concurrent-writer exclusion | `TestOpenExcludesConcurrentWriter` | `ErrAlreadyOpen` |
| Concurrent invariants | `TestConcurrentTransfersPreserveTotal` | Conservation of total balance under race detector |
| Duplicate rendezvous | `TestConcurrentDuplicateExecutesOnce` | Exactly-once under concurrency |
| Lease expiry | `TestIdempotencyLeaseExpiry`, `TestExpiredIdempotencyEntriesAreReclaimed` | Expiry semantics and memory reclamation |

### 4.4 Known gaps and documentation discrepancies

1. **The proposal document contradicts the implementation on corrupt tails.**
   `LedgerDB_Proposal_Detailed.docx` §4.2 states that a final record with "an
   invalid final checksum" is truncated as a torn tail. The implementation does
   not do this: `scanSegment` truncates only *physically incomplete* records
   (short header, or payload extending past EOF). A complete record with a bad
   CRC returns `ErrCorrupt` even at the tail, which is what `DESIGN.md`, the
   README guarantee table, and `TestCompleteCorruptFinalRecordIsRejected` all
   specify. The fail-closed behavior is the correct one; **the proposal text
   should be corrected** before submission.

2. **"Never expire" is unreachable.** `expired()` treats a zero retention as
   "never expire", but `Open` maps a zero `IdempotencyRetention` to the 30-day
   default, so the branch is dead and infinite retention cannot be configured.

3. **Retention is a memory liability at scale.** See §6.4 — the default 30-day
   lease is not survivable at sustained throughput and must be tuned per
   deployment.

4. **No group commit.** Every transfer pays its own barrier; the sharded
   account locks provide no throughput benefit at the commit point (§6.1).

5. **Reads are memory-only.** `GetAccount` reflects the last state applied in
   the current process and remains available after poisoning; there is no
   snapshot read of durable state.

---

## 5. Benchmark protocol

### 5.1 Environment disclosure

Durability results are conditional on the tested stack. This configuration:

```text
LedgerDB revision:      6835bae (main, clean)
Go version:             go1.26.1 darwin/arm64
Operating system:       macOS 26.1 (build 25B78)
Kernel:                 Darwin 25.1.0, xnu-12377.41.6~2, arm64
Filesystem:             APFS (local, journaled, protect) on /System/Volumes/Data
Block device:           /dev/disk3s5, Apple Fabric NVMe, solid state
CPU:                    Apple M5, 10 cores
Memory:                 16 GiB
GOMAXPROCS:             10
```

**Critical for interpreting every latency number below:** Go's `File.Sync` maps
to `fcntl(F_FULLFSYNC)` on Darwin, which requests that the device flush its
volatile write cache. These are therefore true barrier costs, not page-cache
flushes. The same code on Linux `ext4` with `fdatasync` will produce materially
different — typically much lower — per-operation latencies, and the two must
not be compared. Results from this configuration establish *shape* (linearity,
serialization, relative cost) with high confidence and *absolute throughput*
only for this stack.

### 5.2 Workloads

- **disjoint** — fixed non-overlapping account pairs, one pair per worker; no
  account-lock contention by construction.
- **uniform** — random source/destination pairs across the account set.
- **hot-account** — account zero is the source for every transfer; maximum
  account-lock contention.

### 5.3 Commands

```bash
go test -run '^$' -bench . -benchmem -count 5 ./...
go run ./cmd/ledgerdb-bench -operations 5000 -workers 8
```

All Go-benchmark figures below are the **median of 5 runs**.

---

## 6. Results

### 6.1 Transfer throughput and per-operation cost

`BenchmarkTransfers`, `b.RunParallel` across 10 logical CPUs, median of 5:

| Workload | ns/op | Transfers/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| disjoint | 3,699,021 | 270 | 1,449 | 14 |
| uniform | 3,619,759 | 276 | 1,458 | 14 |
| hot-account | 3,613,193 | 277 | 1,404 | 14 |

**The three workloads are statistically indistinguishable** — a 2.4% spread,
within run-to-run variance, with the *most contended* workload nominally the
fastest.

This is the central performance finding, and it is a structural consequence of
the design rather than a tuning problem. Because `commitMu` is held across
`Append` (write + `F_FULLFSYNC`) **and** in-memory publication, and because
there is no group commit, every transfer serializes behind one device barrier.
Throughput is therefore pinned to the barrier rate — roughly 270 ops/s on this
stack — and:

- the 64-way account sharding and ascending lock ordering buy **no throughput**
  at the commit point; they only allow validation and reads to proceed in
  parallel;
- the hot-account limitation that motivates TigerBeetle's single-threaded
  design **does not manifest here**, because commits are already fully
  serialized. LedgerDB has, in effect, adopted the serial design without
  adopting its batching.

The 14 allocations and ~1.4 KB per transfer are dominated by JSON
serialization of the request and result. At 3.6 ms/op this is irrelevant to
latency; it would become the next bottleneck if group commit were added.

### 6.2 Latency percentiles under a fixed worker pool

`ledgerdb-bench`, 5,000 operations, 8 workers, 64 accounts:

| Workload | ops/s | p50 (ms) | p95 (ms) | p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| disjoint | 319 | 24.0 | 30.0 | 32.0 |
| uniform | 292 | 23.0 | 56.0 | 77.0 |
| hot-account | 309 | 25.9 | 32.0 | 46.2 |

Throughput matches §6.1 (~290–320 ops/s), but per-request latency is ~24 ms
rather than 3.6 ms — almost exactly 8× the service time, which is the signature
of 8 workers queueing behind a single server. This is direct confirmation of
the serialization in §6.1: **adding workers adds latency, not throughput.**

The uniform workload shows the worst tail (p99 77 ms, 3.3× its p50) because its
random pairing produces occasional account-lock waits *stacked on top of* the
commit queue, whereas disjoint pairs never wait on locks (p99 only 1.3× p50).

### 6.3 Recovery scaling

`BenchmarkRecovery` — repeated `Open`/`Close` over a WAL of N `create_account`
records, median of 5:

| WAL records | ms/op | µs/record | B/op | allocs/record |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.530 | 5.30 | 214,727 | 27.3 |
| 1,000 | 4.401 | 4.40 | 1,868,302 | 24.9 |
| 10,000 | 45.153 | 4.52 | 18,906,232 | 24.2 |

Recovery is **cleanly linear** in record count at ~4.5 µs/record, with a fixed
open cost below 0.15 ms. Fitting the 1,000 → 10,000 points gives 4.53
µs/record. At 381 bytes/record this is ~84 MB/s of log replay, or ~221,000
records/s — CPU-bound on JSON decoding, not I/O-bound.

Practical restart-time implication, and the argument for checkpointing:

| Uncheckpointed WAL records | Projected recovery time |
| ---: | ---: |
| 100,000 | ~0.45 s |
| 1,000,000 | ~4.5 s |
| 10,000,000 | ~45 s |

Memory during replay is ~1.9 KB/record allocated (transient) — at 10M records
that is a substantial allocation load, which is a second reason to bound WAL
length with checkpoints.

### 6.4 Checkpoint cost and concurrent impact

`BenchmarkCheckpoint`, 1,000 accounts, median of 5: **21.02 ms/op**, 2.19 MB
and 3,134 allocations per checkpoint.

At 21 µs/account this looks expensive, but the cost is barrier-dominated rather
than copy-dominated: publishing a checkpoint performs four synchronous barrier
operations (checkpoint temp-file sync, directory sync after rename, manifest
sync, directory sync after manifest rename). At ~3.6 ms per barrier on this
stack, roughly 14 ms of the 21 ms is barrier time that is **independent of
account count**. Checkpoint cost should therefore be expected to stay near
~15 ms until the snapshot grows large enough for copying to dominate.

`BenchmarkTransferCheckpointImpact` — serial transfers with and without a
continuously-looping background checkpointer, median of 5:

| Condition | ns/op | Transfers/s | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| without checkpoints | 3,912,418 | 256 | 1,394 | 12 |
| with checkpoints | 5,472,887 | 183 | 19,719 | 57 |

Back-to-back checkpointing costs **+39.9% transfer latency and −28.5%
throughput**. This is a deliberate worst case — the checkpointer never idles —
so it bounds the damage rather than predicting it. Under a realistic policy
(checkpoint every N mutations or during a quiet interval) the amortized cost is
a small fraction of this. The finding to carry forward is that checkpointing is
*safe* under load (no correctness impact, and `TestCheckpointFailuresDoNotPoisonMutations`
shows a failed checkpoint does not poison the writer) but not *free*, and it
competes for the same barrier the commit path depends on.

### 6.5 Idempotency retention cost

`BenchmarkIdempotencyIndexGrowth`, median of 5: **233.2 bytes of live heap per
retained key**, 1,409 B and 12 allocations per transfer.

233 B/key is efficient in isolation. Multiplied by the default 30-day lease it
is the most operationally significant number in this document:

| Sustained rate | Keys retained over 30 days | Live heap | WAL/checkpoint bytes (346 B/record) |
| ---: | ---: | ---: | ---: |
| 1 op/s | 2.59 M | ~604 MB | ~0.90 GB |
| 10 ops/s | 25.9 M | ~6.0 GB | ~9.0 GB |
| 300 ops/s (saturation) | 778 M | **~181 GB** | ~269 GB |

At anything approaching the throughput ceiling the default retention is not
survivable, and the index — which is fully resident in memory — fails long
before the disk does. **`IdempotencyRetention` must be set deliberately to the
caller's true maximum retry horizon**, not left at the default, and the README's
current framing of 30 days as a benign default understates this. This is the
concrete form that RIFL's garbage-collection sub-problem takes here: the lease
mechanism is implemented correctly, but its default value encodes an
assumption about traffic that a deployment may not satisfy.

---

## 7. Analysis and threats to validity

### 7.1 What the results support

- **Redo-only recovery works and is cheap.** Recovery is linear at 4.5
  µs/record with negligible fixed cost, and requires no analysis or undo pass.
  The §2.2 claim about what single-record commit buys is supported.
- **The `fsync`-failure policy is implemented as designed.** Poisoning without
  in-process retry is the defensible policy for a money database, it is
  exercised by a dedicated test, and it avoids the specific trap the Rebello
  work identifies. This is a realized design contribution, not a stated
  intention.
- **Crash behavior is bounded-exhaustively tested** across torn writes at 7
  offsets, 4 real subprocess crash phases, `fsync` `EIO`, 3 checkpoint I/O
  failures, and both tail and mid-log corruption, with automatic invariant
  checking as the oracle.

### 7.2 What the results undercut

- **The concurrency design does not pay for itself at the commit point.**
  Sharded accounts, stable lock ordering, and disjoint-pair parallelism produce
  no measurable throughput advantage, because every commit serializes on one
  barrier. The paper cannot claim a concurrency benefit over a serial design
  without first adding group commit and re-measuring. Presented honestly, this
  is a useful negative result: *it shows that exactly-once at the storage layer
  does not, by itself, cost concurrency — the missing batching does.*
- **Absolute throughput (~270–320 transfers/s) is not competitive** with
  batching systems, and should be presented as the cost of an unbatched
  one-record commit on a full-barrier stack rather than as a performance
  result.

### 7.3 Threats to validity

1. **Persistence properties are filesystem-specific.** These results come from
   APFS with `F_FULLFSYNC` on Apple Fabric NVMe. The ALICE work shows that the
   meaning of a durability barrier varies across ext4, XFS, and btrfs; a
   guarantee validated here is not transferable to a Linux deployment without
   re-running the suite there.
2. **Process crashes are not power loss.** `TestProcessCrashMatrix` uses real
   subprocess termination, which validates that recovery handles a process
   vanishing at four points around the barrier. It does **not** discard the
   device's volatile cache. Power-loss claims require a VM snapshot harness, a
   device-mapper fault target (`dm-loki`), or dedicated hardware.
3. **No storage-fault tolerance.** CRC32 detects corruption; it neither repairs
   it nor resists deliberate modification. Latent sector errors and silent
   corruption of previously-durable data are out of scope, as is hardware that
   acknowledges but does not honor a barrier.
4. **Single-machine measurement.** No cross-machine or cross-filesystem
   comparison is offered, and none should be inferred.
5. **Benchmark-specific caveat.** `BenchmarkRecovery` replays only
   `create_account` records; a transfer-heavy log touches two account shards per
   record and may replay at a different rate.

---

## 8. Roadmap

Ordered by what most strengthens the central claim.

| Priority | Item | Why it matters |
| --- | --- | --- |
| 1 | **Group commit** | Directly addresses §6.1/§7.2, the largest weakness. Amortizes the barrier across a batch while preserving a clear acknowledgement boundary, and would make the concurrency design measurable |
| 2 | **Linux + power-loss qualification** | Converts the conditional durability result of §7.3 into a transferable one. `dm-loki` / CuttleFS for injection |
| 3 | **Retention guidance and tooling** | §6.4 shows the default is unsafe at scale. Needs documented sizing guidance and, ideally, a retained-key/heap metric |
| 4 | **Correct the proposal's torn-tail text** | §4.4 item 1 — a factual contradiction with the implementation, and a reviewer will find it |
| 5 | **Observability** | WAL growth, checkpoint age, poisoned state, recovery repairs, retention pressure. Currently invisible |
| 6 | **Automatic checkpoint scheduling** | §6.3 shows recovery time is the direct function of uncheckpointed WAL length; leaving the policy entirely manual is a footgun |
| 7 | Multi-account transactions, strict double-entry | Relaxes the §3.2 scope bound — but note this directly trades against the single-record property that buys undo-free recovery |
| 8 | Storage-fault tolerance, online backup, encryption, replication | Broader hardening; each is independently large |

---

## 9. Reproduction

```bash
go vet ./...
go test ./...
go test -race ./...
go test -run '^$' -bench . -benchmem -count 5 ./...
go run ./cmd/ledgerdb-bench -operations 5000 -workers 8
```

Record the §5.1 disclosure block alongside any published figures. Do not
compare results across machines or filesystems without listing both
configurations.
