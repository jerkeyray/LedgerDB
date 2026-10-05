# Evaluation protocol

LedgerDB's durability result is conditional on the tested storage stack. Record
the following before publishing or comparing results:

```text
LedgerDB revision:
Go version:
Operating system and kernel:
Filesystem and mount options:
Block device and firmware:
CPU and memory:
```

Run correctness and race tests first:

```bash
go test ./...
go test -race ./...
```

Run each benchmark long enough to stabilize and retain raw output:

```bash
go test -run '^$' -bench BenchmarkTransfers -benchmem -count 5 ./... | tee transfer.txt
go test -run '^$' -bench BenchmarkRecovery -benchmem -count 5 ./... | tee recovery.txt
go test -run '^$' -bench BenchmarkCheckpoint -benchmem -count 5 ./... | tee checkpoint.txt
go test -run '^$' -bench BenchmarkIdempotencyIndexGrowth -benchmem -count 5 ./... | tee idempotency.txt
go test -run '^$' -bench BenchmarkTransferCheckpointImpact -benchmem -count 5 ./... | tee checkpoint-impact.txt
```

The transfer benchmark uses fixed non-overlapping pairs for `disjoint`, random
account pairs for `uniform`, and account zero as the source for `hot-account`.
Recovery is parameterized by WAL record count. The idempotency benchmark reports
both allocation rate and retained heap bytes per key.

Collect latency percentiles as JSON with the standalone runner:

```bash
go run ./cmd/ledgerdb-bench -mode compare -trials 3 -operations 1000 -workers 16 | tee latency.jsonl
```

Report operations/second and p50/p95/p99 latency for disjoint, uniform, and
hot-account transfers. Also report recovery time against WAL record count,
checkpoint duration and concurrent throughput loss, and bytes allocated per
retained idempotency result. Do not compare machines or filesystems without
listing their configurations.

The bounded crash suite covers incomplete writes at header and payload offsets,
sync returning `EIO`, subprocess exits before and after WAL write/sync, durable
records not yet applied to memory, complete corrupt tail and mid-log records,
checkpoint write/sync/rename failures, exclusive-open enforcement, and recovery
after WAL segment compaction. Expand these bounds when the record format changes.

Subprocess termination validates process-crash behavior, not removal of power
from the storage device. Power-loss claims require an environment such as a VM,
device-mapper fault target, or dedicated test machine that can discard volatile
device caches at controlled persistence points.

## Payment lifecycle and group commit

`go run ./cmd/ledgerdb-demo` runs an asserted real-subprocess crash walkthrough;
[review-demo.md](review-demo.md) includes the review script and local measurements.
The runner creates fresh datasets per mode and trial; setup is excluded. Report
WAL sync counts, average batch size, configured collection delay, and percentiles
alongside throughput. Default group settings are a 64-operation limit and 1 ms
window. Include workers=1 measurements when evaluating isolated-request latency.

The expanded suite exercises competing reservations, terminal retries after key
expiry, settle/cancel races, queued cancellation/backpressure, shutdown,
checkpoints under lifecycle load, v1 storage reads, v2 format rejection, batch
encoding before writes, shared syncs, rotation, torn prefixes, and uncertain
write/sync outcomes. Acknowledged operations must survive; unacknowledged batch
members may survive as a prefix and are resolved individually by retry.
