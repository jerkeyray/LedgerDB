# Operations

## Directory ownership

Give each LedgerDB instance a dedicated local directory. `Open` creates a
`LOCK` file and holds an exclusive advisory lock until `Close`. Do not place a
live database on a filesystem that lacks reliable file locking, atomic rename,
file sync, or directory sync semantics.

The current lock implementation supports macOS, Linux, and major BSD systems.
Other platforms return `ErrLockUnsupported` until a native locking backend is
implemented.

## Checkpoint policy

Manual `Checkpoint(ctx)` remains available. Optional `CheckpointInterval` and
`CheckpointOperations` schedule a background checkpoint when either trigger
fires. Both default to disabled; the demo enables 30 seconds or 1,000 committed
operations. Failed checkpoints are reported in `Stats` and retried on a later
trigger; they do not poison an otherwise healthy writer. Operation-based retries
wait for another threshold of operations after the last attempt. `Close` stops
scheduling and waits for an in-progress checkpoint. Measure checkpoint latency
on the production storage stack.

## Backups

There is no online backup API. For a consistent filesystem copy:

1. Stop mutations and close the database.
2. Copy the entire directory, including `MANIFEST`, checkpoint, `LOCK`, and WAL
   segments.
3. Reopen the original database.
4. Test the copied directory by opening it separately.

Do not copy individual files from a live directory and assume they form a valid
generation.

## Monitoring

`Stats()` exports queue, batching, WAL synchronization, retained-key, hold,
recovery, and checkpoint metrics. Applications should additionally record
request latency and database-directory size. Monitor checkpoint failures,
terminal-hold growth, retained-key pressure, `ErrPoisoned`, `ErrCorrupt`, and
`ErrAlreadyOpen`. Treat any of the last three as an operational
event requiring investigation.

## Filesystem qualification

Before production use, record the OS/kernel, filesystem and mount options,
device, firmware, and virtualization layer. Run the crash suite and benchmark
protocol from [evaluation.md](evaluation.md) on that exact configuration.


## Retention and batching

Choose `IdempotencyRetention` from the caller's maximum retry horizon, not the
30-day default alone. At 1 operation/second, 30 days retains 2.592 million keys;
the historical v1 estimate of 233.2 bytes/key implies about 604 MB of live heap.
New records and hold state have additional costs; measure your workload.
Zero selects the default, and negative durations are invalid; there is no
infinite-key-retention option.

Terminal holds are retained indefinitely. Account for their growing live memory
and checkpoint size using `Stats().TerminalHolds`; key expiry does not remove
holds or their original terminal response. This release has no archival API.

Group commit is opt-in. Start with `GroupCommitMaxBatch: 64` and
`GroupCommitDelay: time.Millisecond`, then measure throughput and tail latency.
The bounded 1,024-operation queue applies backpressure. A single isolated request
can pay the collection delay; grouping benefits concurrent workloads. A batch
spanning segments syncs each touched segment, plus directory synchronization on
rotation. Successful operations always wait for their batch's barriers.

## Format upgrades

The current reader accepts storage versions 1 and 2; new records, checkpoints,
and manifests use version 2. Version-1 accounts have zero reserved funds. A
read-only open does not rewrite existing records; the first new write or
checkpoint introduces v2. Old binaries reject these versions. Make a closed
backup before upgrading; downgrading a modified directory is unsupported.
