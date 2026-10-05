# LedgerDB storage design

## Directory ownership and versions

`Open` durably creates missing directory components and acquires an exclusive
advisory `LOCK` before checkpoint load, replay, or repair. The lock lasts until
`Close`. One process owns the directory; there is no replication.

Readers accept WAL envelopes, checkpoints, and manifests with versions 1 and 2.
New writes use v2. Legacy account states imply zero reservations. Old binaries
reject v2; downgrade after a write or checkpoint is unsupported.

## Commit coordinator

Mutation callers validate request shape, reserve an idempotency key, and enter a
bounded queue (1,024). Matching duplicates rendezvous on the key owner. No caller
holds account locks while waiting. The coordinator collects up to the configured
batch size within its delay; default size one preserves individual synchronization.

The coordinator identifies affected account shards, locks them in ascending
order, then locks hold state and the commit barrier. Operations validate in queue
order against staged account/hold overlays. Invalid operations do not change the
overlay or consume an LSN. Valid operations get one LSN and one record each.

All records are encoded before writing. Each touched WAL segment is synchronized;
rotation synchronizes the preceding dirty segment before closing it and creates
and directory-syncs the next. Only after all barriers succeed are account state,
holds, idempotency results, and success responses published. The commit barrier
is held through publication. A batch is not an atomic transaction: crash recovery
may retain a prefix, and callers resolve uncertain outcomes by retrying keys.

WAL write/sync failure discards staged state, poisons the handle, and wakes batch
and queued callers. No in-process retry is attempted. Cancellation before
processing releases the key and prevents execution; after processing starts the
owner waits for the real outcome. `Close` stops admission, stops/joins background
checkpointing, drains accepted operations, joins the coordinator, then closes
storage and releases directory ownership.

## Payment state

An account's total balance includes reserved funds. Available funds equal total
minus reserved; ordinary transfers validate availability. Holds move once from
reserved to settled/cancelled. Settlement transfers the actual amount and releases
the entire reservation, atomically with the hold transition and retry result.
Terminal results persist with holds indefinitely. A terminal retry under a new
key appends an idempotency alias without changing financial state, returning the
original terminal LSN/result. Key expiry does not permit hold reuse.

## WAL format

Segments are `wal-%020d.log` and rotate at 64 MiB by default. Records have a
16-byte envelope: magic `LDBW` (4 bytes), little-endian version (2), zero flags
(2), payload length (4), CRC32 of payload (4), followed by JSON.

The JSON contains operation type, LSN, timestamp, key, fingerprint, request,
response, and (for coordinator records) affected post-operation account/hold
snapshots. Payloads are capped at 1 MiB before allocation. LSNs must increase in
replay order. Each object applies only records newer than its own LSN.

## Fuzzy checkpoints and maintenance

Checkpoint capture reads a committed base LSN under the commit barrier, then
copies accounts, holds, and completed keys independently. Immutable terminal
responses can be shared during copying; live mutations replace values. Recovery
replays records after the base, skipping already-newer objects. After full replay,
account reservations must match active holds and terminal results must agree.

Checkpoint and manifest envelopes have length and CRC fields. Each is published
by temp-file write, sync, rename, and directory sync; covered inactive WAL segments
are removed only after manifest publication. An orphan valid checkpoint can be
used when no manifest exists, because compaction cannot yet have occurred.

Expired completed keys are pruned incrementally during commits and eagerly during
checkpointing. Holds are never pruned. Optional interval/operation triggers run
one background checkpointer; failure is observable and retried on a later trigger.
Stats counters are process-local except recovered/live LSN, key, and hold counts.

## Failure model

Physically incomplete final records are truncated and synchronized. Complete
records with bad headers/checksums or inconsistent recovered state fail closed.
CRC32 detects corruption; it cannot repair it or resist malicious modification.
Process crashes and injected I/O failures are tested, including shared-sync
batches and payment transitions. Power-loss qualification remains dependent on
the target filesystem, firmware, hardware, and synchronization semantics.
