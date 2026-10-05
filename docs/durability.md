# Durability and recovery

## Commit ordering

LedgerDB reserves the idempotency key, locks affected accounts, validates the
queued operations against staged state, assigns each accepted operation its own
LSN, appends independently checksummed records, and calls `File.Sync` once per
touched segment. Only after every required sync succeeds does it publish account
and hold state and responses. The default batch size is one.

`File.Sync` is Go's portable full-sync operation. A successful return means the
operating system reported that the WAL file was synchronized. Segment creation,
segment deletion, checkpoint rename, and database-directory creation also sync
the relevant directories.

## Recovery

`Open` acquires the exclusive directory lock before reading or repairing files.
It installs the manifest checkpoint, then replays WAL records newer than the
checkpoint base in strict LSN order. Per-account, per-hold, and per-key LSNs prevent double application
when a fuzzy checkpoint already includes a newer mutation. After replay,
recovery verifies that active hold totals match reserved balances and terminal
hold states/results agree. The new reader accepts v1 and v2; new writes are v2.

A physically incomplete final header or payload is an uncommitted torn tail and
is truncated and synchronized. A complete record with an invalid header, CRC,
version, shape, or LSN fails closed with `ErrCorrupt`; LedgerDB does not silently
discard it.

## Checkpoints

Checkpoint copying proceeds shard by shard while transfers continue. The
checkpoint and manifest each use a length-and-CRC envelope and are published by
temporary write, file sync, atomic rename, and directory sync. WAL compaction
starts only after the new manifest is durable.

## I/O errors

Any WAL write or sync failure makes the durable outcome uncertain. LedgerDB
marks the handle poisoned and never retries sync in-process. Restart recovery
determines which complete records exist. A group can recover as a prefix; it is
not an atomic multi-operation transaction. Retry each original key to resolve
its outcome. Terminal hold state additionally prevents a repeated settlement or
cancellation even after the request key expires.

## Threat model

LedgerDB detects torn writes and checksum mismatches. It does not correct latent
sector errors, malicious file changes, broken device firmware, or hardware that
acknowledges but ignores sync. CRC32 detects corruption; it is not a
cryptographic integrity mechanism.

