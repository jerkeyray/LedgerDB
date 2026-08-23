# LedgerDB storage design

## Directory ownership

`Open` durably creates missing directory components and then acquires an
exclusive advisory lock on `LOCK` before checkpoint loading, WAL replay, or tail
repair. The lock remains held until `Close`. This prevents two writers from
assigning the same LSN or one opener from truncating another writer's partial
append.

## Commit protocol

Mutations reserve their idempotency key, lock affected account shards in stable
order, validate against current balances, and enter the global WAL sequencer.
The sequencer assigns the next LSN, appends one complete record, and synchronizes
the WAL. Only then are balances and the completion record published in memory.

The sequencer is held through publication. This makes a checkpoint's captured
base LSN a barrier: every mutation at or below that LSN is present in both the
account state and idempotency index before checkpoint copying starts.

## WAL format

WAL files are named `wal-%020d.log` and rotate at 64 MiB by default. Each record
has a 16-byte binary envelope followed by JSON payload:

| Field | Size | Meaning |
| --- | ---: | --- |
| Magic | 4 bytes | `LDBW` |
| Version | 2 bytes | Format version 1 |
| Flags | 2 bytes | Reserved, currently zero |
| Payload length | 4 bytes | Little-endian byte count |
| CRC32 | 4 bytes | Checksum of the payload |
| Payload | Variable | Record type, LSN, timestamp, key, fingerprint, request, result |

Segment creation and deletion synchronize the database directory. Every append
handles short writes and synchronizes the active segment before success.
Record payloads are capped at 1 MiB before allocation. Segment filenames must
use the canonical zero-padded form.

## Fuzzy checkpoints

A checkpoint records the current committed LSN and then copies account and
idempotency shards independently. Every copied object includes its last LSN, so
the image may safely contain updates newer than the checkpoint base. Recovery
replays every WAL record after the base and applies it only to objects whose LSN
is older than the record.

Checkpoint and manifest files use length-and-CRC envelopes. Each is written to a
temporary file, synchronized, renamed, and followed by a directory sync. WAL
segments ending at or before the base are removed only after the manifest is
durable. An orphaned valid checkpoint is safe to discover after a crash.

Expired completion records are deleted from live idempotency shards
incrementally during commits and eagerly during checkpoint copying. This makes
the configured lease a memory-retention bound as well as an on-disk compaction
rule.

## Failure model

LedgerDB repairs physically incomplete final records. A complete record with an
invalid header, version, length, or checksum fails closed, even at the WAL tail.
It does not repair latent sector errors,
silent corruption of previously durable data, or hardware that acknowledges but
does not honor sync. A sync error is never retried in-process because operating
systems may discard dirty-page state after reporting the error.

The `internal/failpoint` filesystem can fail or shorten opens, writes, syncs,
truncates, closes, renames, removes, directory reads, and file reads. Tests use
it to explore bounded crash points and automatically check balance and
idempotency invariants after recovery.

The integration suite also runs mutations in a child process and exits abruptly
immediately before and after WAL write and sync operations. This validates
process-crash recovery; qualifying true power-loss behavior still requires the
target filesystem and device stack.
