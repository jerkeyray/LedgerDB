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

V1 exposes manual checkpoints. Schedule them according to acceptable restart
time and WAL disk usage. A practical starting point is after a fixed number of
mutations or during a low-traffic interval, then measure checkpoint latency on
the production storage stack.

## Backups

There is no online backup API in v1. For a consistent filesystem copy:

1. Stop mutations and close the database.
2. Copy the entire directory, including `MANIFEST`, checkpoint, `LOCK`, and WAL
   segments.
3. Reopen the original database.
4. Test the copied directory by opening it separately.

Do not copy individual files from a live directory and assume they form a valid
generation.

## Monitoring

LedgerDB does not yet export metrics. Applications should record mutation
latency, checkpoint duration, database-directory size, `ErrPoisoned`,
`ErrCorrupt`, and `ErrAlreadyOpen`. Treat any of the last three as an operational
event requiring investigation.

## Filesystem qualification

Before production use, record the OS/kernel, filesystem and mount options,
device, firmware, and virtualization layer. Run the crash suite and benchmark
protocol from [evaluation.md](evaluation.md) on that exact configuration.

