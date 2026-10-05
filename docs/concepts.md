# Core concepts

## Accounts and amounts

An account has a caller-supplied ID, a three-letter currency identifier, a
nonnegative total balance, reserved funds, available funds, and the LSN of its last mutation. Amounts use `int64` minor
units; floating-point values never enter the storage format.

Opening balances are explicit external funding. They are not paired with a
second account. Applications needing strict double-entry conservation should
create a source account and fund other accounts through transfers.

## One-record transactions

An operation is one account creation, transfer, reservation, settlement, or cancellation. The request, resulting balances, idempotency key, response, and commit
LSN fit in one WAL record. Account and hold state is applied only after its batch syncs.
That bound is why recovery needs redo but no undo pass.

## Idempotency leases

The first invocation reserves its key. Matching concurrent invocations wait for
that owner. After commit, retries receive the stored response. A different
request fingerprint returns `ErrIdempotencyConflict`.

The default lease is 30 days from commit. Expired entries are pruned
incrementally during writes and eagerly during checkpoints. Reusing an expired
key is a new operation, so callers must choose a retention period longer than
their maximum retry horizon.

## LSNs

Every durable mutation receives a strictly increasing log sequence number.
Accounts, idempotency entries, checkpoints, and recovery records carry LSNs so
fuzzy snapshots can safely contain state observed at different moments.

## Concurrency

Account and idempotency maps are divided into 64 shards. Transfers lock account
shards in numeric order, preventing lock-order cycles. The bounded commit coordinator defines
the global commit order, staging operations in queue order and optionally sharing
WAL syncs. Each record remains an independent recovery boundary. An exclusive directory lock prevents a second process
from replaying, truncating, or appending the same WAL concurrently.


## Payment holds

`Balance` is total owned funds, `Reserved` is the sum of active holds, and
`Available = Balance - Reserved`. Reservation reduces availability without moving
funds. Settlement moves the actual amount and releases the full reservation;
cancellation releases it without a transfer. These transitions and their retry
results commit together. A hold has one terminal outcome, retained independently
of expiring request keys. See [payments.md](payments.md).
