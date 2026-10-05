# API guide

## Open

```go
func Open(path string, options Options) (*DB, error)
```

`Options` contains:

| Field | Default | Meaning |
| --- | --- | --- |
| `IdempotencyRetention` | 30 days | Exactly-once retry horizon; must not be negative |
| `WALSegmentBytes` | 64 MiB | Approximate rotation threshold |
| `FileSystem` | OS filesystem | Fault-injection/custom filesystem; must also implement `LockingFileSystem` |
| `Clock` | `time.Now` | Thread-safe time source used for leases; primarily intended for tests |
| `GroupCommitMaxBatch` | 1 | Operations per batch; valid range 1–1024, zero selects 1 |
| `GroupCommitDelay` | 0 | Maximum collection delay; nonnegative, only used for batches above 1 |
| `CheckpointInterval` | 0 (disabled) | Optional background checkpoint interval; nonnegative |
| `CheckpointOperations` | 0 (disabled) | Optional checkpoint threshold in committed WAL operations |

## Mutations

```go
func (db *DB) CreateAccount(ctx context.Context, key string, request CreateAccountRequest) (CreateAccountResult, error)
func (db *DB) Transfer(ctx context.Context, key string, request TransferRequest) (TransferResult, error)
func (db *DB) Reserve(ctx context.Context, key string, request ReserveRequest) (PaymentResult, error)
func (db *DB) Settle(ctx context.Context, key string, request SettleRequest) (PaymentResult, error)
func (db *DB) Cancel(ctx context.Context, key string, request CancelRequest) (PaymentResult, error)
```

Successful results contain `CommitLSN`. Transfer results also contain both
post-transfer balances. Validation and business-rule failures are not written
to the WAL and do not consume the idempotency key.

Context cancellation is checked before execution and while waiting for an
in-flight duplicate. Cancellation before admission or processing prevents execution. Once processing
starts, the owning call waits for the actual outcome, including after later
cancellation. A duplicate caller can still cancel its own wait independently.

## Reads and maintenance

```go
func (db *DB) GetAccount(ctx context.Context, id string) (Account, error)
func (db *DB) GetHold(ctx context.Context, id string) (Hold, error)
func (db *DB) Stats() Stats
func (db *DB) Checkpoint(ctx context.Context) error
func (db *DB) Close() error
```

Reads remain available after the writer is poisoned, reflecting the last state
applied in the current process. Checkpoints and mutations are rejected until
restart.

## Errors

Use `errors.Is` with the exported sentinel errors. Important operational errors:

| Error | Response |
| --- | --- |
| `ErrAlreadyOpen` | Close the other process or choose another directory |
| `ErrIdempotencyConflict` | Treat as a caller bug; do not retry with changed data |
| `ErrPoisoned` | Stop writes, close, reopen, and retry the original key |
| `ErrCorrupt` | Preserve files, stop operation, and investigate or restore |
| `ErrClosed` | Do not reuse the closed `DB` value |
| `ErrLSNExhausted` | Migrate to a new database; practical exhaustion is extraordinarily unlikely |

Domain errors include `ErrAccountExists`, `ErrAccountNotFound`,
`ErrInvalidAccountID`, `ErrInvalidCurrency`, `ErrInvalidAmount`,
`ErrInvalidOpeningBalance`, `ErrCurrencyMismatch`, `ErrInsufficientFunds`,
`ErrBalanceOverflow`, and `ErrSameAccount`.


Lifecycle errors include `ErrInvalidHoldID`, `ErrHoldExists`, `ErrHoldNotFound`,
and `ErrHoldConflict`. See [payments.md](payments.md) for request/result fields.
`Close` stops admission and background scheduling, drains accepted work, and
joins workers before releasing directory ownership. Repeated calls are safe.

`Stats` reports committed LSN, mutations/batches, WAL sync attempts and duration,
queue depth, duplicate retries, unexpired retained keys, active/terminal holds,
poison state, recovery duration, and checkpoint attempts/successes/last outcome.
Counters and durations are process-local; LSN, keys, and hold counts reflect
recovered/live state. Last checkpoint LSN is initialized from the loaded image;
last checkpoint time/error describe this process. These fields are sampled
independently, not a transactionally consistent balance snapshot. WAL sync
metrics exclude directory and checkpoint syncs. Terminal aliases count as WAL
mutations but do not move money.
