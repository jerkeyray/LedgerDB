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
| `Clock` | `time.Now` | Time source used for leases; primarily intended for tests |

## Mutations

```go
func (db *DB) CreateAccount(ctx context.Context, key string, request CreateAccountRequest) (CreateAccountResult, error)
func (db *DB) Transfer(ctx context.Context, key string, request TransferRequest) (TransferResult, error)
```

Successful results contain `CommitLSN`. Transfer results also contain both
post-transfer balances. Validation and business-rule failures are not written
to the WAL and do not consume the idempotency key.

Context cancellation is checked before execution and while waiting for an
in-flight duplicate. Once a request owns its key and begins commit processing,
the operation may finish despite later cancellation; retry its key to resolve
the result.

## Reads and maintenance

```go
func (db *DB) GetAccount(ctx context.Context, id string) (Account, error)
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

