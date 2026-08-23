# Getting started

## Requirements

- Go 1.24 or newer.
- macOS, Linux, or a supported BSD for exclusive file locking.
- A local filesystem that honors file and directory synchronization.

Install the module:

```bash
go get github.com/jerkeyray/ledgerdb
```

## Open and close

```go
db, err := ledgerdb.Open("./ledger-data", ledgerdb.Options{})
if err != nil {
	return err
}
defer db.Close()
```

`Open` creates missing directory components durably, acquires an exclusive
`LOCK` file, loads the current checkpoint, repairs a physically incomplete WAL
tail, and replays later records. A second process receives `ErrAlreadyOpen`.

Always check the error returned by `Close`; it releases both the active WAL file
and the directory lock.

## Create accounts

Every mutation needs a caller-chosen idempotency key:

```go
created, err := db.CreateAccount(ctx, "account:cash", ledgerdb.CreateAccountRequest{
	ID:             "cash",
	Currency:       "USD",
	OpeningBalance: 50_00,
})
```

Amounts are minor units, so `50_00` represents USD 50.00. Account IDs and keys
must be valid UTF-8. Currency identifiers must contain exactly three uppercase
ASCII letters.

## Transfer funds

```go
result, err := db.Transfer(ctx, "payment:invoice-42", ledgerdb.TransferRequest{
	FromAccount: "cash",
	ToAccount:   "revenue",
	Amount:      12_50,
})
```

Transfers reject missing accounts, cross-currency movement, nonpositive amounts,
overdrafts, destination overflow, and transfers to the same account.

## Retry correctly

If the caller loses the response, repeat the exact request with the exact key.
Within the retention lease, LedgerDB returns the original result. Never create a
new key merely because a request timed out—that can execute a second transfer.

If a mutation returns `ErrPoisoned`, stop issuing writes, close the database,
reopen it, and retry the uncertain operation with its original key.

## Checkpoint

```go
if err := db.Checkpoint(ctx); err != nil {
	return err
}
```

Checkpointing bounds recovery work and compacts fully covered WAL segments. It
does not stop transfers globally, though copying and filesystem I/O can affect
latency.

