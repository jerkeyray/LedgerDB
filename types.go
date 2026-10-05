package ledgerdb

import (
	"context"
	"time"
)

const DefaultIdempotencyRetention = 30 * 24 * time.Hour

// Options configures storage, retention, and test seams for Open.
type Options struct {
	// GroupCommitMaxBatch defaults to 1 (individual synchronization); maximum 1024.
	GroupCommitMaxBatch int
	// GroupCommitDelay bounds collection time after the first queued operation.
	GroupCommitDelay time.Duration
	// CheckpointInterval and CheckpointOperations are optional independent triggers.
	// Zero disables each trigger.
	CheckpointInterval   time.Duration
	CheckpointOperations uint64
	// IdempotencyRetention is the exactly-once retry horizon. Zero selects the
	// 30-day default; negative durations are rejected.
	IdempotencyRetention time.Duration
	// WALSegmentBytes is the approximate rotation threshold. Values less than
	// or equal to zero select the 64 MiB default.
	WALSegmentBytes int64
	// FileSystem overrides the production OS filesystem. Custom implementations
	// must also implement LockingFileSystem.
	FileSystem FileSystem
	// Clock overrides time.Now for deterministic lease tests; it must be thread-safe.
	Clock func() time.Time
}

// CreateAccountRequest describes a new account and its external opening funds.
type CreateAccountRequest struct {
	ID             string
	Currency       string
	OpeningBalance int64
}

// Account is an immutable snapshot returned by GetAccount or CreateAccount.
type Account struct {
	ID        string
	Currency  string
	Balance   int64
	Reserved  int64
	Available int64
	LSN       uint64
}

// CreateAccountResult is the stable response retained for idempotent retries.
type CreateAccountResult struct {
	Account   Account
	CommitLSN uint64
}

// TransferRequest moves a positive amount between same-currency accounts.
type TransferRequest struct {
	FromAccount string
	ToAccount   string
	Amount      int64
}

// TransferResult contains both post-transfer balances and the durable commit LSN.
type TransferResult struct {
	FromBalance int64
	ToBalance   int64
	CommitLSN   uint64
}

// DBAPI documents the context-aware public surface implemented by *DB.
type DBAPI interface {
	CreateAccount(context.Context, string, CreateAccountRequest) (CreateAccountResult, error)
	Transfer(context.Context, string, TransferRequest) (TransferResult, error)
	GetAccount(context.Context, string) (Account, error)
	Reserve(context.Context, string, ReserveRequest) (PaymentResult, error)
	Settle(context.Context, string, SettleRequest) (PaymentResult, error)
	Cancel(context.Context, string, CancelRequest) (PaymentResult, error)
	GetHold(context.Context, string) (Hold, error)
	Stats() Stats
	Checkpoint(context.Context) error
	Close() error
}

// ReserveRequest reserves Amount of the source account's funds for one payment.
type ReserveRequest struct {
	HoldID      string
	FromAccount string
	ToAccount   string
	Amount      int64
}
type SettleRequest struct {
	HoldID string
	Amount int64
}
type CancelRequest struct{ HoldID string }
type HoldStatus string

const (
	HoldReserved  HoldStatus = "reserved"
	HoldSettled   HoldStatus = "settled"
	HoldCancelled HoldStatus = "cancelled"
)

// Hold persists for the database lifetime, including its original terminal response.
type Hold struct {
	ID             string
	Request        ReserveRequest
	Status         HoldStatus
	SettledAmount  int64
	LSN            uint64
	TerminalResult *PaymentResult `json:",omitempty"`
}

// PaymentResult is the stable response for a lifecycle operation.
type PaymentResult struct {
	HoldID        string
	Status        HoldStatus
	SettledAmount int64
	FromBalance   int64
	FromReserved  int64
	ToBalance     int64
	CommitLSN     uint64
}

var _ DBAPI = (*DB)(nil)
