package ledgerdb

import "errors"

var (
	ErrClosed                = errors.New("ledgerdb: database is closed")
	ErrAlreadyOpen           = errors.New("ledgerdb: database is already open by another process")
	ErrLockUnsupported       = errors.New("ledgerdb: filesystem does not support exclusive locking")
	ErrPoisoned              = errors.New("ledgerdb: database is poisoned; close and reopen it")
	ErrAccountExists         = errors.New("ledgerdb: account already exists")
	ErrAccountNotFound       = errors.New("ledgerdb: account not found")
	ErrInvalidAccountID      = errors.New("ledgerdb: account ID must be valid UTF-8 and 1 to 128 bytes")
	ErrInvalidCurrency       = errors.New("ledgerdb: currency must be three uppercase ASCII letters")
	ErrInvalidAmount         = errors.New("ledgerdb: amount must be positive")
	ErrInvalidOpeningBalance = errors.New("ledgerdb: opening balance must be nonnegative")
	ErrCurrencyMismatch      = errors.New("ledgerdb: account currencies do not match")
	ErrInsufficientFunds     = errors.New("ledgerdb: insufficient funds")
	ErrBalanceOverflow       = errors.New("ledgerdb: balance overflow")
	ErrSameAccount           = errors.New("ledgerdb: source and destination accounts must differ")
	ErrInvalidIdempotencyKey = errors.New("ledgerdb: idempotency key must be valid UTF-8 and 1 to 256 bytes")
	ErrIdempotencyConflict   = errors.New("ledgerdb: idempotency key was already used for another request")
	ErrCorrupt               = errors.New("ledgerdb: corrupt storage")
	ErrLSNExhausted          = errors.New("ledgerdb: LSN space exhausted")
)
