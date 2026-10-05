# Crash-safe payment lifecycles

Use a hold when the final charge is known only after service completes. A campus
charging backend can reserve ₹500, settle a ₹180 bill, and release ₹320 in one
operation. The application supplies the bill; LedgerDB only accounts for funds.

```go
// Existing INR accounts: driver has 100000 minor units; operator has zero.
r, err := db.Reserve(ctx, "reserve:session-42", ledgerdb.ReserveRequest{
    HoldID: "session-42", FromAccount: "driver", ToAccount: "operator", Amount: 50000,
})
// Check err before continuing.
_ = r
settled, err := db.Settle(ctx, "settle:session-42", ledgerdb.SettleRequest{
    HoldID: "session-42", Amount: 18000,
})
_ = settled
// Alternatively, cancel a still-reserved session:
// db.Cancel(ctx, "cancel:session-42", ledgerdb.CancelRequest{HoldID: "session-42"})
```

Account `Balance` includes reservations. `Reserved` is unavailable to other
transfers, and `Available = Balance - Reserved`. All amounts are integer minor
units. Reserve requires a positive amount and existing, distinct, same-currency
accounts; insufficient available funds are rejected. A destination overflow is
checked at settlement, not reservation, since its balance may change meanwhile.

A hold is `reserved`, then either `settled` or `cancelled`. Settlement allows
zero through the reserved maximum, releases the full reservation, and transfers
only the actual amount. There is no partial capture followed by a second capture:
settlement is terminal. Cancellation transfers nothing. There is no expiry timer.

`PaymentResult` records hold ID/status, settled amount, source total/reserved,
destination balance, and original commit LSN. `GetHold` returns an immutable copy
with original reservation request, terminal outcome, and terminal result.
Balances in a retry response are historical, not current; use `GetAccount` for
current balances.

Same-key retries return their saved response during the configured lease;
changed requests conflict. After key expiry, retained terminal state still
blocks a second financial effect. Matching settlement (same actual amount) or
cancellation returns the original terminal response, even under a different key.
A new key is durably recorded as an alias to that response, with a new WAL LSN
but the original result's `CommitLSN`; it does not change account or hold state.
Conflicting terminal actions return `ErrHoldConflict`, and an existing hold ID
cannot be reserved again (`ErrHoldExists`).

Retain the original reservation key to retry reservation while its lease is
active. After expiry, inspect `GetHold` rather than recreating the reservation.
Terminal holds persist indefinitely, including through checkpoint compaction;
this prevents duplicate effects but increases memory and storage requirements.

Each operation has one checksummed WAL record containing post-operation state
and its response. Group commit shares disk barriers; independent operations in
a batch can recover as a prefix. On uncertain I/O, close/reopen and retry.
