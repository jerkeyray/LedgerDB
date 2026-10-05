# LedgerDB documentation

Start with [Getting started](getting-started.md), then use the focused guides
below as the project reference.

| Guide | Use it for |
| --- | --- |
| [Getting started](getting-started.md) | Opening a database, creating accounts, transferring funds, and retrying safely |
| [Core concepts](concepts.md) | The transaction model, LSNs, idempotency leases, locking, and checkpoints |
| [Payment lifecycles](payments.md) | Reservations, partial settlement, cancellation, and terminal retries |
| [Review demo](review-demo.md) | The process-crash walkthrough and batching measurements |
| [API guide](api.md) | Public methods, options, types, errors, and context behavior |
| [Durability and recovery](durability.md) | Commit ordering, WAL records, restart behavior, corruption, and I/O failures |
| [Operations](operations.md) | Directory ownership, checkpoint policy, backups, observability, and deployment limits |
| [Evaluation](evaluation.md) | Reproducible tests and benchmark protocol |
| [Research and status](research-status.md) | Problem statement, related-work positioning, implementation status, and measured benchmark results |
| [Storage design](../DESIGN.md) | Internal implementation and file-format details |

LedgerDB is beta software. The documentation describes the current v2 format, with v1 read support. Downgrading after v2 writes
is unsupported.

