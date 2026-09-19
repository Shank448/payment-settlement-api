# Real-Time Event-Driven Payment Settlement API

A simulation of the core architecture behind fintech settlement systems
(Razorpay/PhonePe-style): an API that accepts transfers instantly, and an
independent async worker that settles them safely — with row-level locking
to make double-spending structurally impossible, not just unlikely.

## Architecture

```
Client
  |
  |  POST /transfer
  v
FastAPI (app/main.py)
  |  1. validates request
  |  2. writes a PENDING transaction row (Postgres)
  |  3. publishes payment.initiated event (Kafka)
  |  4. returns 202 Accepted immediately
  v
Kafka topic: payment.initiated
  v
Settlement Worker (worker/consumer.py)
  |  1. consumes event
  |  2. SELECT ... FOR UPDATE on both accounts (locked, fixed order)
  |  3. checks balance, checks idempotency (status still PENDING?)
  |  4. debits/credits atomically, marks COMPLETED or FAILED
  v
Client polls GET /transactions/{id} for final status
```

**Why the API and worker are separate processes:** the API's only job is to
durably record intent and return fast. The worker does the actual money
movement. This decoupling is what "event-driven" buys you — the API stays
responsive under load even if settlement briefly backs up, and you can scale
worker replicas independently (Kafka consumer groups handle that for free).

## Key safety mechanisms

| Mechanism | Prevents | Where |
|---|---|---|
| `SELECT ... FOR UPDATE` on both accounts | Two concurrent transfers reading the same stale balance and both succeeding (double-spend) | `worker/consumer.py::settle_transaction` |
| Fixed lock ordering (sort account IDs) | Deadlock when two transfers target the same pair of accounts in opposite directions | `worker/consumer.py::settle_transaction` |
| `idempotency_key` on transactions | A retried HTTP request creating a duplicate transfer | `app/main.py::initiate_transfer` |
| Re-check `status == PENDING` before settling | A redelivered Kafka message (at-least-once delivery) settling the same transaction twice | `worker/consumer.py::settle_transaction` |
| `Numeric(14,2)` columns, never `float` | Floating-point rounding errors in money math | `app/models.py` |
| Publish event only after DB commit succeeds | A "phantom" event referencing a transaction that doesn't actually exist | `app/main.py::initiate_transfer` |

## Running it

```bash
docker compose up --build
```

Then seed demo accounts:

```bash
docker compose exec api python -m app.seed
```

This prints two account IDs. Use them to test:

```bash
curl -X POST http://localhost:8000/transfer \
  -H "Content-Type: application/json" \
  -d '{"from_account_id": "<alice_id>", "to_account_id": "<bob_id>", "amount": 100}'

curl http://localhost:8000/transactions/<transaction_id>
```

## Proving the double-spend fix actually works

```bash
pip install requests
python tests/test_concurrency.py
```

This fires two simultaneous 80-unit transfers out of a 100-unit balance.
Without locking, both could succeed and push the balance negative. With the
locking in place, exactly one completes, the other fails with "Insufficient
funds", and the balance never goes negative. **Screenshot this output** —
it's your proof piece for interviews and the resume writeup.

## API reference

| Endpoint | Method | Purpose |
|---|---|---|
| `/users` | POST | Create a user |
| `/accounts` | POST | Create an account for a user |
| `/accounts/{id}` | GET | Check balance |
| `/transfer` | POST | Initiate a transfer (202 Accepted, async) |
| `/transactions/{id}` | GET | Poll settlement status |

## Tech stack

Python · FastAPI · PostgreSQL · Apache Kafka (KRaft mode, no Zookeeper) · Docker Compose

## Suggested resume bullet points

- Built an event-driven payment settlement system (FastAPI, PostgreSQL,
  Kafka, Docker) that decouples transfer intake from settlement, keeping the
  API responsive under load while a Kafka-consumer worker pool settles
  transactions asynchronously.
- Implemented pessimistic row-level locking (`SELECT ... FOR UPDATE`) with
  deterministic lock ordering to eliminate double-spend and deadlock risk
  under concurrent transfers targeting the same accounts; verified with a
  concurrency test firing simultaneous transfer requests.
- Designed idempotent transfer handling (client-supplied idempotency keys,
  status re-checks on event redelivery) to make the system safe against
  network retries and Kafka's at-least-once delivery semantics.

## Possible extensions (mention these in an interview even if you don't build them)

- A reconciliation job that scans for PENDING transactions older than N
  minutes (covers the case where the Kafka publish failed after DB commit).
- A dead-letter topic for transactions that fail settlement repeatedly.
- Horizontal scaling: run multiple worker replicas in the same consumer
  group — Kafka partitions the `payment.initiated` topic across them
  automatically.
- Webhook callbacks instead of polling `/transactions/{id}`.
