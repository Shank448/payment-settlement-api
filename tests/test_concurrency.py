"""
Proof-of-safety script (not a pytest unit test — a live demo against the
running stack). This is the artifact to screenshot/log for your resume
writeup and to talk through in interviews.

What it does:
  1. Creates an account with balance = 100.
  2. Fires TWO simultaneous transfers of 80 each out of that account
     (concurrently, via threads hitting the real HTTP API).
  3. Without locking, both could read balance=100, both see "sufficient
     funds", and both succeed -> balance goes to -60 (double-spend bug).
  4. With the worker's SELECT ... FOR UPDATE locking, the transfers are
     serialized: one COMPLETES, the other FAILS with "Insufficient funds",
     and the final balance is never negative.

Run after `docker compose up` and `python -m app.seed`:
    python tests/test_concurrency.py
"""
import time
import uuid
import threading
import requests

BASE_URL = "http://localhost:8000"


def create_test_accounts():
    user = requests.post(f"{BASE_URL}/users", json={
        "name": "Concurrency Test User",
        "email": f"race-{uuid.uuid4()}@example.com",
    }).json()

    source = requests.post(f"{BASE_URL}/accounts", json={
        "user_id": user["id"], "initial_balance": 100,
    }).json()

    sink_user = requests.post(f"{BASE_URL}/users", json={
        "name": "Sink User", "email": f"sink-{uuid.uuid4()}@example.com",
    }).json()
    sink = requests.post(f"{BASE_URL}/accounts", json={
        "user_id": sink_user["id"], "initial_balance": 0,
    }).json()

    return source["id"], sink["id"]


def fire_transfer(from_id, to_id, amount, results, index):
    resp = requests.post(f"{BASE_URL}/transfer", json={
        "from_account_id": from_id,
        "to_account_id": to_id,
        "amount": amount,
    })
    results[index] = resp.json()


def main():
    from_id, to_id = create_test_accounts()
    print(f"Source account: {from_id} (balance: 100)")
    print(f"Sink account:   {to_id} (balance: 0)")
    print("Firing two concurrent transfers of 80 each...")

    results = [None, None]
    t1 = threading.Thread(target=fire_transfer, args=(from_id, to_id, 80, results, 0))
    t2 = threading.Thread(target=fire_transfer, args=(from_id, to_id, 80, results, 1))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Give the async worker a moment to consume both Kafka events
    time.sleep(3)

    txn1_id = results[0]["id"]
    txn2_id = results[1]["id"]
    txn1 = requests.get(f"{BASE_URL}/transactions/{txn1_id}").json()
    txn2 = requests.get(f"{BASE_URL}/transactions/{txn2_id}").json()
    final_balance = requests.get(f"{BASE_URL}/accounts/{from_id}").json()["balance"]

    print(f"\nTransaction 1: {txn1['status']} ({txn1.get('failure_reason', '')})")
    print(f"Transaction 2: {txn2['status']} ({txn2.get('failure_reason', '')})")
    print(f"Final source balance: {final_balance}")

    statuses = {txn1["status"], txn2["status"]}
    if statuses == {"COMPLETED", "FAILED"} and float(final_balance) >= 0:
        print("\nPASS: exactly one transfer completed, balance never went negative.")
    else:
        print("\nFAIL: check locking logic — this indicates a double-spend occurred.")


if __name__ == "__main__":
    main()
