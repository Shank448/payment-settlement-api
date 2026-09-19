"""
Run manually after `docker compose up` to create two demo users + accounts:

    docker compose exec api python -m app.seed

Prints the two account IDs you'll need for testing /transfer.
"""
from app.database import SessionLocal, init_db
from app import models


def run():
    init_db()
    db = SessionLocal()
    try:
        alice = models.User(name="Alice", email="alice@example.com")
        bob = models.User(name="Bob", email="bob@example.com")
        db.add_all([alice, bob])
        db.commit()
        db.refresh(alice)
        db.refresh(bob)

        alice_acc = models.Account(user_id=alice.id, balance=1000, currency="INR")
        bob_acc = models.Account(user_id=bob.id, balance=500, currency="INR")
        db.add_all([alice_acc, bob_acc])
        db.commit()
        db.refresh(alice_acc)
        db.refresh(bob_acc)

        print(f"Alice account_id: {alice_acc.id} (balance: {alice_acc.balance})")
        print(f"Bob   account_id: {bob_acc.id} (balance: {bob_acc.balance})")
    finally:
        db.close()


if __name__ == "__main__":
    run()
