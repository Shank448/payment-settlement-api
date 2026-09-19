import os
import json
import logging
from decimal import Decimal

from kafka import KafkaConsumer
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal, init_db
from app import models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement_worker")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PAYMENT_INITIATED = "payment.initiated"


def settle_transaction(transaction_id: str) -> None:
    """
    The core money-safety logic.

    Key ideas:
    - Lock both account rows with SELECT ... FOR UPDATE, always in a fixed
      order (lower account id first) to prevent deadlocks when two transfers
      target the same pair of accounts in opposite directions.
    - Everything happens in a single DB transaction: either both balances
      update and the transaction is marked COMPLETED, or nothing changes.
    - Re-checking transaction.status inside the lock protects against a
      redelivered Kafka message settling the same transfer twice.
    """
    db = SessionLocal()
    try:
        txn = db.get(models.Transaction, transaction_id)
        if txn is None:
            logger.warning(f"Transaction {transaction_id} not found, skipping")
            return

        if txn.status != models.TransactionStatus.PENDING:
            # Already settled by a previous delivery of this event — no-op.
            logger.info(f"Transaction {transaction_id} already {txn.status}, skipping")
            return

        # Lock account rows in a deterministic order (by id) to avoid deadlocks
        # between two concurrent transfers that touch the same two accounts.
        account_ids = sorted([txn.from_account_id, txn.to_account_id])
        locked_accounts = {}
        for acc_id in account_ids:
            row = db.execute(
                text('SELECT id, balance FROM accounts WHERE id = :id FOR UPDATE'),
                {"id": acc_id},
            ).first()
            locked_accounts[acc_id] = row

        from_row = locked_accounts[txn.from_account_id]
        to_row = locked_accounts[txn.to_account_id]

        if from_row is None or to_row is None:
            txn.status = models.TransactionStatus.FAILED
            txn.failure_reason = "Account not found at settlement time"
            db.commit()
            return

        from_balance = Decimal(from_row.balance)
        if from_balance < txn.amount:
            txn.status = models.TransactionStatus.FAILED
            txn.failure_reason = "Insufficient funds"
            db.commit()
            logger.info(f"Transaction {transaction_id} FAILED: insufficient funds")
            return

        # Both rows are locked — safe to update. No other transaction can
        # read-modify-write these balances until this transaction commits.
        db.execute(
            text("UPDATE accounts SET balance = balance - :amt WHERE id = :id"),
            {"amt": txn.amount, "id": txn.from_account_id},
        )
        db.execute(
            text("UPDATE accounts SET balance = balance + :amt WHERE id = :id"),
            {"amt": txn.amount, "id": txn.to_account_id},
        )

        txn.status = models.TransactionStatus.COMPLETED
        from sqlalchemy.sql import func
        txn.settled_at = func.now()

        db.commit()
        logger.info(f"Transaction {transaction_id} COMPLETED")

    except IntegrityError as e:
        db.rollback()
        logger.error(f"Integrity error settling {transaction_id}: {e}")
    except Exception as e:
        db.rollback()
        logger.exception(f"Unexpected error settling {transaction_id}: {e}")
    finally:
        db.close()


def main():
    init_db()
    consumer = KafkaConsumer(
        TOPIC_PAYMENT_INITIATED,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="settlement-workers",  # consumer group -> can scale to N workers
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    logger.info("Settlement worker started, listening for payment.initiated events...")
    for message in consumer:
        event = message.value
        transaction_id = event["transaction_id"]
        logger.info(f"Received settlement event for transaction {transaction_id}")
        settle_transaction(transaction_id)


if __name__ == "__main__":
    main()
