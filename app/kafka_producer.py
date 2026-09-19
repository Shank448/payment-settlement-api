import os
import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger("kafka_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PAYMENT_INITIATED = "payment.initiated"

_producer = None


def get_producer() -> KafkaProducer:
    """Lazily create a singleton producer so FastAPI doesn't reconnect per-request."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # wait for full ISR ack — don't lose a payment event
            retries=5,
        )
    return _producer


def publish_payment_initiated(transaction_id: str, payload: dict) -> None:
    """
    Publish AFTER the DB commit succeeds, never before — otherwise a worker
    could try to settle a transaction row that doesn't exist yet (or never will).
    """
    producer = get_producer()
    try:
        # Keying by transaction_id keeps all events for one transaction
        # on the same partition, preserving order if you ever emit more
        # than one event per transaction.
        future = producer.send(TOPIC_PAYMENT_INITIATED, key=transaction_id, value=payload)
        future.get(timeout=10)  # block briefly to surface errors synchronously
    except KafkaError as e:
        logger.error(f"Failed to publish event for transaction {transaction_id}: {e}")
        raise
