import os
import uuid
import logging
from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.database import get_db, init_db
from app import models, schemas
from app.kafka_producer import publish_payment_initiated

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("settlement_api")

app = FastAPI(title="Real-Time Event-Driven Payment Settlement API")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def serve_ui():
    """Serves the demo UI at http://localhost:8000/ — a visual front end
    for the API, purely for demoing/screenshotting; all real logic still
    lives in the JSON endpoints below."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="UI not built")


@app.on_event("startup")
def on_startup():
    init_db()


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- Users ----------

@app.post("/users", response_model=schemas.UserOut, status_code=201)
def create_user(payload: schemas.UserCreate, db: Session = Depends(get_db)):
    user = models.User(name=payload.name, email=payload.email)
    db.add(user)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Email already registered")
    db.refresh(user)
    return user


# ---------- Accounts ----------

@app.post("/accounts", response_model=schemas.AccountOut, status_code=201)
def create_account(payload: schemas.AccountCreate, db: Session = Depends(get_db)):
    user = db.get(models.User, payload.user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    account = models.Account(
        user_id=payload.user_id,
        balance=payload.initial_balance,
        currency=payload.currency,
    )
    db.add(account)
    db.commit()
    db.refresh(account)
    return account


@app.get("/accounts/{account_id}", response_model=schemas.AccountOut)
def get_account(account_id: str, db: Session = Depends(get_db)):
    account = db.get(models.Account, account_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return account


@app.get("/accounts", response_model=list[schemas.AccountWithOwner])
def list_accounts(db: Session = Depends(get_db)):
    """Used by the demo UI to populate account cards and the transfer dropdowns."""
    accounts = db.query(models.Account).join(models.User).all()
    return [
        schemas.AccountWithOwner(
            id=acc.id,
            user_id=acc.user_id,
            balance=acc.balance,
            currency=acc.currency,
            owner_name=acc.owner.name,
        )
        for acc in accounts
    ]


@app.post("/demo/seed", response_model=list[schemas.AccountWithOwner])
def demo_seed(db: Session = Depends(get_db)):
    """
    One-click demo data creator for the UI's 'Add demo users' button.
    Uses a random suffix on the email so it can be clicked repeatedly
    without hitting the unique-email constraint.
    """
    suffix = str(uuid.uuid4())[:8]
    alice = models.User(name="Alice", email=f"alice-{suffix}@example.com")
    bob = models.User(name="Bob", email=f"bob-{suffix}@example.com")
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

    return [
        schemas.AccountWithOwner(
            id=alice_acc.id, user_id=alice.id, balance=alice_acc.balance,
            currency=alice_acc.currency, owner_name=alice.name,
        ),
        schemas.AccountWithOwner(
            id=bob_acc.id, user_id=bob.id, balance=bob_acc.balance,
            currency=bob_acc.currency, owner_name=bob.name,
        ),
    ]


# ---------- Transfers ----------

@app.post("/transfer", response_model=schemas.TransactionOut, status_code=202)
def initiate_transfer(payload: schemas.TransferRequest, db: Session = Depends(get_db)):
    """
    Accepts a transfer request FAST: validates basic shape, writes a PENDING
    transaction row, publishes a Kafka event, and returns 202 Accepted.
    Actual money movement (with locking) happens asynchronously in the worker.
    This is what makes the system "event-driven" rather than a blocking RPC.
    """
    if payload.from_account_id == payload.to_account_id:
        raise HTTPException(status_code=400, detail="Cannot transfer to the same account")

    from_account = db.get(models.Account, payload.from_account_id)
    to_account = db.get(models.Account, payload.to_account_id)
    if not from_account or not to_account:
        raise HTTPException(status_code=404, detail="One or both accounts not found")

    # Idempotency: if this key was already used, return the existing transaction
    # instead of creating a duplicate (handles client retries on network blips).
    existing = (
        db.query(models.Transaction)
        .filter(models.Transaction.idempotency_key == payload.idempotency_key)
        .first()
    )
    if existing:
        return existing

    txn = models.Transaction(
        from_account_id=payload.from_account_id,
        to_account_id=payload.to_account_id,
        amount=payload.amount,
        status=models.TransactionStatus.PENDING,
        idempotency_key=payload.idempotency_key,
    )
    db.add(txn)
    db.commit()
    db.refresh(txn)

    # Publish only after commit succeeds — never emit an event for a row
    # that might not actually exist if the commit had failed.
    try:
        publish_payment_initiated(
            txn.id,
            {
                "transaction_id": txn.id,
                "from_account_id": txn.from_account_id,
                "to_account_id": txn.to_account_id,
                "amount": str(txn.amount),
                "idempotency_key": txn.idempotency_key,
            },
        )
    except Exception as e:
        # The row exists as PENDING even if the publish failed; a reconciliation
        # job (or manual replay) can pick up stuck PENDING rows later.
        logger.error(f"Event publish failed for txn {txn.id}: {e}")

    return txn


@app.get("/transactions/{transaction_id}", response_model=schemas.TransactionOut)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    txn = db.get(models.Transaction, transaction_id)
    if not txn:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return txn


@app.get("/transactions", response_model=list[schemas.TransactionOut])
def list_transactions(limit: int = 20, db: Session = Depends(get_db)):
    """Used by the demo UI's 'Recent activity' feed."""
    return (
        db.query(models.Transaction)
        .order_by(models.Transaction.created_at.desc())
        .limit(limit)
        .all()
    )
