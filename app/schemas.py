import uuid
from decimal import Decimal
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, EmailStr


class UserCreate(BaseModel):
    name: str
    email: EmailStr


class UserOut(BaseModel):
    id: str
    name: str
    email: str

    class Config:
        from_attributes = True


class AccountCreate(BaseModel):
    user_id: str
    initial_balance: Decimal = Decimal("0.00")
    currency: str = "INR"


class AccountOut(BaseModel):
    id: str
    user_id: str
    balance: Decimal
    currency: str

    class Config:
        from_attributes = True


class AccountWithOwner(AccountOut):
    owner_name: str


class TransferRequest(BaseModel):
    from_account_id: str
    to_account_id: str
    amount: Decimal = Field(gt=0, description="Must be positive")
    # Client should generate this once and reuse it on retries so a retried
    # HTTP call never creates a duplicate transfer.
    idempotency_key: str = Field(default_factory=lambda: str(uuid.uuid4()))


class TransactionOut(BaseModel):
    id: str
    from_account_id: str
    to_account_id: str
    amount: Decimal
    status: str
    failure_reason: Optional[str] = None
    created_at: datetime
    settled_at: Optional[datetime] = None

    class Config:
        from_attributes = True
