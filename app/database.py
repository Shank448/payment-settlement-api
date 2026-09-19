import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://settlement:settlement_pw@localhost:5432/settlement_db",
)

# pool_pre_ping avoids "server closed the connection" errors after idle periods
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    """FastAPI dependency: yields a DB session and always closes it."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create tables if they don't exist. Called on API startup."""
    from app import models  # noqa: F401 (registers models on Base.metadata)
    Base.metadata.create_all(bind=engine)
