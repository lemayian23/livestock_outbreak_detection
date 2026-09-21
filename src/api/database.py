"""
Database engine, session, and base model.
"""
import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """SQLAlchemy 2.0 declarative base."""
    pass


def _build_url() -> str:
    """Read DATABASE_URL from env. Fall back to SQLite for local dev."""
    url = os.getenv("DATABASE_URL")
    if url:
        # SQLAlchemy 2.0 wants postgresql+psycopg://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+psycopg://", 1)
        elif url.startswith("postgresql://") and "+psycopg" not in url:
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        return url
    return "sqlite:///./local_dev.db"


DATABASE_URL = _build_url()

# SQLite needs check_same_thread=False; Postgres doesn't accept that arg
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args=connect_args,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency yielding a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create tables (dev shortcut; prefer Alembic in prod)."""
    from . import models  # noqa: F401 — ensures models are registered
    Base.metadata.create_all(bind=engine)