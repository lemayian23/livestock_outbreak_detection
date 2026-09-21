"""
SQLAlchemy models for users, auth, runs, anomalies, reports.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, Float, ForeignKey, Integer, String, Text, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), default="")
    organization: Mapped[str] = mapped_column(String(255), default="")
    role: Mapped[str] = mapped_column(String(20), default="user")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    last_login_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    api_keys: Mapped[list["ApiKey"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    runs: Mapped[list["Run"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    key_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    prefix: Mapped[str] = mapped_column(String(12), index=True)  # shown to user
    name: Mapped[str] = mapped_column(String(120), default="")
    last_used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    revoked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="api_keys")


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    records_submitted: Mapped[int] = mapped_column(Integer, default=0)
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    anomalies_detected: Mapped[int] = mapped_column(Integer, default=0)
    quality_score: Mapped[float] = mapped_column(Float, nullable=True)
    duration_ms: Mapped[float] = mapped_column(Float, nullable=True)
    error_message: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, index=True)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="runs")
    anomalies: Mapped[list["Anomaly"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    reports: Mapped[list["Report"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class Anomaly(Base):
    __tablename__ = "anomalies"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    farm_id: Mapped[str] = mapped_column(String(64), nullable=True)
    animal_type: Mapped[str] = mapped_column(String(32), nullable=True)
    date: Mapped[str] = mapped_column(String(32), nullable=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=True)
    score: Mapped[float] = mapped_column(Float, nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    run: Mapped["Run"] = relationship(back_populates="anomalies")


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    format: Mapped[str] = mapped_column(String(16))
    storage_path: Mapped[str] = mapped_column(String(512), nullable=True)
    size_bytes: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    run: Mapped["Run"] = relationship(back_populates="reports")


# Helpful index for "show me this user's runs, newest first"
Index("ix_runs_user_created", Run.user_id, Run.created_at.desc())