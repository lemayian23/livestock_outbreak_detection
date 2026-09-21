"""
API key management endpoints.
"""
import hashlib
import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from ..auth.dependencies import get_current_user
from ..database import get_db
from ..models import ApiKey, User
from ..schemas_auth import ApiKeyCreate, ApiKeyCreated, ApiKeyOut


router = APIRouter(prefix="/api-keys", tags=["api-keys"])


def _hash(plain: str) -> str:
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def _out(k: ApiKey) -> ApiKeyOut:
    return ApiKeyOut(
        id=k.id,
        name=k.name or "",
        prefix=k.prefix,
        created_at=k.created_at.isoformat() if k.created_at else "",
        last_used_at=k.last_used_at.isoformat() if k.last_used_at else None,
        revoked_at=k.revoked_at.isoformat() if k.revoked_at else None,
    )


@router.get("", response_model=list[ApiKeyOut])
def list_keys(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    keys = (
        db.query(ApiKey)
        .filter(ApiKey.user_id == current_user.id)
        .order_by(ApiKey.created_at.desc())
        .all()
    )
    return [_out(k) for k in keys]


@router.post("", response_model=ApiKeyCreated, status_code=201)
def create_key(
    payload: ApiKeyCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a new API key. Plaintext is shown ONCE."""
    raw = "lvk_" + secrets.token_urlsafe(32)
    prefix = raw[:12]
    record = ApiKey(
        user_id=current_user.id,
        key_hash=_hash(raw),
        prefix=prefix,
        name=payload.name or "default",
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return ApiKeyCreated(
        id=record.id,
        name=record.name,
        prefix=record.prefix,
        key=raw,
    )


@router.delete("/{key_id}", status_code=204)
def revoke_key(
    key_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    k = (
        db.query(ApiKey)
        .filter(ApiKey.id == key_id, ApiKey.user_id == current_user.id)
        .first()
    )
    if not k:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "API key not found", "code": "KEY_NOT_FOUND"},
        )
    k.revoked_at = datetime.utcnow()
    db.commit()
    return None