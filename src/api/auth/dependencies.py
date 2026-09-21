"""
FastAPI auth dependencies: current_user, optional API key.
"""
import hashlib
from datetime import datetime, timezone
from typing import Optional

from fastapi import Cookie, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from ..database import get_db
from ..models import ApiKey, User
from .jwt import JWTError, decode_token


def _hash_api_key(plain: str) -> str:
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def _user_from_jwt(token: str, db: Session) -> Optional[User]:
    try:
        payload = decode_token(token)
    except JWTError:
        return None
    if payload.get("type") != "access":
        return None
    user_id = payload.get("sub")
    if not user_id:
        return None
    return db.query(User).filter(User.id == user_id).first()


def get_current_user(
    authorization: Optional[str] = Header(default=None),
    access_token_cookie: Optional[str] = Cookie(default=None, alias="access_token"),
    db: Session = Depends(get_db),
) -> User:
    """
    Resolve the current user from:
      1. Authorization: Bearer <access_token>
      2. access_token httpOnly cookie
    """
    token = None
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
    elif access_token_cookie:
        token = access_token_cookie

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Not authenticated", "code": "NOT_AUTHENTICATED"},
        )

    user = _user_from_jwt(token, db)
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Invalid or expired token", "code": "INVALID_TOKEN"},
        )
    return user


def get_user_from_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> Optional[User]:
    """Return the user owning a valid API key, or None."""
    if not x_api_key:
        return None
    key_hash = _hash_api_key(x_api_key)
    record = (
        db.query(ApiKey)
        .filter(ApiKey.key_hash == key_hash, ApiKey.revoked_at.is_(None))
        .first()
    )
    if not record:
        return None
    record.last_used_at = datetime.now(timezone.utc)
    db.commit()
    return db.query(User).filter(User.id == record.user_id).first()


def get_optional_user(
    authorization: Optional[str] = Header(default=None),
    x_api_key: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> Optional[User]:
    """
    Resolve a user from either a bearer JWT or an API key.
    Used on endpoints that accept both (e.g. /v1/detect).
    """
    if x_api_key:
        user = get_user_from_api_key(x_api_key=x_api_key, db=db)
        if user:
            return user

    if authorization and authorization.lower().startswith("bearer "):
        token = authorization.split(" ", 1)[1].strip()
        user = _user_from_jwt(token, db)
        if user:
            return user

    return None