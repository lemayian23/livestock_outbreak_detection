"""
JWT token creation and verification.
"""
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from jose import JWTError, jwt


def _secret() -> str:
    key = os.getenv("AUTH_SECRET_KEY")
    if key:
        return key
    # Fallback: try config
    try:
        from utils.config import Config
        cfg = Config()
        key = cfg.get("auth.secret_key")
    except Exception:
        key = None
    return key or "dev-insecure-secret-change-me"


ALGORITHM = "HS256"
ACCESS_EXPIRE_MIN = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))


def create_access_token(user_id: str, extra: Optional[Dict[str, Any]] = None) -> str:
    """Create a short-lived access token."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "type": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=ACCESS_EXPIRE_MIN)).timestamp()),
    }
    if extra:
        payload.update(extra)
    return jwt.encode(payload, _secret(), algorithm=ALGORITHM)


def create_refresh_token(user_id: str) -> str:
    """Create a long-lived refresh token."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "type": "refresh",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=REFRESH_EXPIRE_DAYS)).timestamp()),
    }
    return jwt.encode(payload, _secret(), algorithm=ALGORITHM)


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode and verify a JWT. Raises JWTError on failure.
    """
    return jwt.decode(token, _secret(), algorithms=[ALGORITHM])


__all__ = [
    "create_access_token",
    "create_refresh_token",
    "decode_token",
    "JWTError",
    "ACCESS_EXPIRE_MIN",
    "REFRESH_EXPIRE_DAYS",
]