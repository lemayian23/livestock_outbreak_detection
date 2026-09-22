"""
Password hashing utilities — using bcrypt directly (no passlib).
"""
import os

import bcrypt


def _rounds() -> int:
    return int(os.getenv("BCRYPT_ROUNDS", "12"))


def hash_password(plain: str) -> str:
    """Hash a plaintext password with bcrypt."""
    if not isinstance(plain, str):
        raise TypeError("password must be a string")
    # bcrypt hard-caps at 72 bytes
    pwd_bytes = plain.encode("utf-8")[:72]
    salt = bcrypt.gensalt(rounds=_rounds())
    return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    try:
        pwd_bytes = plain.encode("utf-8")[:72]
        return bcrypt.checkpw(pwd_bytes, hashed.encode("utf-8"))
    except Exception:
        return False