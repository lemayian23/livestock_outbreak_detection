"""
Password hashing utilities.
"""
import os
from passlib.context import CryptContext

_rounds = int(os.getenv("BCRYPT_ROUNDS", "12"))

pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__rounds=_rounds,
)


def hash_password(plain: str) -> str:
    """Hash a plaintext password."""
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a hash."""
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return False