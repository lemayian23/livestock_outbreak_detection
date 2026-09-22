"""Auth subpackage — re-exports for convenience."""

from .dependencies import (
    get_current_user,
    get_optional_user,
    get_user_from_api_key,
)
from .jwt import (
    ACCESS_EXPIRE_MIN,
    REFRESH_EXPIRE_DAYS,
    JWTError,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from .password import hash_password, verify_password

__all__ = [
    "get_current_user",
    "get_optional_user",
    "get_user_from_api_key",
    "create_access_token",
    "create_refresh_token",
    "decode_token",
    "JWTError",
    "ACCESS_EXPIRE_MIN",
    "REFRESH_EXPIRE_DAYS",
    "hash_password",
    "verify_password",
]