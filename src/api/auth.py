"""
API key authentication dependency.
"""
import os
from fastapi import Header, HTTPException, status


def _expected_key() -> str:
    """Get the expected API key from env or secrets manager."""
    key = os.getenv("API_KEY")
    if key:
        return key
    # Fall back to secrets manager if configured
    try:
        from config_manager.secrets import get_secrets_manager
        secrets = get_secrets_manager()
        key = secrets.get("api_key")
    except Exception:
        key = None
    return key or "dev-insecure-key"


async def require_api_key(
    x_api_key: str = Header(default=None, alias="X-API-Key"),
    authorization: str = Header(default=None, alias="Authorization"),
) -> str:
    """
    Require a valid API key on protected endpoints.

    Accepts either:
      - X-API-Key: <key>
      - Authorization: Bearer <key>
    """
    provided = x_api_key
    if not provided and authorization and authorization.lower().startswith("bearer "):
        provided = authorization.split(" ", 1)[1].strip()

    if not provided:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Missing API key", "code": "MISSING_API_KEY"},
        )

    if provided != _expected_key():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Invalid API key", "code": "INVALID_API_KEY"},
        )

    return provided