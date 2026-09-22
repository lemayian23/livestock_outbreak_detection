from fastapi import Cookie
from typing import Optional

@router.post("/refresh", response_model=TokenResponse)
def refresh(
    response: Response,
    refresh_token: Optional[str] = Cookie(default=None, alias="refresh_token"),
    db: Session = Depends(get_db),
):
    """Exchange a refresh token (httpOnly cookie) for a new access token."""
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "No refresh token", "code": "NO_REFRESH_TOKEN"},
        )
    try:
        payload = decode_token(refresh_token)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Invalid refresh token", "code": "INVALID_REFRESH_TOKEN"},
        )
    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Wrong token type", "code": "WRONG_TOKEN_TYPE"},
        )

    user = db.query(User).filter(User.id == payload.get("sub")).first()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "User not found or inactive", "code": "USER_NOT_FOUND"},
        )

    access = create_access_token(user.id, extra={"role": user.role})
    _set_access_cookie(response, access)
    return TokenResponse(access_token=access, user=_user_out(user))