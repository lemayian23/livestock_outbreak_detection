"""
Auth endpoints: signup, login, refresh, logout, me, profile.
"""
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from ..auth.dependencies import get_current_user
from ..auth.jwt import (
    ACCESS_EXPIRE_MIN,
    REFRESH_EXPIRE_DAYS,
    JWTError,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from ..auth.password import hash_password, verify_password
from ..database import get_db
from ..models import User
from ..schemas_auth import (
    LoginRequest,
    ProfileUpdate,
    SignupRequest,
    TokenResponse,
    UserOut,
)

router = APIRouter(prefix="/auth", tags=["auth"])

_COOKIE_SECURE = os.getenv("APP_ENV", "development") == "production"


def _set_refresh_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key="refresh_token",
        value=token,
        max_age=REFRESH_EXPIRE_DAYS * 24 * 3600,
        httponly=True,
        secure=_COOKIE_SECURE,
        samesite="lax",
        path="/auth",
    )


def _set_access_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key="access_token",
        value=token,
        max_age=ACCESS_EXPIRE_MIN * 60,
        httponly=True,
        secure=_COOKIE_SECURE,
        samesite="lax",
        path="/",
    )


def _user_out(user: User) -> UserOut:
    return UserOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name or "",
        organization=user.organization or "",
        role=user.role,
        is_active=user.is_active,
    )


@router.post("/signup", response_model=TokenResponse, status_code=201)
def signup(
    payload: SignupRequest,
    response: Response,
    db: Session = Depends(get_db),
):
    """Create a new account and immediately log the user in."""
    existing = db.query(User).filter(User.email == payload.email.lower()).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "Email already registered", "code": "EMAIL_EXISTS"},
        )

    user = User(
        email=payload.email.lower(),
        password_hash=hash_password(payload.password),
        full_name=payload.full_name or "",
        organization=payload.organization or "",
        role="user",
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    access = create_access_token(user.id, extra={"role": user.role})
    refresh_token = create_refresh_token(user.id)
    _set_access_cookie(response, access)
    _set_refresh_cookie(response, refresh_token)

    return TokenResponse(access_token=access, user=_user_out(user))


@router.post("/login", response_model=TokenResponse)
def login(
    payload: LoginRequest,
    response: Response,
    db: Session = Depends(get_db),
):
    """Authenticate with email + password."""
    user = db.query(User).filter(User.email == payload.email.lower()).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"error": "Invalid credentials", "code": "INVALID_CREDENTIALS"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "Account disabled", "code": "ACCOUNT_DISABLED"},
        )

    user.last_login_at = datetime.now(timezone.utc)
    db.commit()

    access = create_access_token(user.id, extra={"role": user.role})
    refresh_token = create_refresh_token(user.id)
    _set_access_cookie(response, access)
    _set_refresh_cookie(response, refresh_token)

    return TokenResponse(access_token=access, user=_user_out(user))


@router.post("/refresh", response_model=TokenResponse)
def refresh(
    response: Response,
    refresh_token: Optional[str] = Cookie(default=None, alias="refresh_token"),
    db: Session = Depends(get_db),
):
    """Exchange an httpOnly refresh cookie for a new access token."""
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
            detail={
                "error": "Invalid or expired refresh token",
                "code": "INVALID_REFRESH_TOKEN",
            },
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


@router.post("/logout", status_code=204)
def logout(response: Response):
    """Clear auth cookies."""
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/auth")
    return None


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)):
    """Return the current authenticated user."""
    return _user_out(current_user)


@router.patch("/me", response_model=UserOut)
def update_me(
    payload: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Update profile fields."""
    if payload.full_name is not None:
        current_user.full_name = payload.full_name
    if payload.organization is not None:
        current_user.organization = payload.organization
    db.commit()
    db.refresh(current_user)
    return _user_out(current_user)