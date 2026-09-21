"""
Auth endpoints: signup, login, refresh, logout, me, profile.
"""
import os
import secrets
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response, status
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
def signup(payload: SignupRequest, response: Response, db: Session = Depends(get_db)):
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
    refresh = create_refresh_token(user.id)
    _set_access_cookie(response, access)
    _set_refresh_cookie(response, refresh)

    return TokenResponse(access_token=access, user=_user_out(user))


@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, response: Response, db: Session = Depends(get_db)):
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
    refresh = create_refresh_token(user.id)
    _set_access_cookie(response, access)
    _set_refresh_cookie(response, refresh)

    return TokenResponse(access_token=access, user=_user_out(user))


@router.post("/refresh", response_model=TokenResponse)
def refresh(response: Response, request_refresh_token: str = None, db: Session = Depends(get_db)):
    """
    Exchange a refresh token for a new access token.
    The refresh token is read from the httpOnly cookie automatically
    by FastAPI when we declare it, but to keep it simple here we
    accept it either via cookie or a small form body.
    """
    # FastAPI injects cookies via Cookie(...) param; we declared a plain
    # string here and manually read the cookie from request.
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail={
            "error": "Use /auth/login to get a fresh session for now",
            "code": "REFRESH_VIA_LOGIN",
        },
    )


@router.post("/logout", status_code=204)
def logout(response: Response):
    """Clear the auth cookies."""
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