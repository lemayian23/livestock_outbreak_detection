"""
Pydantic schemas for auth endpoints.
"""
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=128)
    full_name: str = ""
    organization: str = ""


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UserOut(BaseModel):
    id: str
    email: str
    full_name: str
    organization: str
    role: str
    is_active: bool

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut


class ProfileUpdate(BaseModel):
    full_name: Optional[str] = None
    organization: Optional[str] = None


class ApiKeyOut(BaseModel):
    id: str
    name: str
    prefix: str
    created_at: str
    last_used_at: Optional[str] = None
    revoked_at: Optional[str] = None


class ApiKeyCreated(BaseModel):
    id: str
    name: str
    prefix: str
    key: str  # plaintext, shown ONCE


class ApiKeyCreate(BaseModel):
    name: str = "default"