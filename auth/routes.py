"""FastAPI routes exposing authentication endpoints for Builder.io Fusion."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr

from .service import AuthService, Session

router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    mfa_code: str
    ip_address: str | None = None


class LoginResponse(BaseModel):
    token: str
    admin_id: str
    expires_at: str

    @classmethod
    def from_session(cls, session: Session) -> "LoginResponse":
        return cls(
            token=session.token,
            admin_id=session.admin_id,
            expires_at=session.expires_at.isoformat(),
        )


def get_auth_service() -> AuthService:
    raise RuntimeError("AuthService dependency not configured")


@router.post("/login", response_model=LoginResponse)
def login(
    payload: LoginRequest,
    service: AuthService = Depends(get_auth_service),
) -> LoginResponse:
    try:
        session = service.login(
            email=payload.email,
            password=payload.password,
            mfa_code=payload.mfa_code,
            ip_address=payload.ip_address,
        )
    except PermissionError as exc:  # pragma: no cover - mapped error handling
        detail = str(exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
        ) from exc
    return LoginResponse.from_session(session)


__all__ = ["router", "get_auth_service"]
