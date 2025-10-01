from typing import Optional

from fastapi import Header, HTTPException, status

ADMIN_ACCOUNTS = {"company", "director-1", "director-2"}


def require_admin_account(
    x_account_id: Optional[str] = Header(None, alias="X-Account-ID")
) -> str:
    if not x_account_id or x_account_id not in ADMIN_ACCOUNTS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )
    return x_account_id


def require_mfa_context(x_mfa_context: str = Header(..., alias="X-MFA-Context")) -> str:
    """Ensure the caller has completed MFA challenges."""

    if x_mfa_context.strip().lower() != "verified":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="MFA context is invalid or incomplete.",
        )
    return x_mfa_context

