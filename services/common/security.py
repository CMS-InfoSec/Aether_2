from typing import Optional, Sequence, Tuple

from fastapi import Header, HTTPException, status

ADMIN_ACCOUNTS = {"company", "director-1", "director-2"}
DIRECTOR_ACCOUNTS = {account for account in ADMIN_ACCOUNTS if account.startswith("director-")}


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


def require_dual_director_confirmation(
    x_director_approvals: Optional[str] = Header(None, alias="X-Director-Approvals")
) -> Tuple[str, str]:
    """Enforce the presence of two distinct director approvals for sensitive actions."""

    if not x_director_approvals:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two director approvals are required for this action.",
        )

    approvals: Sequence[str] = [item.strip() for item in x_director_approvals.split(",") if item.strip()]
    if len(approvals) != 2 or len(set(approvals)) != 2:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Two distinct director approvals are required.",
        )

    invalid = [approval for approval in approvals if approval not in DIRECTOR_ACCOUNTS]
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Director approvals must come from authorized directors.",
        )

    return approvals[0], approvals[1]

