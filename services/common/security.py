
from fastapi import Header, HTTPException, status

ADMIN_ACCOUNTS = {"admin-eu", "admin-us", "admin-apac"}


def require_admin_account(x_account_id: str = Header(..., alias="X-Account-ID")) -> str:
    if x_account_id not in ADMIN_ACCOUNTS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is not authorized for administrative access.",
        )
    return x_account_id

