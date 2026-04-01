"""Authentication and shared dependencies."""
from __future__ import annotations

import os
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer(auto_error=False)


def _verify_clerk_jwt(token: str) -> dict[str, Any]:
    """Verify Clerk JWT using JWKS when configured; dev bypass otherwise."""
    secret = os.getenv("SCOUTPRO_CLERK_SECRET_KEY", "")
    if not secret or secret.startswith("sk_test_placeholder"):
        return {"user_id": "dev_user", "email": "dev@localhost"}
    try:
        import httpx
        from jose import jwt

        issuer = os.getenv("SCOUTPRO_CLERK_ISSUER", "").rstrip("/")
        if not issuer:
            raise HTTPException(status_code=500, detail="SCOUTPRO_CLERK_ISSUER not set")
        jwks_url = f"{issuer}/.well-known/jwks.json"
        with httpx.Client(timeout=10.0) as client:
            jwks = client.get(jwks_url).json()
        payload = jwt.decode(
            token,
            jwks,
            algorithms=["RS256"],
            audience=None,
            issuer=issuer if issuer else None,
            options={"verify_aud": False},
        )
        uid = payload.get("sub") or payload.get("user_id")
        return {"user_id": str(uid), "claims": payload}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}") from exc


async def require_auth(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any]:
    if os.getenv("SCOUTPRO_DEV_AUTH", "").lower() in ("1", "true", "yes"):
        return {"user_id": "dev_user", "email": "dev@localhost"}
    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Bearer token required")
    return _verify_clerk_jwt(creds.credentials)


async def optional_auth(
    creds: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any]:
    if creds is None or creds.scheme.lower() != "bearer":
        return {"user_id": None}
    try:
        return _verify_clerk_jwt(creds.credentials)
    except HTTPException:
        return {"user_id": None}
