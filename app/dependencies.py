"""
FastAPI dependency functions for authentication and database access.
"""

import logging
from typing import Optional

from fastapi import Depends, Header
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, Permission, Request
from db.session import get_async_session
from app.exceptions import ForbiddenError, NotFoundError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database session dependency
# ---------------------------------------------------------------------------

async def get_db(session: AsyncSession = Depends(get_async_session)) -> AsyncSession:
    """Alias so routes can use ``db: AsyncSession = Depends(get_db)``."""
    return session


# ---------------------------------------------------------------------------
# Authentication helpers
# ---------------------------------------------------------------------------

async def _get_api_key(
    db: AsyncSession,
    api_key_header: Optional[str],
    required_permission: Permission,
) -> ApiKey:
    """Look up an API key and verify permission level."""
    if api_key_header is None:
        raise ForbiddenError("Missing api-key header.")

    if required_permission == Permission.USER:
        # USER permission accepts both USER and SUPER_USER keys
        stmt = select(ApiKey).where(ApiKey.api_string == api_key_header)
    else:
        stmt = select(ApiKey).where(
            ApiKey.api_string == api_key_header,
            ApiKey.permission == Permission.SUPER_USER,
        )

    result = await db.execute(stmt)
    key = result.scalar_one_or_none()

    if key is None:
        raise ForbiddenError(
            f"API key {api_key_header} either does not exist "
            "or does not have necessary permissions."
        )
    return key


async def get_current_user(
    api_key: str = Header(..., alias="api-key"),
    db: AsyncSession = Depends(get_db),
) -> ApiKey:
    """Dependency: require a valid USER (or SUPER_USER) API key."""
    return await _get_api_key(db, api_key, Permission.USER)


async def get_super_user(
    api_key: str = Header(..., alias="api-key"),
    db: AsyncSession = Depends(get_db),
) -> ApiKey:
    """Dependency: require a valid SUPER_USER API key."""
    return await _get_api_key(db, api_key, Permission.SUPER_USER)


# ---------------------------------------------------------------------------
# Guard helpers
# ---------------------------------------------------------------------------

async def guard_request_ownership(
    db: AsyncSession,
    user: ApiKey,
    request_id: str,
) -> Request:
    """
    Verify that *request_id* exists and belongs to *user*.
    Returns the Request on success; raises NotFoundError / ForbiddenError otherwise.
    """
    try:
        import uuid as _uuid
        rid = _uuid.UUID(request_id)
    except (ValueError, AttributeError):
        raise NotFoundError(f"Request {request_id} does not exist.")

    stmt = select(Request).where(Request.id == rid)
    result = await db.execute(stmt)
    req = result.scalar_one_or_none()

    if req is None:
        raise NotFoundError(f"Request {request_id} does not exist.")

    if req.api_key_id != user.id:
        raise ForbiddenError(
            f"Request {request_id} does not belong to API key {user.api_string}."
        )
    return req
