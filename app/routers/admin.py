"""
Admin routes — require a SUPER_USER API key.

Endpoints:
- POST /admin/users                              — create a new API key
- GET  /admin/users                              — list all API keys
- PUT  /admin/users/{user_id}/suspension         — suspend / unsuspend a user
- GET  /admin/usage_statistics/users             — per-user usage stats
- GET  /admin/usage_statistics/users/{from}
- GET  /admin/usage_statistics/users/{from}/{to}
- GET  /admin/usage_statistics/engines           — per-engine usage stats
- GET  /admin/usage_statistics/engines/{from}
- GET  /admin/usage_statistics/engines/{from}/{to}
"""

import logging
from typing import Optional

import dateutil.parser
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, Permission
from app.dependencies import get_db, get_super_user
from app.exceptions import BadRequestError, NotFoundError, ValidationError
from app.schemas.requests import CreateUserRequest, SuspendUserRequest
from app.schemas.responses import (
    AllUsersUsageStatisticsResponse,
    ApiKeyItem,
    CreateUserResponse,
    EngineUsageStatisticsResponse,
    SuspendUserResponse,
    UserListResponse,
    UserUsageItem,
    EngineUsageItem,
)
from app.crud.api_key import create_api_key, get_all_api_keys, set_suspension
from app.crud.statistics import get_all_users_usage_statistics, get_engine_usage_statistics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])


# ---------------------------------------------------------------------------
# POST /admin/users
# ---------------------------------------------------------------------------

@router.post(
    "/users",
    response_model=CreateUserResponse,
    summary="Create a new API key",
)
async def create_user(
    body: CreateUserRequest,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new API key with the given owner name and permission level."""
    perm_str = body.permission.upper()
    try:
        permission = Permission[perm_str]
    except KeyError:
        raise ValidationError(
            f"Invalid permission '{body.permission}'. Must be 'USER' or 'SUPER_USER'."
        )

    api_string = await create_api_key(db, body.owner, permission)
    return CreateUserResponse(
        status="success",
        api_key=api_string,
        owner=body.owner,
        permission=permission.name,
    )


# ---------------------------------------------------------------------------
# GET /admin/users
# ---------------------------------------------------------------------------

@router.get(
    "/users",
    response_model=UserListResponse,
    summary="List all API keys",
)
async def list_users(
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return all API keys with their metadata."""
    keys = await get_all_api_keys(db)
    return UserListResponse(
        status="success",
        users=[
            ApiKeyItem(
                id=k.id,
                api_string=k.api_string,
                owner=k.owner,
                permission=k.permission.name,
                suspension=k.suspension,
            )
            for k in keys
        ],
    )


# ---------------------------------------------------------------------------
# PUT /admin/users/{user_id}/suspension
# ---------------------------------------------------------------------------

@router.put(
    "/users/{user_id}/suspension",
    response_model=SuspendUserResponse,
    summary="Suspend or unsuspend a user",
)
async def suspend_user(
    user_id: int,
    body: SuspendUserRequest,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Set the suspension flag for an API key."""
    key = await set_suspension(db, user_id, body.suspended)
    if key is None:
        raise NotFoundError(f"API key with id {user_id} not found.")
    return SuspendUserResponse(
        status="success",
        user_id=key.id,
        suspended=key.suspension,
    )


# ---------------------------------------------------------------------------
# Datetime parsing helper
# ---------------------------------------------------------------------------

def _parse_datetime_params(
    from_datetime: Optional[str], to_datetime: Optional[str],
):
    """Parse optional ISO datetime strings, raise BadRequestError on failure."""
    parsed_from = None
    parsed_to = None

    if from_datetime:
        try:
            parsed_from = dateutil.parser.isoparse(from_datetime)
        except ValueError:
            raise BadRequestError("from_datetime is not in a valid ISO format.")

    if to_datetime:
        try:
            parsed_to = dateutil.parser.isoparse(to_datetime)
        except ValueError:
            raise BadRequestError("to_datetime is not in a valid ISO format.")

    return parsed_from, parsed_to


# ---------------------------------------------------------------------------
# GET /admin/usage_statistics/users[/<from>[/<to>]]
# ---------------------------------------------------------------------------

@router.get(
    "/usage_statistics/users",
    response_model=AllUsersUsageStatisticsResponse,
    summary="Per-user usage statistics",
)
@router.get(
    "/usage_statistics/users/{from_datetime}",
    response_model=AllUsersUsageStatisticsResponse,
    include_in_schema=False,
)
@router.get(
    "/usage_statistics/users/{from_datetime}/{to_datetime}",
    response_model=AllUsersUsageStatisticsResponse,
    include_in_schema=False,
)
async def admin_user_usage_statistics(
    from_datetime: Optional[str] = None,
    to_datetime: Optional[str] = None,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return processed page counts per user, optionally filtered by date range."""
    parsed_from, parsed_to = _parse_datetime_params(from_datetime, to_datetime)

    rows = await get_all_users_usage_statistics(
        db, from_datetime=parsed_from, to_datetime=parsed_to,
    )

    resp = AllUsersUsageStatisticsResponse(
        status="success",
        users=[UserUsageItem(**r) for r in rows],
    )
    if parsed_from:
        resp.from_date = parsed_from.isoformat()
    if parsed_to:
        resp.to_date = parsed_to.isoformat()
    return resp


# ---------------------------------------------------------------------------
# GET /admin/usage_statistics/engines[/<from>[/<to>]]
# ---------------------------------------------------------------------------

@router.get(
    "/usage_statistics/engines",
    response_model=EngineUsageStatisticsResponse,
    summary="Per-engine usage statistics",
)
@router.get(
    "/usage_statistics/engines/{from_datetime}",
    response_model=EngineUsageStatisticsResponse,
    include_in_schema=False,
)
@router.get(
    "/usage_statistics/engines/{from_datetime}/{to_datetime}",
    response_model=EngineUsageStatisticsResponse,
    include_in_schema=False,
)
async def admin_engine_usage_statistics(
    from_datetime: Optional[str] = None,
    to_datetime: Optional[str] = None,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return processed page counts per engine, optionally filtered by date range."""
    parsed_from, parsed_to = _parse_datetime_params(from_datetime, to_datetime)

    rows = await get_engine_usage_statistics(
        db, from_datetime=parsed_from, to_datetime=parsed_to,
    )

    resp = EngineUsageStatisticsResponse(
        status="success",
        engines=[EngineUsageItem(**r) for r in rows],
    )
    if parsed_from:
        resp.from_date = parsed_from.isoformat()
    if parsed_to:
        resp.to_date = parsed_to.isoformat()
    return resp
