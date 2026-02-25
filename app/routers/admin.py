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
from app.schemas.requests import CreateUserRequest, SuspendUserRequest, AddCreditsRequest, SetEngineCostRequest
from app.schemas.responses import (
    AddCreditsResponse,
    AllUsersUsageStatisticsResponse,
    ApiKeyItem,
    CreateUserResponse,
    CreditHistoryResponse,
    CreditTransactionItem,
    EngineUsageStatisticsResponse,
    ErrorResponse,
    SetEngineCostResponse,
    SuspendUserResponse,
    UserListResponse,
    UserUsageItem,
    EngineUsageItem,
)
from app.crud.api_key import create_api_key, get_all_api_keys, get_api_key_by_id, set_suspension
from app.crud.credits import add_credits, get_credit_history
from app.crud.engine import get_engine, set_engine_cost
from app.crud.statistics import get_all_users_usage_statistics, get_engine_usage_statistics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["Admin"])

# Reusable response definitions for OpenAPI documentation
_admin_auth_error = {401: {"model": ErrorResponse, "description": "Missing or invalid API key, or the key does not have SUPER_USER permission."}}
_bad_date = {400: {"model": ErrorResponse, "description": "Date parameters are not in valid ISO 8601 format."}}


# ---------------------------------------------------------------------------
# POST /admin/users
# ---------------------------------------------------------------------------

@router.post(
    "/users",
    response_model=CreateUserResponse,
    summary="Create a new API key",
    description=(
        "Create a new API key with the given owner name and permission level. "
        "The `permission` field must be `USER` or `SUPER_USER`.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={
        **_admin_auth_error,
        422: {"model": ErrorResponse, "description": "Invalid permission value. Must be 'USER' or 'SUPER_USER'."},
    },
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
    description=(
        "Return all API keys with their metadata including owner, permission level, "
        "and suspension status.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={**_admin_auth_error},
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
                credit_balance=k.credit_balance,
                pending_cost=k.pending_cost,
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
    description=(
        "Set the suspension flag for an API key. Suspended users' pages are excluded "
        "from the processing queue.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={
        **_admin_auth_error,
        404: {"model": ErrorResponse, "description": "API key with the given user_id was not found."},
    },
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
    description=(
        "Return the number of processed pages per user. Optionally filter by date range "
        "using ISO 8601 path parameters.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={**_admin_auth_error, **_bad_date},
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

    # Enrich with credit info from ApiKey records
    all_keys = await get_all_api_keys(db)
    key_credits = {k.id: (k.credit_balance, k.pending_cost) for k in all_keys}

    users = []
    for r in rows:
        balance, pending = key_credits.get(r["api_key_id"], (0.0, 0.0))
        users.append(UserUsageItem(
            **r,
            credit_balance=balance,
            pending_cost=pending,
        ))

    resp = AllUsersUsageStatisticsResponse(
        status="success",
        users=users,
    )
    if parsed_from:
        resp.from_date = parsed_from.isoformat()
    if parsed_to:
        resp.to_date = parsed_to.isoformat()
    return resp


# ---------------------------------------------------------------------------
# POST /admin/users/{user_id}/credits  —  add credits
# ---------------------------------------------------------------------------

@router.post(
    "/users/{user_id}/credits",
    response_model=AddCreditsResponse,
    summary="Add credits to a user",
    description=(
        "Add processing credits to an API key. The `amount` must be positive. "
        "An optional `note` can describe the reason for the top-up.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={
        **_admin_auth_error,
        404: {"model": ErrorResponse, "description": "API key with the given user_id was not found."},
        422: {"model": ErrorResponse, "description": "Invalid amount (must be positive)."},
    },
)
async def add_user_credits(
    user_id: int,
    body: AddCreditsRequest,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Add processing credits to an API key."""
    if body.amount <= 0:
        raise ValidationError("Amount must be positive.")

    key = await get_api_key_by_id(db, user_id)
    if key is None:
        raise NotFoundError(f"API key with id {user_id} not found.")

    updated = await add_credits(db, user_id, body.amount, _caller.id, body.note)
    return AddCreditsResponse(
        status="success",
        user_id=updated.id,
        new_balance=updated.credit_balance,
        amount=body.amount,
        note=body.note,
    )


# ---------------------------------------------------------------------------
# GET /admin/users/{user_id}/credits  —  credit history
# ---------------------------------------------------------------------------

@router.get(
    "/users/{user_id}/credits",
    response_model=CreditHistoryResponse,
    summary="View credit transaction history",
    description=(
        "Return the complete credit top-up history for an API key.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={
        **_admin_auth_error,
        404: {"model": ErrorResponse, "description": "API key with the given user_id was not found."},
    },
)
async def get_user_credit_history(
    user_id: int,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the credit transaction history for an API key."""
    key = await get_api_key_by_id(db, user_id)
    if key is None:
        raise NotFoundError(f"API key with id {user_id} not found.")

    txs = await get_credit_history(db, user_id)

    # Resolve admin owner names
    admin_ids = {t.admin_api_key_id for t in txs if t.admin_api_key_id}
    admin_names = {}
    for aid in admin_ids:
        admin_key = await get_api_key_by_id(db, aid)
        if admin_key:
            admin_names[aid] = admin_key.owner

    return CreditHistoryResponse(
        status="success",
        user_id=user_id,
        transactions=[
            CreditTransactionItem(
                id=t.id,
                amount=t.amount,
                timestamp=t.timestamp.isoformat() if t.timestamp else "",
                admin_owner=admin_names.get(t.admin_api_key_id),
                note=t.note,
            )
            for t in txs
        ],
    )


# ---------------------------------------------------------------------------
# PUT /admin/engines/{engine_id}/cost  —  set engine cost per page
# ---------------------------------------------------------------------------

@router.put(
    "/engines/{engine_id}/cost",
    response_model=SetEngineCostResponse,
    summary="Set engine cost per page",
    description=(
        "Set the credit cost per page for an OCR engine. The `cost_per_page` must be "
        "non-negative.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={
        **_admin_auth_error,
        404: {"model": ErrorResponse, "description": "Engine with the given engine_id was not found."},
        422: {"model": ErrorResponse, "description": "Invalid cost (must be non-negative)."},
    },
)
async def update_engine_cost(
    engine_id: int,
    body: SetEngineCostRequest,
    _caller: ApiKey = Depends(get_super_user),
    db: AsyncSession = Depends(get_db),
):
    """Set the credit cost per page for an engine."""
    if body.cost_per_page < 0:
        raise ValidationError("cost_per_page must be non-negative.")

    engine = await set_engine_cost(db, engine_id, body.cost_per_page)
    if engine is None:
        raise NotFoundError(f"Engine with id {engine_id} not found.")

    return SetEngineCostResponse(
        status="success",
        engine_id=engine.id,
        cost_per_page=engine.cost_per_page,
    )

@router.get(
    "/usage_statistics/engines",
    response_model=EngineUsageStatisticsResponse,
    summary="Per-engine usage statistics",
    description=(
        "Return the number of processed pages per engine. Optionally filter by date range "
        "using ISO 8601 path parameters.\n\n"
        "Requires a valid `api-key` header with SUPER_USER permission."
    ),
    responses={**_admin_auth_error, **_bad_date},
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
