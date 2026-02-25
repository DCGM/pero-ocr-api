"""Request CRUD operations."""

import datetime
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ApiKey, Engine, Page, PageState, Request,
)
from app.crud.credits import check_sufficient_credits, increment_pending
from app.exceptions import InsufficientCreditsError


async def request_exists(db: AsyncSession, request_id: str) -> Request | None:
    """Fetch a request by its UUID string. Returns None on invalid UUID or missing row."""
    try:
        rid = uuid.UUID(request_id)
    except (ValueError, AttributeError):
        return None
    result = await db.execute(select(Request).where(Request.id == rid))
    return result.scalar_one_or_none()


async def create_request(
    db: AsyncSession, api_key: ApiKey, engine_id: int, images: dict[str, str | None],
) -> Request | None:
    """
    Create a new processing request with pages.
    Returns the Request on success, or None if the engine does not exist.
    Raises InsufficientCreditsError if the user cannot afford the job.
    """
    # Verify engine exists
    result = await db.execute(select(Engine).where(Engine.id == engine_id))
    engine = result.scalar_one_or_none()
    if engine is None:
        return None

    # Credit check
    total_cost = len(images) * engine.cost_per_page
    # Re-fetch api_key inside this session to get current balance
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key.id))
    api_key_fresh = result.scalar_one()
    if not check_sufficient_credits(api_key_fresh, total_cost):
        raise InsufficientCreditsError(
            f"Insufficient credits. Required: {total_cost}, "
            f"available: {api_key_fresh.credit_balance - api_key_fresh.pending_cost}"
        )

    req = Request(engine_id=engine.id, api_key_id=api_key.id)
    db.add(req)
    await db.flush()  # populate req.id

    now = datetime.datetime.now(datetime.UTC)
    for image_name, image_url in images.items():
        if image_url is None:
            page = Page(
                name=image_name, url=None,
                state=PageState.CREATED, request_id=req.id,
                cost=engine.cost_per_page,
            )
        else:
            page = Page(
                name=image_name, url=image_url,
                state=PageState.WAITING, request_id=req.id,
                waiting_timestamp=now,
                cost=engine.cost_per_page,
            )
        db.add(page)

    # Increment pending cost for all pages
    await increment_pending(db, api_key.id, total_cost)

    await db.commit()
    # refresh to make sure id is loaded
    await db.refresh(req)
    return req


async def get_document_pages(db: AsyncSession, request_id: uuid.UUID) -> list[Page]:
    """Return all pages belonging to a request."""
    result = await db.execute(
        select(Page).where(Page.request_id == request_id)
    )
    return list(result.scalars().all())


async def cancel_request_by_id(db: AsyncSession, request_id: uuid.UUID) -> None:
    """Cancel all active pages (CREATED / WAITING / PROCESSING) on a request."""
    result = await db.execute(
        select(Page)
        .where(Page.request_id == request_id)
        .where(Page.state.in_([PageState.CREATED, PageState.WAITING, PageState.PROCESSING]))
    )
    pages = result.scalars().all()

    # Determine the api_key_id from the request
    req_result = await db.execute(select(Request).where(Request.id == request_id))
    req = req_result.scalar_one()

    now = datetime.datetime.now(datetime.UTC)
    total_refund = 0.0
    for page in pages:
        page.state = PageState.CANCELED
        page.finish_timestamp = now
        total_refund += page.cost

    # Decrement pending cost for all canceled pages
    if total_refund > 0:
        from app.crud.credits import decrement_pending
        await decrement_pending(db, req.api_key_id, total_refund)

    await db.commit()


async def request_belongs_to_api_key(
    db: AsyncSession, api_key_string: str, request_id: str,
) -> bool:
    """Check whether a request belongs to the given API key string."""
    try:
        rid = uuid.UUID(request_id)
    except (ValueError, AttributeError):
        return False
    stmt = (
        select(Request)
        .join(ApiKey)
        .where(ApiKey.api_string == api_key_string, Request.id == rid)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none() is not None
