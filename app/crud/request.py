"""Request CRUD operations."""

import datetime
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ApiKey, Engine, Page, PageState, Request,
)


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
    """
    # Verify engine exists
    result = await db.execute(select(Engine).where(Engine.id == engine_id))
    engine = result.scalar_one_or_none()
    if engine is None:
        return None

    req = Request(engine_id=engine.id, api_key_id=api_key.id)
    db.add(req)
    await db.flush()  # populate req.id

    now = datetime.datetime.now(datetime.UTC)
    for image_name, image_url in images.items():
        if image_url is None:
            page = Page(
                name=image_name, url=None,
                state=PageState.CREATED, request_id=req.id,
            )
        else:
            page = Page(
                name=image_name, url=image_url,
                state=PageState.WAITING, request_id=req.id,
                waiting_timestamp=now,
            )
        db.add(page)
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

    now = datetime.datetime.now(datetime.UTC)
    for page in pages:
        page.state = PageState.CANCELED
        page.finish_timestamp = now
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
