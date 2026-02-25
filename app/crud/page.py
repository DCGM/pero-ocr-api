"""Page CRUD operations — scheduling, state transitions, queries."""

import datetime
import uuid
from collections import defaultdict
from typing import Optional, Tuple

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    ApiKey, Engine, Page, PageState, Request,
)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

async def get_page_by_id(db: AsyncSession, page_id: str) -> Page | None:
    """Fetch a page by its UUID string."""
    try:
        pid = uuid.UUID(page_id)
    except (ValueError, AttributeError):
        return None
    result = await db.execute(select(Page).where(Page.id == pid))
    return result.scalar_one_or_none()


async def get_page_and_state(
    db: AsyncSession, request_id: str | uuid.UUID, page_name: str,
) -> Tuple[Optional[Page], Optional[PageState]]:
    """Look up a page by request_id + name; return (page, state) or (None, None)."""
    if isinstance(request_id, str):
        try:
            request_id = uuid.UUID(request_id)
        except (ValueError, AttributeError):
            return None, None

    result = await db.execute(
        select(Page).where(Page.request_id == request_id, Page.name == page_name)
    )
    page = result.scalar_one_or_none()
    if page is None:
        return None, None
    return page, page.state


async def get_engine_by_page_id(db: AsyncSession, page_id: str) -> Engine | None:
    """Get the engine associated with a page (via its request)."""
    page = await get_page_by_id(db, page_id)
    if page is None:
        return None
    result = await db.execute(select(Request).where(Request.id == page.request_id))
    req = result.scalar_one_or_none()
    if req is None:
        return None
    result = await db.execute(select(Engine).where(Engine.id == req.engine_id))
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Fair scheduling
# ---------------------------------------------------------------------------

async def _which_keys_have_requests(
    db: AsyncSession, engine_id: int | None = None,
) -> list[int]:
    """Return api_key ids that have WAITING pages (non-suspended users with credits only)."""
    stmt = (
        select(ApiKey.id)
        .join(Request, Request.api_key_id == ApiKey.id)
        .join(Page, Page.request_id == Request.id)
        .where(Page.state == PageState.WAITING)
        .where(ApiKey.suspension == False)  # noqa: E712
        .where(ApiKey.credit_balance > 0)
    )
    if engine_id is not None:
        stmt = stmt.where(Request.engine_id == engine_id)
    stmt = stmt.group_by(ApiKey.id)
    result = await db.execute(stmt)
    return [row[0] for row in result.all()]


async def _get_processed_page_counts(
    db: AsyncSession,
    time_delta: datetime.timedelta = datetime.timedelta(minutes=1),
) -> dict[int, int]:
    """Count recently-processed pages per api_key."""
    cutoff = datetime.datetime.now(datetime.UTC) - time_delta
    stmt = (
        select(ApiKey.id, func.count(ApiKey.id))
        .join(Request, Request.api_key_id == ApiKey.id)
        .join(Page, Page.request_id == Request.id)
        .where(Page.state == PageState.PROCESSED)
        .where(Page.finish_timestamp > cutoff)
        .group_by(ApiKey.id)
    )
    result = await db.execute(stmt)
    return defaultdict(int, result.all())


async def get_page_by_preferred_engine(
    db: AsyncSession, engine_id: int,
) -> Tuple[Optional[Page], Optional[int]]:
    """
    Fair-scheduling page dispatcher.

    1. Try the preferred engine first.
    2. Fall back to any engine.
    3. Among eligible users, pick the one with fewest recent processed pages.
    4. Among that user's WAITING pages, pick the oldest.
    5. Transition the page to PROCESSING.

    Returns ``(page, engine_id)`` or ``(None, engine_id)`` if nothing available.
    """
    page = None
    counts = None

    # -- Preferred engine --
    if engine_id is not None:
        api_keys = await _which_keys_have_requests(db, engine_id)
        if api_keys:
            counts = await _get_processed_page_counts(db)
            lowest_key = min(api_keys, key=lambda k: counts[k])

            result = await db.execute(
                select(Page)
                .join(Request, Request.id == Page.request_id)
                .join(ApiKey, ApiKey.id == Request.api_key_id)
                .where(Page.state == PageState.WAITING)
                .where(Request.engine_id == engine_id)
                .where(ApiKey.id == lowest_key)
                .order_by(Page.waiting_timestamp.asc())
                .limit(1)
            )
            page = result.scalar_one_or_none()

    # -- Fallback to any engine --
    if page is None:
        api_keys = await _which_keys_have_requests(db)
        if api_keys:
            if counts is None:
                counts = await _get_processed_page_counts(db)
            lowest_key = min(api_keys, key=lambda k: counts[k])

            result = await db.execute(
                select(Page)
                .join(Request, Request.id == Page.request_id)
                .join(ApiKey, ApiKey.id == Request.api_key_id)
                .where(Page.state == PageState.WAITING)
                .where(ApiKey.id == lowest_key)
                .order_by(Page.waiting_timestamp.asc())
                .limit(1)
            )
            page = result.scalar_one_or_none()

            if page:
                # resolve actual engine for this page
                result = await db.execute(
                    select(Request.engine_id).where(Request.id == page.request_id)
                )
                engine_id = result.scalar_one()

    if page:
        page.state = PageState.PROCESSING
        page.processing_timestamp = datetime.datetime.now(datetime.UTC)
        await db.commit()

    return page, engine_id


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

async def _is_request_finished(db: AsyncSession, request_id: uuid.UUID) -> bool:
    """Check whether all pages in a request have reached a terminal state."""
    total = await db.execute(
        select(func.count(Page.id)).where(Page.request_id == request_id)
    )
    total_count = total.scalar_one()

    active = await db.execute(
        select(func.count(Page.id))
        .where(Page.request_id == request_id)
        .where(Page.state.in_([PageState.CREATED, PageState.WAITING, PageState.PROCESSING]))
    )
    active_count = active.scalar_one()

    return active_count == 0 and total_count > 0


async def change_page_to_processed(
    db: AsyncSession,
    page_id: str,
    score: float,
    engine_version_id: int,
) -> None:
    """Mark a page as PROCESSED, update timestamps, and deduct credits."""
    page = await get_page_by_id(db, page_id)
    result = await db.execute(select(Request).where(Request.id == page.request_id))
    req = result.scalar_one()

    now = datetime.datetime.now(datetime.UTC)
    page.score = score
    page.state = PageState.PROCESSED
    page.engine_version_id = engine_version_id
    page.finish_timestamp = now
    req.modification_timestamp = now

    # Deduct credits: reduce balance and pending
    from app.crud.credits import deduct_balance, decrement_pending
    await deduct_balance(db, req.api_key_id, page.cost)
    await decrement_pending(db, req.api_key_id, page.cost)

    await db.commit()

    if await _is_request_finished(db, req.id):
        req.finish_timestamp = now
        await db.commit()


async def change_page_to_failed(
    db: AsyncSession,
    page_id: str,
    fail_type: str,
    traceback_str: str,
    engine_version_id: int,
) -> None:
    """Mark a page as failed (NOT_FOUND / INVALID_FILE / PROCESSING_FAILED)."""
    state_map = {
        "NOT_FOUND": PageState.NOT_FOUND,
        "INVALID_FILE": PageState.INVALID_FILE,
        "PROCESSING_FAILED": PageState.PROCESSING_FAILED,
    }
    page = await get_page_by_id(db, page_id)
    result = await db.execute(select(Request).where(Request.id == page.request_id))
    req = result.scalar_one()

    now = datetime.datetime.now(datetime.UTC)
    page.state = state_map[fail_type]
    page.traceback = traceback_str
    page.engine_version_id = engine_version_id
    page.finish_timestamp = now
    req.modification_timestamp = now

    # Release pending cost (no balance deduction on failure)
    from app.crud.credits import decrement_pending
    await decrement_pending(db, req.api_key_id, page.cost)

    await db.commit()

    if await _is_request_finished(db, req.id):
        req.finish_timestamp = now
        await db.commit()


async def change_page_path(
    db: AsyncSession, request_id: str | uuid.UUID, page_name: str, new_url: str,
) -> None:
    """Update page URL and transition CREATED → WAITING after image upload."""
    if isinstance(request_id, str):
        request_id = uuid.UUID(request_id)
    result = await db.execute(
        select(Page).where(Page.request_id == request_id, Page.name == page_name)
    )
    page = result.scalar_one()
    page.url = new_url
    page.state = PageState.WAITING
    page.waiting_timestamp = datetime.datetime.now(datetime.UTC)
    await db.commit()


# ---------------------------------------------------------------------------
# Timeout reset (used by background task)
# ---------------------------------------------------------------------------

async def reset_timed_out_pages(
    db: AsyncSession, timeout_seconds: int = 60,
) -> list[Page]:
    """
    Find PROCESSING pages that have exceeded the timeout, reset them to WAITING.
    Returns the list of reset pages (for notification).
    """
    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=timeout_seconds)
    result = await db.execute(
        select(Page)
        .where(Page.state == PageState.PROCESSING)
        .where(Page.processing_timestamp < cutoff)
    )
    pages = list(result.scalars().all())

    for page in pages:
        page.state = PageState.WAITING
        page.processing_timestamp = None
    await db.commit()
    return pages
