"""Statistics CRUD operations."""

import datetime

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, Engine, Page, PageState, Request


async def get_page_statistics(
    db: AsyncSession, history_hours: int = 24,
) -> dict[str, int]:
    """
    Count pages by state for the last *history_hours* hours.
    Returns ``{state_name: count}`` (excludes CREATED).
    """
    from_dt = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=history_hours)

    # Finished pages in the window
    result = await db.execute(
        select(Page).where(Page.finish_timestamp > from_dt)
    )
    finished = result.scalars().all()

    # Unfinished WAITING / PROCESSING pages
    result = await db.execute(
        select(Page).where(
            or_(Page.state == PageState.WAITING, Page.state == PageState.PROCESSING),
            Page.finish_timestamp == None,  # noqa: E711
        )
    )
    unfinished = result.scalars().all()

    state_stats: dict[str, int] = {
        s.name: 0 for s in PageState if s != PageState.CREATED
    }
    for p in finished:
        state_stats[p.state.name] += 1
    for p in unfinished:
        state_stats[p.state.name] += 1

    return state_stats


async def get_usage_statistics(
    db: AsyncSession,
    api_key_string: str,
    from_datetime: datetime.datetime | None = None,
    to_datetime: datetime.datetime | None = None,
) -> int:
    """Count PROCESSED + EXPIRED pages for the given API key, optionally filtered by date."""
    stmt = (
        select(func.count(Page.id))
        .where(Page.state.in_([PageState.PROCESSED, PageState.EXPIRED]))
        .join(Request, Request.id == Page.request_id)
        .join(ApiKey, ApiKey.id == Request.api_key_id)
        .where(ApiKey.api_string == api_key_string)
    )
    if from_datetime:
        stmt = stmt.where(Page.finish_timestamp >= from_datetime)
    if to_datetime:
        stmt = stmt.where(Page.finish_timestamp <= to_datetime)

    result = await db.execute(stmt)
    return result.scalar_one()


async def get_notification_timestamp(db: AsyncSession) -> datetime.datetime:
    """Read the last notification timestamp from the singleton row."""
    from db.models import Notification
    result = await db.execute(select(Notification).limit(1))
    notif = result.scalar_one_or_none()
    if notif is None:
        return datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
    return notif.last_notification


async def set_notification_timestamp(db: AsyncSession) -> None:
    """Update the notification timestamp to now."""
    from db.models import Notification
    result = await db.execute(select(Notification).limit(1))
    notif = result.scalar_one_or_none()
    now = datetime.datetime.now(datetime.UTC)
    if notif is None:
        notif = Notification(last_notification=now)
        db.add(notif)
    else:
        notif.last_notification = now
    await db.commit()


async def get_all_users_usage_statistics(
    db: AsyncSession,
    from_datetime: datetime.datetime | None = None,
    to_datetime: datetime.datetime | None = None,
) -> list[dict]:
    """
    Count PROCESSED + EXPIRED pages per API key, optionally filtered by date.

    Returns a list of dicts:
    ``[{"api_key_id": int, "owner": str, "api_string": str, "processed_pages": int}, ...]``
    """
    stmt = (
        select(
            ApiKey.id.label("api_key_id"),
            ApiKey.owner,
            ApiKey.api_string,
            func.count(Page.id).label("processed_pages"),
        )
        .outerjoin(Request, Request.api_key_id == ApiKey.id)
        .outerjoin(
            Page,
            (Page.request_id == Request.id)
            & Page.state.in_([PageState.PROCESSED, PageState.EXPIRED]),
        )
    )

    if from_datetime:
        stmt = stmt.where(
            or_(Page.finish_timestamp >= from_datetime, Page.id == None)  # noqa: E711
        )
    if to_datetime:
        stmt = stmt.where(
            or_(Page.finish_timestamp <= to_datetime, Page.id == None)  # noqa: E711
        )

    stmt = stmt.group_by(ApiKey.id, ApiKey.owner, ApiKey.api_string).order_by(ApiKey.id)

    result = await db.execute(stmt)
    rows = result.all()
    return [
        {
            "api_key_id": row.api_key_id,
            "owner": row.owner,
            "api_string": row.api_string,
            "processed_pages": row.processed_pages,
        }
        for row in rows
    ]


async def get_engine_usage_statistics(
    db: AsyncSession,
    from_datetime: datetime.datetime | None = None,
    to_datetime: datetime.datetime | None = None,
) -> list[dict]:
    """
    Count PROCESSED + EXPIRED pages per engine, optionally filtered by date.

    Returns a list of dicts:
    ``[{"engine_id": int, "engine_name": str, "processed_pages": int}, ...]``
    """
    stmt = (
        select(
            Engine.id.label("engine_id"),
            Engine.name.label("engine_name"),
            func.count(Page.id).label("processed_pages"),
        )
        .outerjoin(Request, Request.engine_id == Engine.id)
        .outerjoin(
            Page,
            (Page.request_id == Request.id)
            & Page.state.in_([PageState.PROCESSED, PageState.EXPIRED]),
        )
    )

    if from_datetime:
        stmt = stmt.where(
            or_(Page.finish_timestamp >= from_datetime, Page.id == None)  # noqa: E711
        )
    if to_datetime:
        stmt = stmt.where(
            or_(Page.finish_timestamp <= to_datetime, Page.id == None)  # noqa: E711
        )

    stmt = stmt.group_by(Engine.id, Engine.name).order_by(Engine.id)

    result = await db.execute(stmt)
    rows = result.all()
    return [
        {
            "engine_id": row.engine_id,
            "engine_name": row.engine_name,
            "processed_pages": row.processed_pages,
        }
        for row in rows
    ]
