"""
Async database engine and session factory.

Provides ``get_async_session()`` — an async generator that yields an
``AsyncSession`` and is used by FastAPI ``Depends()``.
"""

import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

logger = logging.getLogger(__name__)

# Module-level singletons, initialised lazily by get_async_session().
_engine: AsyncEngine | None = None
_async_session_maker: async_sessionmaker[AsyncSession] | None = None


def init_engine(
    database_url: str,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: int = 30,
    pool_recycle: int = 1800,
) -> AsyncEngine:
    """Create (or replace) the global async engine and session maker."""
    global _engine, _async_session_maker

    connect_args: dict = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    _engine = create_async_engine(
        database_url,
        pool_pre_ping=True,
        connect_args=connect_args,
        # Pool params are ignored for SQLite but harmless
        **(
            dict(
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_timeout=pool_timeout,
                pool_recycle=pool_recycle,
            )
            if not database_url.startswith("sqlite")
            else {}
        ),
    )
    _async_session_maker = async_sessionmaker(
        _engine,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    logger.info("Database engine initialised: %s", database_url.split("@")[-1])
    return _engine


def get_engine() -> AsyncEngine:
    """Return the global async engine (must have been initialised)."""
    if _engine is None:
        raise RuntimeError("Database engine not initialised. Call init_engine() first.")
    return _engine


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    """Return the global async session maker (must have been initialised)."""
    if _async_session_maker is None:
        raise RuntimeError("Database engine not initialised. Call init_engine() first.")
    return _async_session_maker


async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields an ``AsyncSession``.

    Usage::

        @router.get("/example")
        async def example(db: AsyncSession = Depends(get_async_session)):
            ...
    """
    if _async_session_maker is None:
        raise RuntimeError("Database engine not initialised. Call init_engine() first.")
    async with _async_session_maker() as session:
        yield session
