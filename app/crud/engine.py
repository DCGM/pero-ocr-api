"""Engine and engine-version CRUD operations."""

from typing import List, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db.models import (
    Engine, EngineVersion, EngineVersionModel, Model,
)


async def get_engine(db: AsyncSession, engine_id: int) -> Engine | None:
    """Fetch an engine by id."""
    result = await db.execute(select(Engine).where(Engine.id == engine_id))
    return result.scalar_one_or_none()


async def get_latest_engine_version(
    db: AsyncSession, engine_id: int,
) -> EngineVersion | None:
    """Return the latest (highest-id) version for the given engine."""
    stmt = (
        select(EngineVersion)
        .where(EngineVersion.engine_id == engine_id)
        .order_by(EngineVersion.id.desc())
        .limit(1)
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def get_engine_version_by_name(
    db: AsyncSession, engine_id: int, version_name: str,
) -> EngineVersion | None:
    """Fetch a specific engine version by engine id and version string."""
    stmt = select(EngineVersion).where(
        EngineVersion.engine_id == engine_id,
        EngineVersion.version == version_name,
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def get_models_for_version(
    db: AsyncSession, engine_version_id: int,
) -> List[Model]:
    """Return all models linked to a given engine version."""
    stmt = (
        select(Model)
        .join(EngineVersionModel)
        .where(EngineVersionModel.engine_version_id == engine_version_id)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def get_latest_models(
    db: AsyncSession, engine_id: int,
) -> Tuple[EngineVersion, List[Model]]:
    """Return (latest_engine_version, [models]) for the given engine."""
    ev = await get_latest_engine_version(db, engine_id)
    if ev is None:
        return None, []
    models = await get_models_for_version(db, ev.id)
    return ev, models


async def get_engine_dict(db: AsyncSession) -> dict:
    """
    Build the engine listing dictionary used by ``GET /get_engines``.
    Returns ``{engine_name: {id, description, engine_version, models: [...]}}``
    """
    result = await db.execute(select(Engine))
    engines = result.scalars().all()

    engines_dict = {}
    for engine in engines:
        ev, models = await get_latest_models(db, engine.id)
        engines_dict[engine.name] = {
            "id": engine.id,
            "description": engine.description,
            "engine_version": ev.version,
            "models": [{"id": m.id, "name": m.name} for m in models],
        }
    return engines_dict
