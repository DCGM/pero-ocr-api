"""API key CRUD operations."""

import base64
import hashlib
import random
from typing import Sequence

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, Permission


def generate_hash_key() -> str:
    """Generate a random API key string."""
    return base64.b64encode(
        hashlib.sha256(str(random.getrandbits(256)).encode("utf-8")).digest(),
        random.choice(["rA", "aZ", "gQ", "hH", "hG", "aR", "DD"]).encode("utf-8"),
    ).decode("utf-8").rstrip("==")


async def match_api_key(
    db: AsyncSession, key: str, permission: Permission,
) -> ApiKey | None:
    """
    Look up an API key in the database.
    For USER permission, both USER and SUPER_USER keys match.
    Returns the ``ApiKey`` row or ``None``.
    """
    if key is None:
        return None

    stmt = select(ApiKey).where(ApiKey.api_string == key)
    if permission == Permission.SUPER_USER:
        stmt = stmt.where(ApiKey.permission == Permission.SUPER_USER)

    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def get_api_key_by_id(db: AsyncSession, api_key_id: int) -> ApiKey | None:
    """Fetch an ApiKey by its integer id."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    return result.scalar_one_or_none()


async def create_api_key(
    db: AsyncSession, owner: str, permission: Permission,
) -> str:
    """Create a new API key, commit it, and return the generated key string."""
    api_string = generate_hash_key()
    api_key = ApiKey(
        api_string=api_string,
        owner=owner,
        permission=permission,
    )
    db.add(api_key)
    await db.commit()
    return api_string


async def get_all_api_keys(db: AsyncSession) -> Sequence[ApiKey]:
    """Return all API keys."""
    result = await db.execute(select(ApiKey).order_by(ApiKey.id))
    return result.scalars().all()


async def set_suspension(
    db: AsyncSession, api_key_id: int, suspended: bool,
) -> ApiKey | None:
    """Set the suspension flag for a single API key. Returns the updated key or None."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    key = result.scalar_one_or_none()
    if key is None:
        return None
    key.suspension = suspended
    await db.commit()
    await db.refresh(key)
    return key
