"""API key CRUD operations."""

import base64
import hashlib
import random

from sqlalchemy import select
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
