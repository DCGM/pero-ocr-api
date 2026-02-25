"""Credit CRUD operations — balance management, pending cost, transaction history."""

import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, CreditTransaction


async def add_credits(
    db: AsyncSession,
    api_key_id: int,
    amount: float,
    admin_api_key_id: int | None = None,
    note: str | None = None,
) -> ApiKey:
    """
    Add credits to an API key and record the transaction.
    Returns the updated ApiKey.
    """
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    key = result.scalar_one()
    key.credit_balance += amount

    tx = CreditTransaction(
        api_key_id=api_key_id,
        amount=amount,
        admin_api_key_id=admin_api_key_id,
        note=note,
    )
    db.add(tx)
    await db.commit()
    await db.refresh(key)
    return key


async def get_credit_history(
    db: AsyncSession, api_key_id: int,
) -> list[CreditTransaction]:
    """Return all credit transactions for an API key, newest first."""
    result = await db.execute(
        select(CreditTransaction)
        .where(CreditTransaction.api_key_id == api_key_id)
        .order_by(CreditTransaction.timestamp.desc())
    )
    return list(result.scalars().all())


def check_sufficient_credits(api_key: ApiKey, total_cost: float) -> bool:
    """Return True if the user can afford total_cost given balance and pending."""
    available = api_key.credit_balance - api_key.pending_cost
    return available >= total_cost


async def increment_pending(db: AsyncSession, api_key_id: int, amount: float) -> None:
    """Increase pending_cost on an API key."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    key = result.scalar_one()
    key.pending_cost += amount


async def decrement_pending(db: AsyncSession, api_key_id: int, amount: float) -> None:
    """Decrease pending_cost on an API key (floor at 0)."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    key = result.scalar_one()
    key.pending_cost = max(0.0, key.pending_cost - amount)


async def deduct_balance(db: AsyncSession, api_key_id: int, amount: float) -> None:
    """Subtract from credit_balance on an API key."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == api_key_id))
    key = result.scalar_one()
    key.credit_balance -= amount
