"""Pydantic models for request bodies and parameters."""

from typing import Dict, Optional
from pydantic import BaseModel


class ProcessingRequestCreate(BaseModel):
    """Body of ``POST /post_processing_request``."""
    engine: int
    images: Dict[str, Optional[str]]


class CreateUserRequest(BaseModel):
    """Body of ``POST /admin/users``."""
    owner: str
    permission: str = "USER"  # "USER" or "SUPER_USER"


class SuspendUserRequest(BaseModel):
    """Body of ``PUT /admin/users/{user_id}/suspension``."""
    suspended: bool
