"""Pydantic models for request bodies and parameters."""

from typing import Dict, Optional
from pydantic import BaseModel, ConfigDict


class ProcessingRequestCreate(BaseModel):
    """Body of ``POST /post_processing_request``."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "engine": 0,
                "images": {
                    "image1.jpg": "https://server.cz/image1.jpg",
                    "image2.jpg": None,
                    "image3.jpg": "https://server.cz/image3.jpg",
                },
            }
        }
    )

    engine: int
    images: Dict[str, Optional[str]]


class CreateUserRequest(BaseModel):
    """Body of ``POST /admin/users``."""
    owner: str
    permission: str = "USER"  # "USER" or "SUPER_USER"


class SuspendUserRequest(BaseModel):
    """Body of ``PUT /admin/users/{user_id}/suspension``."""
    suspended: bool
