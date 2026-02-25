"""Pydantic models for request bodies and parameters."""

from typing import Dict, Optional
from pydantic import BaseModel


class ProcessingRequestCreate(BaseModel):
    """Body of ``POST /post_processing_request``."""
    engine: int
    images: Dict[str, Optional[str]]
