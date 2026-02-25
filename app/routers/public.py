"""
Public routes — no authentication required.

- ``GET /``  and ``GET /index``  — dashboard with page statistics
- ``GET /docs`` — redirect to SwaggerHub (FastAPI also serves /docs natively)
"""

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_db
from app.crud.statistics import get_page_statistics

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Public"])

# Templates are initialised once; path is relative to working dir.
# Overridden in __init__.py after app creation to set the correct path.
_templates: Jinja2Templates | None = None


def set_templates(templates: Jinja2Templates) -> None:
    global _templates
    _templates = templates


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
@router.get("/index", response_class=HTMLResponse, include_in_schema=False)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    """Dashboard page showing page statistics for the last 24 hours."""
    state_stats = await get_page_statistics(db)
    return _templates.TemplateResponse(
        request, "index.html", context={"data": state_stats},
    )


@router.get("/docs_redirect", tags=["Public"])
async def documentation():
    """Redirect to the SwaggerHub API documentation."""
    return RedirectResponse(
        url="https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.4"
    )
