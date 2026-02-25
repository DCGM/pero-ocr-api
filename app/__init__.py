"""
PERO-OCR-API — FastAPI application factory.

Usage::

    from app import create_app
    app = create_app()
"""

import asyncio
import datetime
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from sqlalchemy import select

from db.base import Base
from db.models import Notification
from db.session import get_async_session, get_engine, get_session_maker, init_engine

from app.config import Settings, get_settings
from app.exceptions import register_exception_handlers

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and return the FastAPI application."""
    if settings is None:
        settings = get_settings()

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if settings.DEBUG else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # --- Startup ---
        engine = init_engine(
            settings.DATABASE_URL,
            pool_size=settings.DATABASE_POOL_SIZE,
            max_overflow=settings.DATABASE_MAX_OVERFLOW,
            pool_timeout=settings.DATABASE_POOL_TIMEOUT,
            pool_recycle=settings.DATABASE_POOL_RECYCLE,
        )

        # Create tables (for development / SQLite; production uses Alembic)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Ensure required directories exist
        for folder in [
            settings.PROCESSED_REQUESTS_FOLDER,
            settings.MODELS_FOLDER,
            settings.UPLOAD_IMAGES_FOLDER,
        ]:
            Path(folder).mkdir(parents=True, exist_ok=True)

        # Ensure the notification singleton row exists
        session_maker = get_session_maker()
        async with session_maker() as db:
            result = await db.execute(select(Notification).limit(1))
            notif = result.scalar_one_or_none()
            if notif is not None:
                notif.last_notification = datetime.datetime(1970, 1, 1)
            else:
                db.add(Notification(last_notification=datetime.datetime(1970, 1, 1)))
            await db.commit()

        # Start background tasks
        from app.background.tasks import (
            old_files_removal_loop,
            processing_timeout_loop,
        )
        bg_tasks = [
            asyncio.create_task(processing_timeout_loop(settings)),
            asyncio.create_task(old_files_removal_loop(settings)),
        ]

        logger.info("PERO-OCR-API started")
        yield

        # --- Shutdown ---
        for task in bg_tasks:
            task.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)

        await get_engine().dispose()
        logger.info("PERO-OCR-API shut down")

    app = FastAPI(
        title="PERO-OCR-API",
        description="OCR processing API powered by PERO OCR",
        version="2.0.0",
        lifespan=lifespan,
    )

    # Register exception handlers
    register_exception_handlers(app)

    # Set up Jinja2 templates
    templates_dir = os.path.join(os.path.dirname(__file__), "templates")
    templates = Jinja2Templates(directory=templates_dir)

    # Register routers
    from app.routers.public import router as public_router, set_templates
    from app.routers.user import router as user_router
    from app.routers.worker import router as worker_router
    from app.routers.admin import router as admin_router

    set_templates(templates)
    app.include_router(public_router)
    app.include_router(user_router)
    app.include_router(worker_router)
    app.include_router(admin_router)

    return app
