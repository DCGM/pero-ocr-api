"""
Background async tasks that run during the application lifespan.

Replaces the old APScheduler-based background jobs with native
``asyncio`` loops running inside FastAPI's lifespan context.
"""

import asyncio
import datetime
import logging
import os
import shutil

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ApiKey, Engine, Page, PageState, Request
from db.session import get_session_maker

logger = logging.getLogger(__name__)


async def processing_timeout_loop(settings) -> None:
    """
    Every 60 seconds, find PROCESSING pages that have exceeded the timeout,
    reset them to WAITING, and send an email notification if configured.
    """
    from app.services.mail_service import send_notification_mail_async

    while True:
        try:
            await asyncio.sleep(60)
            session_maker = get_session_maker()
            async with session_maker() as db:
                try:
                    from app.crud.page import reset_timed_out_pages
                    pages = await reset_timed_out_pages(db, timeout_seconds=60)

                    if pages and settings.EMAIL_NOTIFICATION_ADDRESSES:
                        message_body = ""
                        for page in pages:
                            # Fetch related objects for the notification
                            req_result = await db.execute(
                                select(Request).where(Request.id == page.request_id)
                            )
                            req = req_result.scalar_one_or_none()
                            if req is None:
                                continue

                            eng_result = await db.execute(
                                select(Engine).where(Engine.id == req.engine_id)
                            )
                            engine = eng_result.scalar_one_or_none()
                            key_result = await db.execute(
                                select(ApiKey).where(ApiKey.id == req.api_key_id)
                            )
                            api_key = key_result.scalar_one_or_none()

                            message_body += (
                                f"owner_api_key: {api_key.api_string if api_key else 'N/A'}<br>"
                                f"owner_description: {api_key.owner if api_key else 'N/A'}<br>"
                                f"engine_id: {engine.id if engine else 'N/A'}<br>"
                                f"engine_name: {engine.name if engine else 'N/A'}<br>"
                                f"request_id: {req.id}<br>"
                                f"page_id: {page.id}<br>"
                                f"page_name: {page.name}<br>"
                                f"page_url: {page.url}<br><br>"
                                "####################<br><br>"
                            )

                        if message_body:
                            await send_notification_mail_async(
                                subject="API Bot - PROCESSING TIMEOUT",
                                body=message_body,
                                settings=settings,
                            )
                except Exception:
                    logger.exception("Error in processing_timeout task")

        except asyncio.CancelledError:
            logger.info("processing_timeout_loop cancelled")
            break
        except Exception:
            logger.exception("Unexpected error in processing_timeout_loop")


async def old_files_removal_loop(settings) -> None:
    """
    Every 24 hours, expire old PROCESSED pages and delete their files.
    """
    while True:
        try:
            await asyncio.sleep(60 * 60 * 24)  # 24 hours
            session_maker = get_session_maker()
            async with session_maker() as db:
                try:
                    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=7)

                    # Expire processed pages
                    result = await db.execute(
                        select(Page)
                        .join(Request, Request.id == Page.request_id)
                        .where(Request.finish_timestamp < cutoff)
                        .where(Page.state == PageState.PROCESSED)
                    )
                    pages = result.scalars().all()
                    for page in pages:
                        page.state = PageState.EXPIRED
                    await db.commit()

                    # Delete files for old requests
                    result = await db.execute(
                        select(Request).where(Request.finish_timestamp < cutoff)
                    )
                    requests = result.scalars().all()
                    for req in requests:
                        rid = str(req.id)
                        for folder in [
                            settings.PROCESSED_REQUESTS_FOLDER,
                            settings.UPLOAD_IMAGES_FOLDER,
                        ]:
                            dir_path = os.path.join(folder, rid)
                            if os.path.isdir(dir_path):
                                await asyncio.to_thread(shutil.rmtree, dir_path)

                    logger.info(
                        "old_files_removal: expired %d pages, cleaned %d requests",
                        len(pages), len(requests),
                    )
                except Exception:
                    logger.exception("Error in old_files_removal task")

        except asyncio.CancelledError:
            logger.info("old_files_removal_loop cancelled")
            break
        except Exception:
            logger.exception("Unexpected error in old_files_removal_loop")
