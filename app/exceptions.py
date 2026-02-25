"""
Custom exceptions and global exception handlers for PERO-OCR-API.
"""

import logging
import traceback
from asyncio import to_thread

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class ApiError(Exception):
    """Base API error with status code and JSON body."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class NotFoundError(ApiError):
    def __init__(self, message: str = "Resource not found."):
        super().__init__(404, message)


class ForbiddenError(ApiError):
    def __init__(self, message: str = "Access denied."):
        super().__init__(401, message)


class ValidationError(ApiError):
    def __init__(self, message: str = "Validation error."):
        super().__init__(422, message)


class BadRequestError(ApiError):
    def __init__(self, message: str = "Bad request."):
        super().__init__(400, message)


class InsufficientCreditsError(ApiError):
    def __init__(self, message: str = "Insufficient credits."):
        super().__init__(402, message)


def register_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers on the FastAPI application."""

    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"status": "failure", "message": exc.message},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)

        # Send email notification (best-effort, non-blocking)
        try:
            from app.config import get_settings
            settings = get_settings()
            if settings.EMAIL_NOTIFICATION_ADDRESSES:
                from app.services.mail_service import send_notification_mail
                tb = traceback.format_exc().replace("\n", "<br>")
                await to_thread(
                    send_notification_mail,
                    subject="API Bot - INTERNAL SERVER ERROR",
                    body=tb,
                    settings=settings,
                )
        except Exception:
            logger.exception("Failed to send error notification email")

        return JSONResponse(
            status_code=500,
            content={"status": "failure", "message": "Internal server error."},
        )
