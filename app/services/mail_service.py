"""
Email notification service.

Wraps the synchronous ``drymail`` library via ``asyncio.to_thread()``
so it can be called from async code without blocking.
"""

import asyncio
import logging

from drymail import SMTPMailer, Message

logger = logging.getLogger(__name__)


def _send_mail_sync(
    subject: str,
    body: str,
    sender: tuple[str, str],
    password: str,
    recipients: list[str],
    host: str,
) -> None:
    """Send an HTML email (synchronous)."""
    if password:
        client = SMTPMailer(host=host, user=sender[1], password=password, tls=True)
    else:
        client = SMTPMailer(host=host)
    message = Message(subject=subject, sender=sender, receivers=recipients, html=body)
    client.send(message)


def send_notification_mail(
    subject: str,
    body: str,
    settings,
) -> None:
    """
    Synchronous convenience wrapper used from ``asyncio.to_thread()``.
    Reads SMTP settings from the provided ``Settings`` object.
    """
    if not settings.EMAIL_NOTIFICATION_ADDRESSES:
        return
    try:
        _send_mail_sync(
            subject=subject,
            body=body,
            sender=("PERO OCR - API BOT", settings.MAIL_USERNAME),
            password=settings.MAIL_PASSWORD,
            recipients=settings.EMAIL_NOTIFICATION_ADDRESSES,
            host=settings.MAIL_SERVER,
        )
    except Exception:
        logger.exception("Failed to send notification email: %s", subject)


async def send_notification_mail_async(
    subject: str,
    body: str,
    settings,
) -> None:
    """Async version — dispatches to thread pool."""
    await asyncio.to_thread(send_notification_mail, subject, body, settings)
