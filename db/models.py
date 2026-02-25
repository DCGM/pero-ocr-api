"""
SQLAlchemy 2.x ORM models for PERO-OCR-API.

Uses ``Mapped[]`` / ``mapped_column()`` style.  The custom GUID
TypeDecorator has been replaced with the native SQLAlchemy ``Uuid`` type
which stores as native UUID on PostgreSQL and CHAR(32) on other dialects.

All timestamp defaults use ``datetime.now(datetime.UTC)`` for consistency.
"""

import enum
import uuid
import datetime
from typing import Optional, List

from sqlalchemy import (
    Enum, ForeignKey, Integer, String, DateTime, Float, Boolean, Uuid, func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.base import Base


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PageState(enum.Enum):
    """Lifecycle states of an OCR page."""
    CREATED = "Page was created."
    WAITING = "Page is waiting for processing."
    PROCESSING = "Page is being processed."
    NOT_FOUND = "Page image was not found."
    INVALID_FILE = "Page image is invalid."
    PROCESSING_FAILED = "Page processing failed."
    PROCESSED = "Page was processed."
    CANCELED = "Page processing was canceled."
    EXPIRED = "Page expired."


class Permission(enum.Enum):
    """API key permission levels."""
    SUPER_USER = "User can take and process requests."
    USER = "User can create requests."


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class ApiKey(Base):
    __tablename__ = "api_key"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    api_string: Mapped[str] = mapped_column(String, nullable=False, index=True)
    owner: Mapped[str] = mapped_column(String, nullable=False)
    permission: Mapped[Permission] = mapped_column(Enum(Permission), nullable=False)
    suspension: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    requests: Mapped[List["Request"]] = relationship(back_populates="api_key")


class Request(Base):
    __tablename__ = "request"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    creation_timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, index=True,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    modification_timestamp: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, index=True,
        default=lambda: datetime.datetime.now(datetime.UTC),
    )
    finish_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True, index=True,
    )

    engine_id: Mapped[int] = mapped_column(Integer, ForeignKey("engine.id"), nullable=False)
    api_key_id: Mapped[int] = mapped_column(Integer, ForeignKey("api_key.id"), nullable=False)

    api_key: Mapped["ApiKey"] = relationship(back_populates="requests")
    engine: Mapped["Engine"] = relationship(viewonly=True)
    pages: Mapped[List["Page"]] = relationship(back_populates="request")


class Page(Base):
    __tablename__ = "page"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    state: Mapped[PageState] = mapped_column(Enum(PageState), nullable=False, index=True)
    score: Mapped[Optional[float]] = mapped_column(Float, nullable=True, index=True)
    traceback: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    waiting_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True, index=True,
    )
    processing_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True,
    )
    finish_timestamp: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True, index=True,
    )

    request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("request.id"), nullable=False, index=True,
    )
    engine_version_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("engine_version.id"), nullable=True, index=True,
    )

    request: Mapped["Request"] = relationship(back_populates="pages")
    engine_version_rel: Mapped[Optional["EngineVersion"]] = relationship(viewonly=True)


class Engine(Base):
    __tablename__ = "engine"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    versions: Mapped[List["EngineVersion"]] = relationship(back_populates="engine")


class EngineVersion(Base):
    __tablename__ = "engine_version"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    engine_id: Mapped[int] = mapped_column(Integer, ForeignKey("engine.id"), nullable=False)

    engine: Mapped["Engine"] = relationship(back_populates="versions")
    models: Mapped[List["EngineVersionModel"]] = relationship(back_populates="engine_version")


class EngineVersionModel(Base):
    __tablename__ = "engine_version_model"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    engine_version_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("engine_version.id"), nullable=False,
    )
    model_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("model.id"), nullable=False,
    )

    engine_version: Mapped["EngineVersion"] = relationship(back_populates="models")
    model: Mapped["Model"] = relationship(viewonly=True)


class Model(Base):
    __tablename__ = "model"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[str] = mapped_column(String, nullable=False)


class Notification(Base):
    __tablename__ = "notification"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_notification: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False)
