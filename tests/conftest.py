"""
Test configuration and fixtures for PERO-OCR-API (FastAPI + async SQLAlchemy 2.x).

Uses:
- pytest-asyncio (auto mode via pytest.ini)
- httpx.AsyncClient with ASGITransport
- aiosqlite for test database
"""

import os
import shutil
import tempfile
import datetime
import uuid
import zipfile

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from db.base import Base
from db.models import (
    ApiKey,
    Engine,
    EngineVersion,
    EngineVersionModel,
    Model,
    Notification,
    Page,
    PageState,
    Permission,
    Request,
)
from app.config import Settings

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_KEY = "test-user-key-123"
SUPER_USER_KEY = "test-super-key-456"
OTHER_USER_KEY = "test-other-user-key-789"
SUSPENDED_USER_KEY = "test-suspended-key-999"
SECOND_USER_KEY = "test-second-user-key-222"
NO_CREDITS_KEY = "test-no-credits-key-000"
LOW_CREDITS_KEY = "test-low-credits-key-111"

# ---------------------------------------------------------------------------
# Test infrastructure
# ---------------------------------------------------------------------------

_TEST_TMPDIR = tempfile.mkdtemp(prefix="pero_api_test_")

_test_settings = Settings(
    DATABASE_URL=(
        "sqlite+aiosqlite:///"
        + os.path.join(_TEST_TMPDIR, "test.db").replace("\\", "/")
    ),
    DEBUG=True,
    PROCESSED_REQUESTS_FOLDER=os.path.join(_TEST_TMPDIR, "processed_requests"),
    MODELS_FOLDER=os.path.join(_TEST_TMPDIR, "models"),
    UPLOAD_IMAGES_FOLDER=os.path.join(_TEST_TMPDIR, "images"),
    EMAIL_NOTIFICATION_ADDRESSES=[],
    APPLICATION_ROOT="",
)

_engine = create_async_engine(
    _test_settings.DATABASE_URL,
    connect_args={"check_same_thread": False},
)
_session_maker = async_sessionmaker(
    _engine, expire_on_commit=False, autoflush=False,
)

_tables_created = False


async def _ensure_tables():
    global _tables_created
    if not _tables_created:
        async with _engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        _tables_created = True


# ---------------------------------------------------------------------------
# Dependency overrides
# ---------------------------------------------------------------------------

async def _override_get_db():
    async with _session_maker() as session:
        yield session


def _override_get_settings():
    return _test_settings


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client():
    """HTTP client backed by the FastAPI app with a clean test DB."""
    await _ensure_tables()

    # Reset all tables
    async with _session_maker() as session:
        for table in reversed(Base.metadata.sorted_tables):
            await session.execute(table.delete())
        session.add(Notification(last_notification=datetime.datetime(1970, 1, 1)))
        await session.commit()

    # Reset directories
    for folder in [
        _test_settings.PROCESSED_REQUESTS_FOLDER,
        _test_settings.UPLOAD_IMAGES_FOLDER,
        _test_settings.MODELS_FOLDER,
    ]:
        if os.path.isdir(folder):
            shutil.rmtree(folder)
        os.makedirs(folder, exist_ok=True)

    from app import create_app
    from app.dependencies import get_db
    from app.config import get_settings

    app = create_app(_test_settings)
    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_settings] = _override_get_settings

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        yield ac


# ---------------------------------------------------------------------------
# API key fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def user_key(client):
    """Create a USER API key and return the key string."""
    async with _session_maker() as session:
        session.add(
            ApiKey(api_string=USER_KEY, owner="Test User", permission=Permission.USER,
                   credit_balance=10000.0)
        )
        await session.commit()
    return USER_KEY


@pytest_asyncio.fixture
async def super_user_key(client):
    """Create a SUPER_USER API key and return the key string."""
    async with _session_maker() as session:
        session.add(
            ApiKey(
                api_string=SUPER_USER_KEY,
                owner="Test Worker",
                permission=Permission.SUPER_USER,
                credit_balance=10000.0,
            )
        )
        await session.commit()
    return SUPER_USER_KEY


@pytest_asyncio.fixture
async def other_user_key(client):
    """Create another USER API key for ownership tests."""
    async with _session_maker() as session:
        session.add(
            ApiKey(
                api_string=OTHER_USER_KEY,
                owner="Other User",
                permission=Permission.USER,
                credit_balance=10000.0,
            )
        )
        await session.commit()
    return OTHER_USER_KEY


# ---------------------------------------------------------------------------
# Engine fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def engine_with_models(client):
    """
    Create an Engine with one EngineVersion and 2 Models (layout + OCR).
    Also creates model directories with dummy files on disk.
    Returns the engine id.
    """
    async with _session_maker() as session:
        eng = Engine(name="test_engine", description="A test OCR engine", cost_per_page=1.0)
        session.add(eng)
        await session.flush()

        ev = EngineVersion(
            version="v1.0.0", engine_id=eng.id, description="Test version"
        )
        session.add(ev)
        await session.flush()

        m1 = Model(name="layout_model", config="[LAYOUT_PARSER]\nMETHOD = test\n")
        m2 = Model(name="ocr_model", config="[OCR]\nMETHOD = test\n")
        session.add_all([m1, m2])
        await session.flush()

        session.add_all(
            [
                EngineVersionModel(engine_version_id=ev.id, model_id=m1.id),
                EngineVersionModel(engine_version_id=ev.id, model_id=m2.id),
            ]
        )
        await session.commit()

        # Create model directories with dummy weight files
        for model in [m1, m2]:
            model_dir = os.path.join(_test_settings.MODELS_FOLDER, model.name)
            os.makedirs(model_dir, exist_ok=True)
            with open(os.path.join(model_dir, "weights.bin"), "wb") as f:
                f.write(b"dummy model data")

        engine_id = eng.id
    return engine_id


# ---------------------------------------------------------------------------
# Helper to create requests with pages
# ---------------------------------------------------------------------------

async def _create_request_with_pages(user_key_str, engine_id, pages_spec):
    """
    Create a Request and Pages in the DB.
    pages_spec: list of (name, url_or_none, PageState) tuples
    Returns (request_id_str, {page_name: page_id_str})
    """
    async with _session_maker() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.api_string == user_key_str)
        )
        api_key = result.scalar_one()

        req = Request(engine_id=engine_id, api_key_id=api_key.id)
        session.add(req)
        await session.flush()

        page_ids = {}
        for name, url, state in pages_spec:
            ts = datetime.datetime.utcnow() if state == PageState.WAITING else None
            page = Page(
                name=name,
                url=url,
                state=state,
                request_id=req.id,
                waiting_timestamp=ts,
                cost=1.0,
            )
            if state == PageState.PROCESSING:
                page.processing_timestamp = datetime.datetime.now()
            session.add(page)
            await session.flush()
            page_ids[name] = str(page.id)

        request_id = str(req.id)
        await session.commit()
    return request_id, page_ids


# ---------------------------------------------------------------------------
# Request fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def request_with_waiting_pages(user_key, engine_with_models):
    """Request with 2 WAITING pages."""
    rid, pids = await _create_request_with_pages(
        user_key,
        engine_with_models,
        [
            ("page_001", "http://example.com/img1.jpg", PageState.WAITING),
            ("page_002", "http://example.com/img2.jpg", PageState.WAITING),
        ],
    )
    return rid, user_key, pids, engine_with_models


@pytest_asyncio.fixture
async def request_with_created_page(user_key, engine_with_models):
    """Request with 1 CREATED page (image upload pending)."""
    rid, pids = await _create_request_with_pages(
        user_key,
        engine_with_models,
        [("page_upload", None, PageState.CREATED)],
    )
    return rid, user_key, pids, engine_with_models


@pytest_asyncio.fixture
async def request_with_processing_page(user_key, engine_with_models):
    """Request with 1 PROCESSING page."""
    rid, pids = await _create_request_with_pages(
        user_key,
        engine_with_models,
        [("page_proc", "http://example.com/img_proc.jpg", PageState.PROCESSING)],
    )
    return rid, user_key, pids, engine_with_models


@pytest_asyncio.fixture
async def request_with_mixed_pages(user_key, engine_with_models):
    """Request with pages in CREATED, WAITING, and PROCESSING states."""
    rid, pids = await _create_request_with_pages(
        user_key,
        engine_with_models,
        [
            ("page_c", None, PageState.CREATED),
            ("page_w", "http://example.com/img.jpg", PageState.WAITING),
            ("page_p", "http://example.com/img2.jpg", PageState.PROCESSING),
        ],
    )
    return rid, user_key, pids, engine_with_models


@pytest_asyncio.fixture
async def request_with_processed_page(user_key, engine_with_models):
    """Request with 1 PROCESSED page and corresponding results ZIP on disk."""
    async with _session_maker() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.api_string == user_key)
        )
        api_key = result.scalar_one()

        req = Request(engine_id=engine_with_models, api_key_id=api_key.id)
        session.add(req)
        await session.flush()

        page = Page(
            name="page_done",
            url="http://example.com/img_done.jpg",
            state=PageState.PROCESSED,
            request_id=req.id,
            score=95.5,
            finish_timestamp=datetime.datetime.utcnow(),
            cost=1.0,
        )
        session.add(page)
        await session.flush()

        req.finish_timestamp = datetime.datetime.utcnow()
        await session.commit()

        # Create results ZIP file
        results_dir = os.path.join(
            _test_settings.PROCESSED_REQUESTS_FOLDER, str(req.id)
        )
        os.makedirs(results_dir, exist_ok=True)
        zip_path = os.path.join(results_dir, str(req.id) + ".zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("page_done_alto.xml", "<alto>test alto content</alto>")
            zf.writestr("page_done_page.xml", "<page>test page content</page>")
            zf.writestr("page_done.txt", "test text content")

        return str(req.id), user_key, {"page_done": str(page.id)}, engine_with_models


@pytest_asyncio.fixture
async def request_with_expired_page(user_key, engine_with_models):
    """Request with 1 EXPIRED page (results already removed)."""
    async with _session_maker() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.api_string == user_key)
        )
        api_key = result.scalar_one()

        req = Request(engine_id=engine_with_models, api_key_id=api_key.id)
        session.add(req)
        await session.flush()

        page = Page(
            name="page_expired",
            url="http://example.com/img_exp.jpg",
            state=PageState.EXPIRED,
            request_id=req.id,
            finish_timestamp=datetime.datetime.utcnow() - datetime.timedelta(days=10),
            cost=1.0,
        )
        session.add(page)
        await session.flush()

        req.finish_timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=10)
        await session.commit()

        return (
            str(req.id),
            user_key,
            {"page_expired": str(page.id)},
            engine_with_models,
        )


@pytest_asyncio.fixture
async def request_with_uploaded_image(user_key, engine_with_models):
    """Request with a WAITING page and a corresponding image file on disk."""
    async with _session_maker() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.api_string == user_key)
        )
        api_key = result.scalar_one()

        req = Request(engine_id=engine_with_models, api_key_id=api_key.id)
        session.add(req)
        await session.flush()

        page = Page(
            name="page_img",
            url=f"http://localhost/download_image/{req.id}/page_img.jpg",
            state=PageState.WAITING,
            request_id=req.id,
            waiting_timestamp=datetime.datetime.utcnow(),
            cost=1.0,
        )
        session.add(page)
        await session.flush()

        # Place the image file on disk
        img_dir = os.path.join(_test_settings.UPLOAD_IMAGES_FOLDER, str(req.id))
        os.makedirs(img_dir, exist_ok=True)
        with open(os.path.join(img_dir, "page_img.jpg"), "wb") as f:
            f.write(b"\xff\xd8\xff\xe0" + b"\x00" * 100)

        await session.commit()
        return (
            str(req.id),
            user_key,
            {"page_img": str(page.id)},
            engine_with_models,
        )


# ---------------------------------------------------------------------------
# Corner-case fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def suspended_user_key(client, engine_with_models):
    """Create a USER API key that is suspended."""
    async with _session_maker() as session:
        key = ApiKey(
            api_string=SUSPENDED_USER_KEY,
            owner="Suspended User",
            permission=Permission.USER,
            suspension=True,
            credit_balance=10000.0,
        )
        session.add(key)
        await session.commit()
    return SUSPENDED_USER_KEY


@pytest_asyncio.fixture
async def suspended_user_request_with_waiting_page(suspended_user_key, engine_with_models):
    """A request belonging to a suspended user with a WAITING page."""
    rid, pids = await _create_request_with_pages(
        suspended_user_key,
        engine_with_models,
        [("page_suspended", "http://example.com/suspended.jpg", PageState.WAITING)],
    )
    return rid, suspended_user_key, pids, engine_with_models


@pytest_asyncio.fixture
async def second_user_key(client):
    """Create a second USER API key for multi-user fairness tests."""
    async with _session_maker() as session:
        session.add(
            ApiKey(
                api_string=SECOND_USER_KEY,
                owner="Second User",
                permission=Permission.USER,
                credit_balance=10000.0,
            )
        )
        await session.commit()
    return SECOND_USER_KEY


@pytest_asyncio.fixture
async def two_users_waiting_pages(user_key, second_user_key, engine_with_models):
    """Two users each with WAITING pages.  Used for fair-scheduling tests."""
    rid1, pids1 = await _create_request_with_pages(
        user_key,
        engine_with_models,
        [("user1_page", "http://example.com/u1.jpg", PageState.WAITING)],
    )
    rid2, pids2 = await _create_request_with_pages(
        second_user_key,
        engine_with_models,
        [("user2_page", "http://example.com/u2.jpg", PageState.WAITING)],
    )
    return rid1, rid2, pids1, pids2, user_key, second_user_key, engine_with_models


@pytest_asyncio.fixture
async def request_with_one_processed_one_waiting(user_key, engine_with_models):
    """Request with 1 PROCESSED page and 1 WAITING page."""
    async with _session_maker() as session:
        result = await session.execute(
            select(ApiKey).where(ApiKey.api_string == user_key)
        )
        api_key = result.scalar_one()

        req = Request(engine_id=engine_with_models, api_key_id=api_key.id)
        session.add(req)
        await session.flush()

        processed_page = Page(
            name="page_already_done",
            url="http://example.com/done.jpg",
            state=PageState.PROCESSED,
            request_id=req.id,
            score=90.0,
            finish_timestamp=datetime.datetime.utcnow(),
            cost=1.0,
        )
        session.add(processed_page)
        await session.flush()

        waiting_page = Page(
            name="page_still_waiting",
            url="http://example.com/waiting.jpg",
            state=PageState.WAITING,
            request_id=req.id,
            waiting_timestamp=datetime.datetime.utcnow(),
            cost=1.0,
        )
        session.add(waiting_page)
        await session.flush()

        await session.commit()
        return (
            str(req.id),
            user_key,
            {
                "page_already_done": str(processed_page.id),
                "page_still_waiting": str(waiting_page.id),
            },
            engine_with_models,
        )


# ---------------------------------------------------------------------------
# Credit-specific fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def no_credits_user_key(client):
    """Create a USER API key with zero credits."""
    async with _session_maker() as session:
        session.add(
            ApiKey(
                api_string=NO_CREDITS_KEY,
                owner="Broke User",
                permission=Permission.USER,
                credit_balance=0.0,
            )
        )
        await session.commit()
    return NO_CREDITS_KEY


@pytest_asyncio.fixture
async def low_credits_user_key(client):
    """Create a USER API key with only 5 credits."""
    async with _session_maker() as session:
        session.add(
            ApiKey(
                api_string=LOW_CREDITS_KEY,
                owner="Low Credits User",
                permission=Permission.USER,
                credit_balance=5.0,
            )
        )
        await session.commit()
    return LOW_CREDITS_KEY
