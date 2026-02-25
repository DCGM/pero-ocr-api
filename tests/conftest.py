"""
Test configuration and fixtures for PERO-API tests.

This conftest sets up a fake config module with a SQLite test database
BEFORE importing the application, ensuring all module-level initialization
uses test values.

Requirements: all app dependencies must be installed (see requirements_server.txt).
Additionally install: pytest
"""
import sys
import os
import shutil
import tempfile
import types
import datetime
import uuid
import zipfile
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# 1. Mock optional dependencies that may not be installed in test env
# ---------------------------------------------------------------------------

# psycopg2 – only needed at runtime for PostgreSQL; tests use SQLite
try:
    import psycopg2.extras
except ImportError:
    _p = types.ModuleType('psycopg2')
    _pe = types.ModuleType('psycopg2.extras')
    _pe.register_uuid = lambda: None
    _p.extras = _pe
    sys.modules['psycopg2'] = _p
    sys.modules['psycopg2.extras'] = _pe


class _NoOpExtension:
    """Replacement for Flask UI extensions not needed during API testing."""
    def __init__(self, app=None, **kwargs):
        pass

    def init_app(self, app, **kwargs):
        pass


for _mod_name, _cls_name in [
    ('flask_bootstrap', 'Bootstrap'),
    ('flask_jsglue', 'JSGlue'),
    ('flask_dropzone', 'Dropzone'),
]:
    try:
        __import__(_mod_name)
    except ImportError:
        _m = types.ModuleType(_mod_name)
        setattr(_m, _cls_name, _NoOpExtension)
        sys.modules[_mod_name] = _m

# ---------------------------------------------------------------------------
# 2. Set up test config module BEFORE importing app
# ---------------------------------------------------------------------------
_TEST_TMPDIR = tempfile.mkdtemp(prefix='pero_api_test_')

_config_module = types.ModuleType('config')
_config_module.database_url = (
    'sqlite:///' + os.path.join(_TEST_TMPDIR, 'test.db').replace('\\', '/')
)


class _TestConfig:
    DEBUG = True
    TESTING = True
    SECRET_KEY = 'test-secret-key'
    PROCESSED_REQUESTS_FOLDER = os.path.join(_TEST_TMPDIR, 'processed_requests')
    MODELS_FOLDER = os.path.join(_TEST_TMPDIR, 'models')
    UPLOAD_IMAGES_FOLDER = os.path.join(_TEST_TMPDIR, 'images')
    ALLOWED_IMAGE_EXTENSIONS = {'jpg', 'jpeg', 'png'}
    APPLICATION_ROOT = ''
    EMAIL_NOTIFICATION_ADDRESSES = []
    MAX_EMAIL_FREQUENCY = 3600
    MAIL_SERVER = 'smtp.example.com'
    MAIL_USERNAME = ''
    MAIL_PASSWORD = ''


_config_module.Config = _TestConfig
sys.modules['config'] = _config_module

# ---------------------------------------------------------------------------
# 3. Now safe to import app (module-level code will use test config)
# ---------------------------------------------------------------------------
from app import create_app, session_factory  # noqa: E402
from app.db import Base  # noqa: E402
from app.db.model import (  # noqa: E402
    ApiKey, Permission, Engine, EngineVersion, Model,
    EngineVersionModel, Request, Page, PageState, Notification,
)

# ---------------------------------------------------------------------------
# 4. Constants
# ---------------------------------------------------------------------------
USER_KEY = 'test-user-key-123'
SUPER_USER_KEY = 'test-super-key-456'
OTHER_USER_KEY = 'test-other-user-key-789'

# ---------------------------------------------------------------------------
# 5. Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope='session')
def app():
    """Create and configure the Flask application for testing."""
    with patch('app.BackgroundScheduler') as mock_sched:
        mock_sched.return_value = MagicMock()
        application = create_app()
    application.config['TESTING'] = True
    yield application
    shutil.rmtree(_TEST_TMPDIR, ignore_errors=True)


@pytest.fixture(scope='session')
def client(app):
    """Flask test client (session-scoped, reused across tests)."""
    return app.test_client()


@pytest.fixture(autouse=True)
def clean_db(app):
    """Reset all database tables and temp directories before each test."""
    db = session_factory()
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    # Re-add notification singleton required by the app
    db.add(Notification(datetime.datetime(1970, 1, 1)))
    db.commit()
    db.close()

    # Clean up temp file directories
    for folder in [
        _TestConfig.PROCESSED_REQUESTS_FOLDER,
        _TestConfig.UPLOAD_IMAGES_FOLDER,
        _TestConfig.MODELS_FOLDER,
    ]:
        if os.path.isdir(folder):
            shutil.rmtree(folder)
        os.makedirs(folder, exist_ok=True)


# ---- API Key fixtures ----

@pytest.fixture
def user_key():
    """Create a USER API key and return the key string."""
    db = session_factory()
    db.add(ApiKey(USER_KEY, 'Test User', Permission.USER))
    db.commit()
    db.close()
    return USER_KEY


@pytest.fixture
def super_user_key():
    """Create a SUPER_USER API key and return the key string."""
    db = session_factory()
    db.add(ApiKey(SUPER_USER_KEY, 'Test Worker', Permission.SUPER_USER))
    db.commit()
    db.close()
    return SUPER_USER_KEY


@pytest.fixture
def other_user_key():
    """Create another USER API key for ownership tests."""
    db = session_factory()
    db.add(ApiKey(OTHER_USER_KEY, 'Other User', Permission.USER))
    db.commit()
    db.close()
    return OTHER_USER_KEY


# ---- Engine fixtures ----

@pytest.fixture
def engine_with_models():
    """
    Create an Engine with one EngineVersion and 2 Models (layout + OCR).
    Also creates model directories with dummy files on disk.
    Returns the engine id.
    """
    db = session_factory()
    eng = Engine('test_engine', 'A test OCR engine')
    db.add(eng)
    db.commit()

    ev = EngineVersion('v1.0.0', eng.id, 'Test version')
    db.add(ev)
    db.commit()

    m1 = Model('layout_model', '[LAYOUT_PARSER]\nMETHOD = test\n')
    m2 = Model('ocr_model', '[OCR]\nMETHOD = test\n')
    db.add(m1)
    db.add(m2)
    db.commit()

    db.add(EngineVersionModel(ev.id, m1.id))
    db.add(EngineVersionModel(ev.id, m2.id))
    db.commit()

    # Create model directories with dummy weight files
    for model in [m1, m2]:
        model_dir = os.path.join(_TestConfig.MODELS_FOLDER, model.name)
        os.makedirs(model_dir, exist_ok=True)
        with open(os.path.join(model_dir, 'weights.bin'), 'wb') as f:
            f.write(b'dummy model data')

    engine_id = eng.id
    db.close()
    return engine_id


# ---- Helper to create requests with pages ----

def _create_request_with_pages(user_key_str, engine_id, pages_spec):
    """
    Create a Request and Pages in the DB.
    pages_spec: list of (name, url_or_none, PageState) tuples
    Returns (request_id_str, {page_name: page_id_str})
    """
    db = session_factory()
    api_key = db.query(ApiKey).filter(ApiKey.api_string == user_key_str).first()
    req = Request(engine_id, api_key.id)
    db.add(req)
    db.commit()

    page_ids = {}
    for name, url, state in pages_spec:
        ts = datetime.datetime.utcnow() if state == PageState.WAITING else None
        page = Page(name, url, state, req.id, waiting_timestamp=ts)
        if state == PageState.PROCESSING:
            page.processing_timestamp = datetime.datetime.now()
        db.add(page)
        db.commit()
        page_ids[name] = str(page.id)

    request_id = str(req.id)
    db.close()
    return request_id, page_ids


# ---- Request fixtures ----

@pytest.fixture
def request_with_waiting_pages(user_key, engine_with_models):
    """
    Request with 2 WAITING pages.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    rid, pids = _create_request_with_pages(
        user_key, engine_with_models,
        [
            ('page_001', 'http://example.com/img1.jpg', PageState.WAITING),
            ('page_002', 'http://example.com/img2.jpg', PageState.WAITING),
        ]
    )
    return rid, user_key, pids, engine_with_models


@pytest.fixture
def request_with_created_page(user_key, engine_with_models):
    """
    Request with 1 CREATED page (image upload pending).
    Returns (request_id, user_key, page_ids, engine_id).
    """
    rid, pids = _create_request_with_pages(
        user_key, engine_with_models,
        [('page_upload', None, PageState.CREATED)]
    )
    return rid, user_key, pids, engine_with_models


@pytest.fixture
def request_with_processing_page(user_key, engine_with_models):
    """
    Request with 1 PROCESSING page.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    rid, pids = _create_request_with_pages(
        user_key, engine_with_models,
        [('page_proc', 'http://example.com/img_proc.jpg', PageState.PROCESSING)]
    )
    return rid, user_key, pids, engine_with_models


@pytest.fixture
def request_with_mixed_pages(user_key, engine_with_models):
    """
    Request with pages in CREATED, WAITING, and PROCESSING states.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    rid, pids = _create_request_with_pages(
        user_key, engine_with_models,
        [
            ('page_c', None, PageState.CREATED),
            ('page_w', 'http://example.com/img.jpg', PageState.WAITING),
            ('page_p', 'http://example.com/img2.jpg', PageState.PROCESSING),
        ]
    )
    return rid, user_key, pids, engine_with_models


@pytest.fixture
def request_with_processed_page(user_key, engine_with_models):
    """
    Request with 1 PROCESSED page and corresponding results ZIP on disk.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    db = session_factory()
    api_key = db.query(ApiKey).filter(ApiKey.api_string == user_key).first()
    req = Request(engine_with_models, api_key.id)
    db.add(req)
    db.commit()

    page = Page('page_done', 'http://example.com/img_done.jpg',
                PageState.PROCESSED, req.id)
    page.score = 95.5
    page.finish_timestamp = datetime.datetime.utcnow()
    db.add(page)
    db.commit()

    req.finish_timestamp = datetime.datetime.utcnow()
    db.commit()

    # Create results ZIP file
    results_dir = os.path.join(
        _TestConfig.PROCESSED_REQUESTS_FOLDER, str(req.id)
    )
    os.makedirs(results_dir, exist_ok=True)
    zip_path = os.path.join(results_dir, str(req.id) + '.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('page_done_alto.xml', '<alto>test alto content</alto>')
        zf.writestr('page_done_page.xml', '<page>test page content</page>')
        zf.writestr('page_done.txt', 'test text content')

    request_id = str(req.id)
    page_id = str(page.id)
    db.close()
    return request_id, user_key, {'page_done': page_id}, engine_with_models


@pytest.fixture
def request_with_expired_page(user_key, engine_with_models):
    """
    Request with 1 EXPIRED page (results already removed).
    Returns (request_id, user_key, page_ids, engine_id).
    """
    db = session_factory()
    api_key = db.query(ApiKey).filter(ApiKey.api_string == user_key).first()
    req = Request(engine_with_models, api_key.id)
    db.add(req)
    db.commit()

    page = Page('page_expired', 'http://example.com/img_exp.jpg',
                PageState.EXPIRED, req.id)
    page.finish_timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=10)
    db.add(page)
    db.commit()

    req.finish_timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=10)
    db.commit()

    request_id = str(req.id)
    page_id = str(page.id)
    db.close()
    return request_id, user_key, {'page_expired': page_id}, engine_with_models


@pytest.fixture
def request_with_uploaded_image(user_key, engine_with_models):
    """
    Request with a WAITING page and a corresponding image file on disk.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    db = session_factory()
    api_key = db.query(ApiKey).filter(ApiKey.api_string == user_key).first()
    req = Request(engine_with_models, api_key.id)
    db.add(req)
    db.commit()

    page = Page('page_img',
                f'http://localhost/download_image/{req.id}/page_img.jpg',
                PageState.WAITING, req.id,
                waiting_timestamp=datetime.datetime.utcnow())
    db.add(page)
    db.commit()

    # Place the image file on disk
    img_dir = os.path.join(_TestConfig.UPLOAD_IMAGES_FOLDER, str(req.id))
    os.makedirs(img_dir, exist_ok=True)
    with open(os.path.join(img_dir, 'page_img.jpg'), 'wb') as f:
        # Minimal JPEG-like header + padding
        f.write(b'\xff\xd8\xff\xe0' + b'\x00' * 100)

    request_id = str(req.id)
    page_id = str(page.id)
    db.close()
    return request_id, user_key, {'page_img': page_id}, engine_with_models


# ---- Additional fixtures for corner-case tests ----

SUSPENDED_USER_KEY = 'test-suspended-key-999'


@pytest.fixture
def suspended_user_key(engine_with_models):
    """Create a USER API key that is suspended."""
    db = session_factory()
    key = ApiKey(SUSPENDED_USER_KEY, 'Suspended User', Permission.USER)
    key.suspension = True
    db.add(key)
    db.commit()
    db.close()
    return SUSPENDED_USER_KEY


@pytest.fixture
def suspended_user_request_with_waiting_page(suspended_user_key, engine_with_models):
    """
    A request belonging to a suspended user with a WAITING page.
    Returns (request_id, suspended_user_key, page_ids, engine_id).
    """
    rid, pids = _create_request_with_pages(
        suspended_user_key, engine_with_models,
        [('page_suspended', 'http://example.com/suspended.jpg', PageState.WAITING)]
    )
    return rid, suspended_user_key, pids, engine_with_models


SECOND_USER_KEY = 'test-second-user-key-222'


@pytest.fixture
def second_user_key():
    """Create a second USER API key for multi-user fairness tests."""
    db = session_factory()
    db.add(ApiKey(SECOND_USER_KEY, 'Second User', Permission.USER))
    db.commit()
    db.close()
    return SECOND_USER_KEY


@pytest.fixture
def two_users_waiting_pages(user_key, second_user_key, engine_with_models):
    """
    Two users each with WAITING pages. Used for fair-scheduling tests.
    Returns (user1_rid, user2_rid, user_key, second_user_key, engine_id).
    """
    rid1, pids1 = _create_request_with_pages(
        user_key, engine_with_models,
        [('user1_page', 'http://example.com/u1.jpg', PageState.WAITING)]
    )
    rid2, pids2 = _create_request_with_pages(
        second_user_key, engine_with_models,
        [('user2_page', 'http://example.com/u2.jpg', PageState.WAITING)]
    )
    return rid1, rid2, pids1, pids2, user_key, second_user_key, engine_with_models


@pytest.fixture
def request_with_one_processed_one_waiting(user_key, engine_with_models):
    """
    Request with 1 PROCESSED page and 1 WAITING page.
    Used for partial-cancel and request-completion tests.
    Returns (request_id, user_key, page_ids, engine_id).
    """
    db = session_factory()
    api_key = db.query(ApiKey).filter(ApiKey.api_string == user_key).first()
    req = Request(engine_with_models, api_key.id)
    db.add(req)
    db.commit()

    processed_page = Page('page_already_done', 'http://example.com/done.jpg',
                          PageState.PROCESSED, req.id)
    processed_page.score = 90.0
    processed_page.finish_timestamp = datetime.datetime.utcnow()
    db.add(processed_page)
    db.commit()

    waiting_page = Page('page_still_waiting', 'http://example.com/waiting.jpg',
                        PageState.WAITING, req.id,
                        waiting_timestamp=datetime.datetime.utcnow())
    db.add(waiting_page)
    db.commit()

    request_id = str(req.id)
    page_ids = {
        'page_already_done': str(processed_page.id),
        'page_still_waiting': str(waiting_page.id),
    }
    db.close()
    return request_id, user_key, page_ids, engine_with_models
