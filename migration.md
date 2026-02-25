# Migration Guide: Flask → FastAPI

This document describes how to migrate from the old Flask-based PERO-OCR-API
to the new FastAPI + async SQLAlchemy 2.x stack.

---

## 1. Prerequisites

- Python 3.10+
- PostgreSQL 14+ (production) or SQLite (development/testing)

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

Key changes from the old stack:

| Removed                    | Replaced by               |
|----------------------------|---------------------------|
| Flask, Flask-Mail, Flask-WTF | FastAPI, Jinja2         |
| psycopg2                   | asyncpg                   |
| SQLAlchemy 1.3             | SQLAlchemy 2.x (async)    |
| Flask-SQLAlchemy-Session   | async_sessionmaker        |
| APScheduler                | asyncio background tasks  |
| gunicorn                   | uvicorn                   |
| config.py (module)         | .env + pydantic-settings  |

## 3. Configuration

The old `config.py` module has been replaced by **environment variables**
(loaded from a `.env` file via pydantic-settings).

1. Copy the example:
   ```bash
   cp config-example.env .env
   ```

2. Edit `.env` with your database URL and other settings:
   ```ini
   DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/pero_api
   PROCESSED_REQUESTS_FOLDER=./processed_requests
   MODELS_FOLDER=./models
   UPLOAD_IMAGES_FOLDER=./images
   ```

   **Important**: The database URL now uses the **async driver**:
   - PostgreSQL: `postgresql+asyncpg://...` (was `postgresql://...`)
   - SQLite (dev): `sqlite+aiosqlite:///./pero_api.db`

## 4. Database migration (existing databases)

The schema is unchanged. For existing databases, simply stamp the Alembic
revision without running the migration:

```bash
alembic stamp 001_initial
```

This tells Alembic that the database already has the initial schema.

For **new databases**, tables are auto-created on first startup (dev mode)
or you can run:

```bash
alembic upgrade head
```

Future schema changes should be created as Alembic migrations:

```bash
alembic revision --autogenerate -m "describe_change"
alembic upgrade head
```

## 5. Running the server

### Development

```bash
python run_app.py
# → starts uvicorn on http://0.0.0.0:5000 with hot-reload
```

### Production

```bash
uvicorn run_app:app --host 0.0.0.0 --port 5000 --workers 4
```

The old `gunicorn` + `app.wsgi:app` entry point is no longer used.

## 6. API compatibility

All endpoint paths and response JSON structures are preserved.
Processing clients (`processing_client/`) require **no changes**.

### Minor differences

| Aspect              | Old (Flask)                  | New (FastAPI)               |
|---------------------|------------------------------|-----------------------------|
| Docs page           | `/docs` → SwaggerHub redirect | `/docs` → interactive Swagger UI; `/docs_redirect` → SwaggerHub |
| Missing auth header | 401                          | 422 (FastAPI validates Header) |
| Bad JSON body       | TypeError (bug)              | 422 (Pydantic validation)   |
| Missing file upload | 400                          | 422 (FastAPI validates File) |

### Auto-generated API docs

FastAPI provides built-in interactive documentation:
- Swagger UI: `http://localhost:5000/docs`
- ReDoc: `http://localhost:5000/redoc`

## 7. Running tests

```bash
pip install -r requirements.txt   # includes test dependencies
python -m pytest tests/ -v
```

Tests use `aiosqlite` (in-memory SQLite) and `httpx.AsyncClient` with
`ASGITransport`, requiring no running server or PostgreSQL.

## 8. Old files to remove

The following files are superseded and can be safely deleted:

```
config-example.py          → replaced by config-example.env
app/main/                  → replaced by app/routers/
app/db/                    → replaced by db/ (top-level package)
app/wsgi.py                → no longer needed (uvicorn entry point)
app/mail/                  → replaced by app/services/mail_service.py
requirements_server.txt    → consolidated into requirements.txt
```

## 9. Architecture overview

```
run_app.py                  ← uvicorn entry point
app/
  __init__.py               ← FastAPI factory (create_app, lifespan)
  config.py                 ← pydantic-settings (Settings, get_settings)
  dependencies.py           ← Depends(): get_db, get_current_user, etc.
  exceptions.py             ← ApiError hierarchy + global handlers
  schemas/                  ← Pydantic request/response models
  crud/                     ← async DB operations (pure data access)
  services/                 ← file I/O, email (side-effect helpers)
  background/               ← asyncio background loops
  routers/                  ← route handlers (public, user, worker)
  templates/                ← Jinja2 HTML templates
db/
  base.py                   ← DeclarativeBase
  models.py                 ← ORM models (Mapped[] style)
  session.py                ← engine + session factory
alembic/
  env.py                    ← async migration environment
  versions/                 ← migration scripts
```
