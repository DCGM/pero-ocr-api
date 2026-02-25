# PERO-API

## Overview

REST API for document processing (OCR). Users submit images via API key authentication, workers fetch and process pages, and results are downloaded by users.

### Tech Stack

- **Framework:** FastAPI (fully async)
- **ORM:** SQLAlchemy 2.x (async, `Mapped[]` style)
- **Database:** PostgreSQL via `asyncpg` (production) / SQLite via `aiosqlite` (dev/test)
- **Migrations:** Alembic (async)
- **Config:** pydantic-settings (`.env` file)
- **Background tasks:** native `asyncio` loops (processing timeout, old file cleanup)
- **Email:** `drymail` (SMTP)
- **Server:** uvicorn
- **Tests:** pytest + pytest-asyncio + httpx

### Project Structure

```
run_app.py                   # uvicorn entry point
app/
  __init__.py                # FastAPI factory (create_app, lifespan)
  config.py                  # pydantic-settings (Settings)
  dependencies.py            # Depends(): get_db, get_current_user, etc.
  exceptions.py              # ApiError hierarchy + global handlers
  schemas/                   # Pydantic request/response models
  crud/                      # async DB operations
  services/                  # file I/O, email helpers
  background/                # asyncio background loops
  routers/                   # route handlers (public, user, worker, admin)
  templates/                 # Jinja2 HTML templates
db/
  base.py                    # DeclarativeBase
  models.py                  # ORM models
  session.py                 # async engine + session factory
alembic/                     # database migrations
processing_client/           # OCR worker daemon
scripts/                     # admin scripts
tests/                       # async test suite (101 tests)
```

### Quick Start

```bash
cp config-example.env .env   # edit DATABASE_URL etc.
pip install -r requirements.txt
python run_app.py            # starts on http://0.0.0.0:5000
```

Auto-generated API docs: `http://localhost:5000/docs` (Swagger UI) and `/redoc`.

See [migration.md](migration.md) for upgrading from the previous Flask version.

---

## Authentication & Authorization

### Mechanism

API key-based authentication via the `api-key` HTTP header.

- Every request to a protected endpoint must include the header: `api-key: <key_string>`
- Keys are stored in the `api_key` database table.
- Keys are generated using `base64(sha256(random_256_bits))` (see `app/crud/api_key.py :: generate_hash_key()`).

### Permission Levels

Two permission levels exist (enum `Permission`):

| Permission     | Description                                      |
|----------------|--------------------------------------------------|
| `USER`         | Can create requests, upload images, download results, check status, cancel requests, view engines, view usage statistics. |
| `SUPER_USER`   | Can do everything `USER` can **plus** worker-only endpoints: get processing requests, upload results, report failures, download engines/models, download uploaded images, view page statistics. |

### Auth Decorators

Defined as FastAPI dependencies in `app/dependencies.py`:

- **`get_current_user`** — requires any valid API key (`USER` or `SUPER_USER`). Checks that the `api-key` header matches any `ApiKey` row. Returns HTTP 401 if invalid.
- **`get_super_user`** — requires an API key with `SUPER_USER` permission. Checks that the `api-key` header matches an `ApiKey` row where `permission == SUPER_USER`. Returns HTTP 401 if invalid.

### Suspension

API keys have a `suspension` boolean field. Suspended keys are excluded from the work queue (pages from suspended users are not dispatched to workers), but the key itself is not rejected at the auth decorator level. Suspension is managed via `scripts/suspend_users.py`.

### Key Management

- New keys are created via `scripts/add_new_user.py --owner <name> --database <db_url> [--permission USER|SUPER_USER]`
- Suspension is managed via `scripts/suspend_users.py --database <db_url> [--api-keys key1 key2 ...]`

---

## Database Schema

All models use SQLAlchemy 2.x declarative base with `Mapped[]` style (`db/base.py`, `db/models.py`). Tables are auto-created on startup via `Base.metadata.create_all()`.

### Custom Types

- **`Uuid`**: SQLAlchemy 2.x built-in `Uuid` type. Uses native `UUID` type on PostgreSQL, compatible representation on other dialects. Replaces the old custom `GUID` TypeDecorator.

### Tables

#### `api_key`

| Column       | Type         | Constraints                     | Description                     |
|--------------|--------------|----------------------------------|---------------------------------|
| `id`         | `Integer`    | PRIMARY KEY                      | Auto-increment ID               |
| `api_string` | `String`     | NOT NULL, INDEXED                | The API key string              |
| `owner`      | `String`     | NOT NULL                         | Description/name of the owner   |
| `permission` | `Enum(Permission)` | NOT NULL                  | `USER` or `SUPER_USER`          |
| `suspension` | `Boolean`    | NOT NULL, DEFAULT `False`        | Whether the key is suspended    |
| `priority`   | `Integer`    | NOT NULL, DEFAULT `1`            | Priority (unused in current route logic, but exists on model) |

#### `request`

Represents an OCR processing request (a batch of pages).

| Column                   | Type       | Constraints                          | Description                                    |
|--------------------------|------------|--------------------------------------|------------------------------------------------|
| `id`                     | `GUID`    | PRIMARY KEY, DEFAULT `uuid4`          | UUID identifier                                |
| `creation_timestamp`     | `DateTime`| NOT NULL, INDEXED, DEFAULT `utcnow`  | When the request was created                   |
| `modification_timestamp` | `DateTime`| NOT NULL, INDEXED, DEFAULT `utcnow`  | Last modification time                         |
| `finish_timestamp`       | `DateTime`| NULLABLE, INDEXED                     | When all pages finished processing             |
| `engine_id`              | `Integer` | FK → `engine.id`, NOT NULL            | Which OCR engine to use                        |
| `api_key_id`             | `Integer` | FK → `api_key.id`, NOT NULL           | Which API key created this request             |

#### `page`

Represents a single page/image within a request.

| Column                 | Type            | Constraints                            | Description                                      |
|------------------------|-----------------|----------------------------------------|--------------------------------------------------|
| `id`                   | `GUID`         | PRIMARY KEY, DEFAULT `uuid4`            | UUID identifier                                  |
| `name`                 | `String`        | NOT NULL, INDEXED                       | Page name (user-provided identifier)             |
| `url`                  | `String`        | NULLABLE                                | URL of the image (external or internal upload URL)|
| `state`                | `Enum(PageState)` | NOT NULL, INDEXED                    | Current processing state                         |
| `score`                | `Float`         | NULLABLE, INDEXED                       | OCR quality score (0-100)                        |
| `traceback`            | `String`        | NULLABLE                                | Error traceback if processing failed             |
| `waiting_timestamp`    | `DateTime`      | NULLABLE, INDEXED                       | When the page entered WAITING state              |
| `processing_timestamp` | `DateTime`      | NULLABLE                                | When the page entered PROCESSING state           |
| `finish_timestamp`     | `DateTime`      | NULLABLE, INDEXED                       | When the page finished (success or failure)      |
| `request_id`           | `GUID`         | FK → `request.id`, NOT NULL, INDEXED    | Parent request                                   |
| `engine_version`       | `Integer`       | FK → `engine_version.id`, NULLABLE, INDEXED | Engine version used for processing           |

#### `engine`

Represents an OCR engine (e.g., a language-specific model set).

| Column        | Type       | Constraints       | Description            |
|---------------|------------|-------------------|------------------------|
| `id`          | `Integer`  | PRIMARY KEY        | Auto-increment ID      |
| `name`        | `String`   | NOT NULL           | Engine name            |
| `description` | `String`   | NULLABLE           | Engine description     |

#### `engine_version`

Versions of an engine (allows model updates).

| Column        | Type       | Constraints                     | Description                |
|---------------|------------|---------------------------------|----------------------------|
| `id`          | `Integer`  | PRIMARY KEY                      | Auto-increment ID          |
| `version`     | `String`   | NOT NULL                         | Version string (e.g. `v0.0.1`, `2024-01-15`) |
| `description` | `String`   | NULLABLE                         | Version description        |
| `engine_id`   | `Integer`  | FK → `engine.id`, NOT NULL       | Parent engine              |

#### `engine_version_model`

Many-to-many join table between `engine_version` and `model`.

| Column              | Type       | Constraints                           | Description          |
|---------------------|------------|---------------------------------------|----------------------|
| `id`                | `Integer`  | PRIMARY KEY                            | Auto-increment ID    |
| `engine_version_id` | `Integer`  | FK → `engine_version.id`, NOT NULL     | Engine version       |
| `model_id`          | `Integer`  | FK → `model.id`, NOT NULL              | Model                |

#### `model`

Individual ML model files/configs used by engine versions. Each engine version has 2-3 models (layout parser, OCR, optional decoder).

| Column   | Type       | Constraints  | Description                                 |
|----------|------------|--------------|---------------------------------------------|
| `id`     | `Integer`  | PRIMARY KEY   | Auto-increment ID                           |
| `name`   | `String`   | NOT NULL      | Model name (also folder name in MODELS_FOLDER) |
| `config` | `String`   | NOT NULL      | INI-style config section for this model     |

#### `notification`

Singleton table for email notification rate limiting.

| Column              | Type       | Constraints  | Description                              |
|---------------------|------------|--------------|------------------------------------------|
| `id`                | `Integer`  | PRIMARY KEY   | Auto-increment ID                        |
| `last_notification` | `DateTime` | NOT NULL      | Timestamp of last email notification sent |

### Entity Relationships

```
api_key (1) ──< request (1) ──< page (N)
engine  (1) ──< engine_version (1) ──< engine_version_model >── model
page.engine_version ──> engine_version.id
request.engine_id ──> engine.id
request.api_key_id ──> api_key.id
```

### PageState Enum (Lifecycle)

```
CREATED ──> WAITING ──> PROCESSING ──> PROCESSED ──> EXPIRED
                │              │
                │              ├──> NOT_FOUND
                │              ├──> INVALID_FILE
                │              └──> PROCESSING_FAILED
                │
                └──> CANCELED
```

| State               | Description                                  |
|---------------------|----------------------------------------------|
| `CREATED`           | Page entry created, image not yet available (upload pending) |
| `WAITING`           | Image URL is set, page is queued for processing |
| `PROCESSING`        | A worker has claimed this page                |
| `PROCESSED`         | Successfully processed, results available     |
| `NOT_FOUND`         | Worker could not find/download the image      |
| `INVALID_FILE`      | Worker found the image to be invalid          |
| `PROCESSING_FAILED` | Worker encountered an error during OCR        |
| `CANCELED`          | User canceled the request                     |
| `EXPIRED`           | Results expired (older than 7 days), files removed |

---

## API Endpoints

All endpoints are registered on FastAPI routers (`app/routers/public.py`, `app/routers/user.py`, `app/routers/worker.py`, `app/routers/admin.py`). Base URL depends on `APPLICATION_ROOT` config.

### Public Endpoints (No Auth)

#### `GET /` , `GET /index`
- **Description:** Dashboard page showing page statistics for the last 24 hours (bar chart).
- **Response:** HTML page (`index.html` template) with chart data.

#### `GET /docs`
- **Description:** Redirects to SwaggerHub documentation.
- **Response:** HTTP 302 redirect to `https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.4`

---

### User Endpoints (require `@require_user_api_key`)

These endpoints require a valid API key (either `USER` or `SUPER_USER` permission) in the `api-key` header.

#### `POST /post_processing_request`
- **Description:** Create a new OCR processing request.
- **Auth:** `api-key` header (USER)
- **Request Body (JSON):**
  ```json
  {
    "engine": <engine_id (int)>,
    "images": {
      "<page_name>": "<image_url_or_null>",
      "<page_name>": "<image_url_or_null>"
    }
  }
  ```
  - If an image URL is `null`, the page is created in `CREATED` state (image must be uploaded separately via `/upload_image`).
  - If an image URL is provided, the page is created in `WAITING` state (ready for processing).
- **Response (200):**
  ```json
  {"status": "success", "request_id": "<uuid>"}
  ```
- **Errors:** 422 (bad JSON), 404 (engine not found)

#### `GET /usage_statistics` , `GET /usage_statistics/<from_datetime>` , `GET /usage_statistics/<from_datetime>/<to_datetime>`
- **Description:** Get the count of processed/expired pages for the authenticated user.
- **Auth:** `api-key` header (USER)
- **URL Params:** Optional ISO-format datetime strings for filtering.
- **Response (200):**
  ```json
  {"status": "success", "processed_pages": <int>, "from": "<iso>", "to": "<iso>"}
  ```
- **Errors:** 400 (invalid datetime format)

#### `POST /upload_image/<request_id>/<page_name>`
- **Description:** Upload an image file for a page that was created without a URL (state `CREATED`).
- **Auth:** `api-key` header (USER)
- **Request:** Multipart form with `file` field.
- **Behavior:**
  - Validates the request exists and belongs to the API key.
  - Validates the page exists and is in `CREATED` state.
  - Validates the file extension (allowed: `jpg`, `jpeg`, `png`).
  - Saves the file to `UPLOAD_IMAGES_FOLDER/<request_id>/<page_name>.<ext>`.
  - Updates the page URL to point to the internal `/download_image` endpoint.
  - Changes page state to `WAITING`.
- **Response (200):** `{"status": "success"}`
- **Errors:** 404 (request/page not found), 401 (ownership), 400 (wrong state, no file), 422 (unsupported format)

#### `GET /request_status/<request_id>`
- **Description:** Get the processing status of all pages in a request.
- **Auth:** `api-key` header (USER)
- **Response (200):**
  ```json
  {
    "status": "success",
    "request_status": {
      "<page_name>": {"state": "<STATE_NAME>", "quality": <float_or_null>},
      ...
    }
  }
  ```
- **Errors:** 404 (request not found), 401 (ownership)

#### `GET /get_engines`
- **Description:** List all available OCR engines with their latest version and models.
- **Auth:** `api-key` header (USER)
- **Response (200):**
  ```json
  {
    "status": "success",
    "engines": {
      "<engine_name>": {
        "id": <int>,
        "description": "<str>",
        "engine_version": "<version_str>",
        "models": [{"id": <int>, "name": "<str>"}, ...]
      },
      ...
    }
  }
  ```

#### `GET /download_results/<request_id>/<page_name>/<format>`
- **Description:** Download OCR results for a specific page.
- **Auth:** `api-key` header (USER)
- **URL Params:**
  - `format`: one of `alto`, `page`, `txt`
- **Behavior:**
  - Results are stored in a ZIP archive at `PROCESSED_REQUESTS_FOLDER/<request_id>/<request_id>.zip`.
  - Extracts the requested format file from the ZIP: `<page_name>_alto.xml`, `<page_name>_page.xml`, or `<page_name>.txt`.
  - Uses `FileLock` to handle concurrent access to the ZIP file.
- **Response:** File download (XML or TXT).
- **Errors:** 404 (request/page not found, expired, not processed), 401 (ownership), 400 (bad format)

#### `POST /cancel_request/<request_id>`
- **Description:** Cancel all unfinished pages in a request.
- **Auth:** `api-key` header (USER)
- **Behavior:** Sets all pages in `CREATED`/`WAITING`/`PROCESSING` state to `CANCELED`.
- **Response (200):** `{"status": "success"}`
- **Errors:** 404 (request not found), 401 (ownership)

---

### Worker Endpoints (require `@require_super_user_api_key`)

These endpoints are used by processing workers. They require the `SUPER_USER` permission.

#### `GET /get_processing_request/<preferred_engine_id>`
- **Description:** Get the next page to process. Implements fair scheduling across API keys.
- **Auth:** `api-key` header (SUPER_USER)
- **Behavior:**
  1. Finds API keys with waiting pages (excluding suspended keys).
  2. Picks the key with the fewest recently processed pages (last 1 minute) for fairness.
  3. Tries to find a `WAITING` page for the preferred engine first.
  4. Falls back to any engine if no page for the preferred engine.
  5. Changes page state to `PROCESSING` and sets `processing_timestamp`.
- **Response (200):**
  ```json
  {"status": "success", "page_id": "<uuid>", "page_url": "<url>", "engine_id": <int>}
  ```
- **Response (204):** No pages available for processing.

#### `POST /upload_results/<page_id>`
- **Description:** Upload OCR results for a processed page.
- **Auth:** `api-key` header (SUPER_USER)
- **Headers:** `score` (float 0-1), `engine-version` (string)
- **Request:** Multipart form with files: `alto` (ALTO XML), `page` (PAGE XML), `txt` (plain text).
- **Behavior:**
  - Appends results to the request's ZIP archive (with `FileLock`).
  - Updates page state to `PROCESSED`, sets score (multiplied by 100), engine version, finish timestamp.
  - If all pages in the request are done, sets `request.finish_timestamp`.
  - Removes uploaded image file if it exists.
- **Response (200):** `{"status": "success"}`
- **Errors:** 404 (page not found)

#### `GET /latest_engine_version/<engine_id>`
- **Description:** Get metadata about the latest engine version (filename for download).
- **Auth:** `api-key` header (SUPER_USER)
- **Response (200):**
  ```json
  {"status": "success", "filename": "<engine_name>#<version>.zip"}
  ```
- **Errors:** 404 (engine not found), 400 (too many models)

#### `GET /download_engine/<engine_id>`
- **Description:** Download the engine models and config as a ZIP file.
- **Auth:** `api-key` header (SUPER_USER)
- **Behavior:**
  - Packages all model files from `MODELS_FOLDER/<model_name>/` into a ZIP.
  - Generates a `config.ini` with engine configuration based on model count (2 models = no decoder, 3 models = with decoder).
- **Response:** ZIP file download.
- **Errors:** 404 (engine not found), 500 (too many models)

#### `POST /failed_processing/<page_id>`
- **Description:** Report a failed processing attempt.
- **Auth:** `api-key` header (SUPER_USER)
- **Headers:** `type` (`NOT_FOUND`, `INVALID_FILE`, or `PROCESSING_FAILED`), `engine_version` (string), `hostname`, `ip-address`
- **Request Body:** Traceback string.
- **Behavior:**
  - Updates page state to the appropriate failure state.
  - Stores traceback on the page.
  - If type is `PROCESSING_FAILED` and email is configured, sends a notification email (rate-limited by `MAX_EMAIL_FREQUENCY` seconds via `notification` table).
- **Response (200):** `{"status": "success"}`

#### `GET /page_statistics`
- **Description:** Get page state statistics for the last 24 hours.
- **Auth:** `api-key` header (SUPER_USER)
- **Response (200):**
  ```json
  {"status": "success", "state_stats": {"WAITING": <n>, "PROCESSING": <n>, "PROCESSED": <n>, ...}}
  ```

#### `GET /download_image/<request_id>/<page_name>`
- **Description:** Download an uploaded image (used by workers to fetch images uploaded by users).
- **Auth:** `api-key` header (SUPER_USER)
- **Behavior:** Serves the image file from `UPLOAD_IMAGES_FOLDER`.
- **Response:** Image file download.
- **Errors:** 404 (request/page not found, not uploaded, already processed), 405 (already processed)

---

### Admin Endpoints (require `SUPER_USER` API key)

These endpoints provide administrative operations. They require the `SUPER_USER` permission via the `api-key` header.

#### `POST /admin/users`
- **Description:** Create a new API key.
- **Auth:** `api-key` header (SUPER_USER)
- **Request Body (JSON):**
  ```json
  {"owner": "<name>", "permission": "USER|SUPER_USER"}
  ```
  - `permission` defaults to `"USER"` if omitted.
- **Response (200):**
  ```json
  {"status": "success", "api_key": "<generated_key>", "owner": "<name>", "permission": "USER"}
  ```
- **Errors:** 422 (invalid permission)

#### `GET /admin/users`
- **Description:** List all API keys with metadata.
- **Auth:** `api-key` header (SUPER_USER)
- **Response (200):**
  ```json
  {
    "status": "success",
    "users": [
      {"id": <int>, "api_string": "<str>", "owner": "<str>", "permission": "USER", "suspension": false},
      ...
    ]
  }
  ```

#### `PUT /admin/users/<user_id>/suspension`
- **Description:** Suspend or unsuspend an API key.
- **Auth:** `api-key` header (SUPER_USER)
- **Request Body (JSON):**
  ```json
  {"suspended": true}
  ```
- **Response (200):**
  ```json
  {"status": "success", "user_id": <int>, "suspended": true}
  ```
- **Errors:** 404 (API key not found)

#### `GET /admin/usage_statistics/users` , `GET /admin/usage_statistics/users/<from>` , `GET /admin/usage_statistics/users/<from>/<to>`
- **Description:** Get processed page counts for every user, optionally filtered by date range.
- **Auth:** `api-key` header (SUPER_USER)
- **URL Params:** Optional ISO-format datetime strings.
- **Response (200):**
  ```json
  {
    "status": "success",
    "users": [
      {"api_key_id": <int>, "owner": "<str>", "api_string": "<str>", "processed_pages": <int>},
      ...
    ],
    "from_date": "<iso_or_null>",
    "to_date": "<iso_or_null>"
  }
  ```
- **Errors:** 400 (invalid datetime format)

#### `GET /admin/usage_statistics/engines` , `GET /admin/usage_statistics/engines/<from>` , `GET /admin/usage_statistics/engines/<from>/<to>`
- **Description:** Get processed page counts for each engine, optionally filtered by date range.
- **Auth:** `api-key` header (SUPER_USER)
- **URL Params:** Optional ISO-format datetime strings.
- **Response (200):**
  ```json
  {
    "status": "success",
    "engines": [
      {"engine_id": <int>, "engine_name": "<str>", "processed_pages": <int>},
      ...
    ],
    "from_date": "<iso_or_null>",
    "to_date": "<iso_or_null>"
  }
  ```
- **Errors:** 400 (invalid datetime format)

---

## Background Tasks (asyncio)

Two background loops run as native `asyncio` tasks during the application lifespan:

### 1. `processing_timeout` (every 60 seconds)
- Finds pages in `PROCESSING` state where `processing_timestamp` is older than 60 seconds.
- Resets them to `WAITING` state (so another worker can pick them up).
- Sends an email notification with details of timed-out pages.

### 2. `old_files_removals` (every 24 hours)
- Finds pages belonging to requests where `finish_timestamp` is older than 7 days.
- Changes `PROCESSED` pages to `EXPIRED` state.
- Deletes result ZIP files and uploaded images from disk.

---

## Configuration (`config-example.env`)

See `config-example.env` for the template. Settings are loaded via `pydantic-settings` from
environment variables or a `.env` file.

| Setting                        | Type       | Description                                             |
|--------------------------------|------------|---------------------------------------------------------|
| `DATABASE_URL`                 | `str`      | SQLAlchemy async database URL                           |
| `DEBUG`                        | `bool`     | Debug mode                                              |
| `PROCESSED_REQUESTS_FOLDER`    | `str`      | Directory for storing result ZIP files                  |
| `MODELS_FOLDER`                | `str`      | Directory containing OCR model files                    |
| `UPLOAD_IMAGES_FOLDER`         | `str`      | Directory for user-uploaded images                      |
| `ALLOWED_IMAGE_EXTENSIONS`     | `set`      | Allowed image upload extensions (`jpg`, `jpeg`, `png`)  |
| `APPLICATION_ROOT`             | `str`      | URL prefix for the application                          |
| `EMAIL_NOTIFICATION_ADDRESSES` | `list`     | Email addresses for error notifications                 |
| `MAX_EMAIL_FREQUENCY`          | `int`      | Minimum seconds between notification emails             |
| `MAIL_SERVER`                  | `str`      | SMTP server hostname                                    |
| `MAIL_USERNAME`                | `str`      | SMTP username                                           |
| `MAIL_PASSWORD`                | `str`      | SMTP password                                           |

---

## Application Initialization (`app/__init__.py`)

1. `create_app()` builds the `FastAPI` instance:
   - Configures `lifespan` async context manager.
   - Registers exception handlers from `app.exceptions`.
   - Mounts Jinja2 templates and static files.
   - Includes routers: `public`, `user`, `worker`.
2. On startup (lifespan enter):
   - Initialises the async SQLAlchemy engine and session factory.
   - Creates all tables (dev convenience; production uses Alembic).
   - Creates required directories (`PROCESSED_REQUESTS_FOLDER`, `MODELS_FOLDER`, `UPLOAD_IMAGES_FOLDER`).
   - Ensures the `notification` singleton row exists.
   - Starts background asyncio tasks (`processing_timeout`, `old_files_removals`).
3. On shutdown (lifespan exit):
   - Cancels background tasks.
   - Disposes the async engine.

---

## Email Notifications (`app/services/mail_service.py`)

Uses `drymail` library. Sends HTML emails via SMTP (with TLS if password is set).

Emails are sent for:
- **Processing failures** (`PROCESSING_FAILED` type) — includes hostname, IP, API key, engine info, page info, traceback.
- **Processing timeouts** — includes details of all timed-out pages.
- **Internal server errors** (500 handler) — includes the full traceback.

Rate limited via the `notification` table singleton (`MAX_EMAIL_FREQUENCY` seconds between sends).

---

## Error Handling

- **HTTP 500:** Global exception handler sends an email notification and returns a JSON error response.
- **Auth failures:** Return HTTP 401 with message about invalid/missing API key.
- **Ownership checks:** User endpoints verify that a request belongs to the authenticated API key; returns 401 if not.

---

## File Storage

| Directory                  | Content                                        | Cleanup                         |
|----------------------------|------------------------------------------------|---------------------------------|
| `PROCESSED_REQUESTS_FOLDER/<request_id>/` | `<request_id>.zip` (results archive) | Deleted after 7 days            |
| `UPLOAD_IMAGES_FOLDER/<request_id>/`      | `<page_name>.<ext>` (uploaded images) | Deleted after processing or after 7 days |
| `MODELS_FOLDER/<model_name>/`             | OCR model files                       | Persistent (managed by admin)   |

Results ZIP structure (per request):
```
<request_id>.zip
├── <page_name>_alto.xml
├── <page_name>_page.xml
├── <page_name>.txt
├── <page_name2>_alto.xml
├── ...
```

---

## Admin Scripts

### `scripts/add_new_user.py`
```bash
python scripts/add_new_user.py --owner "User Name" --database "postgresql://..." [--permission USER|SUPER_USER]
```
Generates a new API key and prints it to stdout.

### `scripts/add_new_engine_version.py`
```bash
# Add version to existing engine with existing + new models
python scripts/add_new_engine_version.py --engine 3 -m 1 /path/to/model_folder -d "postgresql://..."

# Create new engine with existing models
python scripts/add_new_engine_version.py --engine_name great_ocr -m 1 2 -d "postgresql://..."
```
Creates engine versions and links models. New model folders are copied to `MODELS_FOLDER`.

### `scripts/suspend_users.py`
```bash
# Unsuspend specific keys, suspend all others
python scripts/suspend_users.py --database "postgresql://..." --api-keys key1 key2

# Unsuspend all keys
python scripts/suspend_users.py --database "postgresql://..."
```

### `scripts/db_migrate.py`
```bash
python scripts/db_migrate.py --source-db "postgresql://source" --dest-db "postgresql://dest"
```
Copies all data from source to destination database (table by table in dependency order).

---

## Processing Flow

```
User                          API Server                      Worker
 │                                │                              │
 ├─ POST /post_processing_request─>│                              │
 │  (engine_id + image URLs)      │                              │
 │<─── request_id ────────────────│                              │
 │                                │                              │
 │  [Optional: upload images]     │                              │
 ├─ POST /upload_image ──────────>│                              │
 │<─── success ───────────────────│                              │
 │                                │                              │
 │                                │<── GET /get_processing_request──┤
 │                                │─── page_id, url, engine_id ──>│
 │                                │                              │
 │                                │<── GET /download_engine ──────┤
 │                                │─── models ZIP ───────────────>│
 │                                │                              │
 │                                │    [Worker downloads image,  │
 │                                │     runs OCR]                │
 │                                │                              │
 │                                │<── POST /upload_results ──────┤
 │                                │─── success ──────────────────>│
 │                                │                              │
 ├─ GET /request_status ─────────>│                              │
 │<─── page states ──────────────│                              │
 │                                │                              │
 ├─ GET /download_results ───────>│                              │
 │<─── ALTO/PAGE/TXT file ──────│                              │
```
