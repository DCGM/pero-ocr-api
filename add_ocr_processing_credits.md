# Add OCR processing credits to API keys

## Original specification
The goal of this update is to be able to meter and limit usage of the API. Each API key should have current budget of credits, OCR angines should have a cost. 
If credits of the user run out, jobs should not be further processed. Functionality:
- API endpoint: Add credits to a user. 
- Users/Keys should store current balance, pending job cost estimate, and credit "increase" history. Credit increases can be stored in as a json column.
- Credit balance and pending job cost estimate should be updated when an image processing is successfully finished. The pending job cost estimate should be updated when an image switches to WAITING state.
- The system should not allow new job creation when current balance and pending job cost estimate idicate insufficient funds for the job.
- Balance and pending job cost estimate should show up in the usage statistics for adming and normal users.
- Engines should have fixed cost per page.

## Guideline
Document routes, add tests
---

## Spec issues found & resolutions

1. **CREATED-page loophole** — The spec says pending cost updates when entering WAITING. But pages created without a URL start in CREATED state and bypass the budget check, allowing unlimited page creation. **Fix:** Pending cost increments at request creation for *all* pages (CREATED and WAITING alike).

2. **Failure/cancellation undefined** — The spec doesn't say what happens to credits on failure (NOT_FOUND, INVALID_FILE, PROCESSING_FAILED) or cancellation. **Fix:** Reduce pending cost on terminal failure/cancel; do NOT deduct from balance. Users pay only for PROCESSED pages.

3. **Engine cost mutability** — If engine `cost_per_page` changes after submission, in-flight pages could be charged at the wrong rate. **Fix:** Snapshot `cost` on each `Page` row at creation time.

4. **JSON credit history** — A JSON column is hard to query/audit. **Fix:** Use a separate `credit_transaction` table.

---

## Implementation plan

### 1. Database model changes — `db/models.py`

- **`ApiKey`**: add `credit_balance` (Float, default 0.0) and `pending_cost` (Float, default 0.0).
- **`Engine`**: add `cost_per_page` (Float, default 0.0).
- **`Page`**: add `cost` (Float, default 0.0) — snapshot of engine cost at creation.
- **New model `CreditTransaction`** (table `credit_transaction`):
  - `id` (int PK), `api_key_id` (FK→api_key), `amount` (Float), `timestamp` (DateTime), `admin_api_key_id` (FK→api_key, nullable), `note` (String, nullable).

### 2. Alembic migration — `alembic/versions/002_add_processing_credits.py`

- `add_column` for `api_key`, `engine`, `page`.
- `create_table` for `credit_transaction`.

### 3. Exception — `app/exceptions.py`

- Add `InsufficientCreditsError` (HTTP 402).

### 4. Schemas — `app/schemas/`

**Request schemas:**
- `AddCreditsRequest` (amount, note)
- `SetEngineCostRequest` (cost_per_page)

**Response schemas:**
- Update `ApiKeyItem` — add `credit_balance`, `pending_cost`.
- Update `UsageStatisticsResponse` — add `credit_balance`, `pending_cost`.
- Update `UserUsageItem` — add `credit_balance`, `pending_cost`.
- Update `EngineInfo` — add `cost_per_page`.
- New: `AddCreditsResponse`, `CreditTransactionItem`, `CreditHistoryResponse`, `SetEngineCostResponse`.

### 5. CRUD — new `app/crud/credits.py`

- `add_credits(db, api_key_id, amount, admin_api_key_id, note)`
- `get_credit_history(db, api_key_id)`
- `check_sufficient_credits(api_key, total_cost)` — pure check: `balance - pending >= total_cost`
- `increment_pending(db, api_key_id, amount)`
- `decrement_pending(db, api_key_id, amount)`
- `deduct_balance(db, api_key_id, amount)`

### 6. CRUD updates

- **`create_request`** (`app/crud/request.py`): compute `total_cost = len(images) × engine.cost_per_page`; check credits; set `page.cost`; increment pending. Raise `InsufficientCreditsError` if insufficient.
- **`change_page_to_processed`** (`app/crud/page.py`): deduct balance, decrement pending by `page.cost`.
- **`change_page_to_failed`** (`app/crud/page.py`): decrement pending by `page.cost`.
- **`cancel_request_by_id`** (`app/crud/request.py`): decrement pending for each canceled page.
- **`_which_keys_have_requests`** (`app/crud/page.py`): add `.where(ApiKey.credit_balance > 0)`.
- **`get_engine_dict`** (`app/crud/engine.py`): include `cost_per_page`.
- New: `set_engine_cost` in `app/crud/engine.py`.

### 7. Router updates

- **`POST /post_processing_request`**: handle `InsufficientCreditsError` → HTTP 402.
- **`GET /usage_statistics`**: include `credit_balance`, `pending_cost`.
- **`GET /get_engines`**: include `cost_per_page` per engine.
- **`GET /admin/users`**: include `credit_balance`, `pending_cost`.
- **Admin usage stats**: include `credit_balance`, `pending_cost` per user.
- **New `POST /admin/users/{user_id}/credits`**: add credits.
- **New `GET /admin/users/{user_id}/credits`**: view credit history.
- **New `PUT /admin/engines/{engine_id}/cost`**: set engine cost.

### 8. Tests

- Update fixtures with `credit_balance=10000.0` and `cost_per_page=1.0` so existing tests pass.
- New `tests/test_credits.py` covering: add credits, insufficient credits, deduction on processed, no deduction on failure, refund on cancel, scheduling skip, engine cost CRUD, cost snapshot.

### 9. README update

- Document new columns, table, and endpoints.

---

## Implementation summary

All steps from the plan above have been executed. **124 tests pass** (101 existing + 23 new).

### Files created
| File | Purpose |
|------|---------|
| `alembic/versions/002_add_processing_credits.py` | Alembic migration: adds columns to `api_key`, `engine`, `page`; creates `credit_transaction` table |
| `app/crud/credits.py` | Credit CRUD operations: `add_credits`, `get_credit_history`, `check_sufficient_credits`, `increment_pending`, `decrement_pending`, `deduct_balance` |
| `tests/test_credits.py` | 23 tests covering all credit functionality |

### Files modified
| File | Changes |
|------|---------|
| `db/models.py` | Added `credit_balance`, `pending_cost` to `ApiKey`; `cost_per_page` to `Engine`; `cost` to `Page`; new `CreditTransaction` model |
| `app/exceptions.py` | Added `InsufficientCreditsError` (HTTP 402) |
| `app/schemas/requests.py` | Added `AddCreditsRequest`, `SetEngineCostRequest` |
| `app/schemas/responses.py` | Updated `ApiKeyItem`, `UsageStatisticsResponse`, `UserUsageItem`, `EngineInfo`; added `AddCreditsResponse`, `CreditTransactionItem`, `CreditHistoryResponse`, `SetEngineCostResponse` |
| `app/crud/request.py` | `create_request`: credit check + pending increment + cost snapshot; `cancel_request_by_id`: pending decrement |
| `app/crud/page.py` | `change_page_to_processed`: deduct balance + decrement pending; `change_page_to_failed`: decrement pending; `_which_keys_have_requests`: filter out zero-balance keys |
| `app/crud/engine.py` | `get_engine_dict`: includes `cost_per_page`; new `set_engine_cost()` |
| `app/routers/user.py` | `post_processing_request`: handles 402; `usage_statistics`: returns credit fields |
| `app/routers/admin.py` | `list_users`: credit fields; usage stats: credit fields; 3 new endpoints: `POST/GET /admin/users/{id}/credits`, `PUT /admin/engines/{id}/cost` |
| `tests/conftest.py` | All fixtures updated with credit fields; added `no_credits_user_key` and `low_credits_user_key` fixtures |
| `tests/test_admin_routes.py` | `test_user_fields` asserts `credit_balance`, `pending_cost` |
| `tests/test_user_routes.py` | `TestGetEngines::test_success` asserts `cost_per_page` |
| `README.md` | Documented all new columns, `credit_transaction` table, updated entity relationships, new endpoints, updated existing endpoint docs |

### New API endpoints
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/admin/users/{user_id}/credits` | Add credits (amount > 0, optional note) |
| `GET`  | `/admin/users/{user_id}/credits` | Credit transaction history |
| `PUT`  | `/admin/engines/{engine_id}/cost` | Set engine cost per page |

### Credit lifecycle
```
Request created → pending_cost += total_cost (all pages × engine cost_per_page)
Page PROCESSED  → credit_balance -= page.cost, pending_cost -= page.cost
Page FAILED     → pending_cost -= page.cost (balance unchanged)
Page CANCELED   → pending_cost -= page.cost (balance unchanged)
```