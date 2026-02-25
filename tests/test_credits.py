"""Tests for processing credits feature."""
import uuid

import pytest
from sqlalchemy import select

from tests.conftest import (
    _session_maker,
    NO_CREDITS_KEY,
    LOW_CREDITS_KEY,
    SUPER_USER_KEY,
    USER_KEY,
)
from db.models import ApiKey, Page, PageState


# ---------------------------------------------------------------------------
# POST /admin/users/{user_id}/credits  —  add credits
# ---------------------------------------------------------------------------
class TestAddCredits:

    async def test_add_credits_success(self, client, super_user_key, user_key):
        """Admin can add credits to a user."""
        # Find the user_key's id
        list_resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        user_entry = next(
            u for u in list_resp.json()["users"] if u["api_string"] == user_key
        )
        user_id = user_entry["id"]

        resp = await client.post(
            f"/admin/users/{user_id}/credits",
            json={"amount": 500.0, "note": "Initial top-up"},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["amount"] == 500.0
        assert data["new_balance"] == 10500.0  # 10000 initial + 500
        assert data["note"] == "Initial top-up"

    async def test_add_credits_negative_rejected(self, client, super_user_key, user_key):
        """Negative amount should be rejected."""
        list_resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        user_entry = next(
            u for u in list_resp.json()["users"] if u["api_string"] == user_key
        )
        resp = await client.post(
            f"/admin/users/{user_entry['id']}/credits",
            json={"amount": -10.0},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 422

    async def test_add_credits_user_not_found(self, client, super_user_key):
        resp = await client.post(
            "/admin/users/99999/credits",
            json={"amount": 100.0},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404

    async def test_add_credits_user_key_denied(self, client, user_key):
        resp = await client.post(
            "/admin/users/1/credits",
            json={"amount": 100.0},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /admin/users/{user_id}/credits  —  credit history
# ---------------------------------------------------------------------------
class TestCreditHistory:

    async def test_credit_history(self, client, super_user_key, user_key):
        """After adding credits, transaction should appear in history."""
        list_resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        user_entry = next(
            u for u in list_resp.json()["users"] if u["api_string"] == user_key
        )
        user_id = user_entry["id"]

        # Add credits twice
        await client.post(
            f"/admin/users/{user_id}/credits",
            json={"amount": 100.0, "note": "First"},
            headers={"api-key": super_user_key},
        )
        await client.post(
            f"/admin/users/{user_id}/credits",
            json={"amount": 200.0, "note": "Second"},
            headers={"api-key": super_user_key},
        )

        resp = await client.get(
            f"/admin/users/{user_id}/credits",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert len(data["transactions"]) == 2
        # Newest first
        assert data["transactions"][0]["amount"] == 200.0
        assert data["transactions"][0]["note"] == "Second"
        assert data["transactions"][1]["amount"] == 100.0

    async def test_credit_history_not_found(self, client, super_user_key):
        resp = await client.get(
            "/admin/users/99999/credits",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Insufficient credits
# ---------------------------------------------------------------------------
class TestInsufficientCredits:

    async def test_zero_credits_rejected(
        self, client, no_credits_user_key, engine_with_models
    ):
        """User with 0 credits cannot create a request."""
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {"img1": "http://example.com/1.jpg"},
            },
            headers={"api-key": no_credits_user_key},
        )
        assert resp.status_code == 402
        assert "Insufficient credits" in resp.json()["message"]

    async def test_insufficient_for_batch(
        self, client, low_credits_user_key, engine_with_models
    ):
        """User with 5 credits, engine cost 1.0, submitting 10 pages → 402."""
        images = {f"img_{i}": f"http://example.com/{i}.jpg" for i in range(10)}
        resp = await client.post(
            "/post_processing_request",
            json={"engine": engine_with_models, "images": images},
            headers={"api-key": low_credits_user_key},
        )
        assert resp.status_code == 402

    async def test_sufficient_credits_accepted(
        self, client, low_credits_user_key, engine_with_models
    ):
        """User with 5 credits, engine cost 1.0, submitting 3 pages → success."""
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {
                    "a": "http://example.com/a.jpg",
                    "b": "http://example.com/b.jpg",
                    "c": "http://example.com/c.jpg",
                },
            },
            headers={"api-key": low_credits_user_key},
        )
        assert resp.status_code == 200

    async def test_pending_cost_updated_on_creation(
        self, client, user_key, engine_with_models
    ):
        """After creating a request, pending_cost should increase."""
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {
                    "p1": "http://example.com/p1.jpg",
                    "p2": "http://example.com/p2.jpg",
                },
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200

        # Check usage statistics for pending_cost
        stats = await client.get(
            "/usage_statistics", headers={"api-key": user_key}
        )
        data = stats.json()
        assert data["pending_cost"] == 2.0  # 2 pages × 1.0 cost


# ---------------------------------------------------------------------------
# Credit deduction on processing
# ---------------------------------------------------------------------------
class TestCreditDeductionOnProcessed:

    async def test_balance_decreases_on_processed(
        self, client, request_with_processing_page, super_user_key
    ):
        """After uploading results, balance should decrease by page cost."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

        # Get balance before
        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == ukey)
            )
            key = result.scalar_one()
            balance_before = key.credit_balance

        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("alto.xml", b"<alto>result</alto>"),
                "page": ("page.xml", b"<page>result</page>"),
                "txt": ("result.txt", b"OCR text result"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.95",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 200

        # Balance should have decreased
        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == ukey)
            )
            key = result.scalar_one()
            assert key.credit_balance == balance_before - 1.0  # cost = 1.0


# ---------------------------------------------------------------------------
# No deduction on failure
# ---------------------------------------------------------------------------
class TestNoCreditDeductionOnFailure:

    async def test_balance_unchanged_on_failure(
        self, client, request_with_processing_page, super_user_key
    ):
        """After a failure, balance should NOT decrease."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == ukey)
            )
            key = result.scalar_one()
            balance_before = key.credit_balance

        resp = await client.post(
            f"/failed_processing/{page_id}",
            content=b"Traceback: something went wrong",
            headers={
                "api-key": super_user_key,
                "type": "PROCESSING_FAILED",
                "engine_version": "v1.0.0",
                "hostname": "test-host",
                "ip-address": "127.0.0.1",
            },
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == ukey)
            )
            key = result.scalar_one()
            assert key.credit_balance == balance_before  # unchanged


# ---------------------------------------------------------------------------
# Credit refund on cancel
# ---------------------------------------------------------------------------
class TestCreditRefundOnCancel:

    async def test_pending_decreases_on_cancel(
        self, client, user_key, engine_with_models
    ):
        """Canceling should release pending cost."""
        # Create request through the API so pending_cost is incremented
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {
                    "cancel_1": "http://example.com/c1.jpg",
                    "cancel_2": "http://example.com/c2.jpg",
                },
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        rid = resp.json()["request_id"]

        # Check pending before cancel
        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == user_key)
            )
            key = result.scalar_one()
            pending_before = key.pending_cost
            balance_before = key.credit_balance

        assert pending_before == 2.0  # 2 pages × 1.0 cost

        resp = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200

        # Pending should decrease, balance should stay the same
        async with _session_maker() as db:
            result = await db.execute(
                select(ApiKey).where(ApiKey.api_string == user_key)
            )
            key = result.scalar_one()
            assert key.pending_cost == 0.0
            assert key.credit_balance == balance_before


# ---------------------------------------------------------------------------
# Scheduling skips bankrupt users
# ---------------------------------------------------------------------------
class TestSchedulingSkipsBankruptUsers:

    async def test_no_pages_for_zero_balance(
        self, client, super_user_key, engine_with_models, no_credits_user_key
    ):
        """User with 0 balance who has WAITING pages should be skipped by scheduler."""
        # Directly insert a WAITING page for the zero-balance user
        async with _session_maker() as session:
            result = await session.execute(
                select(ApiKey).where(ApiKey.api_string == NO_CREDITS_KEY)
            )
            key = result.scalar_one()

            from db.models import Request
            import datetime

            req = Request(engine_id=engine_with_models, api_key_id=key.id)
            session.add(req)
            await session.flush()

            page = Page(
                name="bankrupt_page",
                url="http://example.com/bankrupt.jpg",
                state=PageState.WAITING,
                request_id=req.id,
                waiting_timestamp=datetime.datetime.utcnow(),
                cost=1.0,
            )
            session.add(page)
            await session.commit()

        resp = await client.get(
            f"/get_processing_request/{engine_with_models}",
            headers={"api-key": super_user_key},
        )
        # Should return 204 (no content) since the only user has 0 balance
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# PUT /admin/engines/{engine_id}/cost
# ---------------------------------------------------------------------------
class TestSetEngineCost:

    async def test_set_cost(self, client, super_user_key, engine_with_models):
        resp = await client.put(
            f"/admin/engines/{engine_with_models}/cost",
            json={"cost_per_page": 2.5},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["cost_per_page"] == 2.5

    async def test_set_cost_reflected_in_engines(
        self, client, super_user_key, user_key, engine_with_models
    ):
        """After setting cost, GET /get_engines should reflect it."""
        await client.put(
            f"/admin/engines/{engine_with_models}/cost",
            json={"cost_per_page": 3.0},
            headers={"api-key": super_user_key},
        )
        resp = await client.get(
            "/get_engines", headers={"api-key": user_key}
        )
        data = resp.json()
        assert data["engines"]["test_engine"]["cost_per_page"] == 3.0

    async def test_negative_cost_rejected(self, client, super_user_key, engine_with_models):
        resp = await client.put(
            f"/admin/engines/{engine_with_models}/cost",
            json={"cost_per_page": -1.0},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 422

    async def test_engine_not_found(self, client, super_user_key):
        resp = await client.put(
            "/admin/engines/99999/cost",
            json={"cost_per_page": 1.0},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Cost snapshot on page
# ---------------------------------------------------------------------------
class TestCostSnapshot:

    async def test_page_cost_is_snapshot(
        self, client, super_user_key, user_key, engine_with_models
    ):
        """Page.cost should reflect engine cost at creation time, not current cost."""
        # Create request with current cost (1.0)
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {"snap_page": "http://example.com/snap.jpg"},
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200

        # Change engine cost
        await client.put(
            f"/admin/engines/{engine_with_models}/cost",
            json={"cost_per_page": 99.0},
            headers={"api-key": super_user_key},
        )

        # Verify page still has old cost
        async with _session_maker() as db:
            result = await db.execute(
                select(Page).where(Page.name == "snap_page")
            )
            page = result.scalar_one()
            assert page.cost == 1.0  # original cost, not 99.0


# ---------------------------------------------------------------------------
# Credit info in responses
# ---------------------------------------------------------------------------
class TestCreditInfoInResponses:

    async def test_usage_statistics_includes_credits(self, client, user_key):
        resp = await client.get(
            "/usage_statistics", headers={"api-key": user_key}
        )
        data = resp.json()
        assert "credit_balance" in data
        assert "pending_cost" in data
        assert data["credit_balance"] == 10000.0

    async def test_admin_list_users_includes_credits(
        self, client, super_user_key, user_key
    ):
        resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        data = resp.json()
        for u in data["users"]:
            assert "credit_balance" in u
            assert "pending_cost" in u

    async def test_engines_include_cost(self, client, user_key, engine_with_models):
        resp = await client.get(
            "/get_engines", headers={"api-key": user_key}
        )
        data = resp.json()
        assert data["engines"]["test_engine"]["cost_per_page"] == 1.0

    async def test_admin_user_stats_include_credits(
        self, client, super_user_key, user_key
    ):
        resp = await client.get(
            "/admin/usage_statistics/users",
            headers={"api-key": super_user_key},
        )
        data = resp.json()
        for u in data["users"]:
            assert "credit_balance" in u
            assert "pending_cost" in u
