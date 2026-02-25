"""Tests for admin-level API endpoints (require SUPER_USER api-key)."""
import datetime
import uuid

import pytest
from tests.conftest import _session_maker, USER_KEY, SUPER_USER_KEY

from db.models import ApiKey, Page, PageState, Permission, Request
from sqlalchemy import select


# ---------------------------------------------------------------------------
# POST /admin/users  —  create a new user
# ---------------------------------------------------------------------------
class TestCreateUser:

    async def test_create_user_default_permission(self, client, super_user_key):
        resp = await client.post(
            "/admin/users",
            json={"owner": "New User"},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["owner"] == "New User"
        assert data["permission"] == "USER"
        assert len(data["api_key"]) > 10  # generated key is non-trivial

    async def test_create_super_user(self, client, super_user_key):
        resp = await client.post(
            "/admin/users",
            json={"owner": "Admin User", "permission": "SUPER_USER"},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["permission"] == "SUPER_USER"
        assert data["api_key"]

    async def test_invalid_permission(self, client, super_user_key):
        resp = await client.post(
            "/admin/users",
            json={"owner": "Bad", "permission": "ADMIN"},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 422
        assert resp.json()["status"] == "failure"

    async def test_user_key_denied(self, client, user_key):
        resp = await client.post(
            "/admin/users",
            json={"owner": "Attempt"},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401

    async def test_no_key_denied(self, client):
        resp = await client.post(
            "/admin/users",
            json={"owner": "Attempt"},
        )
        assert resp.status_code == 422  # missing header


# ---------------------------------------------------------------------------
# GET /admin/users  —  list all users
# ---------------------------------------------------------------------------
class TestListUsers:

    async def test_list_users(self, client, super_user_key, user_key):
        resp = await client.get(
            "/admin/users",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        users = data["users"]
        # Should have at least the super_user and user keys
        assert len(users) >= 2
        owners = {u["owner"] for u in users}
        assert "Test User" in owners
        assert "Test Worker" in owners

    async def test_user_key_denied(self, client, user_key):
        resp = await client.get(
            "/admin/users",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401

    async def test_user_fields(self, client, super_user_key, user_key):
        """Each user entry has the expected fields."""
        resp = await client.get(
            "/admin/users",
            headers={"api-key": super_user_key},
        )
        data = resp.json()
        for u in data["users"]:
            assert "id" in u
            assert "api_string" in u
            assert "owner" in u
            assert "permission" in u
            assert "suspension" in u


# ---------------------------------------------------------------------------
# PUT /admin/users/{user_id}/suspension  —  suspend / unsuspend
# ---------------------------------------------------------------------------
class TestSuspendUser:

    async def test_suspend_user(self, client, super_user_key, user_key):
        # Find the user_key's id
        list_resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        user_entry = next(
            u for u in list_resp.json()["users"] if u["api_string"] == user_key
        )
        user_id = user_entry["id"]

        resp = await client.put(
            f"/admin/users/{user_id}/suspension",
            json={"suspended": True},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["user_id"] == user_id
        assert data["suspended"] is True

    async def test_unsuspend_user(self, client, super_user_key, user_key):
        # Find and suspend first
        list_resp = await client.get(
            "/admin/users", headers={"api-key": super_user_key}
        )
        user_entry = next(
            u for u in list_resp.json()["users"] if u["api_string"] == user_key
        )
        user_id = user_entry["id"]

        await client.put(
            f"/admin/users/{user_id}/suspension",
            json={"suspended": True},
            headers={"api-key": super_user_key},
        )

        # Now unsuspend
        resp = await client.put(
            f"/admin/users/{user_id}/suspension",
            json={"suspended": False},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        assert resp.json()["suspended"] is False

    async def test_not_found(self, client, super_user_key):
        resp = await client.put(
            "/admin/users/99999/suspension",
            json={"suspended": True},
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404

    async def test_user_key_denied(self, client, user_key):
        resp = await client.put(
            "/admin/users/1/suspension",
            json={"suspended": True},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /admin/usage_statistics/users  —  per-user usage stats
# ---------------------------------------------------------------------------
class TestAdminUserUsageStatistics:

    async def test_empty(self, client, super_user_key, user_key):
        """No processed pages yet → all counts should be zero."""
        resp = await client.get(
            "/admin/usage_statistics/users",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert isinstance(data["users"], list)
        # At least 2 users exist (super + user)
        assert len(data["users"]) >= 2
        for u in data["users"]:
            assert u["processed_pages"] == 0

    async def test_with_processed_pages(
        self, client, super_user_key, request_with_processed_page
    ):
        """User with a processed page shows count > 0."""
        resp = await client.get(
            "/admin/usage_statistics/users",
            headers={"api-key": super_user_key},
        )
        data = resp.json()
        user_stats = {u["api_string"]: u["processed_pages"] for u in data["users"]}
        user_key_str = request_with_processed_page[1]
        assert user_stats[user_key_str] >= 1

    async def test_with_date_range(self, client, super_user_key, user_key):
        resp = await client.get(
            "/admin/usage_statistics/users/2020-01-01T00:00:00/2020-12-31T23:59:59",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["from_date"] is not None
        assert data["to_date"] is not None

    async def test_invalid_date(self, client, super_user_key):
        resp = await client.get(
            "/admin/usage_statistics/users/not-a-date",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 400

    async def test_user_key_denied(self, client, user_key):
        resp = await client.get(
            "/admin/usage_statistics/users",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /admin/usage_statistics/engines  —  per-engine usage stats
# ---------------------------------------------------------------------------
class TestAdminEngineUsageStatistics:

    async def test_empty(self, client, super_user_key, engine_with_models):
        """No processed pages yet → engine count should be zero."""
        resp = await client.get(
            "/admin/usage_statistics/engines",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert isinstance(data["engines"], list)
        # At least 1 engine exists
        assert len(data["engines"]) >= 1
        assert data["engines"][0]["processed_pages"] == 0

    async def test_with_processed_pages(
        self, client, super_user_key, request_with_processed_page
    ):
        """Engine with a processed page shows count > 0."""
        resp = await client.get(
            "/admin/usage_statistics/engines",
            headers={"api-key": super_user_key},
        )
        data = resp.json()
        assert any(e["processed_pages"] >= 1 for e in data["engines"])

    async def test_with_date_range(self, client, super_user_key, engine_with_models):
        resp = await client.get(
            "/admin/usage_statistics/engines/2020-01-01T00:00:00/2020-12-31T23:59:59",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["from_date"] is not None
        assert data["to_date"] is not None

    async def test_invalid_date(self, client, super_user_key):
        resp = await client.get(
            "/admin/usage_statistics/engines/not-a-date",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 400

    async def test_user_key_denied(self, client, user_key):
        resp = await client.get(
            "/admin/usage_statistics/engines",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401

    async def test_engine_fields(self, client, super_user_key, engine_with_models):
        """Each engine entry has the expected fields."""
        resp = await client.get(
            "/admin/usage_statistics/engines",
            headers={"api-key": super_user_key},
        )
        data = resp.json()
        for e in data["engines"]:
            assert "engine_id" in e
            assert "engine_name" in e
            assert "processed_pages" in e
