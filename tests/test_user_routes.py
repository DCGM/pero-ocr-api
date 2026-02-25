"""Tests for user-level API endpoints (require USER or SUPER_USER api-key)."""
import uuid

import pytest


# ---------------------------------------------------------------------------
# POST /post_processing_request
# ---------------------------------------------------------------------------
class TestPostProcessingRequest:

    async def test_success_with_image_urls(self, client, user_key, engine_with_models):
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {
                    "img_001": "http://example.com/photo1.jpg",
                    "img_002": "http://example.com/photo2.jpg",
                },
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "request_id" in data

    async def test_success_with_null_urls(self, client, user_key, engine_with_models):
        """Pages with null URLs should be created in CREATED state."""
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {"page_a": None, "page_b": None},
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"

        # Verify page states via request_status
        rid = str(data["request_id"])
        status_resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": user_key},
        )
        status_data = status_resp.json()
        assert status_data["request_status"]["page_a"]["state"] == "CREATED"
        assert status_data["request_status"]["page_b"]["state"] == "CREATED"

    async def test_invalid_engine(self, client, user_key, engine_with_models):
        resp = await client.post(
            "/post_processing_request",
            json={"engine": 99999, "images": {"img": "http://example.com/i.jpg"}},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 404
        assert resp.json()["status"] == "failure"

    async def test_bad_json(self, client, user_key, engine_with_models):
        """Bad JSON rejected by Pydantic validation → 422."""
        resp = await client.post(
            "/post_processing_request",
            json={"bad": "data"},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 422

    async def test_no_auth(self, client, engine_with_models):
        resp = await client.post(
            "/post_processing_request",
            json={"engine": engine_with_models, "images": {"a": None}},
        )
        assert resp.status_code == 422  # missing required header

    async def test_invalid_api_key(self, client, engine_with_models):
        resp = await client.post(
            "/post_processing_request",
            json={"engine": engine_with_models, "images": {"a": None}},
            headers={"api-key": "nonexistent-key"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /usage_statistics
# ---------------------------------------------------------------------------
class TestUsageStatistics:

    async def test_no_pages(self, client, user_key):
        resp = await client.get(
            "/usage_statistics",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["processed_pages"] == 0

    async def test_with_processed_pages(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            "/usage_statistics",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["processed_pages"] >= 1

    async def test_with_date_range(self, client, user_key):
        resp = await client.get(
            "/usage_statistics/2020-01-01T00:00:00/2099-12-31T23:59:59",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "from_date" in data
        assert "to_date" in data

    async def test_invalid_from_date(self, client, user_key):
        resp = await client.get(
            "/usage_statistics/NOT-A-DATE",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 400

    async def test_invalid_to_date(self, client, user_key):
        resp = await client.get(
            "/usage_statistics/2020-01-01T00:00:00/NOT-A-DATE",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /upload_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------
class TestUploadImage:

    async def test_success(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            files={"file": ("photo.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 50, "image/jpeg")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"

        # Verify page transitioned to WAITING
        status_resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": ukey},
        )
        assert status_resp.json()["request_status"]["page_upload"]["state"] == "WAITING"

    async def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = await client.post(
            f"/upload_image/{fake_rid}/page",
            files={"file": ("photo.jpg", b"img", "image/jpeg")},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 404

    async def test_wrong_owner(self, client, request_with_created_page, other_user_key):
        rid, _, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            files={"file": ("photo.jpg", b"img", "image/jpeg")},
            headers={"api-key": other_user_key},
        )
        assert resp.status_code == 401

    async def test_page_not_in_created_state(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.post(
            f"/upload_image/{rid}/page_001",
            files={"file": ("photo.jpg", b"img", "image/jpeg")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 400

    async def test_page_not_found(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/nonexistent_page",
            files={"file": ("photo.jpg", b"img", "image/jpeg")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 404

    async def test_no_file(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            headers={"api-key": ukey},
        )
        # FastAPI rejects missing required File(...) with 422
        assert resp.status_code == 422

    async def test_unsupported_format(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            files={"file": ("anim.gif", b"GIF89a", "image/gif")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /request_status/<request_id>
# ---------------------------------------------------------------------------
class TestRequestStatus:

    async def test_success(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "page_001" in data["request_status"]
        assert data["request_status"]["page_001"]["state"] == "WAITING"

    async def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = await client.get(
            f"/request_status/{fake_rid}",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 404

    async def test_wrong_owner(self, client, request_with_waiting_pages, other_user_key):
        rid, _, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": other_user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /get_engines
# ---------------------------------------------------------------------------
class TestGetEngines:

    async def test_success(self, client, user_key, engine_with_models):
        resp = await client.get("/get_engines", headers={"api-key": user_key})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        engines = data["engines"]
        assert "test_engine" in engines
        eng = engines["test_engine"]
        assert eng["id"] == engine_with_models
        assert eng["engine_version"] == "v1.0.0"
        assert len(eng["models"]) == 2


# ---------------------------------------------------------------------------
# GET /download_results/<request_id>/<page_name>/<format>
# ---------------------------------------------------------------------------
class TestDownloadResults:

    async def test_alto_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/page_done/alto",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert b"test alto content" in resp.content

    async def test_page_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/page_done/page",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert b"test page content" in resp.content

    async def test_txt_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/page_done/txt",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert b"test text content" in resp.content

    async def test_bad_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/page_done/html",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 400

    async def test_not_processed_yet(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/download_results/{rid}/page_001/alto",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 404

    async def test_expired_page(self, client, request_with_expired_page):
        rid, ukey, pids, eid = request_with_expired_page
        resp = await client.get(
            f"/download_results/{rid}/page_expired/alto",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 404
        assert "expired" in resp.json()["message"].lower()

    async def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = await client.get(
            f"/download_results/{fake_rid}/page/alto",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 404

    async def test_wrong_owner(self, client, request_with_processed_page, other_user_key):
        rid, _, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/page_done/alto",
            headers={"api-key": other_user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /cancel_request/<request_id>
# ---------------------------------------------------------------------------
class TestCancelRequest:

    async def test_success(self, client, request_with_mixed_pages):
        rid, ukey, pids, eid = request_with_mixed_pages
        resp = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"

        # Verify all unfinished pages are now CANCELED
        status_resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": ukey},
        )
        statuses = status_resp.json()["request_status"]
        for page_name in statuses:
            assert statuses[page_name]["state"] == "CANCELED"

    async def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = await client.post(
            f"/cancel_request/{fake_rid}",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 404

    async def test_wrong_owner(self, client, request_with_waiting_pages, other_user_key):
        rid, _, pids, eid = request_with_waiting_pages
        resp = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": other_user_key},
        )
        assert resp.status_code == 401
