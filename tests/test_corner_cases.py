"""
Additional corner-case and integration tests that were missing from the
initial test suite.  Covers scheduling atomicity, fair round-robin, suspension
exclusion, request completion, filesystem cleanup, user isolation, and a
full end-to-end workflow.
"""
import os
import uuid
import datetime
import zipfile
from io import BytesIO

from sqlalchemy import select

from tests.conftest import (
    USER_KEY,
    SUPER_USER_KEY,
    OTHER_USER_KEY,
    SECOND_USER_KEY,
    _session_maker,
    _test_settings,
    _create_request_with_pages,
)
from db.models import Page, PageState, Request, ApiKey


# ---------------------------------------------------------------------------
# GET /get_processing_request — scheduling corner cases
# ---------------------------------------------------------------------------
class TestGetProcessingRequestScheduling:
    """Tests for worker task scheduling atomicity and fairness."""

    async def test_consecutive_calls_drain_queue(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """Two consecutive calls should return two different pages, then 204."""
        rid, ukey, pids, eid = request_with_waiting_pages

        resp1 = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp1.status_code == 200
        pid1 = resp1.json()["page_id"]

        resp2 = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp2.status_code == 200
        pid2 = resp2.json()["page_id"]

        # Two distinct pages returned
        assert str(pid1) != str(pid2)

        # Queue is now drained
        resp3 = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp3.status_code == 204

    async def test_suspended_user_pages_not_dispatched(
        self, client, suspended_user_request_with_waiting_page, super_user_key
    ):
        """Pages from suspended users must not be dispatched to workers."""
        _rid, _sukey, _pids, eid = suspended_user_request_with_waiting_page

        resp = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        # Only the suspended user has WAITING pages → nothing available
        assert resp.status_code == 204

    async def test_fair_scheduling_across_users(
        self, client, two_users_waiting_pages, super_user_key
    ):
        """
        When two users each have waiting pages and neither has recent
        processed pages, the scheduler should serve both (order may vary).
        """
        rid1, rid2, pids1, pids2, ukey1, ukey2, eid = two_users_waiting_pages

        resp1 = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp1.status_code == 200

        resp2 = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp2.status_code == 200

        # After two fetches both user pages should have been dispatched
        dispatched = {str(resp1.json()["page_id"]), str(resp2.json()["page_id"])}
        all_pages = set(pids1.values()) | set(pids2.values())
        assert dispatched == all_pages

    async def test_processing_timestamp_is_set(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """Fetched page must have processing_timestamp set."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        page_id = resp.json()["page_id"]

        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.processing_timestamp is not None


# ---------------------------------------------------------------------------
# POST /upload_results — completion and cleanup corner cases
# ---------------------------------------------------------------------------
class TestUploadResultsCompletion:

    async def test_request_finish_timestamp_set_when_last_page_processed(
        self, client, request_with_processing_page, super_user_key
    ):
        """When the only/last page is processed, request.finish_timestamp should be set."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("a.xml", b"<alto/>"),
                "page": ("p.xml", b"<page/>"),
                "txt": ("t.txt", b"text"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.88",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(select(Request).where(Request.id == uuid.UUID(rid)))
            req = result.scalar_one()
            assert req.finish_timestamp is not None

    async def test_uploaded_image_removed_after_processing(
        self, client, request_with_uploaded_image, super_user_key
    ):
        """After upload_results, the source image should be deleted from disk."""
        rid, ukey, pids, eid = request_with_uploaded_image
        page_id = pids["page_img"]

        # The image should exist before processing
        img_path = os.path.join(
            _test_settings.UPLOAD_IMAGES_FOLDER, rid, "page_img.jpg"
        )
        assert os.path.isfile(img_path)

        # First change the page state to PROCESSING (simulate worker fetch)
        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            page.state = PageState.PROCESSING
            page.processing_timestamp = datetime.datetime.now()
            await db.commit()

        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("a.xml", b"<alto/>"),
                "page": ("p.xml", b"<page/>"),
                "txt": ("t.txt", b"text"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.90",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 200

        # Image should have been removed
        assert not os.path.isfile(img_path)

    async def test_score_rounding(
        self, client, request_with_processing_page, super_user_key
    ):
        """Score 0.12345 should be stored as round(0.12345 * 100, 2) = 12.35."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("a.xml", b"<alto/>"),
                "page": ("p.xml", b"<page/>"),
                "txt": ("t.txt", b"text"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.12345",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.score == 12.35


# ---------------------------------------------------------------------------
# POST /cancel_request — partial cancel corner case
# ---------------------------------------------------------------------------
class TestCancelRequestPartial:

    async def test_cancel_leaves_processed_pages_untouched(
        self, client, request_with_one_processed_one_waiting
    ):
        """Cancellation should only affect CREATED/WAITING/PROCESSING pages,
        leaving already-PROCESSED pages in their original state."""
        rid, ukey, pids, eid = request_with_one_processed_one_waiting

        resp = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200

        status_resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": ukey},
        )
        statuses = status_resp.json()["request_status"]
        assert statuses["page_already_done"]["state"] == "PROCESSED"
        assert statuses["page_already_done"]["quality"] == 90.0
        assert statuses["page_still_waiting"]["state"] == "CANCELED"

    async def test_cancel_idempotent(
        self, client, request_with_waiting_pages
    ):
        """Cancelling an already-cancelled request should succeed (no-op)."""
        rid, ukey, pids, eid = request_with_waiting_pages
        # Cancel once
        resp1 = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": ukey},
        )
        assert resp1.status_code == 200
        # Cancel again
        resp2 = await client.post(
            f"/cancel_request/{rid}",
            headers={"api-key": ukey},
        )
        assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# POST /post_processing_request — mixed image URLs
# ---------------------------------------------------------------------------
class TestPostProcessingRequestMixed:

    async def test_mixed_null_and_url_images(self, client, user_key, engine_with_models):
        """Pages with null URLs start as CREATED; pages with URLs start as WAITING."""
        resp = await client.post(
            "/post_processing_request",
            json={
                "engine": engine_with_models,
                "images": {
                    "upload_later": None,
                    "ready_now": "http://example.com/img.jpg",
                },
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        rid = str(resp.json()["request_id"])

        status_resp = await client.get(
            f"/request_status/{rid}",
            headers={"api-key": user_key},
        )
        statuses = status_resp.json()["request_status"]
        assert statuses["upload_later"]["state"] == "CREATED"
        assert statuses["ready_now"]["state"] == "WAITING"


# ---------------------------------------------------------------------------
# GET /download_results — page not found within valid request
# ---------------------------------------------------------------------------
class TestDownloadResultsPageNotFound:

    async def test_page_name_not_in_request(self, client, request_with_processed_page):
        """Request exists but the page name doesn't."""
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_results/{rid}/nonexistent_page/alto",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 404
        assert "does not exist" in resp.json()["message"]


# ---------------------------------------------------------------------------
# GET /get_engines — auth
# ---------------------------------------------------------------------------
class TestGetEnginesAuth:

    async def test_no_auth(self, client, engine_with_models):
        resp = await client.get("/get_engines")
        assert resp.status_code == 422  # missing required header


# ---------------------------------------------------------------------------
# GET /usage_statistics — user isolation
# ---------------------------------------------------------------------------
class TestUsageStatisticsIsolation:

    async def test_does_not_count_other_users_pages(
        self, client, request_with_processed_page, other_user_key
    ):
        """Other user's processed pages should not be counted."""
        resp = await client.get(
            "/usage_statistics",
            headers={"api-key": other_user_key},
        )
        assert resp.status_code == 200
        assert resp.json()["processed_pages"] == 0

    async def test_counts_own_expired_pages(
        self, client, request_with_expired_page
    ):
        """EXPIRED pages should still be counted in usage statistics."""
        rid, ukey, pids, eid = request_with_expired_page
        resp = await client.get(
            "/usage_statistics",
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200
        assert resp.json()["processed_pages"] >= 1


# ---------------------------------------------------------------------------
# POST /upload_image — filesystem verification
# ---------------------------------------------------------------------------
class TestUploadImageFilesystem:

    async def test_file_saved_to_disk(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            files={"file": ("photo.png", b"\xff\xd8\xff\xe0" + b"\x00" * 50, "image/png")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200

        # Verify the file landed on disk
        expected_path = os.path.join(
            _test_settings.UPLOAD_IMAGES_FOLDER, rid, "page_upload.png"
        )
        assert os.path.isfile(expected_path)

    async def test_url_points_to_download_image(self, client, request_with_created_page):
        """After upload, page URL should point to the /download_image endpoint."""
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.post(
            f"/upload_image/{rid}/page_upload",
            files={"file": ("photo.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 50, "image/jpeg")},
            headers={"api-key": ukey},
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(
                select(Page).where(Page.name == "page_upload")
            )
            page = result.scalar_one()
            assert "/download_image/" in page.url
            assert rid in page.url


# ---------------------------------------------------------------------------
# POST /failed_processing — request completion on failure
# ---------------------------------------------------------------------------
class TestFailedProcessingCompletion:

    async def test_request_finished_when_only_page_fails(
        self, client, request_with_processing_page, super_user_key
    ):
        """If the only page in a request fails, request.finish_timestamp should be set."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

        resp = await client.post(
            f"/failed_processing/{page_id}",
            content=b"Traceback: error",
            headers={
                "api-key": super_user_key,
                "type": "NOT_FOUND",
                "engine_version": "v1.0.0",
                "hostname": "test-host",
                "ip-address": "127.0.0.1",
            },
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(
                select(Request).where(Request.id == uuid.UUID(rid))
            )
            req = result.scalar_one()
            assert req.finish_timestamp is not None

    async def test_traceback_stored_for_all_fail_types(
        self, client, super_user_key, user_key, engine_with_models
    ):
        """Traceback should be stored in the DB for every failure type."""
        for fail_type in ["NOT_FOUND", "INVALID_FILE", "PROCESSING_FAILED"]:
            # Create a fresh request/page for each type
            rid, pids = await _create_request_with_pages(
                user_key,
                engine_with_models,
                [(f"fail_{fail_type}", "http://example.com/x.jpg", PageState.PROCESSING)],
            )
            page_id = list(pids.values())[0]

            await client.post(
                f"/failed_processing/{page_id}",
                content=b"detailed traceback text",
                headers={
                    "api-key": super_user_key,
                    "type": fail_type,
                    "engine_version": "v1.0.0",
                    "hostname": "h",
                    "ip-address": "1.2.3.4",
                },
            )

            async with _session_maker() as db:
                result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
                page = result.scalar_one()
                assert page.traceback is not None
                assert "traceback" in page.traceback.lower()


# ---------------------------------------------------------------------------
# End-to-end workflow test
# ---------------------------------------------------------------------------
class TestEndToEndWorkflow:
    """
    Full lifecycle:
      1. Create request (user)
      2. Upload image (user)
      3. Check status (user) → WAITING
      4. Worker fetches page → PROCESSING
      5. Worker uploads results → PROCESSED
      6. User downloads results
    """

    async def test_full_lifecycle(
        self, client, user_key, super_user_key, engine_with_models
    ):
        eid = engine_with_models

        # 1. Create request with one page (upload pending)
        resp = await client.post(
            "/post_processing_request",
            json={"engine": eid, "images": {"doc_page_1": None}},
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200
        rid = str(resp.json()["request_id"])

        # 2. Upload image
        resp = await client.post(
            f"/upload_image/{rid}/doc_page_1",
            files={
                "file": ("scan.jpg", b"\xff\xd8\xff\xe0" + b"\x00" * 50, "image/jpeg"),
            },
            headers={"api-key": user_key},
        )
        assert resp.status_code == 200

        # 3. Status should be WAITING
        resp = await client.get(
            f"/request_status/{rid}", headers={"api-key": user_key}
        )
        assert resp.json()["request_status"]["doc_page_1"]["state"] == "WAITING"

        # 4. Worker fetches the page
        resp = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        page_id = resp.json()["page_id"]
        assert resp.json()["engine_id"] == eid

        # Verify PROCESSING
        resp = await client.get(
            f"/request_status/{rid}", headers={"api-key": user_key}
        )
        assert resp.json()["request_status"]["doc_page_1"]["state"] == "PROCESSING"

        # 5. Worker uploads results
        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("alto.xml", b"<alto>OCR output</alto>"),
                "page": ("page.xml", b"<page>OCR output</page>"),
                "txt": ("result.txt", b"OCR text output"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.92",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 200

        # 6. User checks status → PROCESSED with score
        resp = await client.get(
            f"/request_status/{rid}", headers={"api-key": user_key}
        )
        status = resp.json()["request_status"]["doc_page_1"]
        assert status["state"] == "PROCESSED"
        assert status["quality"] == 92.0

        # 7. User downloads results in all three formats
        for fmt, expected in [
            ("alto", b"OCR output"),
            ("page", b"OCR output"),
            ("txt", b"OCR text output"),
        ]:
            resp = await client.get(
                f"/download_results/{rid}/doc_page_1/{fmt}",
                headers={"api-key": user_key},
            )
            assert resp.status_code == 200
            assert expected in resp.content

        # 8. Request should be marked as finished
        async with _session_maker() as db:
            result = await db.execute(
                select(Request).where(Request.id == uuid.UUID(rid))
            )
            req = result.scalar_one()
            assert req.finish_timestamp is not None

        # 9. Uploaded image should have been cleaned up
        img_path = os.path.join(
            _test_settings.UPLOAD_IMAGES_FOLDER, rid, "doc_page_1.jpg"
        )
        assert not os.path.isfile(img_path)

        # 10. Usage statistics should count the processed page
        resp = await client.get(
            "/usage_statistics", headers={"api-key": user_key}
        )
        assert resp.json()["processed_pages"] >= 1
