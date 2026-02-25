"""Tests for worker-level API endpoints (require SUPER_USER api-key)."""
import os
import uuid
import zipfile
from io import BytesIO

from sqlalchemy import select

from tests.conftest import _session_maker, _test_settings
from db.models import Page, PageState


# ---------------------------------------------------------------------------
# GET /get_processing_request/<preferred_engine_id>
# ---------------------------------------------------------------------------
class TestGetProcessingRequest:

    async def test_success(self, client, request_with_waiting_pages, super_user_key):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "page_id" in data
        assert "page_url" in data
        assert data["engine_id"] == eid

    async def test_no_pages_available(self, client, super_user_key, engine_with_models):
        """No WAITING pages → 204 No Content."""
        resp = await client.get(
            f"/get_processing_request/{engine_with_models}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 204

    async def test_user_key_denied(self, client, user_key, engine_with_models):
        """USER key should not be able to access worker endpoints."""
        resp = await client.get(
            f"/get_processing_request/{engine_with_models}",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401

    async def test_changes_page_to_processing(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """After fetching, the page state should be PROCESSING."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            f"/get_processing_request/{eid}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        page_id = resp.json()["page_id"]

        # Verify state changed in DB
        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.state == PageState.PROCESSING

    async def test_fallback_to_any_engine(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """Requesting a non-existent engine should fall back to any available page."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            "/get_processing_request/99999",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["engine_id"] == eid


# ---------------------------------------------------------------------------
# POST /upload_results/<page_id>
# ---------------------------------------------------------------------------
class TestUploadResults:

    async def test_success(self, client, request_with_processing_page, super_user_key):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]

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
        assert resp.json()["status"] == "success"

        # Verify page is now PROCESSED with correct score
        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.state == PageState.PROCESSED
            assert page.score == 95.0  # 0.95 * 100
            assert page.finish_timestamp is not None

        # Verify results ZIP was created and contains the files
        zip_path = os.path.join(
            _test_settings.PROCESSED_REQUESTS_FOLDER, rid, rid + ".zip"
        )
        assert os.path.isfile(zip_path)
        with zipfile.ZipFile(zip_path, "r") as zf:
            assert "page_proc_alto.xml" in zf.namelist()
            assert "page_proc_page.xml" in zf.namelist()
            assert "page_proc.txt" in zf.namelist()

    async def test_page_not_found(self, client, super_user_key, engine_with_models):
        fake_pid = str(uuid.uuid4())
        resp = await client.post(
            f"/upload_results/{fake_pid}",
            files={
                "alto": ("a.xml", b"<alto/>"),
                "page": ("p.xml", b"<page/>"),
                "txt": ("t.txt", b"txt"),
            },
            headers={
                "api-key": super_user_key,
                "score": "0.5",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 404

    async def test_user_key_denied(
        self, client, request_with_processing_page, user_key
    ):
        """USER key cannot upload results."""
        rid, _, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]
        resp = await client.post(
            f"/upload_results/{page_id}",
            files={
                "alto": ("a.xml", b"<a/>"),
                "page": ("p.xml", b"<p/>"),
                "txt": ("t.txt", b"t"),
            },
            headers={
                "api-key": user_key,
                "score": "0.5",
                "engine-version": "v1.0.0",
            },
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /latest_engine_version/<engine_id>
# ---------------------------------------------------------------------------
class TestLatestEngineVersion:

    async def test_success(self, client, super_user_key, engine_with_models):
        resp = await client.get(
            f"/latest_engine_version/{engine_with_models}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "test_engine" in data["filename"]
        assert "v1.0.0" in data["filename"]

    async def test_engine_not_found(self, client, super_user_key):
        resp = await client.get(
            "/latest_engine_version/99999",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /download_engine/<engine_id>
# ---------------------------------------------------------------------------
class TestDownloadEngine:

    async def test_success(self, client, super_user_key, engine_with_models):
        resp = await client.get(
            f"/download_engine/{engine_with_models}",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200

        # Verify the response is a valid ZIP containing model files + config
        zf = zipfile.ZipFile(BytesIO(resp.content))
        names = zf.namelist()
        assert "config.ini" in names
        # Should contain files from both models
        assert any("layout_model" in n for n in names)
        assert any("ocr_model" in n for n in names)

        config_content = zf.read("config.ini").decode("utf-8")
        assert "RUN_LAYOUT_PARSER" in config_content
        assert "RUN_OCR" in config_content

    async def test_engine_not_found(self, client, super_user_key):
        resp = await client.get(
            "/download_engine/99999",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /failed_processing/<page_id>
# ---------------------------------------------------------------------------
class TestFailedProcessing:

    async def _call_failed(self, client, page_id, super_user_key, fail_type):
        return await client.post(
            f"/failed_processing/{page_id}",
            content=b"Traceback: something went wrong",
            headers={
                "api-key": super_user_key,
                "type": fail_type,
                "engine_version": "v1.0.0",
                "hostname": "test-host",
                "ip-address": "127.0.0.1",
            },
        )

    async def test_not_found_type(
        self, client, request_with_processing_page, super_user_key
    ):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]
        resp = await self._call_failed(client, page_id, super_user_key, "NOT_FOUND")
        assert resp.status_code == 200
        assert resp.json()["status"] == "success"

        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.state == PageState.NOT_FOUND

    async def test_invalid_file_type(
        self, client, request_with_processing_page, super_user_key
    ):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]
        resp = await self._call_failed(client, page_id, super_user_key, "INVALID_FILE")
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.state == PageState.INVALID_FILE

    async def test_processing_failed_type(
        self, client, request_with_processing_page, super_user_key
    ):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids["page_proc"]
        resp = await self._call_failed(
            client, page_id, super_user_key, "PROCESSING_FAILED"
        )
        assert resp.status_code == 200

        async with _session_maker() as db:
            result = await db.execute(select(Page).where(Page.id == uuid.UUID(page_id)))
            page = result.scalar_one()
            assert page.state == PageState.PROCESSING_FAILED
            assert page.traceback is not None


# ---------------------------------------------------------------------------
# GET /page_statistics
# ---------------------------------------------------------------------------
class TestPageStatistics:

    async def test_success_empty(self, client, super_user_key):
        resp = await client.get(
            "/page_statistics",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "state_stats" in data
        stats = data["state_stats"]
        assert "WAITING" in stats
        assert "PROCESSED" in stats

    async def test_with_pages(self, client, request_with_waiting_pages, super_user_key):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = await client.get(
            "/page_statistics",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        stats = resp.json()["state_stats"]
        assert stats["WAITING"] >= 2

    async def test_user_key_denied(self, client, user_key):
        resp = await client.get(
            "/page_statistics",
            headers={"api-key": user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /download_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------
class TestDownloadImage:

    async def test_success(self, client, request_with_uploaded_image, super_user_key):
        rid, ukey, pids, eid = request_with_uploaded_image
        resp = await client.get(
            f"/download_image/{rid}/page_img.jpg",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 200
        # Verify we got the dummy JPEG data back
        assert resp.content[:4] == b"\xff\xd8\xff\xe0"

    async def test_not_uploaded_yet(
        self, client, request_with_created_page, super_user_key
    ):
        rid, ukey, pids, eid = request_with_created_page
        resp = await client.get(
            f"/download_image/{rid}/page_upload.jpg",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404
        assert "not been uploaded" in resp.json()["message"]

    async def test_already_processed(
        self, client, request_with_processed_page, super_user_key
    ):
        rid, ukey, pids, eid = request_with_processed_page
        resp = await client.get(
            f"/download_image/{rid}/page_done.jpg",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 405
        assert "already processed" in resp.json()["message"]

    async def test_request_not_found(self, client, super_user_key):
        fake_rid = str(uuid.uuid4())
        resp = await client.get(
            f"/download_image/{fake_rid}/img.jpg",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404

    async def test_page_not_found(
        self, client, request_with_uploaded_image, super_user_key
    ):
        rid, ukey, pids, eid = request_with_uploaded_image
        resp = await client.get(
            f"/download_image/{rid}/nonexistent.jpg",
            headers={"api-key": super_user_key},
        )
        assert resp.status_code == 404
