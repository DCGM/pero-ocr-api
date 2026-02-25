"""Tests for worker-level API endpoints (require SUPER_USER api-key)."""
import os
import uuid
import zipfile
from io import BytesIO

from tests.conftest import (
    USER_KEY, SUPER_USER_KEY, session_factory, _TestConfig,
)
from app.db.model import Page, PageState


# ---------------------------------------------------------------------------
# GET /get_processing_request/<preferred_engine_id>
# ---------------------------------------------------------------------------
class TestGetProcessingRequest:

    def test_success(self, client, request_with_waiting_pages, super_user_key):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert 'page_id' in data
        assert 'page_url' in data
        assert data['engine_id'] == eid

    def test_no_pages_available(self, client, super_user_key, engine_with_models):
        """No WAITING pages → 204 No Content."""
        resp = client.get(
            f'/get_processing_request/{engine_with_models}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 204

    def test_user_key_denied(self, client, user_key, engine_with_models):
        """USER key should not be able to access worker endpoints."""
        resp = client.get(
            f'/get_processing_request/{engine_with_models}',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 401

    def test_changes_page_to_processing(self, client, request_with_waiting_pages, super_user_key):
        """After fetching, the page state should be PROCESSING."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        page_id = resp.get_json()['page_id']

        # Verify state changed in DB
        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.state == PageState.PROCESSING
        db.close()

    def test_fallback_to_any_engine(self, client, request_with_waiting_pages, super_user_key):
        """Requesting a non-existent engine should fall back to any available page."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            '/get_processing_request/99999',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert data['engine_id'] == eid


# ---------------------------------------------------------------------------
# POST /upload_results/<page_id>
# ---------------------------------------------------------------------------
class TestUploadResults:

    def test_success(self, client, request_with_processing_page, super_user_key):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']

        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<alto>result</alto>'), 'alto.xml'),
                'page': (BytesIO(b'<page>result</page>'), 'page.xml'),
                'txt': (BytesIO(b'OCR text result'), 'result.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.95',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'success'

        # Verify page is now PROCESSED with correct score
        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.state == PageState.PROCESSED
        assert page.score == 95.0  # 0.95 * 100
        assert page.finish_timestamp is not None
        db.close()

        # Verify results ZIP was created and contains the files
        zip_path = os.path.join(
            _TestConfig.PROCESSED_REQUESTS_FOLDER, rid, rid + '.zip'
        )
        assert os.path.isfile(zip_path)
        with zipfile.ZipFile(zip_path, 'r') as zf:
            assert 'page_proc_alto.xml' in zf.namelist()
            assert 'page_proc_page.xml' in zf.namelist()
            assert 'page_proc.txt' in zf.namelist()

    def test_page_not_found(self, client, super_user_key, engine_with_models):
        fake_pid = str(uuid.uuid4())
        resp = client.post(
            f'/upload_results/{fake_pid}',
            data={
                'alto': (BytesIO(b'<alto/>'), 'a.xml'),
                'page': (BytesIO(b'<page/>'), 'p.xml'),
                'txt': (BytesIO(b'txt'), 't.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.5',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 404

    def test_user_key_denied(self, client, request_with_processing_page, user_key):
        """USER key cannot upload results."""
        rid, _, pids, eid = request_with_processing_page
        page_id = pids['page_proc']
        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<a/>'), 'a.xml'),
                'page': (BytesIO(b'<p/>'), 'p.xml'),
                'txt': (BytesIO(b't'), 't.txt'),
            },
            headers={
                'api-key': user_key,
                'score': '0.5',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /latest_engine_version/<engine_id>
# ---------------------------------------------------------------------------
class TestLatestEngineVersion:

    def test_success(self, client, super_user_key, engine_with_models):
        resp = client.get(
            f'/latest_engine_version/{engine_with_models}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert 'test_engine' in data['filename']
        assert 'v1.0.0' in data['filename']

    def test_engine_not_found(self, client, super_user_key):
        resp = client.get(
            '/latest_engine_version/99999',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /download_engine/<engine_id>
# ---------------------------------------------------------------------------
class TestDownloadEngine:

    def test_success(self, client, super_user_key, engine_with_models):
        resp = client.get(
            f'/download_engine/{engine_with_models}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200

        # Verify the response is a valid ZIP containing model files + config
        zf = zipfile.ZipFile(BytesIO(resp.data))
        names = zf.namelist()
        assert 'config.ini' in names
        # Should contain files from both models
        assert any('layout_model' in n for n in names)
        assert any('ocr_model' in n for n in names)

        config_content = zf.read('config.ini').decode('utf-8')
        assert 'RUN_LAYOUT_PARSER' in config_content
        assert 'RUN_OCR' in config_content

    def test_engine_not_found(self, client, super_user_key):
        resp = client.get(
            '/download_engine/99999',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# POST /failed_processing/<page_id>
# ---------------------------------------------------------------------------
class TestFailedProcessing:

    def _call_failed(self, client, page_id, super_user_key, fail_type):
        return client.post(
            f'/failed_processing/{page_id}',
            data=b'Traceback: something went wrong',
            headers={
                'api-key': super_user_key,
                'type': fail_type,
                'engine_version': 'v1.0.0',
                'hostname': 'test-host',
                'ip-address': '127.0.0.1',
            },
        )

    def test_not_found_type(self, client, request_with_processing_page, super_user_key):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']
        resp = self._call_failed(client, page_id, super_user_key, 'NOT_FOUND')
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'success'

        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.state == PageState.NOT_FOUND
        db.close()

    def test_invalid_file_type(self, client, request_with_processing_page, super_user_key):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']
        resp = self._call_failed(client, page_id, super_user_key, 'INVALID_FILE')
        assert resp.status_code == 200

        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.state == PageState.INVALID_FILE
        db.close()

    def test_processing_failed_type(self, client, request_with_processing_page, super_user_key):
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']
        resp = self._call_failed(client, page_id, super_user_key, 'PROCESSING_FAILED')
        assert resp.status_code == 200

        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.state == PageState.PROCESSING_FAILED
        assert page.traceback is not None
        db.close()


# ---------------------------------------------------------------------------
# GET /page_statistics
# ---------------------------------------------------------------------------
class TestPageStatistics:

    def test_success_empty(self, client, super_user_key):
        resp = client.get(
            '/page_statistics',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert 'state_stats' in data
        stats = data['state_stats']
        assert 'WAITING' in stats
        assert 'PROCESSED' in stats

    def test_with_pages(self, client, request_with_waiting_pages, super_user_key):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            '/page_statistics',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        stats = resp.get_json()['state_stats']
        assert stats['WAITING'] >= 2

    def test_user_key_denied(self, client, user_key):
        resp = client.get(
            '/page_statistics',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /download_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------
class TestDownloadImage:

    def test_success(self, client, request_with_uploaded_image, super_user_key):
        rid, ukey, pids, eid = request_with_uploaded_image
        resp = client.get(
            f'/download_image/{rid}/page_img.jpg',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        # Verify we got the dummy JPEG data back
        assert resp.data[:4] == b'\xff\xd8\xff\xe0'

    def test_not_uploaded_yet(self, client, request_with_created_page, super_user_key):
        rid, ukey, pids, eid = request_with_created_page
        resp = client.get(
            f'/download_image/{rid}/page_upload.jpg',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 404
        assert 'not been uploaded' in resp.get_json()['message']

    def test_already_processed(self, client, request_with_processed_page, super_user_key):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_image/{rid}/page_done.jpg',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 405
        assert 'already processed' in resp.get_json()['message']

    def test_request_not_found(self, client, super_user_key):
        fake_rid = str(uuid.uuid4())
        resp = client.get(
            f'/download_image/{fake_rid}/img.jpg',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 404

    def test_page_not_found(self, client, request_with_uploaded_image, super_user_key):
        rid, ukey, pids, eid = request_with_uploaded_image
        resp = client.get(
            f'/download_image/{rid}/nonexistent.jpg',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 404
