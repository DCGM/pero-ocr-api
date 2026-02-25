"""Tests for user-level API endpoints (require USER or SUPER_USER api-key)."""
import uuid
from io import BytesIO

import pytest

from tests.conftest import USER_KEY, OTHER_USER_KEY, session_factory
from app.db.model import Page, PageState


# ---------------------------------------------------------------------------
# POST /post_processing_request
# ---------------------------------------------------------------------------
class TestPostProcessingRequest:

    def test_success_with_image_urls(self, client, user_key, engine_with_models):
        resp = client.post(
            '/post_processing_request',
            json={
                'engine': engine_with_models,
                'images': {
                    'img_001': 'http://example.com/photo1.jpg',
                    'img_002': 'http://example.com/photo2.jpg',
                },
            },
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert 'request_id' in data

    def test_success_with_null_urls(self, client, user_key, engine_with_models):
        """Pages with null URLs should be created in CREATED state."""
        resp = client.post(
            '/post_processing_request',
            json={
                'engine': engine_with_models,
                'images': {'page_a': None, 'page_b': None},
            },
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'

        # Verify page states via request_status
        rid = str(data['request_id'])
        status_resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': user_key},
        )
        status_data = status_resp.get_json()
        assert status_data['request_status']['page_a']['state'] == 'CREATED'
        assert status_data['request_status']['page_b']['state'] == 'CREATED'

    def test_invalid_engine(self, client, user_key, engine_with_models):
        resp = client.post(
            '/post_processing_request',
            json={'engine': 99999, 'images': {'img': 'http://example.com/i.jpg'}},
            headers={'api-key': user_key},
        )
        assert resp.status_code == 404
        assert resp.get_json()['status'] == 'failure'

    def test_bad_json(self, client, user_key, engine_with_models):
        """Bad JSON triggers KeyError in create_request().  The app's except
        clause calls exception.encode('utf-8') producing bytes, which
        jsonify cannot serialize — a known bug.  In Flask test mode this
        surfaces as a TypeError."""
        with pytest.raises(TypeError):
            client.post(
                '/post_processing_request',
                json={'bad': 'data'},
                headers={'api-key': user_key},
            )

    def test_no_auth(self, client, engine_with_models):
        resp = client.post(
            '/post_processing_request',
            json={'engine': engine_with_models, 'images': {'a': None}},
        )
        assert resp.status_code == 401

    def test_invalid_api_key(self, client, engine_with_models):
        resp = client.post(
            '/post_processing_request',
            json={'engine': engine_with_models, 'images': {'a': None}},
            headers={'api-key': 'nonexistent-key'},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /usage_statistics
# ---------------------------------------------------------------------------
class TestUsageStatistics:

    def test_no_pages(self, client, user_key):
        resp = client.get(
            '/usage_statistics',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert data['processed_pages'] == 0

    def test_with_processed_pages(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            '/usage_statistics',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['processed_pages'] >= 1

    def test_with_date_range(self, client, user_key):
        resp = client.get(
            '/usage_statistics/2020-01-01T00:00:00/2099-12-31T23:59:59',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'from' in data
        assert 'to' in data

    def test_invalid_from_date(self, client, user_key):
        resp = client.get(
            '/usage_statistics/NOT-A-DATE',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 400

    def test_invalid_to_date(self, client, user_key):
        resp = client.get(
            '/usage_statistics/2020-01-01T00:00:00/NOT-A-DATE',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# POST /upload_image/<request_id>/<page_name>
# ---------------------------------------------------------------------------
class TestUploadImage:

    def test_success(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        data = {
            'file': (BytesIO(b'\xff\xd8\xff\xe0' + b'\x00' * 50), 'photo.jpg'),
        }
        resp = client.post(
            f'/upload_image/{rid}/page_upload',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'success'

        # Verify page transitioned to WAITING
        status_resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': ukey},
        )
        assert status_resp.get_json()['request_status']['page_upload']['state'] == 'WAITING'

    def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        data = {'file': (BytesIO(b'img'), 'photo.jpg')}
        resp = client.post(
            f'/upload_image/{fake_rid}/page',
            data=data,
            headers={'api-key': user_key},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 404

    def test_wrong_owner(self, client, request_with_created_page, other_user_key):
        rid, _, pids, eid = request_with_created_page
        data = {'file': (BytesIO(b'img'), 'photo.jpg')}
        resp = client.post(
            f'/upload_image/{rid}/page_upload',
            data=data,
            headers={'api-key': other_user_key},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 401

    def test_page_not_in_created_state(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        data = {'file': (BytesIO(b'img'), 'photo.jpg')}
        resp = client.post(
            f'/upload_image/{rid}/page_001',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 400

    def test_page_not_found(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        data = {'file': (BytesIO(b'img'), 'photo.jpg')}
        resp = client.post(
            f'/upload_image/{rid}/nonexistent_page',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 404

    def test_no_file(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        resp = client.post(
            f'/upload_image/{rid}/page_upload',
            data={},
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 400

    def test_unsupported_format(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        data = {'file': (BytesIO(b'GIF89a'), 'anim.gif')}
        resp = client.post(
            f'/upload_image/{rid}/page_upload',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /request_status/<request_id>
# ---------------------------------------------------------------------------
class TestRequestStatus:

    def test_success(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        assert 'page_001' in data['request_status']
        assert data['request_status']['page_001']['state'] == 'WAITING'

    def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = client.get(
            f'/request_status/{fake_rid}',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 404

    def test_wrong_owner(self, client, request_with_waiting_pages, other_user_key):
        rid, _, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': other_user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /get_engines
# ---------------------------------------------------------------------------
class TestGetEngines:

    def test_success(self, client, user_key, engine_with_models):
        resp = client.get('/get_engines', headers={'api-key': user_key})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'success'
        engines = data['engines']
        assert 'test_engine' in engines
        eng = engines['test_engine']
        assert eng['id'] == engine_with_models
        assert eng['engine_version'] == 'v1.0.0'
        assert len(eng['models']) == 2


# ---------------------------------------------------------------------------
# GET /download_results/<request_id>/<page_name>/<format>
# ---------------------------------------------------------------------------
class TestDownloadResults:

    def test_alto_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/page_done/alto',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        assert b'test alto content' in resp.data

    def test_page_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/page_done/page',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        assert b'test page content' in resp.data

    def test_txt_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/page_done/txt',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        assert b'test text content' in resp.data

    def test_bad_format(self, client, request_with_processed_page):
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/page_done/html',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 400

    def test_not_processed_yet(self, client, request_with_waiting_pages):
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/download_results/{rid}/page_001/alto',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 404

    def test_expired_page(self, client, request_with_expired_page):
        rid, ukey, pids, eid = request_with_expired_page
        resp = client.get(
            f'/download_results/{rid}/page_expired/alto',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 404
        assert 'expired' in resp.get_json()['message'].lower()

    def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = client.get(
            f'/download_results/{fake_rid}/page/alto',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 404

    def test_wrong_owner(self, client, request_with_processed_page, other_user_key):
        rid, _, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/page_done/alto',
            headers={'api-key': other_user_key},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /cancel_request/<request_id>
# ---------------------------------------------------------------------------
class TestCancelRequest:

    def test_success(self, client, request_with_mixed_pages):
        rid, ukey, pids, eid = request_with_mixed_pages
        resp = client.post(
            f'/cancel_request/{rid}',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        assert resp.get_json()['status'] == 'success'

        # Verify all unfinished pages are now CANCELED
        status_resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': ukey},
        )
        statuses = status_resp.get_json()['request_status']
        for page_name in statuses:
            assert statuses[page_name]['state'] == 'CANCELED'

    def test_request_not_found(self, client, user_key):
        fake_rid = str(uuid.uuid4())
        resp = client.post(
            f'/cancel_request/{fake_rid}',
            headers={'api-key': user_key},
        )
        assert resp.status_code == 404

    def test_wrong_owner(self, client, request_with_waiting_pages, other_user_key):
        rid, _, pids, eid = request_with_waiting_pages
        resp = client.post(
            f'/cancel_request/{rid}',
            headers={'api-key': other_user_key},
        )
        assert resp.status_code == 401
