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

from tests.conftest import (
    USER_KEY, SUPER_USER_KEY, OTHER_USER_KEY, SECOND_USER_KEY,
    session_factory, _TestConfig, _create_request_with_pages,
)
from app.db.model import Page, PageState, Request, ApiKey


# ---------------------------------------------------------------------------
# GET /get_processing_request — scheduling corner cases
# ---------------------------------------------------------------------------
class TestGetProcessingRequestScheduling:
    """Tests for worker task scheduling atomicity and fairness."""

    def test_consecutive_calls_drain_queue(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """Two consecutive calls should return two different pages, then 204."""
        rid, ukey, pids, eid = request_with_waiting_pages

        resp1 = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp1.status_code == 200
        pid1 = resp1.get_json()['page_id']

        resp2 = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp2.status_code == 200
        pid2 = resp2.get_json()['page_id']

        # Two distinct pages returned
        assert str(pid1) != str(pid2)

        # Queue is now drained
        resp3 = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp3.status_code == 204

    def test_suspended_user_pages_not_dispatched(
        self, client, suspended_user_request_with_waiting_page, super_user_key
    ):
        """Pages from suspended users must not be dispatched to workers."""
        _rid, _sukey, _pids, eid = suspended_user_request_with_waiting_page

        resp = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        # Only the suspended user has WAITING pages → nothing available
        assert resp.status_code == 204

    def test_fair_scheduling_across_users(
        self, client, two_users_waiting_pages, super_user_key
    ):
        """
        When two users each have waiting pages and neither has recent
        processed pages, the scheduler should serve both (order may vary).
        """
        rid1, rid2, pids1, pids2, ukey1, ukey2, eid = two_users_waiting_pages

        resp1 = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp1.status_code == 200

        resp2 = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp2.status_code == 200

        # After two fetches both user pages should have been dispatched
        dispatched = {str(resp1.get_json()['page_id']),
                      str(resp2.get_json()['page_id'])}
        all_pages = set(pids1.values()) | set(pids2.values())
        assert dispatched == all_pages

    def test_processing_timestamp_is_set(
        self, client, request_with_waiting_pages, super_user_key
    ):
        """Fetched page must have processing_timestamp set."""
        rid, ukey, pids, eid = request_with_waiting_pages
        resp = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        page_id = resp.get_json()['page_id']

        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.processing_timestamp is not None
        db.close()


# ---------------------------------------------------------------------------
# POST /upload_results — completion and cleanup corner cases
# ---------------------------------------------------------------------------
class TestUploadResultsCompletion:

    def test_request_finish_timestamp_set_when_last_page_processed(
        self, client, request_with_processing_page, super_user_key
    ):
        """When the only/last page is processed, request.finish_timestamp should be set."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']

        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<alto/>'), 'a.xml'),
                'page': (BytesIO(b'<page/>'), 'p.xml'),
                'txt': (BytesIO(b'text'), 't.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.88',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        db = session_factory()
        req = db.query(Request).filter(Request.id == rid).first()
        assert req.finish_timestamp is not None
        db.close()

    def test_uploaded_image_removed_after_processing(
        self, client, request_with_uploaded_image, super_user_key
    ):
        """After upload_results, the source image should be deleted from disk."""
        rid, ukey, pids, eid = request_with_uploaded_image
        page_id = pids['page_img']

        # The image should exist before processing
        img_path = os.path.join(
            _TestConfig.UPLOAD_IMAGES_FOLDER, rid, 'page_img.jpg'
        )
        assert os.path.isfile(img_path)

        # First change the page state to PROCESSING (simulate worker fetch)
        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        page.state = PageState.PROCESSING
        page.processing_timestamp = datetime.datetime.now()
        db.commit()
        db.close()

        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<alto/>'), 'a.xml'),
                'page': (BytesIO(b'<page/>'), 'p.xml'),
                'txt': (BytesIO(b'text'), 't.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.90',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        # Image should have been removed
        assert not os.path.isfile(img_path)

    def test_score_rounding(self, client, request_with_processing_page, super_user_key):
        """Score 0.12345 should be stored as round(0.12345 * 100, 2) = 12.35."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']

        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<alto/>'), 'a.xml'),
                'page': (BytesIO(b'<page/>'), 'p.xml'),
                'txt': (BytesIO(b'text'), 't.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.12345',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        db = session_factory()
        page = db.query(Page).filter(Page.id == page_id).first()
        assert page.score == 12.35
        db.close()


# ---------------------------------------------------------------------------
# POST /cancel_request — partial cancel corner case
# ---------------------------------------------------------------------------
class TestCancelRequestPartial:

    def test_cancel_leaves_processed_pages_untouched(
        self, client, request_with_one_processed_one_waiting
    ):
        """Cancellation should only affect CREATED/WAITING/PROCESSING pages,
        leaving already-PROCESSED pages in their original state."""
        rid, ukey, pids, eid = request_with_one_processed_one_waiting

        resp = client.post(
            f'/cancel_request/{rid}',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200

        status_resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': ukey},
        )
        statuses = status_resp.get_json()['request_status']
        assert statuses['page_already_done']['state'] == 'PROCESSED'
        assert statuses['page_already_done']['quality'] == 90.0
        assert statuses['page_still_waiting']['state'] == 'CANCELED'

    def test_cancel_idempotent(
        self, client, request_with_waiting_pages
    ):
        """Cancelling an already-cancelled request should succeed (no-op)."""
        rid, ukey, pids, eid = request_with_waiting_pages
        # Cancel once
        resp1 = client.post(
            f'/cancel_request/{rid}', headers={'api-key': ukey},
        )
        assert resp1.status_code == 200
        # Cancel again
        resp2 = client.post(
            f'/cancel_request/{rid}', headers={'api-key': ukey},
        )
        assert resp2.status_code == 200


# ---------------------------------------------------------------------------
# POST /post_processing_request — mixed image URLs
# ---------------------------------------------------------------------------
class TestPostProcessingRequestMixed:

    def test_mixed_null_and_url_images(self, client, user_key, engine_with_models):
        """Pages with null URLs start as CREATED; pages with URLs start as WAITING."""
        resp = client.post(
            '/post_processing_request',
            json={
                'engine': engine_with_models,
                'images': {
                    'upload_later': None,
                    'ready_now': 'http://example.com/img.jpg',
                },
            },
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        rid = str(resp.get_json()['request_id'])

        status_resp = client.get(
            f'/request_status/{rid}',
            headers={'api-key': user_key},
        )
        statuses = status_resp.get_json()['request_status']
        assert statuses['upload_later']['state'] == 'CREATED'
        assert statuses['ready_now']['state'] == 'WAITING'


# ---------------------------------------------------------------------------
# GET /download_results — page not found within valid request
# ---------------------------------------------------------------------------
class TestDownloadResultsPageNotFound:

    def test_page_name_not_in_request(self, client, request_with_processed_page):
        """Request exists but the page name doesn't."""
        rid, ukey, pids, eid = request_with_processed_page
        resp = client.get(
            f'/download_results/{rid}/nonexistent_page/alto',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 404
        assert 'does not exist' in resp.get_json()['message']


# ---------------------------------------------------------------------------
# GET /get_engines — auth
# ---------------------------------------------------------------------------
class TestGetEnginesAuth:

    def test_no_auth(self, client, engine_with_models):
        resp = client.get('/get_engines')
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /usage_statistics — user isolation
# ---------------------------------------------------------------------------
class TestUsageStatisticsIsolation:

    def test_does_not_count_other_users_pages(
        self, client, request_with_processed_page, other_user_key
    ):
        """Other user's processed pages should not be counted."""
        resp = client.get(
            '/usage_statistics',
            headers={'api-key': other_user_key},
        )
        assert resp.status_code == 200
        assert resp.get_json()['processed_pages'] == 0

    def test_counts_own_expired_pages(
        self, client, request_with_expired_page
    ):
        """EXPIRED pages should still be counted in usage statistics."""
        rid, ukey, pids, eid = request_with_expired_page
        resp = client.get(
            '/usage_statistics',
            headers={'api-key': ukey},
        )
        assert resp.status_code == 200
        assert resp.get_json()['processed_pages'] >= 1


# ---------------------------------------------------------------------------
# POST /upload_image — filesystem verification
# ---------------------------------------------------------------------------
class TestUploadImageFilesystem:

    def test_file_saved_to_disk(self, client, request_with_created_page):
        rid, ukey, pids, eid = request_with_created_page
        data = {
            'file': (BytesIO(b'\xff\xd8\xff\xe0' + b'\x00' * 50), 'photo.png'),
        }
        resp = client.post(
            f'/upload_image/{rid}/page_upload',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        # Verify the file landed on disk
        expected_path = os.path.join(
            _TestConfig.UPLOAD_IMAGES_FOLDER, rid, 'page_upload.png'
        )
        assert os.path.isfile(expected_path)

    def test_url_points_to_download_image(self, client, request_with_created_page):
        """After upload, page URL should point to the /download_image endpoint."""
        rid, ukey, pids, eid = request_with_created_page
        data = {
            'file': (BytesIO(b'\xff\xd8\xff\xe0' + b'\x00' * 50), 'photo.jpg'),
        }
        client.post(
            f'/upload_image/{rid}/page_upload',
            data=data,
            headers={'api-key': ukey},
            content_type='multipart/form-data',
        )

        db = session_factory()
        page = db.query(Page).filter(Page.name == 'page_upload').first()
        assert '/download_image/' in page.url
        assert rid in page.url
        db.close()


# ---------------------------------------------------------------------------
# POST /failed_processing — request completion on failure
# ---------------------------------------------------------------------------
class TestFailedProcessingCompletion:

    def test_request_finished_when_only_page_fails(
        self, client, request_with_processing_page, super_user_key
    ):
        """If the only page in a request fails, request.finish_timestamp should be set."""
        rid, ukey, pids, eid = request_with_processing_page
        page_id = pids['page_proc']

        resp = client.post(
            f'/failed_processing/{page_id}',
            data=b'Traceback: error',
            headers={
                'api-key': super_user_key,
                'type': 'NOT_FOUND',
                'engine_version': 'v1.0.0',
                'hostname': 'test-host',
                'ip-address': '127.0.0.1',
            },
        )
        assert resp.status_code == 200

        db = session_factory()
        req = db.query(Request).filter(Request.id == rid).first()
        assert req.finish_timestamp is not None
        db.close()

    def test_traceback_stored_for_all_fail_types(
        self, client, super_user_key, user_key, engine_with_models
    ):
        """Traceback should be stored in the DB for every failure type."""
        for fail_type in ['NOT_FOUND', 'INVALID_FILE', 'PROCESSING_FAILED']:
            # Create a fresh request/page for each type
            rid, pids = _create_request_with_pages(
                user_key, engine_with_models,
                [(f'fail_{fail_type}', 'http://example.com/x.jpg', PageState.PROCESSING)]
            )
            page_id = list(pids.values())[0]

            client.post(
                f'/failed_processing/{page_id}',
                data=b'detailed traceback text',
                headers={
                    'api-key': super_user_key,
                    'type': fail_type,
                    'engine_version': 'v1.0.0',
                    'hostname': 'h',
                    'ip-address': '1.2.3.4',
                },
            )

            db = session_factory()
            page = db.query(Page).filter(Page.id == page_id).first()
            assert page.traceback is not None
            assert 'traceback' in page.traceback.lower()
            db.close()


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

    def test_full_lifecycle(self, client, user_key, super_user_key, engine_with_models):
        eid = engine_with_models

        # 1. Create request with one page (upload pending)
        resp = client.post(
            '/post_processing_request',
            json={'engine': eid, 'images': {'doc_page_1': None}},
            headers={'api-key': user_key},
        )
        assert resp.status_code == 200
        rid = str(resp.get_json()['request_id'])

        # 2. Upload image
        resp = client.post(
            f'/upload_image/{rid}/doc_page_1',
            data={'file': (BytesIO(b'\xff\xd8\xff\xe0' + b'\x00' * 50), 'scan.jpg')},
            headers={'api-key': user_key},
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        # 3. Status should be WAITING
        resp = client.get(f'/request_status/{rid}', headers={'api-key': user_key})
        assert resp.get_json()['request_status']['doc_page_1']['state'] == 'WAITING'

        # 4. Worker fetches the page
        resp = client.get(
            f'/get_processing_request/{eid}',
            headers={'api-key': super_user_key},
        )
        assert resp.status_code == 200
        page_id = resp.get_json()['page_id']
        assert resp.get_json()['engine_id'] == eid

        # Verify PROCESSING
        resp = client.get(f'/request_status/{rid}', headers={'api-key': user_key})
        assert resp.get_json()['request_status']['doc_page_1']['state'] == 'PROCESSING'

        # 5. Worker uploads results
        resp = client.post(
            f'/upload_results/{page_id}',
            data={
                'alto': (BytesIO(b'<alto>OCR output</alto>'), 'alto.xml'),
                'page': (BytesIO(b'<page>OCR output</page>'), 'page.xml'),
                'txt': (BytesIO(b'OCR text output'), 'result.txt'),
            },
            headers={
                'api-key': super_user_key,
                'score': '0.92',
                'engine-version': 'v1.0.0',
            },
            content_type='multipart/form-data',
        )
        assert resp.status_code == 200

        # 6. User checks status → PROCESSED with score
        resp = client.get(f'/request_status/{rid}', headers={'api-key': user_key})
        status = resp.get_json()['request_status']['doc_page_1']
        assert status['state'] == 'PROCESSED'
        assert status['quality'] == 92.0

        # 7. User downloads results in all three formats
        for fmt, expected in [
            ('alto', b'OCR output'),
            ('page', b'OCR output'),
            ('txt', b'OCR text output'),
        ]:
            resp = client.get(
                f'/download_results/{rid}/doc_page_1/{fmt}',
                headers={'api-key': user_key},
            )
            assert resp.status_code == 200
            assert expected in resp.data

        # 8. Request should be marked as finished
        db = session_factory()
        req = db.query(Request).filter(Request.id == rid).first()
        assert req.finish_timestamp is not None
        db.close()

        # 9. Uploaded image should have been cleaned up
        img_path = os.path.join(
            _TestConfig.UPLOAD_IMAGES_FOLDER, rid, 'doc_page_1.jpg'
        )
        assert not os.path.isfile(img_path)

        # 10. Usage statistics should count the processed page
        resp = client.get('/usage_statistics', headers={'api-key': user_key})
        assert resp.get_json()['processed_pages'] >= 1
