"""Tests for public (unauthenticated) routes."""


class TestIndex:
    """GET / and GET /index — dashboard page."""

    def test_index_returns_html(self, client):
        resp = client.get('/')
        assert resp.status_code == 200
        assert b'PERO API' in resp.data

    def test_index_alt_url(self, client):
        resp = client.get('/index')
        assert resp.status_code == 200
        assert b'PERO API' in resp.data


class TestDocs:
    """GET /docs — redirect to SwaggerHub."""

    def test_docs_redirects(self, client):
        resp = client.get('/docs')
        assert resp.status_code == 302
        assert 'swaggerhub.com' in resp.headers.get('Location', '')
