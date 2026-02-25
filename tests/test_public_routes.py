"""Tests for public (unauthenticated) routes."""


class TestIndex:
    """GET / and GET /index — dashboard page."""

    async def test_index_returns_html(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "PERO API" in resp.text

    async def test_index_alt_url(self, client):
        resp = await client.get("/index")
        assert resp.status_code == 200
        assert "PERO API" in resp.text


class TestDocs:
    """GET /docs_redirect — redirect to SwaggerHub."""

    async def test_docs_redirects(self, client):
        resp = await client.get("/docs_redirect", follow_redirects=False)
        assert resp.status_code == 307
        assert "swaggerhub.com" in resp.headers.get("location", "")
