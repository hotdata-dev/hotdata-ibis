from __future__ import annotations

import pytest

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer


@pytest.fixture(autouse=True)
def _jwt_exchange(httpserver: HTTPServer) -> None:
    """Stub the mandatory API-token -> JWT exchange added in hotdata 0.4.1.

    The SDK's ``_TokenManager`` does ``POST {host}/v1/auth/jwt`` to mint a JWT
    before any API call. Register a default handler so every test inherits a
    valid token-exchange response without per-test setup. The body must carry an
    ``access_token`` (read directly); ``expires_in`` is optional (defaults 300).
    """

    httpserver.expect_request("/v1/auth/jwt", method="POST").respond_with_json(
        {"access_token": "eyJtest", "expires_in": 3600}
    )


@pytest.fixture
def srv(httpserver: HTTPServer) -> str:
    """Base URL without trailing slash (matches Hotdata client normalization)."""

    return httpserver.url_for("/").rstrip("/")
