from __future__ import annotations

import json
from collections.abc import Callable

import pytest

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request, Response


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


def mock_presigned_upload_flow(
    httpserver: HTTPServer,
    *,
    upload_id: str = "upl_1",
    finalize_token: str = "tok_1",
    on_create_session: Callable[[Request], Response] | None = None,
    on_storage_put: Callable[[Request], Response] | None = None,
    on_finalize: Callable[[Request], Response] | None = None,
) -> None:
    """Mock ``hotdata.uploads.UploadsApi.upload_file``'s presigned flow: a single-
    ``PUT`` upload session (``POST /v1/uploads``), the storage ``PUT`` itself, and
    finalize (``POST /v1/uploads/{upload_id}/finalize``). Each stage accepts an
    override handler for tests that need to assert on that stage's request.
    """
    storage_path = f"/mock-storage/{upload_id}"

    def default_create_session(req: Request) -> Response:
        return Response(
            json.dumps(
                {
                    "mode": "single",
                    "url": httpserver.url_for(storage_path),
                    "headers": {},
                    "upload_id": upload_id,
                    "finalize_token": finalize_token,
                }
            ),
            status=201,
            content_type="application/json",
        )

    httpserver.expect_oneshot_request("/v1/uploads", method="POST").respond_with_handler(
        on_create_session or default_create_session
    )

    def default_storage_put(req: Request) -> Response:
        return Response(b"", status=200)

    httpserver.expect_oneshot_request(storage_path, method="PUT").respond_with_handler(
        on_storage_put or default_storage_put
    )

    def default_finalize(req: Request) -> Response:
        assert req.headers.get("X-Upload-Finalize-Token") == finalize_token
        return Response(
            json.dumps(
                {
                    "upload_id": upload_id,
                    "status": "ready",
                    "size_bytes": len(req.get_data()),
                    "created_at": "2026-01-01T00:00:00Z",
                    "content_type": "application/parquet",
                }
            ),
            status=200,
            content_type="application/json",
        )

    httpserver.expect_oneshot_request(
        f"/v1/uploads/{upload_id}/finalize", method="POST"
    ).respond_with_handler(on_finalize or default_finalize)
