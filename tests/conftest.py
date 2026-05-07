from __future__ import annotations

import pytest

pytest.importorskip("pytest_httpserver")
from pytest_httpserver import HTTPServer


@pytest.fixture
def srv(httpserver: HTTPServer) -> str:
    """Base URL without trailing slash (matches Hotdata client normalization)."""

    return httpserver.url_for("/").rstrip("/")
