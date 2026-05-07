#!/usr/bin/env python3
"""CLI helpers shared by runnable examples."""

from __future__ import annotations

import argparse
import os
import urllib.parse


def parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--api-url",
        default=os.environ.get("HOTDATA_API_URL", "https://api.hotdata.dev"),
        help="Hotdata API base URL (env HOTDATA_API_URL)",
    )
    p.add_argument(
        "--token",
        default=os.environ.get("HOTDATA_TOKEN", ""),
        help="API bearer token (env HOTDATA_TOKEN)",
    )
    p.add_argument(
        "--workspace",
        dest="workspace_id",
        default=os.environ.get("HOTDATA_WORKSPACE_ID", ""),
        help="Workspace public id (env HOTDATA_WORKSPACE_ID)",
    )
    p.add_argument(
        "--session",
        dest="session_id",
        default=os.environ.get("HOTDATA_SESSION_ID") or None,
        help="Sandbox id for X-Session-Id (env HOTDATA_SESSION_ID, optional)",
    )
    p.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS verification (dev only)",
    )
    p.add_argument(
        "--prefer-async",
        action="store_true",
        help="Prefer async POST /v1/query",
    )
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument(
        "--default-connection",
        dest="default_connection",
        default=os.environ.get("HOTDATA_DEFAULT_CONNECTION") or None,
        help="Connection id (= Ibis catalog). Env HOTDATA_DEFAULT_CONNECTION.",
    )
    p.add_argument(
        "--default-schema",
        dest="default_schema",
        default=os.environ.get("HOTDATA_DEFAULT_SCHEMA") or None,
        help="Remote schema (= Ibis database). Env HOTDATA_DEFAULT_SCHEMA.",
    )
    return p


def parsed_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    ns = parser.parse_args()
    if not ns.token.strip() or not ns.workspace_id.strip():
        parser.error(
            "Set HOTDATA_TOKEN and HOTDATA_WORKSPACE_ID, or pass --token and --workspace."
        )
    if os.environ.get("HOTDATA_PREFER_ASYNC", "").lower() in ("1", "true", "yes"):
        ns.prefer_async = True
    return ns


def connect_kwargs(ns: argparse.Namespace, **extras) -> dict:
    kwargs = {
        "api_url": ns.api_url.rstrip("/"),
        "token": ns.token.strip(),
        "workspace_id": ns.workspace_id.strip(),
        "timeout": ns.timeout,
        "prefer_async": ns.prefer_async,
        "verify_ssl": False if getattr(ns, "insecure", False) else True,
    }
    if ns.session_id:
        kwargs["session_id"] = ns.session_id
    if ns.default_connection:
        kwargs["default_connection"] = ns.default_connection
    if ns.default_schema:
        kwargs["default_schema"] = ns.default_schema
    kwargs.update(extras)
    return kwargs


def api_host(api_url: str) -> str:
    s = api_url.strip().rstrip("/")
    if "://" not in s:
        s = f"https://{s.lstrip('/')}"
    u = urllib.parse.urlparse(s)
    if u.netloc:
        return u.netloc
    return u.path.strip("/").split("/")[0]


def hotdata_connect_uri(ns: argparse.Namespace) -> str:
    """Minimal hotdata:// URL for ibis.connect."""
    verify_ssl = not getattr(ns, "insecure", False)
    qs: dict[str, str] = {
        "token": ns.token.strip(),
        "workspace_id": ns.workspace_id.strip(),
        "verify_ssl": "true" if verify_ssl else "false",
    }
    if ns.session_id:
        qs["session_id"] = ns.session_id
    if ns.default_connection:
        qs["default_connection"] = ns.default_connection
    if ns.default_schema:
        qs["default_schema"] = ns.default_schema
    if ns.prefer_async:
        qs["prefer_async"] = "true"
    q = urllib.parse.urlencode(qs)
    return f"hotdata://{api_host(ns.api_url)}/?{q}"
