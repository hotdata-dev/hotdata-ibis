#!/usr/bin/env python3
"""CLI helpers shared by runnable examples.

Environment hooks (see code for full list):

- ``HOTDATA_DEFAULT_CONNECTION`` / ``HOTDATA_DEFAULT_SCHEMA`` — fixed REST ids.
- ``HOTDATA_TPCH_RESOLVE`` — set ``false`` to skip auto-resolution of ``tpch`` / ``tpch_sf1``.
- ``HOTDATA_TPCH_CONNECTION_MATCH`` — comma-separated substrings matched against connection
  ids/names (default includes ``tpch``, ``tpc-h``, …). Among matches, prefers a connection
  that exposes schema ``tpch_sf1``.
"""

from __future__ import annotations

import argparse
import os
import urllib.parse

import httpx

DEFAULT_TPCH_CONNECTION = "tpch"
DEFAULT_TPCH_SCHEMA = "tpch_sf1"


def _verify(ns: argparse.Namespace) -> bool:
    return not getattr(ns, "insecure", False)


def _headers(ns: argparse.Namespace) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ns.token.strip()}",
        "X-Workspace-Id": ns.workspace_id.strip(),
        "Accept": "application/json",
    }


def fetch_connections(ns: argparse.Namespace) -> list[dict]:
    base = ns.api_url.rstrip("/")
    with httpx.Client(timeout=ns.timeout, verify=_verify(ns)) as h:
        r = h.get(f"{base}/v1/connections", headers=_headers(ns))
        r.raise_for_status()
    return list(r.json().get("connections") or [])


def pick_tpch_connection_id(ns: argparse.Namespace, conns: list[dict]) -> str | None:
    raw = os.environ.get("HOTDATA_TPCH_CONNECTION_MATCH", "tpch,tpc-h,tpc_h,tpc")
    needles = tuple(s.strip().lower() for s in raw.split(",") if s.strip())
    hinted: list[dict] = []
    for c in conns:
        cid = str(c.get("id", ""))
        cname = str(c.get("name", "")).lower()
        hay = f"{cid.lower()} {cname}"
        if cid.lower() == DEFAULT_TPCH_CONNECTION or any(n in hay for n in needles):
            hinted.append(c)
    if not hinted:
        return None
    if len(hinted) == 1:
        return str(hinted[0]["id"])

    candidates: list[tuple[tuple[int, int, int], str]] = []
    for c in hinted:
        cid = str(c["id"])
        try:
            schemas = set(fetch_schema_names(ns, cid))
        except Exception:
            continue
        has_sf1 = DEFAULT_TPCH_SCHEMA in schemas
        has_pub = "public" in schemas
        key = (-int(has_sf1), -int(has_pub), len(schemas))
        candidates.append((key, cid))

    if candidates:
        return min(candidates, key=lambda x: x[0])[1]
    return str(hinted[0]["id"])


def fetch_schema_names(ns: argparse.Namespace, connection_id: str) -> list[str]:
    base = ns.api_url.rstrip("/")
    found: set[str] = set()
    cursor: str | None = None
    while True:
        params: dict[str, str | bool] = {
            "connection_id": connection_id,
            "limit": 500,
            "include_columns": False,
        }
        if cursor:
            params["cursor"] = cursor
        with httpx.Client(timeout=ns.timeout, verify=_verify(ns)) as h:
            r = h.get(f"{base}/v1/information_schema", params=params, headers=_headers(ns))
            r.raise_for_status()
            chunk = r.json()
        for row in chunk.get("tables") or []:
            if isinstance(row.get("schema"), str):
                found.add(row["schema"])
        if not chunk.get("has_more"):
            break
        cursor = chunk.get("next_cursor")
        if not cursor:
            break
    return sorted(found)


def normalize_tpch_defaults(ns: argparse.Namespace) -> None:
    """Resolve friendly ``tpch`` / ``tpch_sf1`` defaults to REST connection id + schema when needed."""

    if os.environ.get("HOTDATA_TPCH_RESOLVE", "true").lower() in ("0", "false", "no"):
        return

    if ns.default_connection != DEFAULT_TPCH_CONNECTION:
        if ns.default_schema == DEFAULT_TPCH_SCHEMA:
            schemas = fetch_schema_names(ns, ns.default_connection)
            if ns.default_schema not in schemas:
                if len(schemas) == 1:
                    ns.default_schema = schemas[0]
                elif "public" in schemas:
                    ns.default_schema = "public"
        return

    try:
        conns = fetch_connections(ns)
    except Exception:
        return

    rid = pick_tpch_connection_id(ns, conns)
    if rid is None:
        return

    ns.default_connection = rid

    if ns.default_schema != DEFAULT_TPCH_SCHEMA:
        return

    try:
        schemas = fetch_schema_names(ns, rid)
    except Exception:
        return

    if DEFAULT_TPCH_SCHEMA in schemas:
        return
    if len(schemas) == 1:
        ns.default_schema = schemas[0]
    elif "public" in schemas:
        ns.default_schema = "public"
    else:
        ns.default_schema = None


def parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--api-url",
        default=os.environ.get("HOTDATA_API_URL", "https://api.hotdata.dev"),
        help="Hotdata API base URL (env HOTDATA_API_URL)",
    )
    p.add_argument(
        "--token",
        default=os.environ.get("HOTDATA_API_KEY", ""),
        help="API bearer token (env HOTDATA_API_KEY)",
    )
    p.add_argument(
        "--workspace",
        dest="workspace_id",
        default=os.environ.get("HOTDATA_WORKSPACE", ""),
        help="Workspace public id (env HOTDATA_WORKSPACE)",
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
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument(
        "--default-connection",
        dest="default_connection",
        default=os.environ.get("HOTDATA_DEFAULT_CONNECTION") or DEFAULT_TPCH_CONNECTION,
        help=f"Connection id (= Ibis catalog). Env HOTDATA_DEFAULT_CONNECTION. Default {DEFAULT_TPCH_CONNECTION!r}.",
    )
    p.add_argument(
        "--default-schema",
        dest="default_schema",
        default=os.environ.get("HOTDATA_DEFAULT_SCHEMA") or DEFAULT_TPCH_SCHEMA,
        help=f"Remote schema (= Ibis database). Env HOTDATA_DEFAULT_SCHEMA. Default {DEFAULT_TPCH_SCHEMA!r}.",
    )
    return p


def parsed_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    ns = parser.parse_args()
    if not ns.token.strip() or not ns.workspace_id.strip():
        parser.error("Set HOTDATA_API_KEY and HOTDATA_WORKSPACE, or pass --token and --workspace.")
    normalize_tpch_defaults(ns)
    return ns


def connect_kwargs(ns: argparse.Namespace, **extras) -> dict:
    dc = getattr(ns, "default_connection", None)
    ds = getattr(ns, "default_schema", None)
    kwargs = {
        "api_url": ns.api_url.rstrip("/"),
        "token": ns.token.strip(),
        "workspace_id": ns.workspace_id.strip(),
        "timeout": ns.timeout,
        "verify_ssl": not getattr(ns, "insecure", False),
    }
    if ns.session_id:
        kwargs["session_id"] = ns.session_id
    if dc:
        kwargs["default_connection"] = dc
    if ds:
        kwargs["default_schema"] = ds
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
    dc = getattr(ns, "default_connection", None)
    ds = getattr(ns, "default_schema", None)
    if dc:
        qs["default_connection"] = dc
    if ds:
        qs["default_schema"] = ds
    q = urllib.parse.urlencode(qs)
    return f"hotdata://{api_host(ns.api_url)}/?{q}"
