#!/usr/bin/env python3
"""
Same as the other examples, but connects with ``ibis.connect("hotdata://...")``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

_examples = Path(__file__).resolve().parent
sys.path.insert(0, str(_examples))

import ibis
from _helpers import hotdata_connect_uri, parsed_args, parser

_ns = parsed_args(parser("Connect via hotdata:// URL-style string."))
url = hotdata_connect_uri(_ns)


def safe_hotdata_url(u: str) -> str:
    parts = urlparse(u)
    q = parse_qs(parts.query, keep_blank_values=True)
    if q.get("token"):
        q["token"] = ["<redacted>"]
    pairs = [(k, v[0]) for k, v in sorted(q.items()) if v]
    return parts._replace(query=urlencode(pairs)).geturl()


def main() -> None:
    print("Connecting with:", safe_hotdata_url(url))
    con = ibis.connect(url)

    cats = con.list_catalogs()
    preview = repr(cats) if len(cats) <= 10 else repr(cats[:10]) + f" ... (+{len(cats) - 10} more)"
    print("connections (Ibis catalogs):", preview)


if __name__ == "__main__":
    main()
