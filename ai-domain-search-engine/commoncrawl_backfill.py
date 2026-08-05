#!/usr/bin/env python3
"""Common Crawl .ai domain backfill.

.ai has no ICANN CZDS zone file -- it's Anguilla's ccTLD, not a gTLD, so
unlike .com/.app (see ../zone-universe/czds.py) there's no periodic zone
file to re-download here. CertStream already covers new .ai domains in
real time, as they get their first TLS certificate. This script is a
supplementary, manually-run backfill against Common Crawl's public index
for older domains CertStream may never have seen a cert event for (sites
that existed before this pipeline started watching, or whose cert
predates it).

Common Crawl publishes a new crawl roughly monthly. Re-run `sync`
whenever a new one lands to pick up anything new.

Usage:
    ./commoncrawl_backfill.py status
    ./commoncrawl_backfill.py sync
    ./commoncrawl_backfill.py sync --crawl-id CC-MAIN-2026-30 --max-pages 50
    ./commoncrawl_backfill.py sync --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SRC = PROJECT_ROOT / "src"
REPO_ROOT = PROJECT_ROOT.parent
for path in (SRC, REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from ai_search.config import load_settings  # noqa: E402
from ai_search.database import add_domain, connect  # noqa: E402
from ai_search.domains import normalize_domain  # noqa: E402

COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
USER_AGENT = "glasswing-sourcing-commoncrawl-backfill/1.0 (+manual research tool)"
RETRY_STATUSES = (429, 500, 502, 503, 504)


def _get(url: str, *, retries: int = 4) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRY_STATUSES:
                raise
        except urllib.error.URLError as exc:
            last_error = exc
        time.sleep(2**attempt)
    raise RuntimeError(f"Failed to fetch {url}") from last_error


def latest_crawl_id() -> str:
    collections = json.loads(_get(COLLINFO_URL))
    if not collections:
        raise RuntimeError("Common Crawl returned no collections")
    return str(collections[0]["id"])


def _cdx_api_for(crawl_id: str) -> str:
    return f"https://index.commoncrawl.org/{crawl_id}-index"


def page_count(crawl_id: str) -> tuple[int, int]:
    url = (
        f"{_cdx_api_for(crawl_id)}?url=ai&matchType=domain"
        "&output=json&showNumPages=true"
    )
    info = json.loads(_get(url))
    return int(info["pages"]), int(info.get("pageSize", 0) or 0)


def fetch_domains(
    crawl_id: str,
    *,
    max_pages: int | None = None,
    on_progress=None,
) -> set[str]:
    total_pages, _ = page_count(crawl_id)
    pages_to_fetch = total_pages if max_pages is None else min(total_pages, max_pages)
    domains: set[str] = set()
    for page in range(pages_to_fetch):
        url = (
            f"{_cdx_api_for(crawl_id)}?url=ai&matchType=domain"
            f"&output=json&page={page}&fl=url"
        )
        body = _get(url)
        for line in body.decode("utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            domain = normalize_domain(record.get("url", ""))
            if domain:
                domains.add(domain)
        if on_progress:
            on_progress(page + 1, pages_to_fetch, len(domains))
    return domains


def sync(crawl_id: str, *, max_pages: int | None, dry_run: bool = False) -> dict[str, object]:
    def progress(done: int, total: int, found: int) -> None:
        print(
            f"  page {done}/{total} -- {found:,} unique .ai domains so far",
            file=sys.stderr,
        )

    fetched = fetch_domains(crawl_id, max_pages=max_pages, on_progress=progress)
    settings = load_settings()
    conn = connect(settings.database)
    try:
        existing = {str(row["domain"]) for row in conn.execute("SELECT domain FROM domains")}
        new_domains = fetched - existing
        if not dry_run:
            for domain in new_domains:
                add_domain(conn, domain, source="commoncrawl_backfill")
    finally:
        conn.close()
    return {
        "crawl_id": crawl_id,
        "fetched": len(fetched),
        "already_known": len(fetched) - len(new_domains),
        "newly_added": 0 if dry_run else len(new_domains),
        "would_add": len(new_domains) if dry_run else 0,
        "dry_run": dry_run,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    status_cmd = sub.add_parser(
        "status", help="show the latest crawl id and .ai page count (no writes)"
    )
    status_cmd.add_argument("--crawl-id", default=None)

    sync_cmd = sub.add_parser(
        "sync", help="fetch .ai domains from Common Crawl and add any new ones"
    )
    sync_cmd.add_argument(
        "--crawl-id", default=None, help="defaults to the latest available crawl"
    )
    sync_cmd.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="cap pages fetched (each page is a few thousand URLs); omit to fetch all",
    )
    sync_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and diff against the known domain set, but don't write anything",
    )

    args = parser.parse_args(argv)

    if args.command == "status":
        crawl_id = args.crawl_id or latest_crawl_id()
        pages, page_size = page_count(crawl_id)
        print(f"Latest crawl: {crawl_id}")
        print(f"'.ai' pages available: {pages} (~{page_size or 'unknown'} rows/page)")
        return 0

    if args.command == "sync":
        crawl_id = args.crawl_id or latest_crawl_id()
        print(f"Syncing against crawl {crawl_id}...", file=sys.stderr)
        result = sync(crawl_id, max_pages=args.max_pages, dry_run=args.dry_run)
        print(json.dumps(result, indent=2))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
