from __future__ import annotations

import argparse
import asyncio
import csv
import json
from pathlib import Path

from .config import load_settings
from .crawler import crawl_pending
from .database import add_domain, connect, diagnostics
from .maintenance import compact_existing_pages
from .search import SearchOptions, search


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="ai-search")
    commands = root.add_subparsers(dest="command", required=True)
    add = commands.add_parser("add")
    add.add_argument("domains", nargs="+")
    imp = commands.add_parser("import")
    imp.add_argument("path", type=Path)
    imp.add_argument("--source", default="import")
    crawl = commands.add_parser("crawl")
    crawl.add_argument("--limit", type=int, default=100)
    query = commands.add_parser("search")
    query.add_argument("query")
    query.add_argument("--limit", type=int, default=25)
    query.add_argument("--include-parked", action="store_true")
    query.add_argument("--title-only", action="store_true")
    commands.add_parser("stats")
    compact = commands.add_parser("compact-storage")
    compact.add_argument("--no-backup", action="store_true")
    compact.add_argument("--no-vacuum", action="store_true")
    return root


def import_path(conn, path: Path, source: str) -> tuple[int, int]:
    added = rejected = 0
    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows = csv.DictReader(handle)
            for row in rows:
                value = next(
                    (row.get(key) for key in ("domain", "registrable", "hostname")
                     if row.get(key)), ""
                )
                if add_domain(conn, value, source):
                    added += 1
                else:
                    rejected += 1
    else:
        for line in path.read_text(encoding="utf-8").splitlines():
            if add_domain(conn, line, source):
                added += 1
            elif line.strip():
                rejected += 1
    return added, rejected


def main() -> None:
    args = parser().parse_args()
    settings = load_settings()
    conn = connect(settings.database)
    if args.command == "add":
        accepted = sum(add_domain(conn, item) for item in args.domains)
        print(f"Accepted {accepted}; rejected {len(args.domains) - accepted}")
    elif args.command == "import":
        added, rejected = import_path(conn, args.path, args.source)
        print(f"Accepted {added}; rejected {rejected}")
    elif args.command == "crawl":
        print(json.dumps(
            asyncio.run(crawl_pending(conn, settings, max(1, args.limit))),
            indent=2,
        ))
    elif args.command == "search":
        results = search(conn, args.query, SearchOptions(
            limit=args.limit,
            exclude_parked=not args.include_parked,
            title_only=args.title_only,
        ))
        for row in results:
            print(f"\n{row['domain']} — {row['title']}")
            print(row["final_url"])
            print(row["snippet"].replace("<mark>", "[").replace("</mark>", "]"))
        print(f"\n{len(results)} result(s)")
    elif args.command == "stats":
        print(json.dumps(diagnostics(conn), indent=2))
    elif args.command == "compact-storage":
        result = compact_existing_pages(
            conn,
            database=settings.database,
            archive_root=settings.html_archive,
            make_backup=not args.no_backup,
            vacuum=not args.no_vacuum,
            progress=print,
        )
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
