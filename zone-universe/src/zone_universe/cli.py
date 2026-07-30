from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .config import DATABASE, PROJECT_ROOT, load_settings
from .crawler import crawl_once
from .database import connect, diagnostics, get_state
from .maintenance import (
    prune_noise,
    purge_screening_records,
    summarize_rejection_audits,
)
from .rdap import lookup_and_save
from .search import raw_html, search_pages
from .zone_parser import import_zone


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="zone-universe",
        description="ICANN zone ingestion and resumable website crawling.",
    )
    root.add_argument("--database", type=Path, default=DATABASE)
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init")

    ingest = commands.add_parser("import-zone")
    ingest.add_argument("zone_path", type=Path)
    ingest.add_argument("--tld", required=True)
    ingest.add_argument("--shards", type=int, default=64)
    ingest.add_argument("--limit", type=int, default=0)
    ingest.add_argument(
        "--keep-gibberish",
        action="store_true",
        help="Disable the conservative generated-domain exclusion filter.",
    )

    commands.add_parser("stats")
    commands.add_parser("prune-noise")
    commands.add_parser("retire-zonefiles")
    commands.add_parser("compact-rejection-audits")

    crawl = commands.add_parser("crawl-once")
    crawl.add_argument("--tld", required=True)

    search = commands.add_parser("search")
    search.add_argument("query")
    search.add_argument("--tld", default="")
    search.add_argument("--limit", type=int, default=25)
    search.add_argument("--minimum-words", type=int, default=0)

    html = commands.add_parser("export-html")
    html.add_argument("domain")
    html.add_argument("--output", type=Path, required=True)

    registration = commands.add_parser("registration")
    registration.add_argument("domains", nargs="+")
    return root


def main() -> None:
    options = parser().parse_args()
    if options.command == "init":
        conn = connect(options.database)
        print(json.dumps(diagnostics(conn), indent=2))
        conn.close()
        return
    if options.command == "import-zone":
        result = import_zone(
            options.zone_path,
            tld=options.tld,
            shard_count=options.shards,
            limit=options.limit,
            database=options.database,
            reject_gibberish=not options.keep_gibberish,
        )
        print(json.dumps(result, indent=2))
        return
    if options.command == "stats":
        conn = connect(options.database)
        payload = diagnostics(conn)
        payload["worker_state"] = get_state(conn)
        payload["zone_inventory"] = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM zones ORDER BY tld"
            )
        ]
        print(json.dumps(payload, indent=2))
        conn.close()
        return
    if options.command == "prune-noise":
        conn = connect(options.database)
        result = prune_noise(conn)
        conn.close()
        result["screening_cleanup"] = purge_screening_records(
            list(result["domains"]),
            project_root=PROJECT_ROOT.parent,
        )
        print(json.dumps(result, indent=2))
        return
    if options.command == "retire-zonefiles":
        from .worker import import_new_zonefiles

        settings = load_settings()
        if options.database != settings.database:
            settings = settings.__class__(
                **{
                    **settings.__dict__,
                    "database": options.database,
                }
            )
        if not settings.delete_imported_zonefiles:
            raise SystemExit(
                "ZONE_UNIVERSE_DELETE_IMPORTED_ZONEFILES is disabled."
            )
        conn = connect(options.database)
        before = {
            path.name: path.stat().st_size
            for path in settings.database.parent.joinpath("zonefiles").glob(
                "*.gz"
            )
        }
        imported = asyncio.run(import_new_zonefiles(conn, settings))
        conn.close()
        remaining = {
            path.name
            for path in settings.database.parent.joinpath("zonefiles").glob(
                "*.gz"
            )
        }
        print(json.dumps(
            {
                "newly_imported": imported,
                "retired": {
                    name: size
                    for name, size in before.items()
                    if name not in remaining
                },
            },
            indent=2,
        ))
        return
    if options.command == "compact-rejection-audits":
        result = summarize_rejection_audits(
            options.database.parent / "rejected"
        )
        print(json.dumps(result, indent=2))
        return
    if options.command == "crawl-once":
        settings = load_settings()
        if options.database != settings.database:
            settings = settings.__class__(
                **{
                    **settings.__dict__,
                    "database": options.database,
                }
            )
        conn = connect(options.database)
        result = asyncio.run(
            crawl_once(conn, settings, tld=options.tld)
        )
        print(json.dumps(result, indent=2))
        conn.close()
        return
    if options.command == "search":
        rows = search_pages(
            options.query,
            tld=options.tld,
            limit=options.limit,
            minimum_words=options.minimum_words,
            database=options.database,
        )
        print(json.dumps(rows, indent=2))
        return
    if options.command == "export-html":
        html = raw_html(options.domain, database=options.database)
        if not html:
            raise SystemExit(
                f"No retained HTML found for {options.domain}."
            )
        options.output.parent.mkdir(parents=True, exist_ok=True)
        options.output.write_text(html, encoding="utf-8")
        print(options.output)
        return
    if options.command == "registration":
        if len(options.domains) > 10:
            raise SystemExit("Look up at most 10 domains per command.")
        results = [
            lookup_and_save(domain, database=options.database).__dict__
            for domain in options.domains
        ]
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
