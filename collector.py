#!/usr/bin/env python3
"""Collect newly observed domains for selected TLDs from CertStream."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import websockets

from common import RAW, stable_domain

FIELDS = ["observed_at", "registrable", "hostname"]

SUPPORTED_EXTENSIONS = {
    ".ai",
    ".io",
    ".app",
    ".dev",
    ".co",
    ".xyz",
    ".tech",
    ".com",
}


def normalize_extensions(values: list[str] | None) -> set[str]:
    normalized: set[str] = set()
    for value in values or []:
        item = str(value).strip().lower()
        if not item:
            continue
        if not item.startswith("."):
            item = f".{item}"
        if item in SUPPORTED_EXTENSIONS:
            normalized.add(item)
    return normalized or {".ai"}


def domain_extension(domain: str) -> str:
    host = stable_domain(domain)
    if "." not in host:
        return ""
    return f".{host.rsplit('.', 1)[-1]}"


def registrable_domain(
    hostname: str,
    allowed_extensions: set[str] | None = None,
) -> str:
    """Return the apex domain for a supported single-label TLD."""
    allowed = allowed_extensions or SUPPORTED_EXTENSIONS
    host = stable_domain(hostname)
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return ""
    extension = f".{labels[-1]}"
    if extension not in allowed:
        return ""
    return ".".join(labels[-2:]).lower()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://localhost:8080/domains-only")
    parser.add_argument("--seconds", type=int, default=3300)
    parser.add_argument("--max", type=int, default=1_000_000)
    parser.add_argument("--out", default=str(RAW / "domains_master.csv"))
    parser.add_argument(
        "--extensions",
        default=".ai",
        help="Comma-separated extensions: .ai,.io,.app",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()

def names_from_message(raw: str | bytes) -> list[str]:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return []
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return [name for name in data if isinstance(name, str)]

async def run(args: argparse.Namespace) -> None:
    output = Path(args.out).expanduser()
    allowed_extensions = normalize_extensions(
        str(args.extensions).split(",")
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    seen: set[str] = set()
    if output.exists() and output.stat().st_size:
        with output.open(newline="", encoding="utf-8") as fh:
            seen.update(
                stable_domain(row.get("registrable", ""))
                for row in csv.DictReader(fh)
                if row.get("registrable")
            )

    has_data = output.exists() and output.stat().st_size > 0
    deadline = asyncio.get_running_loop().time() + args.seconds
    added = 0

    with output.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if not has_data:
            writer.writeheader()

        while asyncio.get_running_loop().time() < deadline and added < args.max:
            try:
                async with websockets.connect(
                    args.url, ping_interval=20, ping_timeout=20
                ) as websocket:
                    while (
                        asyncio.get_running_loop().time() < deadline
                        and added < args.max
                    ):
                        timeout = max(
                            0.1, deadline - asyncio.get_running_loop().time()
                        )
                        raw = await asyncio.wait_for(websocket.recv(), timeout)
                        for hostname in names_from_message(raw):
                            host = hostname.lower().lstrip("*.").rstrip(".")
                            domain = registrable_domain(
                                host,
                                allowed_extensions,
                            )
                            if not domain:
                                continue
                            if domain in seen:
                                continue

                            seen.add(domain)
                            added += 1
                            writer.writerow({
                                "observed_at": datetime.now(timezone.utc)
                                    .isoformat().replace("+00:00", "Z"),
                                "registrable": domain,
                                "hostname": host,
                            })
                            fh.flush()
                            if args.verbose:
                                print(f"[NEW {added}] {domain}")
            except asyncio.TimeoutError:
                break
            except (OSError, websockets.WebSocketException) as exc:
                print(f"CertStream reconnect after error: {exc}")
                await asyncio.sleep(5)

    print(f"Added {added} domains to {output}")

def main() -> None:
    asyncio.run(run(parse_args()))

if __name__ == "__main__":
    main()
