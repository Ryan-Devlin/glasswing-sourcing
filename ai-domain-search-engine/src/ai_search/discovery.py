from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable

import websockets

from .database import add_domain, connect, set_worker_state
from .domains import normalize_domain

LOGGER = logging.getLogger(__name__)


def names_from_event(raw: str | bytes) -> list[str]:
    try:
        message = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(message, dict):
        return []
    data = message.get("data")
    if isinstance(data, list):
        return [str(item) for item in data]
    if isinstance(data, dict):
        leaf = data.get("leaf_cert", {})
        names = leaf.get("all_domains", []) if isinstance(leaf, dict) else []
        return [str(item) for item in names] if isinstance(names, list) else []
    return []


def registrable_ai_domains(names: Iterable[str]) -> set[str]:
    results: set[str] = set()
    for name in names:
        host = str(name).lower().strip().lstrip("*.").rstrip(".")
        labels = host.split(".")
        if len(labels) < 2 or labels[-1] != "ai":
            continue
        domain = normalize_domain(".".join(labels[-2:]))
        if domain:
            results.add(domain)
    return results


async def stream_domains(database, url: str, stop: asyncio.Event) -> None:
    """Continuously register newly observed .ai domains with reconnect backoff."""
    delay = 2
    while not stop.is_set():
        conn = connect(database)
        try:
            set_worker_state(conn, "discovery_status", "connecting")
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, open_timeout=20
            ) as websocket:
                set_worker_state(conn, "discovery_status", "connected")
                delay = 2
                while not stop.is_set():
                    raw = await asyncio.wait_for(websocket.recv(), timeout=45)
                    domains = registrable_ai_domains(names_from_event(raw))
                    for domain in domains:
                        add_domain(conn, domain, "certificate_transparency")
                    if domains:
                        set_worker_state(
                            conn, "last_discovery_count", len(domains)
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("Discovery reconnect: %s", exc)
            set_worker_state(
                conn, "discovery_status", f"reconnecting: {type(exc).__name__}"
            )
            set_worker_state(conn, "last_discovery_error", str(exc)[:300])
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)
        finally:
            conn.close()

