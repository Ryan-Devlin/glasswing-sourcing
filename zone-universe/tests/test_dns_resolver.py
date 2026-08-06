import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

import aiodns

from zone_universe import crawler


def test_domain_resolves_does_not_use_loop_getaddrinfo() -> None:
    # Confirmed live: loop.getaddrinfo() runs on the event loop's small
    # default executor thread pool, and cancelling asyncio.wait_for around
    # it does not stop the underlying blocking call once it's running.
    # Against a queue of 100M+ mostly-unregistered .com names, enough
    # slow/hanging lookups pile up to exhaust that pool and throttle the
    # whole crawler far below its configured concurrency (1.6 domains/sec
    # against 100 configured concurrent quick checks).
    source = Path(crawler.__file__).read_text(encoding="utf-8")
    section = source.split("async def domain_resolves(", 1)[1].split(
        "\ndef ", 1
    )[0]
    assert "type=socket.SOCK_STREAM" not in section
    assert "resolver.query(" in section


def test_connector_uses_a_native_async_dns_resolver() -> None:
    source = Path(crawler.__file__).read_text(encoding="utf-8")
    connector_section = source.split("aiohttp.TCPConnector(", 1)[1].split(
        "\n\n", 1
    )[0]
    assert "resolver=aiohttp.AsyncResolver()" in connector_section


def test_aiodns_is_a_declared_dependency() -> None:
    pyproject = Path(
        crawler.__file__
    ).resolve().parents[2] / "pyproject.toml"
    assert "aiodns" in pyproject.read_text(encoding="utf-8")


def test_domain_resolves_true_when_apex_or_www_has_a_record() -> None:
    crawler._dns_resolver = None
    fake_resolver = AsyncMock()
    fake_resolver.query = AsyncMock(side_effect=[
        aiodns.error.DNSError("no A record"),
        object(),
    ])
    with patch.object(aiodns, "DNSResolver", return_value=fake_resolver):
        result = asyncio.run(crawler.domain_resolves("example.com"))
    assert result is True
    crawler._dns_resolver = None


def test_domain_resolves_false_when_neither_apex_nor_www_resolves() -> None:
    crawler._dns_resolver = None
    fake_resolver = AsyncMock()
    fake_resolver.query = AsyncMock(
        side_effect=aiodns.error.DNSError("no A record")
    )
    with patch.object(aiodns, "DNSResolver", return_value=fake_resolver):
        result = asyncio.run(crawler.domain_resolves("ghost-domain.com"))
    assert result is False
    crawler._dns_resolver = None
