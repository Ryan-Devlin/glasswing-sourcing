from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlsplit

import aiohttp
from bs4 import BeautifulSoup

from .config import Settings
from .domains import normalize_domain


@dataclass(frozen=True)
class WebResult:
    url: str
    title: str = ""
    snippet: str = ""


def provider_name(settings: Settings) -> str:
    requested = settings.web_provider
    if requested == "brave" or (
        requested == "auto" and settings.brave_search_api_key
    ):
        return "brave"
    if requested == "bing" or (
        requested == "auto" and settings.bing_search_api_key
    ):
        return "bing"
    return "duckduckgo"


def _unwrap_duckduckgo(url: str) -> str:
    parsed = urlsplit(url)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        return unquote(parse_qs(parsed.query).get("uddg", [url])[0])
    return url


async def _brave(
    session: aiohttp.ClientSession, query: str, settings: Settings
) -> list[WebResult]:
    results: list[WebResult] = []
    headers = {
        "X-Subscription-Token": settings.brave_search_api_key,
        "Accept": "application/json",
    }
    for offset in range(settings.provider_pages):
        async with session.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params={
                "q": f"site:.ai {query}",
                "count": 20,
                "offset": offset,
                "safesearch": "moderate",
                "text_decorations": "false",
            },
        ) as response:
            response.raise_for_status()
            payload = await response.json()
        for item in payload.get("web", {}).get("results", []):
            results.append(WebResult(
                str(item.get("url", "")),
                str(item.get("title", "")),
                str(item.get("description", "")),
            ))
        if not payload.get("query", {}).get("more_results_available", False):
            break
    return results


async def _bing(
    session: aiohttp.ClientSession, query: str, settings: Settings
) -> list[WebResult]:
    results: list[WebResult] = []
    headers = {"Ocp-Apim-Subscription-Key": settings.bing_search_api_key}
    for page in range(settings.provider_pages):
        async with session.get(
            "https://api.bing.microsoft.com/v7.0/search",
            headers=headers,
            params={
                "q": f"site:.ai {query}",
                "count": 50,
                "offset": page * 50,
                "responseFilter": "Webpages",
            },
        ) as response:
            response.raise_for_status()
            payload = await response.json()
        items = payload.get("webPages", {}).get("value", [])
        for item in items:
            results.append(WebResult(
                str(item.get("url", "")),
                str(item.get("name", "")),
                str(item.get("snippet", "")),
            ))
        if len(items) < 50:
            break
    return results


async def _duckduckgo(
    session: aiohttp.ClientSession, query: str, settings: Settings
) -> list[WebResult]:
    """Keyless fallback. Keep it shallow so a blocked provider fails quickly."""
    async def fetch_page(page: int) -> list[WebResult]:
        data = {"q": f"site:.ai {query}", "s": str(page * 30)}
        async with session.post(
            "https://html.duckduckgo.com/html/",
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 Chrome/126.0 Safari/537.36"
                ),
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            response.raise_for_status()
            markup = await response.text()
        soup = BeautifulSoup(markup, "html.parser")
        page_results: list[WebResult] = []
        cards = soup.select(".result")
        for card in cards:
            anchor = card.select_one(".result__a")
            if not anchor:
                continue
            snippet = card.select_one(".result__snippet")
            page_results.append(WebResult(
                _unwrap_duckduckgo(anchor.get("href", "")),
                anchor.get_text(" ", strip=True),
                snippet.get_text(" ", strip=True) if snippet else "",
            ))
        return page_results

    # More pages improve API-backed search, but public HTML fallbacks are both
    # shallow and aggressively rate-limited. Two concurrent pages bound latency.
    page_count = min(settings.provider_pages, 2)
    batches = await asyncio.gather(
        *(fetch_page(page) for page in range(page_count)),
        return_exceptions=True,
    )
    return [
        result
        for batch in batches
        if isinstance(batch, list)
        for result in batch
    ]


async def discover_query(
    session: aiohttp.ClientSession, query: str, settings: Settings
) -> tuple[str, list[WebResult]]:
    provider = provider_name(settings)
    if provider == "brave":
        raw = await _brave(session, query, settings)
    elif provider == "bing":
        raw = await _bing(session, query, settings)
    else:
        raw = await _duckduckgo(session, query, settings)
    unique: dict[str, WebResult] = {}
    for result in raw:
        domain = normalize_domain(result.url)
        if domain and domain not in unique:
            unique[domain] = result
    return provider, list(unique.values())
