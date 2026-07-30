from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    database: Path
    html_archive: Path
    retain_raw_html: bool
    concurrency: int
    timeout_seconds: float
    max_response_bytes: int
    max_pages_per_domain: int
    min_text_chars: int
    user_agent: str
    recrawl_days: int
    discovery_stream: str
    crawl_batch_size: int
    crawl_poll_seconds: float
    web_provider: str
    brave_search_api_key: str
    bing_search_api_key: str
    provider_pages: int
    rediscover_hours: int


def load_settings() -> Settings:
    raw_db = Path(os.getenv("AI_SEARCH_DB", "data/search.sqlite3"))
    database = raw_db if raw_db.is_absolute() else PROJECT_ROOT / raw_db
    raw_archive = Path(
        os.getenv("AI_SEARCH_HTML_ARCHIVE", "data/html")
    ).expanduser()
    html_archive = (
        raw_archive
        if raw_archive.is_absolute()
        else PROJECT_ROOT / raw_archive
    )
    return Settings(
        database=database,
        html_archive=html_archive,
        retain_raw_html=os.getenv(
            "AI_SEARCH_RETAIN_RAW_HTML",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        concurrency=max(1, int(os.getenv("AI_SEARCH_CONCURRENCY", "12"))),
        timeout_seconds=max(
            1.0, float(os.getenv("AI_SEARCH_TIMEOUT_SECONDS", "12"))
        ),
        max_response_bytes=max(
            10_000, int(os.getenv("AI_SEARCH_MAX_RESPONSE_BYTES", "3000000"))
        ),
        max_pages_per_domain=max(
            1, int(os.getenv("AI_SEARCH_MAX_PAGES_PER_DOMAIN", "3"))
        ),
        min_text_chars=max(
            1, int(os.getenv("AI_SEARCH_MIN_TEXT_CHARS", "80"))
        ),
        user_agent=os.getenv(
            "AI_SEARCH_USER_AGENT",
            "AnySiteAISearch/0.1 (+private research crawler)",
        ),
        recrawl_days=max(1, int(os.getenv("AI_SEARCH_RECRAWL_DAYS", "14"))),
        discovery_stream=os.getenv(
            "AI_SEARCH_DISCOVERY_STREAM",
            "",
        ).strip(),
        crawl_batch_size=max(
            1, int(os.getenv("AI_SEARCH_CRAWL_BATCH_SIZE", "50"))
        ),
        crawl_poll_seconds=max(
            1.0, float(os.getenv("AI_SEARCH_CRAWL_POLL_SECONDS", "5"))
        ),
        web_provider=os.getenv("AI_SEARCH_WEB_PROVIDER", "off").strip().lower(),
        brave_search_api_key=os.getenv(
            "BRAVE_SEARCH_API_KEY", ""
        ).strip(),
        bing_search_api_key=os.getenv("BING_SEARCH_API_KEY", "").strip(),
        provider_pages=max(
            1, min(10, int(os.getenv("AI_SEARCH_PROVIDER_PAGES", "3")))
        ),
        rediscover_hours=max(
            1, int(os.getenv("AI_SEARCH_REDISCOVER_HOURS", "24"))
        ),
    )
