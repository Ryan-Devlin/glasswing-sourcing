from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(
    os.getenv("ZONE_UNIVERSE_DATA_ROOT", str(PROJECT_ROOT / "data"))
).expanduser().resolve()
ZONEFILES = DATA_ROOT / "zonefiles"
QUEUES = DATA_ROOT / "queues"
LOGS = Path(
    os.getenv("ZONE_UNIVERSE_LOG_ROOT", str(PROJECT_ROOT / "logs"))
).expanduser().resolve()
DATABASE = Path(
    os.getenv("ZONE_UNIVERSE_DB", str(DATA_ROOT / "universe.sqlite3"))
).expanduser().resolve()
HTML_ARCHIVE = DATA_ROOT / "html"
PID_FILE = LOGS / "worker.pid"
STOP_FILE = LOGS / "worker.stop"
WORKER_LOG = LOGS / "worker.log"

for folder in (DATA_ROOT, ZONEFILES, QUEUES, HTML_ARCHIVE, LOGS):
    folder.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Settings:
    database: Path
    tlds: tuple[str, ...]
    concurrency: int
    batch_size: int
    preflight_enabled: bool
    preflight_concurrency: int
    preflight_max_response_bytes: int
    timeout_seconds: float
    max_response_bytes: int
    minimum_free_gb: float
    retain_raw_html: bool
    html_archive_minimum_score: int
    html_archive: Path
    max_pages_per_domain: int
    max_crawl_priority: int
    dns_preflight: bool
    max_retries: int
    auto_screen: bool
    auto_classify: bool
    classifier_workers: int
    auto_registration: bool
    registration_daily_limit: int
    registration_interval_seconds: float
    registration_timeout_seconds: float
    auto_import_zones: bool
    delete_imported_zonefiles: bool
    screening_queue: Path
    poll_seconds: float
    user_agent: str


def load_settings() -> Settings:
    tlds = tuple(
        dict.fromkeys(
            value.strip().lower().lstrip(".")
            for value in os.getenv("ZONE_UNIVERSE_TLDS", "app,com").split(",")
            if value.strip()
        )
    )
    return Settings(
        database=Path(
            os.getenv("ZONE_UNIVERSE_DB", str(DATABASE))
        ).expanduser(),
        tlds=tlds,
        concurrency=max(
            1,
            min(200, int(os.getenv("ZONE_UNIVERSE_CONCURRENCY", "40"))),
        ),
        batch_size=max(
            1,
            min(5000, int(os.getenv("ZONE_UNIVERSE_BATCH_SIZE", "100"))),
        ),
        preflight_enabled=os.getenv(
            "ZONE_UNIVERSE_PREFLIGHT_ENABLED",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        preflight_concurrency=max(
            1,
            min(
                250,
                int(os.getenv("ZONE_UNIVERSE_PREFLIGHT_CONCURRENCY", "100")),
            ),
        ),
        preflight_max_response_bytes=max(
            32_000,
            min(
                512_000,
                int(
                    os.getenv(
                        "ZONE_UNIVERSE_PREFLIGHT_MAX_RESPONSE_BYTES",
                        "131072",
                    )
                ),
            ),
        ),
        timeout_seconds=max(
            2.0,
            min(
                30.0,
                float(os.getenv("ZONE_UNIVERSE_TIMEOUT_SECONDS", "8")),
            ),
        ),
        max_response_bytes=max(
            50_000,
            min(
                10_000_000,
                int(
                    os.getenv(
                        "ZONE_UNIVERSE_MAX_RESPONSE_BYTES",
                        "1500000",
                    )
                ),
            ),
        ),
        minimum_free_gb=max(
            2.0,
            float(os.getenv("ZONE_UNIVERSE_MINIMUM_FREE_GB", "20")),
        ),
        retain_raw_html=os.getenv(
            "ZONE_UNIVERSE_RETAIN_RAW_HTML",
            "0",
        ).strip().lower() not in {"0", "false", "no"},
        # At this scale (~170M .com/.app names), archiving raw HTML for
        # every single crawl is not viable -- only domains that clear this
        # Python rule-based score get the raw HTML actually archived.
        # Metadata (title/description/cleaned_text) is always saved
        # regardless, since scoring itself depends on it.
        html_archive_minimum_score=int(
            os.getenv("ZONE_UNIVERSE_HTML_ARCHIVE_MINIMUM_SCORE", "75")
        ),
        html_archive=Path(
            os.getenv("ZONE_UNIVERSE_HTML_ARCHIVE", str(HTML_ARCHIVE))
        ).expanduser(),
        max_pages_per_domain=max(
            1,
            min(
                5,
                int(os.getenv("ZONE_UNIVERSE_MAX_PAGES_PER_DOMAIN", "3")),
            ),
        ),
        max_crawl_priority=max(
            0,
            min(
                3,
                int(
                    os.getenv(
                        "ZONE_UNIVERSE_MAX_CRAWL_PRIORITY",
                        "2",
                    )
                ),
            ),
        ),
        dns_preflight=os.getenv(
            "ZONE_UNIVERSE_DNS_PREFLIGHT",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        max_retries=max(
            0,
            min(8, int(os.getenv("ZONE_UNIVERSE_MAX_RETRIES", "3"))),
        ),
        auto_screen=os.getenv(
            "ZONE_UNIVERSE_AUTO_SCREEN",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        auto_classify=os.getenv(
            "ZONE_UNIVERSE_AUTO_CLASSIFY",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        classifier_workers=max(
            1,
            min(
                36,
                int(os.getenv("ZONE_UNIVERSE_CLASSIFIER_WORKERS", "12")),
            ),
        ),
        auto_registration=os.getenv(
            "ZONE_UNIVERSE_AUTO_REGISTRATION",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        registration_daily_limit=max(
            1,
            min(
                500,
                int(
                    os.getenv(
                        "ZONE_UNIVERSE_REGISTRATION_DAILY_LIMIT",
                        "100",
                    )
                ),
            ),
        ),
        registration_interval_seconds=max(
            5.0,
            float(
                os.getenv(
                    "ZONE_UNIVERSE_REGISTRATION_INTERVAL_SECONDS",
                    "15",
                )
            ),
        ),
        registration_timeout_seconds=max(
            2.0,
            min(
                30.0,
                float(
                    os.getenv(
                        "ZONE_UNIVERSE_REGISTRATION_TIMEOUT_SECONDS",
                        "10",
                    )
                ),
            ),
        ),
        auto_import_zones=os.getenv(
            "ZONE_UNIVERSE_AUTO_IMPORT_ZONES",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        delete_imported_zonefiles=os.getenv(
            "ZONE_UNIVERSE_DELETE_IMPORTED_ZONEFILES",
            "1",
        ).strip().lower() not in {"0", "false", "no"},
        screening_queue=Path(
            os.getenv(
                "ZONE_UNIVERSE_SCREENING_QUEUE",
                str(PROJECT_ROOT.parent / "data" / "pipeline_queue.sqlite3"),
            )
        ).expanduser(),
        poll_seconds=max(
            1.0,
            float(os.getenv("ZONE_UNIVERSE_POLL_SECONDS", "5")),
        ),
        user_agent=os.getenv(
            "ZONE_UNIVERSE_USER_AGENT",
            "Glasswing-Zone-Research/0.1 (+private research crawler)",
        ).strip(),
    )
