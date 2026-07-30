from __future__ import annotations

import json
from pathlib import Path

from zone_universe.database import connect
from zone_universe.rdap import (
    claim_registration,
    cached_registrations,
    finish_registration,
    lookup_registration,
    parse_rdap,
    queue_registrations,
    rdap_url,
    save_registration,
)


def test_rdap_endpoints() -> None:
    assert rdap_url("example.com") == (
        "https://rdap.verisign.com/com/v1/domain/example.com"
    )
    assert rdap_url("example.app") == (
        "https://pubapi.registry.google/rdap/domain/example.app"
    )


def test_parse_and_cache_registration(tmp_path: Path) -> None:
    payload = {
        "status": ["active"],
        "events": [
            {
                "eventAction": "registration",
                "eventDate": "1995-08-14T04:00:00Z",
            },
            {
                "eventAction": "expiration",
                "eventDate": "2027-08-13T04:00:00Z",
            },
        ],
        "entities": [
            {
                "roles": ["registrar"],
                "vcardArray": [
                    "vcard",
                    [["fn", {}, "text", "Example Registrar"]],
                ],
            }
        ],
    }
    record = parse_rdap("example.com", payload)
    assert record.registration_at == "1995-08-14T04:00:00Z"
    assert record.registrar == "Example Registrar"

    database = tmp_path / "universe.sqlite3"
    connect(database).close()
    save_registration(record, database=database)
    cached = cached_registrations(["example.com"], database=database)
    assert cached["example.com"]["registration_at"] == (
        "1995-08-14T04:00:00Z"
    )


def test_lookup_uses_rdap_json() -> None:
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self) -> bytes:
            return json.dumps(
                {
                    "events": [
                        {
                            "eventAction": "registration",
                            "eventDate": "2018-05-01T00:00:00Z",
                        }
                    ]
                }
            ).encode()

    def opener(_request, *, timeout):
        assert timeout == 3
        return Response()

    result = lookup_registration(
        "example.app",
        timeout=3,
        opener=opener,
    )
    assert result.lookup_status == "ok"
    assert result.registration_at == "2018-05-01T00:00:00Z"


def test_registration_queue_prioritizes_and_skips_cached(
    tmp_path: Path,
) -> None:
    database = tmp_path / "universe.sqlite3"
    connect(database).close()
    assert queue_registrations(
        ["review.app"],
        database=database,
        priority=50,
    ) == 1
    assert queue_registrations(
        ["priority.com"],
        database=database,
        priority=100,
    ) == 1
    assert claim_registration(database=database) == "priority.com"
    record = parse_rdap(
        "priority.com",
        {
            "events": [
                {
                    "eventAction": "registration",
                    "eventDate": "2024-01-01T00:00:00Z",
                }
            ]
        },
    )
    save_registration(record, database=database)
    finish_registration(record, database=database)
    assert queue_registrations(
        ["priority.com"],
        database=database,
        priority=100,
    ) == 0
    assert claim_registration(database=database) == "review.app"
