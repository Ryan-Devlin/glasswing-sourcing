"""The /api/funnel endpoint over the real stores.

funnel.py covers the arithmetic; this covers the wiring — that the endpoint
reads both stores, keeps the chain closed end to end, and degrades to a
labelled "no data" rather than a run of misleading zeros when a read fails.
"""

from fastapi.testclient import TestClient

import web_api

client = TestClient(web_api.app)


def _payload():
    response = client.get("/api/funnel")
    assert response.status_code == 200, response.text
    return response.json()


def test_returns_both_funnels_with_their_stages():
    payload = _payload()
    assert payload["intake"]["name"] == "intake"
    assert payload["screening"]["name"] == "screening"
    assert [s["key"] for s in payload["screening"]["stages"]] == [
        "queued", "screened", "reachable", "substantive",
        "passed_rules", "candidate", "claude_reviewed", "claude_kept",
    ]


def test_the_live_chain_stays_arithmetically_closed():
    # Each stage reads its entered count from the previous stage's survivors.
    # If a query starts returning a different population, the percentages
    # silently describe a funnel that doesn't exist — catch it here.
    stages = _payload()["screening"]["stages"]
    for previous, nxt in zip(stages, stages[1:]):
        assert nxt["entered"] == previous["survived"], nxt["key"]


def test_no_stage_reports_negative_counts():
    # The queue and classification stores are queried separately and can
    # disagree mid-write; the UI must never render a negative.
    for funnel in ("intake", "screening"):
        for stage in _payload()[funnel]["stages"]:
            assert stage["survived"] >= 0, stage["key"]
            assert stage["dropped"] >= 0, stage["key"]
            assert stage["pending"] >= 0, stage["key"]


def test_backlog_is_reported_separately_from_drops():
    # The distinction the view exists to make: unscreened queue rows are
    # pending, never dropped. Reading them as rejections would send you
    # tuning a filter that never ran.
    stages = {s["key"]: s for s in _payload()["screening"]["stages"]}
    assert stages["queued"]["dropped"] == 0
    assert stages["claude_reviewed"]["dropped"] == 0


def test_liveness_flags_distinguish_no_data_from_a_real_zero():
    payload = _payload()
    assert isinstance(payload["queue_counts_live"], bool)
    assert isinstance(payload["intake_counts_live"], bool)


def test_a_failed_queue_read_still_renders_the_classification_half(monkeypatch):
    def explode():
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(web_api, "connect_pipeline_db", explode)
    payload = _payload()
    assert payload["queue_counts_live"] is False
    # The classification stages are independent of the queue and must survive.
    assert payload["screening"]["stages"][2]["key"] == "reachable"


def test_biggest_losses_are_ranked_and_bounded():
    losses = _payload()["biggest_losses"]
    assert len(losses) <= 3
    assert [l["dropped"] for l in losses] == sorted(
        (l["dropped"] for l in losses), reverse=True
    )


def test_throughput_is_reported_per_window():
    payload = _payload()
    windows = {t["window_hours"] for t in payload["throughput"]}
    assert windows == {24, 168}
    for entry in payload["throughput"]:
        assert entry["arrived"] >= 0
        assert entry["fetched"] >= 0
        # A stalled worker and a worker keeping up both leave the backlog
        # looking calm; only these flags tell them apart.
        assert not (entry["keeping_up"] and entry["stalled"])


def test_a_failed_queue_read_reports_no_throughput_rather_than_zeros(monkeypatch):
    def explode():
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(web_api, "connect_pipeline_db", explode)
    payload = _payload()
    assert payload["throughput"] == []
    assert payload["queue_counts_live"] is False
