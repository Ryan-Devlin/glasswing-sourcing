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


def test_returns_exactly_one_funnel():
    # One pipeline. Three tables in two different units was the whole problem.
    payload = _payload()
    assert payload["funnel"]["name"] == "run"
    assert "screening" not in payload
    assert "certstream" not in payload


def test_hostname_rejections_never_exceed_hostnames_seen():
    # A cumulative guard tally against a reset hostname count reported more
    # rejections than observations and rendered as a 4040% drop rate. When the
    # two are out of step the tally is discarded rather than shown.
    intake = _payload()["intake"]
    assert intake["hostnames_rejected"] <= max(intake["hostnames_seen"], 0)


def test_the_funnel_stages_are_the_run_pipeline():
    assert [s["key"] for s in _payload()["funnel"]["stages"]] == [
        "received", "screened", "resolved",
        "substantive", "passed_rules", "candidate",
    ]


def test_the_live_chain_stays_arithmetically_closed():
    # Each stage reads its entered count from the previous stage's survivors.
    # If a query starts returning a different population, the percentages
    # silently describe a funnel that doesn't exist — catch it here.
    stages = _payload()["funnel"]["stages"]
    for previous, nxt in zip(stages, stages[1:]):
        assert nxt["entered"] == previous["survived"], nxt["key"]


def test_no_stage_reports_negative_counts():
    # The queue and classification stores are queried separately and can
    # disagree mid-write; the UI must never render a negative.
    for stage in _payload()["funnel"]["stages"]:
        if True:
            assert stage["survived"] >= 0, stage["key"]
            assert stage["dropped"] >= 0, stage["key"]
            assert stage["pending"] >= 0, stage["key"]


def test_backlog_is_reported_separately_from_drops():
    # The distinction the view exists to make: unscreened queue rows are
    # pending, never dropped. Reading them as rejections would send you
    # tuning a filter that never ran.
    stages = {s["key"]: s for s in _payload()["funnel"]["stages"]}
    assert stages["received"]["dropped"] == 0


def test_liveness_flags_distinguish_no_data_from_a_real_zero():
    payload = _payload()
    assert isinstance(payload["queue_counts_live"], bool)
    assert isinstance(payload["intake"]["live"], bool)


def test_a_failed_queue_read_still_renders_the_classification_half(monkeypatch):
    def explode():
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(web_api, "connect_pipeline_db", explode)
    payload = _payload()
    assert payload["queue_counts_live"] is False
    # The classification stages are independent of the queue and must survive.
    assert payload["funnel"]["stages"][0]["key"] == "received"


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
