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
    assert payload["funnel"]["name"] == "pipeline"
    assert "screening" not in payload
    assert "certstream" not in payload


def test_total_drops_never_exceed_the_top_line():
    # A cumulative guard tally against a reset hostname count reported more
    # rejections than observations and rendered as a 4040% drop rate. When the
    # two are out of step the tally is discarded rather than shown.
    payload = _payload()
    dropped = sum(s["dropped"] for s in payload["funnel"]["stages"])
    assert dropped <= max(payload["domains_observed"], 0)


def test_the_funnel_runs_from_intake_to_claude():
    # It used to stop at "hard-denied by a rule", so a reader could not see
    # what happened to the candidates that survived.
    keys = [s["key"] for s in _payload()["funnel"]["stages"]]
    assert keys[0] == "extension_not_tracked"
    assert "repeat" in keys and "unclassified" in keys
    assert keys[-2:] == ["not_diligenced", "claude_reject"]


def test_unclassified_domains_leave_the_funnel_explicitly():
    # Measured live: 100 rows were marked complete with no verdict recorded.
    # Letting them fall through counted them as having survived every
    # screening step and reported 100 candidates where there were 5.
    keys = [s["key"] for s in _payload()["funnel"]["stages"]]
    assert "unclassified" in keys


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
            assert stage["drop_rate"] <= 1.0, stage["key"]


def test_no_stage_reports_a_backlog_column():
    # IN and OUT only. "Waiting" was a third number in every row a reader had
    # to add back before the arithmetic closed.
    for stage in _payload()["funnel"]["stages"]:
        assert "pending" not in stage


def test_the_top_line_is_domains_observed_not_survivors():
    # "Domains discovered" used to report what survived the filter, which made
    # layer 0 look like a trickle when the feed delivers far more.
    payload = _payload()
    assert payload["funnel"]["entered"] == payload["domains_observed"]


def test_liveness_flags_distinguish_no_data_from_a_real_zero():
    payload = _payload()
    assert isinstance(payload["queue_counts_live"], bool)
    assert isinstance(payload["funnel_live"], bool)


def test_a_failed_queue_read_still_renders_the_classification_half(monkeypatch):
    def explode():
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(web_api, "connect_pipeline_db", explode)
    payload = _payload()
    assert payload["queue_counts_live"] is False
    # The classification stages are independent of the queue and must survive.
    assert payload["funnel"]["stages"]


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


def test_certstream_page_and_funnel_report_the_same_top_line():
    # "Domains discovered" and the top of the funnel are the same claim --
    # every distinct domain the feed showed us this run, before filtering --
    # so they read the same counter rather than being two measurements that
    # have to agree.
    payload = _payload()
    if payload["domains_observed"]:
        status = client.get("/api/certstream/status").json()
        assert status["domains_received"] == payload["domains_observed"]
        assert payload["funnel"]["entered"] == payload["domains_observed"]
