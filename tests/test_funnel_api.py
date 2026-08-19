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
    # rejections than observations and rendered as a 4040% drop rate. Each
    # stage is now clamped to what actually reached it, so the sum cannot
    # exceed the population it describes.
    payload = _payload()
    dropped = sum(s["dropped"] for s in payload["funnel"]["stages"])
    assert dropped <= max(payload["funnel"]["entered"], 0)


def test_the_funnel_is_the_six_steps_the_pipeline_actually_has():
    # Find domains, drop junk, drop what never served a page, drop what did
    # not fit, bucket, spend Claude. Rejection reasons nest inside the step
    # they belong to; as peers they made this a 28-row table.
    keys = [s["key"] for s in _payload()["funnel"]["stages"]]
    assert keys == [
        "junk", "unscreened", "no_page", "no_fit",
        "not_diligenced", "claude_reject",
    ]


def test_unscreened_domains_leave_the_funnel_explicitly():
    # Measured live: 100 rows were marked complete with no verdict recorded.
    # Letting them fall through counted them as having survived every
    # screening step and reported 100 candidates where there were 5.
    keys = [s["key"] for s in _payload()["funnel"]["stages"]]
    assert "unscreened" in keys


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


def test_the_top_line_is_every_source_summed():
    # "Domains discovered" used to report what survived the filter, which made
    # layer 0 look like a trickle when the feed delivers far more. It is now
    # every domain either source found today, before any judgement.
    payload = _payload()
    assert payload["funnel"]["entered"] == sum(payload["sources"].values())


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


def test_every_source_is_named_in_the_top_line():
    # The two sources are summed rather than shown as separate funnels, so
    # the split has to be readable somewhere or the top line is unaccountable.
    payload = _payload()
    assert set(payload["sources"]) <= {"certstream", "zone"}
    assert all(count >= 0 for count in payload["sources"].values())


def test_the_day_is_reported_with_the_numbers():
    # A per-day funnel that does not say which day is unreadable the moment
    # it is stale.
    payload = _payload()
    assert len(payload["day"]) == 10 and payload["day"].count("-") == 2


def test_reasons_nest_inside_their_stage():
    # The specific fix: reasons are detail on a step, not steps themselves.
    for stage in _payload()["funnel"]["stages"]:
        assert isinstance(stage["reasons"], list)
        assert sum(r["dropped"] for r in stage["reasons"]) <= stage["dropped"]
