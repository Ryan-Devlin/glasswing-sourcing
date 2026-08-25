import health_monitor


def _base_counts() -> dict[str, int]:
    return {
        "classifications": 10,
        "manual_decisions": 1,
        "pipeline_pending": 5,
        "pipeline_processing": 200,
        "ai_pages": 3,
        "ai_indexed": 3,
        "ai_orphans": 0,
        "zone_pages": 4,
        "zone_indexed": 4,
        "zone_orphans": 0,
    }


def _patch_common(monkeypatch, *, pipeline_running) -> None:
    monkeypatch.setattr(health_monitor, "_snapshot_cache", None)
    monkeypatch.setattr(health_monitor, "_database_counts", lambda: _base_counts())
    monkeypatch.setattr(
        health_monitor,
        "runtime_status",
        lambda: {"services": {"pipeline": {"running": pipeline_running, "pid": None, "desired": True}}},
    )
    monkeypatch.setattr(health_monitor, "list_manual_snapshots", lambda: [])
    monkeypatch.setattr(health_monitor, "performance_metrics", lambda: {})
    monkeypatch.setattr(health_monitor, "using_postgres", lambda *a, **k: False)


def test_ordinary_health_skips_remote_snapshot_validation(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(
        health_monitor,
        "list_manual_snapshots",
        lambda: (_ for _ in ()).throw(AssertionError("deep check ran")),
    )

    report = health_monitor.health_report()

    backup = next(row for row in report["checks"] if row["Check"] == "Manual truth backup")
    assert backup == {
        "Check": "Manual truth backup",
        "Status": "Healthy",
        "Detail": "Run full checks to validate backups",
    }


def test_full_health_validates_and_caches_remote_snapshots(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=None)
    calls = []
    monkeypatch.setattr(
        health_monitor,
        "list_manual_snapshots",
        lambda: calls.append(True) or [{"valid": True}],
    )

    deep = health_monitor.health_report(include_integrity=True)
    quick = health_monitor.health_report()

    assert len(calls) == 1
    assert deep["manual_snapshots"] == [{"valid": True}]
    assert quick["manual_snapshots"] == [{"valid": True}]


def _stuck_claims_check(report: dict) -> dict:
    return next(row for row in report["checks"] if row["Check"] == "Stuck pipeline claims")


def test_stuck_claims_healthy_when_cloud_liveness_unknown(monkeypatch):
    """Cloud mode reports pipeline running as None (no local PID to check).
    A nonzero processing count there is normal in-flight work, not evidence
    of a stuck claim, so it must not flag a false Warning."""
    _patch_common(monkeypatch, pipeline_running=None)
    report = health_monitor.health_report()
    check = _stuck_claims_check(report)
    assert check["Status"] == "Healthy"
    assert "cloud-managed" in check["Detail"]


def test_stuck_claims_warning_when_locally_confirmed_stopped(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=False)
    report = health_monitor.health_report()
    check = _stuck_claims_check(report)
    assert check["Status"] == "Warning"
    assert "left processing" in check["Detail"]


def test_stuck_claims_healthy_when_locally_confirmed_running(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=True)
    report = health_monitor.health_report()
    check = _stuck_claims_check(report)
    assert check["Status"] == "Healthy"
    assert "actively processing" in check["Detail"]


class _DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def close(self) -> None:
        pass


def test_postgres_backed_subsystem_checks_ping_instead_of_checking_local_file(
    monkeypatch,
):
    """Once a subsystem (pipeline/ai_search/zone_universe) has its own
    dedicated *_DATABASE_URL set, its local sqlite path is an unused
    leftover, not a missing database -- checking path.exists() there is
    wrong and would report a false "Missing" Error the moment that (now
    orphaned) local file is cleaned up."""
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(
        health_monitor,
        "using_postgres",
        lambda env_var="DATABASE_URL": env_var
        in {
            health_monitor.PIPELINE_DATABASE_ENV_VAR,
            health_monitor.AI_SEARCH_DATABASE_ENV_VAR,
            health_monitor.ZONE_UNIVERSE_DATABASE_ENV_VAR,
        },
    )
    monkeypatch.setattr(
        health_monitor, "connect_postgres", lambda *a, **k: _DummyConn()
    )
    monkeypatch.setattr(
        health_monitor, "connect_ai_search", lambda *a, **k: _DummyConn()
    )
    monkeypatch.setattr(
        health_monitor, "connect_zone_universe", lambda *a, **k: _DummyConn()
    )

    report = health_monitor.health_report()
    by_name = {row["Check"]: row for row in report["checks"]}
    for name in ("Pipeline queue", ".AI search database", "Zone database"):
        assert by_name[name]["Status"] == "Healthy"
        assert by_name[name]["Detail"] == "Connected (Postgres)"


def _disk_check(report: dict) -> dict:
    return next(row for row in report["checks"] if row["Check"] == "Free disk reserve")


class _FakeDiskUsage:
    def __init__(self, free_bytes: int, total_bytes: int):
        self.free = free_bytes
        self.total = total_bytes


def test_disk_reserve_healthy_on_a_small_volume_with_plenty_free(monkeypatch):
    """A 5 GB cloud volume with 4.5 GB free is obviously fine, but a flat
    "20 GB free" threshold used to flag it Warning forever -- that bar can
    never be met even at 0% used on a volume this size."""
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(
        health_monitor.shutil,
        "disk_usage",
        lambda path: _FakeDiskUsage(free_bytes=4.5 * 1024**3, total_bytes=5 * 1024**3),
    )
    report = health_monitor.health_report()
    assert _disk_check(report)["Status"] == "Healthy"


def test_disk_reserve_warning_when_a_small_volume_is_actually_nearly_full(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(
        health_monitor.shutil,
        "disk_usage",
        lambda path: _FakeDiskUsage(free_bytes=0.2 * 1024**3, total_bytes=5 * 1024**3),
    )
    report = health_monitor.health_report()
    assert _disk_check(report)["Status"] == "Warning"


def test_disk_reserve_healthy_on_a_large_local_disk(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(
        health_monitor.shutil,
        "disk_usage",
        lambda path: _FakeDiskUsage(free_bytes=25 * 1024**3, total_bytes=500 * 1024**3),
    )
    report = health_monitor.health_report()
    assert _disk_check(report)["Status"] == "Healthy"


def test_a_retained_page_with_no_index_is_not_an_error(monkeypatch):
    """Corpus search was retired, so "every page is indexed" describes a
    feature that no longer exists. Asserting it made System Health read Error
    permanently: .COM/.APP showed "4 pages, 0 indexed" and .AI passed only
    because it had no pages yet.
    """
    counts = _base_counts()
    counts.update({"ai_indexed": 0, "zone_indexed": 0})
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(health_monitor, "_database_counts", lambda: counts)

    report = health_monitor.health_report()

    pages = [row for row in report["checks"] if "retained pages" in row["Check"]]
    assert len(pages) == 2
    for row in pages:
        assert row["Status"] == "Healthy", row
    assert report["overall"] != "Error"


def test_index_rows_left_over_from_before_the_retirement_are_named(monkeypatch):
    counts = _base_counts()
    counts.update({"zone_indexed": 4, "zone_orphans": 2})
    _patch_common(monkeypatch, pipeline_running=None)
    monkeypatch.setattr(health_monitor, "_database_counts", lambda: counts)

    report = health_monitor.health_report()

    row = next(r for r in report["checks"] if r["Check"] == ".COM/.APP retained pages")
    assert row["Status"] == "Warning"
    assert "stale index row" in row["Detail"]
    assert "2 orphans" in row["Detail"]


def _patch_heartbeats(monkeypatch, beats, *, desired=True):
    monkeypatch.setattr(health_monitor, "_worker_heartbeats", lambda: beats)
    monkeypatch.setattr(
        health_monitor,
        "runtime_status",
        lambda: {
            "services": {
                key: {"desired": desired, "pid": None, "running": None}
                for key in ("pipeline", "ai_crawler", "zone_crawler")
            }
        },
    )


def _row(report, check):
    return next(r for r in report["checks"] if r["Check"] == check)


def test_a_dead_worker_that_still_says_running_is_caught(monkeypatch):
    """The .AI crawler's heartbeat sat frozen at 2026-08-17T19:18Z for eight
    days while worker_status read "running", the Railway service read "Online"
    and System Health was green. Nothing compared the heartbeat to the clock.
    """
    from datetime import datetime, timedelta, timezone
    _patch_common(monkeypatch, pipeline_running=None)
    stale = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _patch_heartbeats(monkeypatch, {"ai_crawler": stale})

    report = health_monitor.health_report()

    row = _row(report, ".AI crawler heartbeat")
    assert row["Status"] == "Error"
    assert "8 days ago" in row["Detail"]
    assert report["overall"] == "Error"


def test_a_worker_stopped_on_purpose_is_not_an_alarm(monkeypatch):
    """A deliberately stopped worker has a stale heartbeat by definition, and
    calling that an error is the false alarm that teaches people to ignore
    the check."""
    from datetime import datetime, timedelta, timezone
    _patch_common(monkeypatch, pipeline_running=None)
    stale = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    _patch_heartbeats(monkeypatch, {"ai_crawler": stale}, desired=False)

    report = health_monitor.health_report()

    assert _row(report, ".AI crawler heartbeat")["Status"] == "Healthy"
    assert report["overall"] != "Error"


def test_a_quiet_stretch_inside_normal_work_is_not_an_alarm(monkeypatch):
    """The zone worker goes silent for the ~27 minutes a .com import takes."""
    from datetime import datetime, timedelta, timezone
    _patch_common(monkeypatch, pipeline_running=None)
    recent = (datetime.now(timezone.utc) - timedelta(minutes=27)).isoformat()
    _patch_heartbeats(monkeypatch, {"zone_crawler": recent})

    report = health_monitor.health_report()

    row = _row(report, "Zone crawler heartbeat")
    assert row["Status"] == "Healthy"
    assert "27m ago" in row["Detail"]


def test_an_unparseable_heartbeat_does_not_crash_the_page(monkeypatch):
    _patch_common(monkeypatch, pipeline_running=None)
    _patch_heartbeats(monkeypatch, {"ai_crawler": "not a timestamp"})

    row = _row(health_monitor.health_report(), ".AI crawler heartbeat")
    assert row["Status"] == "Warning"
    assert row["Detail"] == "Never reported"
