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
    monkeypatch.setattr(health_monitor, "_database_counts", lambda: _base_counts())
    monkeypatch.setattr(
        health_monitor,
        "runtime_status",
        lambda: {"services": {"pipeline": {"running": pipeline_running, "pid": None, "desired": True}}},
    )
    monkeypatch.setattr(health_monitor, "list_manual_snapshots", lambda: [])
    monkeypatch.setattr(health_monitor, "performance_metrics", lambda: {})
    monkeypatch.setattr(health_monitor, "using_postgres", lambda *a, **k: False)


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
