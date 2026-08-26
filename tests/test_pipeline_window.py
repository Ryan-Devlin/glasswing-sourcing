

def test_an_indefinite_stop_beats_the_daily_clock(monkeypatch):
    """Pausing is not stopping. The window scheduler exists precisely to start
    the pipeline at 8am whether or not somebody paused it the night before, so
    "off until further notice" needs a switch it consults -- and one that
    outlives a restart, which is why it lives in the control store.
    """
    import runtime_control

    calls = []
    monkeypatch.setattr(runtime_control, "automation_enabled", lambda: False)
    monkeypatch.setattr(
        runtime_control, "request_services",
        lambda *a, **k: calls.append((a, k)),
    )

    result = runtime_control.apply_pipeline_window()

    assert result == {"applied": False, "reason": "automation_off"}
    assert calls == [], "must not have touched the pipeline"


def test_the_clock_still_runs_when_automation_is_on(monkeypatch):
    import runtime_control

    calls = []
    monkeypatch.setattr(runtime_control, "automation_enabled", lambda: True)
    monkeypatch.setattr(runtime_control, "read_schedule_state", lambda _k: "")
    monkeypatch.setattr(runtime_control, "write_schedule_state", lambda *a: None)
    monkeypatch.setattr(
        runtime_control, "request_services",
        lambda *a, **k: calls.append((a, k)),
    )

    result = runtime_control.apply_pipeline_window()

    assert result["applied"] is True
    assert len(calls) == 1
