import os
import subprocess
import sys

import pytest

import runtime_control
from common import atomic_write_json


def _live_postgres_url() -> str | None:
    """Same opt-in pattern as test_classification_store.py: skip unless a
    reachable TEST_DATABASE_URL is set, so the normal run never depends on a
    live database."""
    url = os.getenv("TEST_DATABASE_URL", "").strip()
    if not url:
        return None
    try:
        import psycopg

        with psycopg.connect(url, connect_timeout=5):
            pass
    except Exception:
        return None
    return url


def _spawn_detached_process(seconds: int = 30) -> int:
    """Start a process reparented to init, mirroring ensure_supervisor's Popen.

    A child spawned directly by the test process would linger as a zombie
    after being signaled (since only the real OS parent's wait() reaps it),
    which would make process_alive report it as still alive. Detaching
    through a short-lived launcher, like start_supervisor.sh does, avoids
    that test-only artifact.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import subprocess,sys; "
            f"p = subprocess.Popen(['sleep', '{seconds}'], "
            "start_new_session=True, stdout=subprocess.DEVNULL, "
            "stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL); "
            "print(p.pid)",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return int(result.stdout.strip())


def test_runtime_control_defaults_paused_and_merges_requests(
    tmp_path,
    monkeypatch,
):
    control_path = tmp_path / "runtime_control.json"
    monkeypatch.setattr(runtime_control, "CONTROL_PATH", control_path)
    monkeypatch.setattr(runtime_control, "RUNTIME_MODE", "local")

    initial = runtime_control.read_control()
    assert initial["desired"] == {
        "pipeline": False,
        "ai_crawler": False,
        "zone_crawler": False,
    }

    runtime_control.request_services(
        {"ai_crawler": True},
        requested_by="unit_test",
    )
    updated = runtime_control.read_control()
    assert updated["desired"]["ai_crawler"] is True
    assert updated["desired"]["pipeline"] is False
    assert updated["requested_by"] == "unit_test"


def test_cloud_runtime_rejects_local_process_mutation(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        runtime_control,
        "CONTROL_PATH",
        tmp_path / "runtime_control.json",
    )
    monkeypatch.setattr(runtime_control, "RUNTIME_MODE", "cloud")

    monkeypatch.delenv(
        runtime_control.RUNTIME_CONTROL_DATABASE_ENV_VAR, raising=False
    )
    try:
        runtime_control.request_services({"pipeline": True})
    except RuntimeError as exc:
        assert "shared control store" in str(exc)
    else:
        raise AssertionError("cloud mode accepted a local process mutation")


@pytest.mark.skipif(
    _live_postgres_url() is None,
    reason="set TEST_DATABASE_URL to a reachable Postgres to run this",
)
def test_postgres_control_store_persists_and_merges_across_connections(
    monkeypatch,
):
    """Opt-in parity check: the shared cloud control store behaves like the
    local JSON file -- persists desired state, merges rather than clobbers,
    and is readable from a fresh connection (standing in for a second
    service process). Cleans up its own row before and after."""
    url = _live_postgres_url()
    monkeypatch.setenv(runtime_control.RUNTIME_CONTROL_DATABASE_ENV_VAR, url)
    monkeypatch.setattr(runtime_control, "RUNTIME_MODE", "cloud")

    def _cleanup() -> None:
        conn = runtime_control._connect_runtime_control()
        try:
            conn.execute(
                "DELETE FROM worker_control WHERE service IN (?, ?, ?)",
                runtime_control.SERVICES,
            )
            conn.commit()
        finally:
            conn.close()

    _cleanup()
    try:
        assert runtime_control.control_backend_available() is True
        initial = runtime_control.read_control()
        assert initial["desired"] == {
            "pipeline": False,
            "ai_crawler": False,
            "zone_crawler": False,
        }

        runtime_control.request_services(
            {"ai_crawler": True},
            requested_by="unit_test_postgres",
        )
        updated = runtime_control.request_services(
            {"pipeline": True},
            requested_by="unit_test_postgres",
        )
        assert updated["desired"]["ai_crawler"] is True
        assert updated["desired"]["pipeline"] is True
        assert updated["desired"]["zone_crawler"] is False
        assert updated["requested_by"] == "unit_test_postgres"

        reread = runtime_control.read_control()
        assert reread["desired"] == updated["desired"]
    finally:
        _cleanup()


def test_runtime_status_does_not_trust_stale_supervisor_state(
    tmp_path,
    monkeypatch,
):
    pid_path = tmp_path / "supervisor.pid"
    status_path = tmp_path / "supervisor.json"
    pid_path.write_text("999999", encoding="utf-8")
    atomic_write_json(status_path, {"pid": 999999, "running": True})
    monkeypatch.setattr(runtime_control, "SUPERVISOR_PID", pid_path)
    monkeypatch.setattr(runtime_control, "SUPERVISOR_STATUS", status_path)
    monkeypatch.setattr(
        runtime_control,
        "CONTROL_PATH",
        tmp_path / "runtime_control.json",
    )
    monkeypatch.setattr(runtime_control, "process_alive", lambda _pid: False)

    status = runtime_control.runtime_status()

    assert status["supervisor"]["pid"] == 999999
    assert status["supervisor"]["running"] is False


def test_find_supervisor_pids_filters_out_dead_pids(monkeypatch):
    monkeypatch.setattr(runtime_control, "_pgrep", lambda pattern: [999999999])
    assert runtime_control.find_supervisor_pids() == []


def test_ensure_supervisor_adopts_untracked_live_process(tmp_path, monkeypatch):
    """A supervisor whose PID file was lost must be adopted, not duplicated."""
    pid_path = tmp_path / "supervisor.pid"
    monkeypatch.setattr(runtime_control, "SUPERVISOR_PID", pid_path)
    monkeypatch.setattr(runtime_control, "RUNTIME_MODE", "local")

    pid = _spawn_detached_process()
    try:
        def _forbid_popen(*_args, **_kwargs):
            raise AssertionError(
                "ensure_supervisor spawned a new process instead of adopting "
                "the untracked one"
            )

        monkeypatch.setattr(runtime_control.subprocess, "Popen", _forbid_popen)
        monkeypatch.setattr(
            runtime_control, "find_supervisor_pids", lambda: [pid]
        )
        adopted = runtime_control.ensure_supervisor()
        assert adopted == pid
        assert pid_path.read_text(encoding="utf-8").strip() == str(pid)
    finally:
        try:
            import os
            import signal

            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def test_stop_supervisor_terminates_untracked_process_and_clears_pid_file(
    tmp_path, monkeypatch
):
    """An orphaned supervisor (PID file lost) must still be stoppable."""
    pid_path = tmp_path / "supervisor.pid"
    pid_path.write_text("0", encoding="utf-8")
    monkeypatch.setattr(runtime_control, "SUPERVISOR_PID", pid_path)

    pid = _spawn_detached_process()
    monkeypatch.setattr(runtime_control, "find_supervisor_pids", lambda: [pid])

    stopped_cleanly = runtime_control.stop_supervisor(timeout_seconds=5)

    assert stopped_cleanly is True
    assert not runtime_control.process_alive(pid)
    assert not pid_path.exists()
