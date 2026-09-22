"""ProcessBackend output capture — the process analog of `docker logs` (release-eyeball fix).

The original backend spawned workloads with stdout/stderr=DEVNULL, so a worker that died at startup
(lite's agent launcher hitting `ModuleNotFoundError: No module named 'llm'`) was undiagnosable BY
DESIGN: the terminal showed only "No chat output arrived before the stream closed". These tests pin
the fix: every workload's output lands in a per-workload log file (PROCESS_LOG_DIR, default
<tempdir>/vexa-workloads), and a nonzero self-exit surfaces the tail at ERROR level through the
runtime's own logs — exactly once, and never for a backend-initiated stop (SIGTERM/SIGKILL is an
expected nonzero, not a crash)."""
from __future__ import annotations

import json
import logging
import os
import signal
import stat
import sys
import tempfile

import pytest

from runtime_kernel import process_backend as process_backend_module
from runtime_kernel.backend import WorkloadHandle
from runtime_kernel.process_backend import ProcessBackend, _log_dir
from runtime_kernel.profiles import Runnable


def _py(code: str) -> Runnable:
    return Runnable(command=[sys.executable, "-c", code])


def _start_and_wait(backend: ProcessBackend, workload_id: str, runnable: Runnable):
    h = backend.start(workload_id, runnable, {})
    h._impl.wait(timeout=10)
    return h


# ── the log dir seam ──────────────────────────────────────────────────────────────────────────────
def test_log_dir_defaults_under_tempdir(monkeypatch):
    monkeypatch.delenv("PROCESS_LOG_DIR", raising=False)
    assert _log_dir() == os.path.join(tempfile.gettempdir(), "vexa-workloads")


def test_log_dir_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path / "wl"))
    assert _log_dir() == str(tmp_path / "wl")


# ── capture + failure tail ────────────────────────────────────────────────────────────────────────
def test_failed_spawn_output_lands_in_log_file_and_error_tail(monkeypatch, tmp_path, caplog):
    """The release-blocker shape: a worker that prints and dies nonzero. Its stdout AND stderr must
    be on disk, and the tail must reach the runtime logger at ERROR."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    backend = ProcessBackend()
    h = _start_and_wait(
        backend, "w-crash",
        _py("import sys; print('boom-stdout'); sys.stderr.write('boom-stderr\\n'); sys.exit(3)"),
    )
    with caplog.at_level(logging.ERROR, logger="runtime_kernel.process"):
        assert backend.exit_code(h) == 3

    text = (tmp_path / "w-crash.log").read_text()
    assert "boom-stdout" in text and "boom-stderr" in text  # both streams, interleaved

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    msg = errors[0].getMessage()
    assert "w-crash" in msg and "exited 3" in msg
    assert "boom-stderr" in msg                              # the tail itself
    assert str(tmp_path / "w-crash.log") in msg              # where the full log lives


def test_failure_tail_logged_once(monkeypatch, tmp_path, caplog):
    """exit_code is POLLED (kernel.get / stop loop) — the tail must not repeat per poll."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    backend = ProcessBackend()
    h = _start_and_wait(backend, "w-poll", _py("raise SystemExit(1)"))
    with caplog.at_level(logging.ERROR, logger="runtime_kernel.process"):
        for _ in range(3):
            assert backend.exit_code(h) == 1
    assert len([r for r in caplog.records if r.levelno == logging.ERROR]) == 1


def test_clean_exit_captures_output_without_error(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    backend = ProcessBackend()
    h = _start_and_wait(backend, "w-ok", _py("print('all fine')"))
    with caplog.at_level(logging.WARNING, logger="runtime_kernel.process"):
        assert backend.exit_code(h) == 0
    assert "all fine" in (tmp_path / "w-ok.log").read_text()  # captured even on success
    assert not caplog.records


def test_backend_initiated_stop_is_not_reported_as_failure(monkeypatch, tmp_path, caplog):
    """kernel.stop() terminates/kills — the resulting signal exit is EXPECTED, not a crash tail."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    backend = ProcessBackend()
    h = backend.start("w-stop", _py("import time; time.sleep(30)"), {})
    with caplog.at_level(logging.ERROR, logger="runtime_kernel.process"):
        backend.terminate(h)
        h._impl.wait(timeout=10)
        code = backend.exit_code(h)
    assert code is not None and code != 0
    assert not [r for r in caplog.records if r.levelno == logging.ERROR]
    backend.cleanup(h)


def test_unwritable_log_dir_falls_back_to_devnull(monkeypatch, tmp_path, caplog):
    """Capture is fail-open: an unusable PROCESS_LOG_DIR must not stop workloads from starting."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("file, not dir")
    monkeypatch.setenv("PROCESS_LOG_DIR", str(blocker / "sub"))
    backend = ProcessBackend()
    with caplog.at_level(logging.WARNING, logger="runtime_kernel.process"):
        h = _start_and_wait(backend, "w-nolog", _py("raise SystemExit(2)"))
        assert backend.exit_code(h) == 2
    assert any("cannot capture output" in r.getMessage() for r in caplog.records)
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1 and "not captured" in errors[0].getMessage()


def test_workload_env_still_layered_over_process_env(monkeypatch, tmp_path):
    """The capture change must not disturb the env contract: spec env wins over os.environ."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("MARKER", "from-os")
    backend = ProcessBackend()
    echo = Runnable(command=[sys.executable, "-c", "import os; print(os.environ['MARKER'])"])
    h = backend.start("w-env", echo, {"MARKER": "from-spec"})
    h._impl.wait(timeout=10)
    assert backend.exit_code(h) == 0
    assert "from-spec" in (tmp_path / "w-env.log").read_text()


def test_workload_logs_are_private_operator_files(monkeypatch, tmp_path):
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("PROCESS_LOG_DIR", str(log_dir))
    backend = ProcessBackend()
    h = _start_and_wait(backend, "w-private", _py("print('sensitive transcript context')"))
    assert backend.exit_code(h) == 0

    assert stat.S_IMODE(log_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE((log_dir / "w-private.log").stat().st_mode) == 0o600


def test_untrusted_workload_id_cannot_escape_the_log_directory(monkeypatch, tmp_path):
    log_dir = tmp_path / "logs"
    victim = tmp_path / "operator-secret.log"
    victim.write_text("do-not-touch")
    monkeypatch.setenv("PROCESS_LOG_DIR", str(log_dir))
    backend = ProcessBackend()

    h = _start_and_wait(backend, "../operator-secret", _py("print('attacker-controlled')"))
    assert backend.exit_code(h) == 0

    assert victim.read_text() == "do-not-touch"
    generated = list(log_dir.glob("workload-*.log"))
    assert len(generated) == 1
    assert "attacker-controlled" in generated[0].read_text()


def test_operator_credentials_are_not_inherited_from_runtime_process(monkeypatch, tmp_path):
    """A Lite workload is an untrusted child, not another control-plane service.  Credentials held
    by the ambient runtime process must therefore be absent unless the WorkloadSpec projects them
    explicitly."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    operator_credentials = {
        "INTERNAL_API_SECRET": "internal",
        "RUNTIME_CONTROL_SECRET": "runtime-control",
        "RUNTIME_CALLBACK_SECRET": "runtime-callback",
        "ADMIN_TOKEN": "meeting-admin",
        "ADMIN_API_TOKEN": "identity-admin",
        "VEXA_INTERNAL_API_SECRET": "agent-internal",
        "VEXA_RUNTIME_CONTROL_SECRET": "agent-runtime-control",
        "VEXA_RUNTIME_CALLBACK_SECRET": "agent-runtime-callback",
        "VEXA_DISPATCH_SIGNING_KEY": "dispatch-signer",
        "VEXA_AGENT_IDENTITY_TOKEN": "ambient-agent-token",
        "VEXA_BOT_API_KEY": "bot-api-key",
        "VEXA_API_KEY": "terminal-shared-key",
        "NEXTAUTH_SECRET": "terminal-session-secret",
        "JWT_SECRET": "identity-jwt-secret",
        "DATABASE_URL": "postgresql://operator:password@db/control",
        "DB_PASSWORD": "database-password",
        "PGPASSWORD": "postgres-password",
        "REDIS_URL": "redis://:password@redis/0",
        "REDIS_PASSWORD": "redis-password",
        "TRANSCRIPTION_SERVICE_TOKEN": "transcription-service-token",
        "MINIO_ACCESS_KEY": "object-store-access-key",
        "MINIO_SECRET_KEY": "object-store-secret-key",
        "S3_ACCESS_KEY": "s3-access-key",
        "S3_SECRET_KEY": "s3-secret-key",
        "HOST_CLAUDE_CREDENTIALS": "/run/operator/claude-credentials.json",
        "CLAUDE_CODE_OAUTH_TOKEN": "claude-subscription-token",
        "ANTHROPIC_API_KEY": "model-api-key",
        "ANTHROPIC_AUTH_TOKEN": "model-auth-token",
        "VEXA_LLM_API_KEY": "model-gateway-key",
        # Unknown/future credentials prove the boundary is an allowlist, not a stale deny-list.
        "AWS_SECRET_ACCESS_KEY": "cloud-secret",
        "GITHUB_TOKEN": "source-control-token",
        "OPENAI_API_KEY": "another-provider-key",
        "ZAKI_READ_TOKEN_MINUTES": "minutes-read",
        "ZAKI_AGENT_ERASURE_SIGNING_SECRET": "agent-erasure-signer",
        "ZAKI_AGENT_ERASURE_VERIFICATION_SECRET": "agent-erasure-verifier",
        "ZAKI_MINUTES_ERASURE_SIGNING_SECRET": "minutes-erasure-signer",
        "ZAKI_MINUTES_ERASURE_PREVIOUS_VERIFICATION_SECRET": "previous-erasure-verifier",
        "ZAKI_MINUTES_FINALIZED_SECRET": "minutes-finalized-signer",
    }
    for key, value in operator_credentials.items():
        monkeypatch.setenv(key, value)

    backend = ProcessBackend()
    h = _start_and_wait(
        backend,
        "w-ambient-credentials",
        _py(
            "import json, os; "
            f"keys = {sorted(operator_credentials)!r}; "
            "print(json.dumps([key for key in keys if key in os.environ]))"
        ),
    )
    assert backend.exit_code(h) == 0
    inherited = json.loads((tmp_path / "w-ambient-credentials.log").read_text())
    assert inherited == []


def test_explicit_workload_env_can_project_a_scoped_credential(monkeypatch, tmp_path):
    """The deny policy applies only to ambient authority. The authenticated WorkloadSpec remains
    the projection boundary, even if a scoped credential deliberately reuses an operator key name."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("INTERNAL_API_SECRET", "operator-wide-secret")
    backend = ProcessBackend()
    h = backend.start(
        "w-explicit-credential",
        _py("import os; print(os.environ['INTERNAL_API_SECRET'])"),
        {"INTERNAL_API_SECRET": "scoped-workload-secret"},
    )
    h._impl.wait(timeout=10)
    assert backend.exit_code(h) == 0
    assert (tmp_path / "w-explicit-credential.log").read_text().strip() == "scoped-workload-secret"


def test_model_provider_credentials_require_explicit_workload_projection(monkeypatch, tmp_path):
    """A meeting bot must not inherit the Agent model credential merely because both run below the
    Lite runtime. Agent-api explicitly stamps its allowlisted credential into Agent WorkloadSpecs."""
    monkeypatch.setenv("PROCESS_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "model-api-key")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "model-oauth-token")
    backend = ProcessBackend()
    h = _start_and_wait(
        backend,
        "w-model-credentials",
        _py(
            "import json, os; print(json.dumps({"
            "'api': os.environ.get('ANTHROPIC_API_KEY'), "
            "'oauth': os.environ.get('CLAUDE_CODE_OAUTH_TOKEN')}))"
        ),
    )
    assert backend.exit_code(h) == 0
    projected = json.loads((tmp_path / "w-model-credentials.log").read_text())
    assert projected == {"api": None, "oauth": None}


def test_agent_profile_fails_closed_when_process_isolation_is_unavailable(monkeypatch):
    """An Agent worker executes user-driven code. If Lite cannot derive its uid/workspace wall, the
    spawn must fail; running it as the root runtime is not a supported degradation mode."""
    monkeypatch.setattr(process_backend_module, "plan_process_isolation", lambda _env: None)
    backend = ProcessBackend()
    agent = Runnable(
        command=[sys.executable, "-c", "raise AssertionError('must not execute')"],
        broker_model_credentials=True,
    )

    with pytest.raises(RuntimeError, match="Agent process isolation is unavailable"):
        backend.start("agent-no-wall", agent, {"VEXA_UNIT_ID": "unit-1"})


def test_agent_profile_fails_closed_when_process_isolation_setup_errors(monkeypatch):
    plan = object()
    monkeypatch.setattr(process_backend_module, "plan_process_isolation", lambda _env: plan)

    def fail_apply(_plan):
        raise OSError("permission wall failed")

    monkeypatch.setattr(process_backend_module, "apply_process_isolation", fail_apply)
    backend = ProcessBackend()
    agent = Runnable(
        command=[sys.executable, "-c", "raise AssertionError('must not execute')"],
        broker_model_credentials=True,
    )

    with pytest.raises(RuntimeError, match="Agent process isolation setup failed"):
        backend.start("agent-broken-wall", agent, {"VEXA_UNIT_ID": "unit-1"})


def test_cleanup_kills_the_workload_process_group_even_after_the_leader_exits(monkeypatch):
    """Chromium/ffmpeg children must not survive their launcher and retain a per-meeting uid."""
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: calls.append((pgid, sig)))

    class ExitedLeader:
        pid = 4242

        @staticmethod
        def poll():
            return 0

        @staticmethod
        def wait(timeout):
            return 0

    backend = ProcessBackend()
    backend._capture["meeting"] = {"log_path": None, "reported": False}
    backend.cleanup(WorkloadHandle(id="meeting", impl=ExitedLeader()))

    assert calls == [(4242, signal.SIGKILL)]
