"""Pure fail-closed checks for Identity-owned Minutes policy projection."""
from types import SimpleNamespace

from admin_api.app.main import _minutes_view


def _user(minutes_prefs):
    return SimpleNamespace(data={"minutes_prefs": minutes_prefs})


def test_corrupt_stored_retention_disables_capture_instead_of_extending_to_defaults(monkeypatch):
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "true")
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "true")
    view = _minutes_view(_user({
        "capture_enabled": True,
        "policy_version": "minutes-capture.v1",
        "attested_at": "2026-07-15T10:00:00+00:00",
        "retention_days": {"audio": 7, "transcript": "corrupt", "summary": 30},
    }))

    assert view["capture_enabled"] is False
    assert view["attested_at"] is None
    assert view["retention_days"] == {"audio": 7, "transcript": 30, "summary": 30}
    assert view["capture_repair"] == "retention_repair_required"


def test_invalid_or_future_attestation_disables_capture(monkeypatch):
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "true")
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "true")

    for attested_at in ("not-a-time", "2099-01-01T00:00:00+00:00", "2026-07-15T10:00:00"):
        view = _minutes_view(_user({
            "capture_enabled": True,
            "policy_version": "minutes-capture.v1",
            "attested_at": attested_at,
            "retention_days": {"audio": 7, "transcript": 30, "summary": 30},
        }))
        assert view["capture_enabled"] is False
        assert view["attested_at"] is None
        assert view["capture_repair"] == "reconsent_required"


def test_operator_rollback_preserves_requested_permissions_in_the_user_view(monkeypatch):
    monkeypatch.setenv("ZAKI_MINUTES_CAPTURE_ENABLED", "false")
    monkeypatch.setenv("ZAKI_MINUTES_READ_ENABLED", "false")

    view = _minutes_view(_user({
        "capture_enabled": True,
        "agent_read_enabled": True,
        "policy_version": "minutes-capture.v1",
        "attested_at": "2026-07-15T10:00:00+00:00",
        "retention_days": {"audio": 7, "transcript": 30, "summary": 30},
    }))

    assert view["capture_enabled"] is False
    assert view["agent_read_enabled"] is False
    assert view["capture_requested"] is True
    assert view["agent_read_requested"] is True
    assert view["capture_repair"] is None
