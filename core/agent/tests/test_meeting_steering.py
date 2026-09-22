"""State-aware meeting-chat grounding (design-spec meeting-lifecycle-v2, W4).

_meeting_grounding branches on the meeting status the terminal passes in ``active``:
prep (idle/scheduled) never reads a stream and steers toward preparation; live/post
meeting content is never copied into the durable generic-chat prompt and instead requires
the dedicated bounded Minutes read path. Steering templates are overridable from the
_global workspace file ``agents/meeting-lifecycle.md`` — malformed overrides fail loud
and fall back.
"""
from __future__ import annotations

import json

from control_plane import meeting_steering
from control_plane.api import _fold_meeting_processed, _meeting_grounding


def _fake_redis(monkeypatch, seed):
    """fakeredis with streams pre-seeded: seed = {stream_name: [fields, …]}."""
    import fakeredis
    import redis

    r = fakeredis.FakeRedis(decode_responses=True)
    for name, entries in seed.items():
        for fields in entries:
            r.xadd(name, fields)
    monkeypatch.setattr(redis, "from_url", lambda *a, **k: r)
    return "redis://fake"


def _note(nid, speaker, text):
    return {"note": json.dumps({"id": nid, "speaker": speaker, "text": text, "pass": 1})}


# ── phase mapping ────────────────────────────────────────────────────────────────────

def test_phase_for_maps_lifecycle_and_defaults_live():
    assert meeting_steering.phase_for("idle") == "prep"
    assert meeting_steering.phase_for("scheduled") == "prep"
    for s in ("requested", "joining", "awaiting_admission", "active", "needs_help", "stopping"):
        assert meeting_steering.phase_for(s) == "live"
    for s in ("completed", "failed", "stopped"):
        assert meeting_steering.phase_for(s) == "post"
    # absent/unknown → live: a status-less legacy client keeps today's behavior
    assert meeting_steering.phase_for(None) == "live"
    assert meeting_steering.phase_for("") == "live"
    assert meeting_steering.phase_for("something-new") == "live"


# ── prep branch ──────────────────────────────────────────────────────────────────────

def test_prep_grounding_reads_no_stream_and_names_the_workspace(monkeypatch):
    """A scheduled meeting grounds a PREPARATION prompt: no redis read at all (redis_url absent
    would warn on a read — none happens), the title/time/bound workspace are named, and the
    steering pushes agenda/research/brief."""
    ctx, tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {
            "platform": "google_meet", "native_id": "abc-defg-hij", "meeting_id": 46,
            "status": "scheduled", "title": "OeNB pilot discussion",
            "scheduled_at": "2026-07-13T10:00:00Z", "workspace_id": "oenb-1424e3"}},
        session="main", prompt="build the agenda", redis_url=None)
    assert ctx == {"kind": "none", "session": "main"} and tools == []
    assert "PREPARE" in prompt and "OeNB pilot discussion" in prompt
    assert "scheduled for 2026-07-13T10:00:00Z" in prompt
    assert 'oenb-1424e3' in prompt
    assert "there is no transcript" in prompt.lower() or "has not happened yet" in prompt
    assert prompt.endswith("build the agenda")


def test_prep_grounding_without_workspace_says_so():
    _ctx, _tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {
            "meeting_id": 1, "native_id": "n1", "status": "idle", "title": "Untitled",
        }},
        session="s", prompt="hi", redis_url=None)
    assert "No shared prep workspace is bound" in prompt
    assert "user's OWN workspace" in prompt  # own-workspace brief note is the steer, not a dead end
    assert "(no time set yet)" in prompt


# ── post branch ──────────────────────────────────────────────────────────────────────

def test_post_grounding_does_not_copy_processed_or_raw_content_into_durable_chat(monkeypatch):
    url = _fake_redis(monkeypatch, {
        "proc:meeting:46": [_note("s1", "Jane", "we agreed on the Q3 pilot")],
        "tc:meeting:46": [{"payload": json.dumps({"type": "transcription", "segments": [
            {"segment_id": "s1", "speaker": "Jane", "text": "uh so we kinda agreed Q3??"}]})}],
    })
    _ctx, _tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {"native_id": "abc", "meeting_id": 46, "status": "completed",
                                         "title": "Acme kickoff"}},
        session="s", prompt="what was decided?", redis_url=url)
    assert "dedicated, bounded Minutes read path" in prompt
    assert "we agreed on the Q3 pilot" not in prompt
    assert "kinda agreed Q3??" not in prompt
    assert prompt.endswith("what was decided?")


def test_post_grounding_never_reads_the_legacy_redis_transcript_carriers(monkeypatch):
    import redis

    class RedisMustNotBeOpened:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("meeting grounding reopened a transcript carrier")

    monkeypatch.setattr(redis, "from_url", RedisMustNotBeOpened)
    _ctx, _tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {"native_id": "abc", "meeting_id": 46, "status": "completed"}},
        session="s", prompt="recap", redis_url="redis://must-not-open")
    assert "dedicated, bounded Minutes read path" in prompt
    assert "Raj: SSO first" not in prompt


def test_post_grounding_without_the_safe_read_path_is_honest(monkeypatch):
    url = _fake_redis(monkeypatch, {})
    _ctx, _tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {"native_id": "abc", "meeting_id": 46, "status": "failed",
                                         "title": "Ghost"}},
        session="s", prompt="summary?", redis_url=url)
    assert "cannot be read safely from this chat" in prompt
    assert "do not reconstruct or invent" in prompt


def test_fold_processed_upserts_by_id_and_skips_view_end(monkeypatch):
    url = _fake_redis(monkeypatch, {
        "proc:meeting:9": [
            _note("a", "Jane", "baseline text"),
            _note("a", "Jane", "polished text"),          # same id → upgrade in place
            {"type": "view_end", "cursor": "1-1"},        # terminal marker → skipped
            _note("b", "Raj", "second note"),
        ],
    })
    folded = _fold_meeting_processed(url, "9", limit=400)
    assert folded == "Jane: polished text\nRaj: second note"


def test_fold_processed_uses_a_finite_reverse_read_budget(monkeypatch):
    import redis

    class BoundedRedis:
        count = None

        def xrevrange(self, key, count=None):
            assert key == "proc:meeting:9"
            self.count = count
            return [("999-0", _note("latest", "Raj", "bounded"))]

    fake = BoundedRedis()
    monkeypatch.setattr(redis, "from_url", lambda *_a, **_k: fake)

    folded = _fold_meeting_processed("redis://fake", "9", limit=400)

    assert folded == "Raj: bounded"
    assert isinstance(fake.count, int) and 400 <= fake.count <= 1600


# ── native-only legacy context cannot address a tenant carrier ─────────────────────

def test_statusless_native_only_active_context_is_rejected(monkeypatch):
    url = _fake_redis(monkeypatch, {
        "tc:meeting:abc-defg-hij": [{"payload": json.dumps({"type": "transcription", "segments": [
            {"segment_id": "s1", "speaker": "Jane", "text": "ship it Friday"}]})}],
    })
    _ctx, _tools, prompt = _meeting_grounding(
        {"kind": "meeting", "meeting": {"platform": "google_meet", "native_id": "abc-defg-hij"}},
        session="main", prompt="who spoke last?", redis_url=url)
    assert prompt == "who spoke last?"
    assert "Jane: ship it Friday" not in prompt


# ── the _global override file ────────────────────────────────────────────────────────

def test_global_override_replaces_a_section(tmp_path):
    (tmp_path / "agents").mkdir()
    (tmp_path / "agents" / "meeting-lifecycle.md").write_text(
        "---\nversion: 1\n---\n\nintro prose ignored\n\n## prep\nCUSTOM PREP for {title}.\n\n## post\nCUSTOM POST {source}:\n{transcript}\n",
        encoding="utf-8")
    t = meeting_steering.steering_templates(str(tmp_path))
    assert t["prep"].startswith("CUSTOM PREP")
    assert t["post"].startswith("CUSTOM POST")
    assert t["live"] == meeting_steering.DEFAULT_TEMPLATES["live"]  # untouched section keeps default
    out = meeting_steering.render("prep", {"title": "X", "platform": "p", "native": "n",
                                            "when": "", "workspace": ""}, global_ws_path=str(tmp_path))
    assert out == "CUSTOM PREP for X.\n\n"


def test_malformed_override_fails_loud_and_falls_back(tmp_path, caplog):
    (tmp_path / "agents").mkdir()
    (tmp_path / "agents" / "meeting-lifecycle.md").write_text(
        "## prep\nBad {unknown_placeholder} here.\n", encoding="utf-8")
    fields = {"title": "X", "platform": "p", "native": "n", "when": "", "workspace": ""}
    with caplog.at_level("WARNING"):
        out = meeting_steering.render("prep", fields, global_ws_path=str(tmp_path))
    assert out == meeting_steering.DEFAULT_TEMPLATES["prep"].format(**fields)
    assert any("bad placeholder" in r.message for r in caplog.records)


def test_missing_override_file_is_silent_default(tmp_path):
    assert meeting_steering.steering_templates(str(tmp_path)) == meeting_steering.DEFAULT_TEMPLATES
    assert meeting_steering.steering_templates("") == meeting_steering.DEFAULT_TEMPLATES
