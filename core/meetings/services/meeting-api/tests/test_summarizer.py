"""WP-M12 — the summary generator: candidates, prompt shape, the write, the barrier."""
import contextlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from meeting_api.collector.fakes import InMemoryTranscriptStore
from meeting_api.collector.summarizer import (
    MAX_PROMPT_CHARS,
    SUMMARY_LANGUAGE_DOMINANCE,
    build_summary_messages,
    dominant_language,
    openai_chat_llm,
    summarize_tick,
)

pytestmark = pytest.mark.asyncio

USER = 7


def _store(*, status="completed", data=None, segments=None) -> InMemoryTranscriptStore:
    store = InMemoryTranscriptStore()
    store.seed_meeting(
        meeting_id=1, user_id=USER, platform="google_meet", native_meeting_id="nat-1",
        status=status,
        start_time="2026-07-21T09:00:00+00:00", end_time="2026-07-21T09:30:00+00:00",
        created_at="2026-07-21T08:59:00+00:00", updated_at="2026-07-21T09:30:00+00:00",
        data=data if data is not None else {
            "zaki_capture": {"state": "withdrawn", "withdrawal_reason": "capture_stopped"},
        },
        segments=segments if segments is not None else [
            {"segment_id": "s1", "start": 1.0, "end": 2.0, "speaker": "Al",
             "text": "We agreed to ship the summary generator this week.", "language": "en",
             "completed": True},
            {"segment_id": "s2", "start": 3.0, "end": 4.0, "speaker": "Nova",
             "text": "Nova owns the deploy.", "language": "en", "completed": True},
        ],
    )
    return store


async def _llm_recording(calls):
    async def llm(messages):
        calls.append(messages)
        return "## TL;DR\nShipped it."
    return llm


async def test_terminal_meeting_with_transcript_gets_a_summary():
    store = _store()
    calls: list = []
    written = await summarize_tick(store, await _llm_recording(calls), model="m-1")

    assert written == 1
    summary = store._meetings[1]["data"]["summary"]
    assert summary["text"] == "## TL;DR\nShipped it."
    assert summary["model"] == "m-1"
    assert summary["updated_at"]
    # the prompt carried the speaker-attributed lines
    user_msg = calls[0][-1]["content"]
    assert "Al: We agreed to ship the summary generator this week." in user_msg
    assert "Nova: Nova owns the deploy." in user_msg


async def test_existing_summary_is_never_regenerated():
    store = _store()
    store._meetings[1]["data"]["summary"] = {"text": "already", "updated_at": "x"}
    calls: list = []
    assert await summarize_tick(store, await _llm_recording(calls), model="m-1") == 0
    assert calls == []
    assert store._meetings[1]["data"]["summary"]["text"] == "already"


async def test_privacy_withdrawal_refuses_the_summary_write():
    store = _store(data={
        "zaki_capture": {"state": "withdrawn", "withdrawal_reason": "consent_withdrawn"},
    })
    calls: list = []
    # candidates exclude nothing in the fake beyond summary/status/text — the BARRIER refuses
    written = await summarize_tick(store, await _llm_recording(calls), model="m-1")
    assert written == 0
    assert "summary" not in store._meetings[1]["data"]


async def test_non_terminal_and_empty_transcripts_are_not_candidates():
    active = _store(status="active")
    assert await active.meetings_needing_summary(limit=5) == []
    empty = _store(segments=[{"segment_id": "s1", "start": 1.0, "end": 2.0,
                              "speaker": "Al", "text": "   ", "language": "en",
                              "completed": True}])
    assert await empty.meetings_needing_summary(limit=5) == []


async def test_llm_failure_is_contained_and_retryable():
    store = _store()

    async def bad_llm(messages):
        raise RuntimeError("backend down")

    assert await summarize_tick(store, bad_llm, model="m-1") == 0
    assert "summary" not in store._meetings[1]["data"]  # untouched → retried next tick


def test_long_transcripts_keep_head_and_tail():
    segments = [
        {"segment_id": f"s{i}", "speaker": "Al", "text": f"line {i} " + "x" * 80}
        for i in range(600)
    ]
    messages = build_summary_messages(segments)
    body = messages[-1]["content"]
    assert len(body) < MAX_PROMPT_CHARS + 500
    assert "line 0 " in body and "line 599 " in body
    assert "elided for length" in body


@contextlib.contextmanager
def _chat_stub():
    """Local OpenAI-shaped stub; yields (base_url, seen) where seen collects Authorization."""
    seen: list[str | None] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            seen.append(self.headers.get("Authorization"))
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            body = json.dumps(
                {"choices": [{"message": {"content": "## TL;DR\nStub said so."}}]}
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep pytest output clean
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", seen
    finally:
        server.shutdown()
        server.server_close()


async def test_empty_token_omits_authorization_header():
    """Self-host/no-auth-backend: token is "" → send no header at all.

    Regression: ``{"Authorization": f"Bearer {token}"}`` yielded ``b'Bearer '``,
    which httpx rejects with LocalProtocolError, failing every summarize_tick.
    """
    with _chat_stub() as (base_url, seen):
        llm = openai_chat_llm(base_url, "", "m-1")
        assert await llm([{"role": "user", "content": "hi"}]) == "## TL;DR\nStub said so."

    assert seen == [None], f"expected no Authorization header, got {seen!r}"


async def test_blank_token_omits_authorization_header():
    with _chat_stub() as (base_url, seen):
        llm = openai_chat_llm(base_url, "   ", "m-1")
        await llm([{"role": "user", "content": "hi"}])

    assert seen == [None], f"expected no Authorization header, got {seen!r}"


async def test_real_token_still_sends_authorization_header():
    with _chat_stub() as (base_url, seen):
        llm = openai_chat_llm(base_url, "sk-live-123", "m-1")
        await llm([{"role": "user", "content": "hi"}])

    assert seen == ["Bearer sk-live-123"]


# ── L-0270: a transcript with provider gaps must never read as complete ──────────
# Staging meeting 49 (2026-09-15): the STT provider 503'd for 14 minutes, the bot
# dropped the audio, and the summary of the surviving two thirds read as if it were
# the whole meeting. The bot now marks each lost span with a ``gap:`` segment; the
# summariser must carry that truth into the prompt as ONE explicit notice.

def _gap_seg(segment_id, start, end):
    return {
        "segment_id": segment_id, "speaker": "system", "completed": True,
        "start": start, "end": end,
        "text": "[transcription unavailable 09:03:50-09:05:50 UTC - provider unavailable (HTTP 503)]",
    }


def test_gap_markers_put_a_not_transcribed_notice_in_the_prompt():
    segments = [
        {"segment_id": "s1", "speaker": "Al", "text": "Opening.", "start": 0.0, "end": 10.0},
        _gap_seg("gap:10000", 10.0, 130.0),          # 2 minutes lost
        {"segment_id": "s2", "speaker": "Al", "text": "Closing.", "start": 130.0, "end": 140.0},
    ]
    body = build_summary_messages(segments)[-1]["content"]
    assert "2 minutes not transcribed (provider unavailable)" in body
    # the marker stays inline too, so the model can see WHERE the hole is
    assert "[transcription unavailable" in body
    # and the real speech is still there
    assert "Al: Opening." in body and "Al: Closing." in body


def test_a_gapless_transcript_carries_no_notice():
    body = build_summary_messages(
        [{"segment_id": "s1", "speaker": "Al", "text": "All good.", "start": 0.0, "end": 10.0}]
    )[-1]["content"]
    assert "not transcribed" not in body


def test_a_sub_minute_gap_is_still_declared():
    body = build_summary_messages([
        {"segment_id": "s1", "speaker": "Al", "text": "Opening.", "start": 0.0, "end": 10.0},
        _gap_seg("gap:10000", 10.0, 40.0),
    ])[-1]["content"]
    assert "less than a minute not transcribed (provider unavailable)" in body


async def test_a_meeting_that_is_only_gaps_is_not_summarized():
    """A total outage has no minutes to write — the marker text is not content."""
    store = _store(segments=[_gap_seg("gap:0", 0.0, 600.0)])
    calls: list = []
    assert await summarize_tick(store, await _llm_recording(calls), model="m-1") == 0
    assert calls == []
    assert "summary" not in store._meetings[1]["data"]


def test_a_gap_line_is_never_attributed_to_a_speaker():
    """The marker is the transcript's own hole: rendered bare, so the model cannot credit it
    to a person (the bot stores it under speaker "system"; a store may also drop the name)."""
    named = _gap_seg("gap:10000", 10.0, 130.0)
    unnamed = {**_gap_seg("gap:200000", 200.0, 260.0), "speaker": None}
    body = build_summary_messages([
        {"segment_id": "s1", "speaker": "Al", "text": "Opening.", "start": 0.0, "end": 10.0},
        named,
        unnamed,
    ])[-1]["content"]
    lines = body.splitlines()
    gap_lines = [ln for ln in lines if "UTC - provider unavailable (HTTP 503)]" in ln]  # the markers, not the NOTICE
    assert len(gap_lines) == 2
    assert all(ln.startswith("[transcription unavailable") for ln in gap_lines), gap_lines
    assert "system:" not in body and "Speaker: [transcription" not in body


# ── L-0271: the summary must be written in the meeting's own language ────────────────────────────
# Staging meeting 49 (German, 09-15) produced a German transcript and an ENGLISH summary:
# SUMMARY_SYSTEM asks for "the MEETING'S OWN dominant language" but nothing ever told the model
# which language that was, so the model guessed and defaulted to English.

GERMAN = "Wir haben beschlossen, den Zusammenfassungsgenerator diese Woche auszuliefern."


def _seg(i: int, text: str, language, speaker: str = "Al") -> dict:
    return {"segment_id": f"s{i}", "start": float(i), "end": float(i) + 1.0, "speaker": speaker,
            "text": text, "language": language, "completed": True}


def test_german_majority_states_the_language_in_the_user_message():
    segments = [_seg(1, GERMAN, "de"),
                _seg(2, "Wir brauchen noch einen Termin für die Freigabe.", "de"),
                _seg(3, "Okay.", "en")]

    body = build_summary_messages(segments)[-1]["content"]

    assert "The meeting was held in de." in body
    assert "Write the summary in de." in body


def test_dominance_is_by_transcribed_characters_not_by_turn_count():
    # twelve one-word turns must not outvote two long ones: turn count is not the measure
    segments = [_seg(i, "Okay.", "en") for i in range(12)]
    segments += [_seg(98, GERMAN, "de"), _seg(99, GERMAN, "de")]

    assert dominant_language(segments) == "de"


def test_blank_turns_and_out_of_bound_languages_do_not_vote():
    """Blank text carries no language, and the read plane's sealed bounds (2..35 chars,
    ``zaki_read/router.py``) decide what counts as a language at all."""
    segments = [_seg(1, GERMAN, "de"),
                _seg(2, "", "fr"), _seg(3, "   ", "fr"),
                _seg(4, "x" * 400, "x"), _seg(5, "y" * 400, ""),
                _seg(6, "z" * 400, "x" * 36), _seg(7, "q" * 400, None)]

    assert dominant_language(segments) == "de"


def test_mixed_below_the_threshold_keeps_the_system_mixed_language_rule():
    segments = [_seg(1, "Wir sprechen über den Zeitplan. " * 4, "de"),
                _seg(2, "Nous parlons du calendrier maintenant. " * 4, "fr")]

    messages = build_summary_messages(segments)

    assert dominant_language(segments) is None
    assert "The meeting was held in" not in messages[-1]["content"]
    assert "mixes languages" in messages[0]["content"]  # the system rule still decides


def test_the_en_default_is_never_asserted_as_the_meeting_language():
    """``chunked-transcriber.ts`` labels an UNDETECTED chunk 'en'
    (``this.cb.language || result.language || 'en'``) and the probability that would separate a
    real detection from that fallback is not persisted — so 'en' is never claimed with confidence."""
    segments = [_seg(1, "We agreed to ship the summary generator this week.", "en"),
                _seg(2, "Nova owns the deploy.", "en")]

    assert dominant_language(segments) is None
    assert "The meeting was held in" not in build_summary_messages(segments)[-1]["content"]


def test_the_dominance_threshold_is_the_one_constant():
    total = 1000
    at = int(total * SUMMARY_LANGUAGE_DOMINANCE)

    assert dominant_language([_seg(1, "d" * at, "de"), _seg(2, "f" * (total - at), "fr")]) == "de"
    assert dominant_language([_seg(1, "d" * (at - 1), "de"),
                              _seg(2, "f" * (total - at + 1), "fr")]) is None


def test_language_sentence_and_gap_notice_ride_the_same_prompt():
    """L-0270 × L-0271 (#63): both user-message additions survive together, and a gap marker
    (stored with no language) casts no language vote."""
    segments = [
        _seg(1, GERMAN, "de"),
        _gap_seg("gap:2000", 2.0, 182.0),                      # 3 minutes lost
        _seg(200, "Wir brauchen noch einen Termin für die Freigabe.", "de"),
    ]
    body = build_summary_messages(segments)[-1]["content"]
    assert body.startswith("NOTICE — this transcript is INCOMPLETE: 3 minutes not transcribed (provider unavailable).")
    assert "The meeting was held in de. Write the summary in de." in body
    assert body.index("NOTICE") < body.index("Transcript:") < body.index("The meeting was held in de.")
    assert dominant_language(segments) == "de"
