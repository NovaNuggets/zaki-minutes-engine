"""WP-M12 — the post-meeting summary generator ("minutes-the-document").

The read plane and the archive UI have rendered ``meeting.data['summary']``
since the first activation ("Summary not available yet.") — nothing ever WROTE
it: upstream's copilot/agent stack is deliberately not deployed here. This
worker is the missing writer, engine-native and provider-thin: one sweep per
``SUMMARY_INTERVAL_S`` finds terminal ZAKI meetings that have transcript rows
and no summary, builds one prompt from the speaker-attributed segments, calls
the SAME OpenAI-compatible backend the STT leg already trusts (chat
completions instead of transcriptions; ``SUMMARY_*`` envs may point it
elsewhere), and persists ``{"text", "updated_at", "model"}`` under the shared
meeting-write barrier — so a privacy withdrawal wins against a late summary
exactly as it wins against a late transcript flush.

Deliberately NOT here: realtime copilot notes (the ``processed`` views lane),
per-user templates, regeneration. One good document per meeting first.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional, Protocol

from .ports import TranscriptWriteRefused

log = logging.getLogger("collector.summarizer")

#: messages → completion text. The live implementation is ``openai_chat_llm``;
#: tests inject a fake.
ChatLLM = Callable[[list[dict]], Awaitable[str]]

# Input budget: keep the prompt comfortably inside small-context serving tiers.
# Long meetings keep their head and tail (openings set agenda, endings carry
# decisions); the elided middle is marked so the model never hallucinates
# continuity across the cut.
MAX_PROMPT_CHARS = 24_000

SUMMARY_SYSTEM = (
    "You write the official minutes of a meeting from a speaker-attributed "
    "transcript. Respond in the MEETING'S OWN dominant language (if the meeting "
    "mixes languages, use the one carrying the substantive discussion). Be "
    "faithful AND specific: use the actual names of the people, products, "
    "features, tools, companies and numbers exactly as spoken — never flatten "
    "them into vague phrases like 'various tools' or 'execute tasks'. When a "
    "distinctive idea, mechanism, product capability or claim is discussed, name "
    "it concretely so a reader who missed the meeting understands what makes it "
    "notable. Never invent facts, names, decisions or dates that are not in the "
    "transcript; if the transcript is too thin to support a section, write a "
    "single short line saying so.\n\n"
    "When the transcript makes it clear, open the TL;DR by naming what KIND of "
    "meeting this was and its purpose (e.g. a product demo, an investor or sales "
    "pitch, a planning sync, a status review) plus important context such as "
    "funding, timeline or project status.\n\n"
    "Structure the minutes exactly as:\n"
    "## TL;DR\ntwo or three sentences capturing the purpose and the most "
    "important, specific substance (named features, claims, status) — not a "
    "generic description.\n"
    "## Key points\nshort bullets, each naming a concrete thing discussed (a "
    "specific feature, mechanism, number, status or piece of context) that a "
    "reader actually needs.\n"
    "## Decisions\nbullets; only genuine decisions actually made.\n"
    "## Action items\nbullets as 'owner — action'; only if actually assigned.\n"
    "## Open questions\nbullets; only if left unresolved."
)


class SummaryStore(Protocol):
    """The two store legs the summarizer needs (implemented by the SQL adapter
    and the in-memory fake alongside the other collector ports)."""

    async def meetings_needing_summary(self, *, limit: int) -> list[dict]:
        """Terminal ZAKI meetings owning ≥1 transcript row and no ``data.summary``
        yet — each as ``{"id", "user_id"}``, oldest first."""
        ...

    async def get_transcript_by_id(
        self, user_id: int, meeting_id: int, member_workspaces: Optional[set] = None
    ) -> Optional[dict]: ...

    async def write_summary(self, meeting_id: int, summary: dict) -> None:
        """Set ``data['summary']`` under the shared meeting-write barrier;
        raises ``TranscriptWriteRefused`` for privacy-withdrawn meetings."""
        ...


#: L-0270 — the bot marks audio it LOST to a dead STT provider with a segment whose
#: ``segment_id`` starts with this prefix (see the bot's ``pipeline.ts``
#: ``GAP_ID_PREFIX``). It is a marker, not speech: it must never be summarised as
#: content, and its minutes must be declared so a summary of two thirds of a meeting
#: never reads as the whole meeting.
GAP_PREFIX = "gap:"


def is_gap_marker(seg: dict) -> bool:
    return str(seg.get("segment_id") or "").startswith(GAP_PREFIX)


def gap_notice(segments: list[dict]) -> Optional[str]:
    """The one-line truth about what the transcript is missing, or ``None`` when it
    is whole. Derived from the gap markers themselves — never from a provider health
    signal the summariser cannot see."""
    lost = 0.0
    for seg in segments:
        if not is_gap_marker(seg):
            continue
        try:
            lost += max(0.0, float(seg["end"]) - float(seg["start"]))
        except (KeyError, TypeError, ValueError):
            continue
    if lost <= 0:
        return None
    minutes = round(lost / 60)
    amount = f"{minutes} minutes" if minutes >= 1 else "less than a minute"
    return (
        f"NOTICE — this transcript is INCOMPLETE: {amount} not transcribed "
        "(provider unavailable). The missing stretches are marked inline as "
        "[transcription unavailable …]. Say so plainly in the TL;DR and never "
        "present these minutes as covering the whole meeting."
    )


#: Share of transcribed characters one language must carry before the prompt NAMES it. Below this
#: the meeting is genuinely mixed and ``SUMMARY_SYSTEM``'s own mixed-language rule decides.
SUMMARY_LANGUAGE_DOMINANCE = 0.6

#: The transcriber's fallback label, NOT a detection: ``chunked-transcriber.ts`` writes
#: ``this.cb.language || result.language || 'en'``, so every chunk Whisper could not identify is
#: stored as 'en'. ``language_probability`` — the only thing that would separate the two — is
#: dropped before the segment is persisted (``collector/ingest.py`` keeps ``language`` alone), so a
#: dominant 'en' is never asserted: the model keeps its own default instead of being told a guess.
#: Deliberately coarse, with a known ceiling: an English meeting is simply not named (the model
#: defaults there anyway). Upgrade path: persist ``language_probability`` through ingest and weight
#: by it, then English can be asserted like any other language.
UNRELIABLE_DEFAULT_LANGUAGE = "en"

#: The read plane's sealed bounds for a segment language (``zaki_read/router.py``): anything else is
#: not a language and does not vote.
_LANGUAGE_BOUNDS = (2, 35)


def dominant_language(segments: list[dict]) -> Optional[str]:
    """The meeting's own language, or ``None`` when the transcript does not settle one.

    Weighted by TRANSCRIBED CHARACTERS, not by turn count — a dozen "okay"s must not outvote the
    substantive discussion. Blank turns and languages outside the sealed bounds do not vote, and the
    transcriber's ``'en'`` fallback is never returned (see ``UNRELIABLE_DEFAULT_LANGUAGE``)."""
    chars: dict[str, int] = {}
    for seg in segments:
        text = (seg.get("text") or "").strip()
        language = seg.get("language")
        if not text or not isinstance(language, str):
            continue
        language = language.strip().lower()
        if not _LANGUAGE_BOUNDS[0] <= len(language) <= _LANGUAGE_BOUNDS[1]:
            continue
        chars[language] = chars.get(language, 0) + len(text)
    if not chars:
        return None
    language, count = max(chars.items(), key=lambda item: item[1])
    if count / sum(chars.values()) < SUMMARY_LANGUAGE_DOMINANCE:
        return None
    return None if language == UNRELIABLE_DEFAULT_LANGUAGE else language


def build_summary_messages(segments: list[dict]) -> list[dict]:
    """One prompt from speaker-attributed segments, head+tail bounded, in the meeting's language.

    ``SUMMARY_SYSTEM`` has always asked for "the MEETING'S OWN dominant language" — but the model was
    never TOLD what that was, so it guessed and defaulted to English (staging meeting 49, German
    transcript, English minutes). The segments carry the detection; ``dominant_language`` reads it and
    the user message states it. A meeting that does not settle one keeps the system's mixed rule."""
    lines = []
    for seg in segments:
        text = (seg.get("text") or "").strip()
        if not text:
            continue
        if is_gap_marker(seg):
            lines.append(text)  # the transcript's own hole, not an utterance by anyone
            continue
        speaker = (seg.get("speaker") or "").strip() or "Speaker"
        lines.append(f"{speaker}: {text}")
    transcript = "\n".join(lines)
    if len(transcript) > MAX_PROMPT_CHARS:
        half = MAX_PROMPT_CHARS // 2
        transcript = (
            transcript[:half]
            + "\n[… middle of the meeting elided for length …]\n"
            + transcript[-half:]
        )
    language = dominant_language(segments)
    held = (
        f"The meeting was held in {language}. Write the summary in {language}.\n\n"
        if language
        else ""
    )
    # The gap notice rides ABOVE the (possibly elided) transcript so it always survives (L-0270).
    notice = gap_notice(segments)
    return [
        {"role": "system", "content": SUMMARY_SYSTEM},
        {
            "role": "user",
            "content": (f"{notice}\n\n" if notice else "")
            + f"Transcript:\n\n{transcript}\n\n{held}Write the minutes.",
        },
    ]


def openai_chat_llm(base_url: str, token: str, model: str, *, timeout_s: float = 60.0) -> ChatLLM:
    """The live LLM leg: POST {base}/v1/chat/completions, OpenAI-compatible."""
    import httpx

    url = base_url.rstrip("/")
    if not url.endswith("/v1/chat/completions"):
        url = f"{url}/v1/chat/completions"

    # No token (self-host / no-auth backend) → send no header. "Bearer " with an
    # empty value is an illegal header value and httpx rejects it outright.
    token = token.strip()
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    async def call(messages: list[dict]) -> str:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            response = await client.post(
                url,
                headers=headers,
                json={"model": model, "messages": messages, "temperature": 0.2},
            )
            response.raise_for_status()
            body = response.json()
            text = (body.get("choices") or [{}])[0].get("message", {}).get("content", "")
            if not isinstance(text, str) or not text.strip():
                raise ValueError("summary backend returned an empty completion")
            return text.strip()

    return call


async def summarize_tick(
    store: SummaryStore,
    llm: ChatLLM,
    *,
    model: str,
    limit: int = 3,
    now: Optional[datetime] = None,
) -> int:
    """ONE sweep: summarize up to ``limit`` candidates. Per-meeting failures are
    contained (the next tick retries); a privacy-refused write is final for that
    meeting and logged loudly (the candidates query stops offering it once the
    erasure purges the row). Returns the number of summaries written."""
    written = 0
    for candidate in await store.meetings_needing_summary(limit=limit):
        meeting_id = int(candidate["id"])
        try:
            doc = await store.get_transcript_by_id(int(candidate["user_id"]), meeting_id)
            segments = (doc or {}).get("segments") or []
            if not any(
                (seg.get("text") or "").strip() and not is_gap_marker(seg)
                for seg in segments
            ):
                # Rows may exist with empty text only — nothing to summarize. A transcript
                # of nothing but gap markers is the same case: a total STT outage has no
                # minutes to write, and the markers are not content (L-0270).
                continue
            text = await llm(build_summary_messages(segments))
            stamp = (now or datetime.now(timezone.utc)).isoformat()
            await store.write_summary(
                meeting_id, {"text": text, "updated_at": stamp, "model": model}
            )
            written += 1
            log.info("summary written for meeting %s (%d segments)", meeting_id, len(segments))
        except TranscriptWriteRefused:
            log.warning(
                "summary write refused for meeting %s (privacy barrier) — not retrying", meeting_id
            )
        except Exception:  # noqa: BLE001 — isolate per meeting; the next tick retries
            log.exception("summary generation failed for meeting %s", meeting_id)
    return written
