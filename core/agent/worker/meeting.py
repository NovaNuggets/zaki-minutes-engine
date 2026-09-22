"""meeting.py — the live MEETING copilot of the in-container agent harness.

Consumes the meeting's ``transcript.v1`` Stream, gates cheaply, cleans the transcript + surfaces
proactive cards via a DIRECT ``CompletionPort`` call (a card beat is a pure prompt→text turn — no
tools, no subprocess, no workspace memory), and (on ``session_end``) writes the post-meeting kg
meeting entity through deterministic structured serialization. Untrusted meeting content never
enters a tool-enabled prompt.

Imports the GENERIC helpers it needs from ``worker.engine`` (one direction); the engine imports the
meeting entry functions only inside ``main()`` (function-local) to avoid an import cycle.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Callable, Iterator

import yaml

from llm import (
    LLMAuthError,
    auth_error_event,
    completion_from_env,
    looks_like_auth_failure,
    model_error_event,
)
from shared.agent_config import (
    DEFAULT_CARD_KINDS,
    DEFAULT_POLISH_RULES,
    DEFAULT_TAG_RULES,
)
from shared.adapters import workspace_write_lock
from shared.meeting_retention import (
    carrier_is_writable,
    meeting_id_from_proc_stream,
    processing_deadline_from_token,
    processing_is_current,
    retention_fence_key,
    set_if_writable,
    set_if_writable_and_current,
    xadd_if_writable,
    xadd_if_writable_and_current,
)
from worker.engine import _Stream
from worker.engine import run_turn_over_workspace as _engine_run_turn_over_workspace

# Back-compat aliases (pre-llm-split names the worker.worker shim re-exports).
_auth_error_event = auth_error_event
_model_error_event = model_error_event


def run_turn_over_workspace(*args, **kwargs):
    """Indirection so a test that patches ``worker.worker.run_turn_over_workspace`` reaches the call
    sites in this module (the harness was historically one module). Resolves the name through the
    ``worker.worker`` shim at CALL time, falling back to the engine implementation."""
    import worker.worker as _w
    fn = getattr(_w, "run_turn_over_workspace", _engine_run_turn_over_workspace)
    if fn is run_turn_over_workspace:  # avoid self-recursion if the shim points back here
        fn = _engine_run_turn_over_workspace
    yield from fn(*args, **kwargs)

log = logging.getLogger("agent_api.worker")


# ── meeting mode: consume the transcript Stream, gate, emit proactive cards ───────────────────────

# build_card_prompt is a THIN COMPOSER: the MECHANISM (the transcript-window framing + the strict JSON
# shape the parser depends on) lives here in code; the POLICY (how to polish each line, and what to tag)
# is INJECTED from the workspace-governed MeetingConfig.polish_rules / .tag_rules — so a user can change
# copilot behavior by prompting the agent to edit agents/meeting.md, no redeploy.

# The fixed frame around the workspace policy. `{polish}` / `{tags}` are the governed rules; `{kinds}` is
# the wanted card kinds; `{steering}` is the (optional) free-text steering section.
_CARD_FRAME = (
    "You are a live meeting copilot watching a conversation in real time. Here is the mutable transcript "
    "processing window. Each line is sent through at most three passes; pass 1 is fresh, pass 2 should "
    "repair obvious ASR/name/entity errors, and pass 3 should be the final clean version before the line "
    "freezes and leaves this window.\n\n{lines}\n\n"
    "Return a processed transcript plus tag cards. For each input line, emit one note with the SAME id "
    "and speaker, following the POLISH RULES below.\n\n"
    "## Polish rules (governed by this workspace)\n{polish}\n\n"
    "Do not create topic headings. Set chapter to an empty string unless the source text itself gives a "
    "literal section title.\n\n"
    "## Tag rules (governed by this workspace)\n{tags}\n\n"
    "Emit tags ONLY as cards of these kinds: {kinds}. Do not use any other kind.\n\n"
    "Respond with ONLY this JSON object (no prose, no markdown fence, and do NOT write any files):\n"
    "{{\"notes\":[{{\"id\":\"<input id>\",\"speaker\":\"<speaker>\",\"chapter\":\"\",\"text\":\"<clean one-line note>\"}}],"
    "\"cards\":[{{\"kind\":\"<one of {kinds}>\",\"title\":\"<short>\",\"body\":\"<one line>\",\"actionable\":true}}]}}\n"
    "Use an empty cards array if these specific lines add no tags.{steering}"
)

# Appended to the frame only when the workspace config carries non-empty steering.
_STEERING_SECTION = (
    "\n\n## Standing instructions from this workspace\n"
    "Follow these workspace-set instructions about what to watch / ignore / tone:\n\n{steering}\n"
)


def build_card_prompt(
    lines: str,
    card_kinds: list[str],
    steering: str = "",
    *,
    polish_rules: str = DEFAULT_POLISH_RULES,
    tag_rules: str = DEFAULT_TAG_RULES,
) -> str:
    """Compose the copilot prompt: inject the workspace-governed ``polish_rules`` + ``tag_rules`` (the
    POLICY) and the wanted ``card_kinds`` around the transcript ``lines`` (the MECHANISM frame), plus the
    optional ``steering`` section. Changing a governed rule (via agents/meeting.md) changes the prompt."""
    section = _STEERING_SECTION.format(steering=steering.strip()) if steering.strip() else ""
    return _CARD_FRAME.format(
        lines=lines,
        kinds=", ".join(card_kinds),
        polish=(polish_rules or DEFAULT_POLISH_RULES).strip(),
        tags=(tag_rules or DEFAULT_TAG_RULES).strip(),
        steering=section,
    )


def _extract_json_value(reply: str | None):
    if not reply:
        return None
    text = reply.strip()
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        pass
    import re

    matches = []
    for pattern in (r"\{.*\}", r"\[.*\]"):
        m = re.search(pattern, reply, re.DOTALL)
        if m:
            matches.append((m.start(), m.group(0)))
    for _start, raw in sorted(matches, key=lambda item: item[0]):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            continue
    return None


def parse_cards(reply: str | None, card_kinds: list[str] | None = None) -> list[dict]:
    """Tolerantly pull the JSON card array out of the agent's reply (it may wrap it in prose/fences).
    When ``card_kinds`` is given, only cards of those kinds are kept."""
    value = _extract_json_value(reply)
    if isinstance(value, dict):
        arr = value.get("cards") or []
    elif isinstance(value, list):
        arr = value
    else:
        return []
    allowed = {k.lower() for k in card_kinds} if card_kinds else None
    out = []
    for c in arr:
        if not (isinstance(c, dict) and c.get("title") and c.get("kind")):
            continue
        if allowed is not None and str(c.get("kind")).lower() not in allowed:
            continue
        out.append(c)
    return out


def parse_notes(
    reply: str | None,
    stage_by_id: dict[str, int] | None = None,
    segment_by_id: dict[str, dict] | None = None,
) -> list[dict]:
    """Pull processed transcript notes from the copilot JSON object."""
    value = _extract_json_value(reply)
    if not isinstance(value, dict):
        return []
    arr = value.get("notes") or []
    if not isinstance(arr, list):
        return []
    stages = stage_by_id or {}
    segments = segment_by_id or {}
    out = []
    for item in arr:
        if not isinstance(item, dict):
            continue
        note_id = str(item.get("id") or "").strip()
        if segments and note_id not in segments:
            continue
        text = _first_person_note_text(str(item.get("text") or ""))
        if not note_id or not text:
            continue
        stage = int(item.get("pass") or stages.get(note_id) or 1)
        source = segments.get(note_id) or {}
        note = {
            "id": note_id,
            "speaker": str(item.get("speaker") or "").strip() or "Speaker",
            "chapter": str(item.get("chapter") or item.get("chapter_title") or "").strip(),
            "text": text,
            "pass": max(1, min(3, stage)),
            "frozen": stage >= 3,
        }
        ts = item.get("t", item.get("ts", source.get("start")))
        if ts is not None:
            note["t"] = ts
        out.append(note)
    return out


def _first_person_note_text(text: str) -> str:
    out = " ".join(text.split()).strip()
    rewrites = [
        (re.compile(r"^Speaker\s+mentions\s+using\s+(.+?)\s+and\s+finding\s+(.+?)\s+appealing\.?$", re.I), r"I use \1 and find \2 appealing."),
        (re.compile(r"^Speaker\s+(?:describes|frames)\s+(.+?)\s+as\s+", re.I), r"\1 is "),
        (re.compile(r"^Speaker\s+read\s+more\s+about\s+", re.I), "I read more about "),
        (re.compile(r"^Speaker\s+(?:expresses|has)\s+fear\s+that\s+", re.I), "I'm afraid "),
        (re.compile(r"^Speaker\s+(?:initially\s+)?thought\s+", re.I), "I initially thought "),
        (re.compile(r"^Speaker\s+(?:believes|thinks)\s+(?:that\s+)?", re.I), ""),
        (re.compile(r"^Speaker\s+(?:announces|states|says|notes|reports|explains|emphasizes)\s+(?:that\s+)?", re.I), ""),
        (re.compile(r"^Speaker\s+will\s+", re.I), "I will "),
        (re.compile(r"^Speaker\s+wants\s+", re.I), "I want "),
        (re.compile(r"^Speaker\s+needs\s+", re.I), "I need "),
        (re.compile(r"^Speaker\s+plans\s+to\s+", re.I), "I plan to "),
        (re.compile(r"^Speaker\s+asks\s+", re.I), "I ask "),
        (re.compile(r"^Speaker\s+", re.I), ""),
    ]
    for pattern, replacement in rewrites:
        next_out = pattern.sub(replacement, out, count=1).strip()
        if next_out != out:
            out = next_out
            break
    return out[:1].upper() + out[1:] if out else out


def fallback_processed_notes(segments: list[dict], stage_by_id: dict[str, int] | None = None) -> list[dict]:
    stages = stage_by_id or {}
    notes: list[dict] = []
    for index, segment in enumerate(segments):
        note_id = str(segment.get("segment_id") or segment.get("id") or f"segment-{index + 1}").strip()
        text = _first_person_note_text(str(segment.get("text") or ""))
        if not note_id or not text:
            continue
        stage = max(1, min(3, int(segment.get("rewrite_pass") or stages.get(note_id) or 1)))
        note = {
            "id": note_id,
            "speaker": str(segment.get("speaker") or "").strip() or "Speaker",
            "chapter": "Live Transcript",
            "text": text,
            "pass": stage,
            "frozen": stage >= 3,
        }
        ts = segment.get("start")
        if ts is not None:
            note["t"] = ts
        notes.append(note)
    return notes


def meeting_card_turn(
    work: Path, segments: list[dict], *, model: str | None = None,
    card_kinds: list[str] | None = None, steering: str = "",
    polish_rules: str = DEFAULT_POLISH_RULES, tag_rules: str = DEFAULT_TAG_RULES,
    completion=None,
) -> Iterator[dict]:
    """One copilot beat: polish the recent transcript window + emit a proactive card per salient
    finding, via ONE direct ``CompletionPort`` call. A beat is a pure prompt→text turn — everything
    it needs is already in the prompt window, so there is no harness, no subprocess, and no
    workspace memory (the old tool-granted turn let the model explore the KG every beat — the
    dominant per-beat latency; a direct completion also never auto-loads workspace conventions, so
    steering lives EXCLUSIVELY in agents/meeting.md). The wanted ``card_kinds`` + workspace
    ``steering`` shape the prompt, and cards are filtered to the allowed kinds.

    ``completion`` is injectable; by default it resolves through the ``worker.worker``
    ``completion_factory`` seam (env-selected adapter, ``VEXA_LLM_PROVIDER``)."""
    kinds = card_kinds or list(DEFAULT_CARD_KINDS)
    segment_by_id = {str(s.get("segment_id") or s.get("id") or ""): s for s in segments}
    stage_by_id = {seg_id: int(s.get("rewrite_pass") or 1) for seg_id, s in segment_by_id.items()}
    lines = "\n".join(
        f"[pass {int(s.get('rewrite_pass') or 1)}/3 id={s.get('segment_id') or s.get('id') or '?'} "
        f"speaker={s.get('speaker', '?')}] {s.get('text', '')}"
        for s in segments
    )
    prompt = build_card_prompt(lines, kinds, steering, polish_rules=polish_rules, tag_rules=tag_rules)
    # We DON'T forward raw model output as turn events — the JSON reply would leak into the UI as a
    # "note"; the meeting feed wants only the parsed notes/cards.
    try:
        if completion is None:
            import worker.worker as _w
            completion = getattr(_w, "completion_factory", completion_from_env)()
        reply = completion.complete(prompt, model=model).text
    except LLMAuthError as exc:
        # Fail LOUD on a 401/auth mismatch: a distinct auth-error (provider host + the
        # BASE_URL-vs-KEY fix) instead of the opaque generic model-error (WS1b).
        yield auth_error_event(exc, model=model, stage="meeting-card")
        return
    except Exception as exc:
        if looks_like_auth_failure(exc):
            yield auth_error_event(exc, model=model, stage="meeting-card")
            return
        yield model_error_event(exc, model=model, stage="meeting-card")
        return
    notes = parse_notes(reply, stage_by_id, segment_by_id)
    cards = parse_cards(reply, kinds)
    if segments and not notes:
        yield _model_error_event("model response did not include processed transcript notes", model=model, stage="meeting-card")
        notes = fallback_processed_notes(segments, stage_by_id)
    for note in notes:
        yield {"type": "note", "note": note}
    for card in cards:
        yield {"type": "card", "card": card}


# ── Auth-B/#3(a): persist the processed transcript to the workspace, INCREMENTALLY ────────────────
# As the worker emits 1:1 cleaned `proc:meeting` notes, it ALSO upserts a per-meeting workspace file at
# kg/entities/meeting/<row_id>.md so a chat agent focused on the meeting can `Read` it and answer "what's
# the meeting about" — WITHOUT waiting for the post-meeting doc turn. The write is idempotent: each line
# is keyed by its note id (== segment_id) with an HTML-comment marker, so a refining pass UPDATES the
# line in place rather than duplicating it. This reuses the same VISIBLE, git-tracked kg/entities/meeting
# path the post-meeting doc turn authors (the doc turn later distills a summary into the SAME tree).

_PROC_LINE_RE = re.compile(r"^<!-- id:(?P<id>.*?) -->", )
_MEETING_ROW_RE = re.compile(r"^[1-9][0-9]{0,18}$")
_SAFE_NOTE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


def _note_marker_id(value: object) -> str:
    raw = str(value or "").strip()
    if _SAFE_NOTE_ID_RE.fullmatch(raw):
        return raw
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def meeting_artifact_paths(work: Path, row_id: str | int) -> tuple[Path, Path]:
    """Return containment-checked artifact paths for one canonical meetings-domain row.

    Native meeting ids and titles are display data, not storage authority.  Only a positive decimal
    database row may select a file, and an existing symlink must not redirect either artifact outside
    the mounted workspace.
    """
    canonical = str(row_id)
    if not _MEETING_ROW_RE.fullmatch(canonical):
        raise ValueError("meeting row id must be a positive decimal")
    root = Path(work).resolve()
    meeting_dir = root / "kg" / "entities" / "meeting"
    resolved_dir = meeting_dir.resolve(strict=False)
    if resolved_dir != root and root not in resolved_dir.parents:
        raise ValueError("meeting artifact path resolves outside the workspace")
    document = meeting_dir / f"{canonical}.md"
    envelope = meeting_dir / f"{canonical}.envelope.json"
    for candidate in (document, envelope):
        resolved = candidate.resolve(strict=False)
        if resolved != resolved_dir and resolved_dir not in resolved.parents:
            raise ValueError("meeting artifact path resolves outside the workspace")
    return document, envelope


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    """Replace one sensitive artifact atomically with owner-only permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _atomic_write_text(path: Path, content: str) -> None:
    _atomic_write_bytes(path, content.encode("utf-8"))


def write_artifact_if_current(
    work: Path,
    path: Path,
    writer: Callable[[], None],
    authority_current: Callable[[], bool],
) -> bool:
    """Run one local artifact write and roll it back if authority expires in flight.

    Redis and the filesystem cannot share a transaction.  Holding the workspace write lock prevents a
    replacement worker from racing the snapshot/rollback window; checks immediately before and after
    the write close the worker-owned part of the gap.  The caller remains responsible for an atomic
    Redis generation/fence check inside ``authority_current``.
    """
    root = Path(work).resolve()
    target = Path(path)
    resolved = target.resolve(strict=False)
    if resolved != root and root not in resolved.parents:
        raise ValueError("meeting artifact path resolves outside the workspace")
    if target.is_symlink():
        raise ValueError("meeting artifact path resolves outside the workspace")

    def current() -> bool:
        try:
            return bool(authority_current())
        except Exception:  # noqa: BLE001 — authority errors fail closed
            return False

    with workspace_write_lock(root):
        if not current():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        resolved = target.resolve(strict=False)
        if resolved != root and root not in resolved.parents:
            raise ValueError("meeting artifact path resolves outside the workspace")
        existed = target.exists()
        previous = target.read_bytes() if existed else b""
        writer()
        if current():
            return True
        if existed:
            _atomic_write_bytes(target, previous)
        else:
            target.unlink(missing_ok=True)
        return False


def _meeting_frontmatter_text(meta: dict) -> str:
    fm_keys = ("type", "id", "title", "meeting_id", "session_uid", "platform", "date")
    frontmatter = {key: str(meta[key]) for key in fm_keys if meta.get(key) is not None}
    return yaml.safe_dump(
        frontmatter,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    ).rstrip()


def _meeting_transcript_body(notes: list[dict]) -> list[str]:
    speakers: list[str] = []
    for n in notes:
        sp = _structured_inline(n.get("speaker") or "Speaker", limit=200) or "Speaker"
        sp = sp.replace("*", "").replace("`", "")
        if sp not in speakers:
            speakers.append(sp)
    parts = ["## Speakers", ""]
    parts += [f"- {sp}" for sp in speakers] or ["- (none yet)"]
    parts += ["", "## Transcript", ""]
    for n in notes:
        nid = _note_marker_id(n.get("id"))
        sp = _structured_inline(n.get("speaker") or "Speaker", limit=200) or "Speaker"
        sp = sp.replace("*", "").replace("`", "")
        text = _structured_inline(n.get("text"), limit=10_000)
        tags = n.get("tags") or []
        safe_tags = [value for tag in tags if (value := _structured_inline(tag, limit=100))]
        suffix = f"  _[tags: {', '.join(safe_tags)}]_" if safe_tags else ""
        parts.append(f"<!-- id:{nid} --> **{sp}:** {text}{suffix}")
    return parts


def render_meeting_transcript(meta: dict, notes: list[dict]) -> str:
    """Render YAML frontmatter plus one deterministic, id-keyed line per processed note."""
    parts = ["---", _meeting_frontmatter_text(meta), "---", "", *_meeting_transcript_body(notes)]
    return "\n".join(parts) + "\n"


def persist_envelope(path: Path, envelope: dict) -> None:
    """Persist the SERIALIZED ENVELOPE (the same notes/cards shape redis carries) as the durable render
    source — NOT the prose markdown ``render_meeting_transcript`` produces.

    This is the deterministic dual-source seam: live folds the redis stream, finished reads THIS file,
    and one renderer renders both identically. The serialization is DETERMINISTIC (``indent=2``,
    ``sort_keys=True``) so the persisted file is byte-stable and == the folded live stream."""
    _atomic_write_text(path, json.dumps(envelope, indent=2, sort_keys=True))


def _seed_dir() -> Path:
    """The mounted seed template dir (the envelope SSOT schema lives under ``agents/``). Resolves the
    selected template out of the registry root (env override wins)."""
    from shared.seeding import resolve_seed_dir

    return resolve_seed_dir()


def load_meeting_schema(seed_dir: Path) -> dict | None:
    """Load the envelope SSOT schema at ``{seed_dir}/agents/meeting.schema.json``. Tolerant: a missing
    (or unreadable / malformed) file → None, so behavior is unchanged when the schema is absent."""
    path = Path(seed_dir) / "agents" / "meeting.schema.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def validate_envelope(envelope: dict, seed_dir: Path) -> list[str]:
    """Validate ``envelope`` against the SSOT schema; return a list of error messages (empty == valid).
    Tolerant: a missing schema → ``[]`` (nothing to validate against, so behavior is unchanged)."""
    schema = load_meeting_schema(seed_dir)
    if schema is None:
        return []
    import jsonschema

    validator = jsonschema.Draft202012Validator(schema)
    return [e.message for e in sorted(validator.iter_errors(envelope), key=lambda e: list(e.path))]


def upsert_meeting_transcript_file(path: Path, meta: dict, note: dict) -> None:
    """Idempotently UPSERT one cleaned ``note`` (keyed by ``note['id']``) into the per-meeting transcript
    file at ``path``. Reads the current id-keyed lines, replaces the matching id (or appends a new one),
    preserves order, and rewrites the whole file. Re-running with the same note id never duplicates a
    line; a refining note for an existing id overwrites its text."""
    raw_nid = str(note.get("id") or "").strip()
    if not raw_nid:
        return
    nid = _note_marker_id(raw_nid)
    notes = _read_meeting_notes(path)
    replaced = False
    for existing in notes:
        if existing.get("id") == nid:
            existing["speaker"] = note.get("speaker") or existing.get("speaker")
            existing["text"] = note.get("text") or existing.get("text")
            replaced = True
            break
    if not replaced:
        notes.append({"id": nid, "speaker": note.get("speaker") or "Speaker", "text": note.get("text") or ""})
    _atomic_write_text(path, render_meeting_transcript(meta, notes))


def _set_cursor(
    stream: _Stream,
    cursor_key: str | None,
    raw_id: str,
    *,
    fence_key: str | None = None,
    processing_flag_key: str | None = None,
    processing_token: str | None = None,
    processing_expires_at_ms: int | None = None,
) -> bool:
    """Freeze the per-meeting processed CURSOR = the last raw transcript stream-id cleaned. Best-effort:
    a fake stream without ``set`` (or a transient redis error) must never break the live beat."""
    if not cursor_key:
        return True
    if fence_key is not None:
        try:
            if processing_flag_key is not None and processing_token is not None:
                return set_if_writable_and_current(
                    stream,
                    cursor_key,
                    str(raw_id),
                    fence_key=fence_key,
                    flag_key=processing_flag_key,
                    token=processing_token,
                    expires_at_ms=processing_expires_at_ms,
                )
            return set_if_writable(
                stream,
                cursor_key,
                str(raw_id),
                fence_key=fence_key,
                expires_at_ms=processing_expires_at_ms,
            )
        except Exception:  # noqa: BLE001 — retention authority failure is fail-closed
            log.warning("serve_meeting: atomic cursor fence check failed", exc_info=True)
            return False
    setter = getattr(stream, "set", None)
    if setter is None:
        return True
    try:
        setter(cursor_key, str(raw_id))
    except Exception:  # noqa: BLE001 — the cursor is an optimization; never crash the meeting loop
        return True
    return True


def _proc_note(segment: dict) -> dict | None:
    """The baseline 1:1 cleaned note for ONE segment — id == segment_id, the speaker, and the cleaned
    text (same first-person cleanup the fallback uses). None when there's nothing to emit (empty text)."""
    notes = fallback_processed_notes([segment])
    return notes[0] if notes else None


def serve_meeting(
    stream: _Stream, *, transcript_stream: str, out_topic: str,
    card_turn: Callable[[list[dict]], Iterator[dict]], idle_ms: int,
    beat_segments: int = 4,
    doc_turn: Callable[[list[dict]], Iterator[dict]] | None = None,
    enabled: bool = True,
    start_id: str = "0",
    proc_stream: str | None = None,
    cursor_key: str | None = None,
    on_proc_note: Callable[[dict], bool | None] | None = None,
    on_envelope: Callable[[dict], bool | None] | None = None,
    proc_params: dict | None = None,
    processing_flag_key: str | None = None,
    processing_token: str | None = None,
    processing_expires_at_ms: int | None = None,
    deadline_now_ms: Callable[[], int] | None = None,
) -> None:
    """Consume the meeting's ``transcript.v1`` Stream (the meetings⊥agent seam — read by schema), gate
    cheaply (a NEW speaker, or ``beat_segments`` segments), and run a copilot beat that XADDs proactive
    cards to ``out_topic``. Live drafts (``completed:false``) are included and upserted by ``segment_id``
    — a refining draft updates its line in place rather than piling up a duplicate. The transcript keeps
    it non-idle; ``session_end`` (or an idle gap) reaps it.

    Cards surfaced across the meeting are accumulated (de-duped by title) and, on ``session_end``,
    handed to ``doc_turn`` — the post-meeting WRITE turn that authors/updates the kg meeting entity.
    """
    seen_speakers: set[str] = set()
    buffer: list[dict] = []
    processing_window: list[dict] = []
    window_by_id: dict[str, dict] = {}  # segment_id → its window item, so a refining draft upserts
    cards: list[dict] = []          # running, de-duped (by title) list of every surfaced card
    seen_titles: set[str] = set()
    notes: list[dict] = []          # running 1:1 cleaned notes (by id), the envelope render source
    notes_by_id: dict[str, dict] = {}
    last = start_id
    n = 0

    fence_key = None
    if proc_stream:
        fence_key = retention_fence_key(meeting_id_from_proc_stream(proc_stream))

    # Managed processing always carries both values.  Treat a half-configured guard as revoked rather
    # than silently falling back to the legacy unguarded worker path.
    processing_guarded = (
        processing_flag_key is not None or processing_token is not None
    )
    if processing_guarded and not (processing_flag_key and processing_token):
        log.warning("serve_meeting: incomplete processing consent generation; refusing work")
        return
    if processing_guarded:
        try:
            bound_deadline = processing_deadline_from_token(processing_token)
        except ValueError:
            log.warning("serve_meeting: invalid processing deadline generation; refusing work")
            return
        if processing_expires_at_ms is None:
            processing_expires_at_ms = bound_deadline
        elif bound_deadline != processing_expires_at_ms:
            log.warning("serve_meeting: processing deadline does not match generation; refusing work")
            return
    now_ms = deadline_now_ms or (lambda: int(time.time() * 1000))

    def _local_deadline_current() -> bool:
        return (
            processing_expires_at_ms is None
            or now_ms() < processing_expires_at_ms
        )

    def _processed_writable() -> bool:
        if not _local_deadline_current():
            return False
        if fence_key is None:
            return not processing_guarded
        try:
            if processing_guarded:
                return processing_is_current(
                    stream,
                    fence_key=fence_key,
                    flag_key=processing_flag_key,
                    token=processing_token,
                    expires_at_ms=processing_expires_at_ms,
                )
            return carrier_is_writable(
                stream, fence_key, expires_at_ms=processing_expires_at_ms
            )
        except Exception:  # noqa: BLE001 — unavailable authority must not permit derivatives
            log.warning("serve_meeting: retention fence check failed", exc_info=True)
            return False

    def _guarded_xadd(name: str, fields: dict) -> bool:
        if not _local_deadline_current():
            return False
        if fence_key is None:
            stream.xadd(name, fields)
            return True
        try:
            if processing_guarded:
                return xadd_if_writable_and_current(
                    stream,
                    name,
                    fields,
                    fence_key=fence_key,
                    flag_key=processing_flag_key,
                    token=processing_token,
                    expires_at_ms=processing_expires_at_ms,
                )
            return xadd_if_writable(
                stream,
                name,
                fields,
                fence_key=fence_key,
                expires_at_ms=processing_expires_at_ms,
            )
        except Exception:  # noqa: BLE001 — Redis/script failures fail closed
            log.warning("serve_meeting: retention-fenced append failed", exc_info=True)
            return False

    def _persist_envelope() -> bool:
        """DURABLE RENDER SOURCE (deterministic dual-source): persist the SAME running notes/cards the
        worker already tracks as the envelope, alongside (not replacing) the markdown. Best-effort —
        the render-source mirror is an optimization and must never crash the live loop."""
        if on_envelope is None:
            return True
        if not _processed_writable():
            return False
        try:
            if on_envelope({"notes": notes, "cards": cards}) is False:
                return False
        except Exception:  # noqa: BLE001 — render-source mirror is an optimization; never crash the loop
            log.warning("serve_meeting: on_envelope persist failed", exc_info=True)
        return True

    mirror_dirty: dict[str, dict] = {}  # note id → latest merged note, awaiting a mirror flush

    def _emit_proc_note(note: dict) -> bool:
        """XADD ONE cleaned note (id == segment_id) onto the per-meeting processed STREAM — the ONE
        live carrier of cleaned notes (processed-notes.v1; the SSE tails it, the db-writer persists
        it) — and accumulate it into the running envelope + the mirror batch. The workspace-file and
        envelope mirrors are DERIVED views flushed per beat / at session_end (ADR 0027) — writing
        them per note was an O(n²) rewrite in the hot loop."""
        if not note:
            return True
        if proc_stream:
            fields = {"note": json.dumps(note)}
            if proc_params:
                # The processing metadata APPLIED (provider/model/pipeline) rides every entry so the
                # durable consumer (meeting-api db-writer → meeting.data processed views) records
                # reproducible provenance for the view it persists.
                fields["params"] = json.dumps(proc_params)
            if not _guarded_xadd(proc_stream, fields):
                return False
        # Accumulate the SAME 1:1 cleaned note (keyed by id) into the running envelope notes — a refining
        # pass UPDATES its line in place rather than duplicating, exactly like the markdown upsert.
        nid = str(note.get("id") or "").strip()
        if nid:
            existing = notes_by_id.get(nid)
            if existing is None:
                notes_by_id[nid] = note
                notes.append(note)
            else:
                existing.update(note)
            mirror_dirty[nid] = notes_by_id[nid]
        return True

    def _flush_mirror() -> bool:
        """Flush the DERIVED views (per beat + session_end): upsert each dirty note into the
        per-meeting workspace file (Auth-B/#3a — a chat agent can Read the meeting mid-flight) and
        re-persist the envelope render source. Best-effort — mirrors never crash the live loop."""
        if on_proc_note is not None:
            for note in mirror_dirty.values():
                # Re-check for every local write. A callback may block long enough for OFF or the
                # immutable processed-content cutoff to land between two notes in the same batch.
                if not _processed_writable():
                    return False
                try:
                    if on_proc_note(note) is False:
                        return False
                except Exception:  # noqa: BLE001 — the workspace mirror is an optimization
                    log.warning("serve_meeting: on_proc_note upsert failed", exc_info=True)
        if mirror_dirty:
            if not _persist_envelope():
                return False
        mirror_dirty.clear()
        return True

    def _run_beat(segs: list[dict], idx: int) -> bool:
        # Do not send a transcript window to the model once processed retention is already fenced.
        # Output XADDs remain atomically guarded too, covering a fence installed during the call.
        if not _processed_writable():
            return False
        tid = f"beat{idx}"
        staged = [{**seg, "rewrite_pass": int(seg.get("_rewrite_passes", 0)) + 1} for seg in segs]
        events = iter(card_turn(staged))
        while True:
            # A generator advances into the remote model call on ``next``. Check both immediately
            # before that boundary and after it returns so a call crossing OFF/deadline cannot publish.
            if not _processed_writable():
                return False
            try:
                ev = next(events)
            except StopIteration:
                break
            if not _processed_writable():
                return False
            if ev.get("type") == "card":
                _accumulate_card(cards, seen_titles, ev.get("card") or {})
            elif ev.get("type") == "note":
                # The LLM rewrite returned a valid note for this id → UPGRADE the cleaned-stream text
                # (baseline already emitted at ingest; this is the richer pass, still 1:1 by segment_id).
                # Notes ride ONLY processed-notes.v1 — the out-stream is cards + agent activity, per
                # its name (ADR 0027 carrier collapse; the SSE tails the proc stream directly).
                if not _emit_proc_note(ev.get("note") or {}):
                    return False
                continue
            if not _guarded_xadd(
                out_topic, {"event": json.dumps({**ev, "turn_id": tid})}
            ):
                return False
        if not _guarded_xadd(
            out_topic,
            {"event": json.dumps({"type": "turn-complete", "turn_id": tid})},
        ):
            return False
        sent_ids = {id(seg) for seg in segs}
        for seg in processing_window:
            if id(seg) in sent_ids:
                seg["_rewrite_passes"] = int(seg.get("_rewrite_passes", 0)) + 1
        return _flush_mirror()

    def _emit_guarded_turn(turn: Callable[[], Iterator[dict]], tid: str) -> bool:
        if not _processed_writable():
            return False
        events = iter(turn())
        while True:
            if not _processed_writable():
                return False
            try:
                ev = next(events)
            except StopIteration:
                break
            if not _processed_writable():
                return False
            if not _guarded_xadd(
                out_topic, {"event": json.dumps({**ev, "turn_id": tid})}
            ):
                return False
        return _guarded_xadd(
            out_topic,
            {"event": json.dumps({"type": "turn-complete", "turn_id": tid})},
        )

    while True:
        # OFF removes the generation before stopping the runtime workload.  Check before blocking so a
        # stale or race-spawned worker never reads transcript content, then check again after XREAD so a
        # revocation that wakes the blocked read cannot reach the model or any derivative carrier.
        if not _processed_writable():
            return
        resp = stream.xread({transcript_stream: last}, count=50, block=idle_ms)
        if not resp:
            return  # transcript idle/ended → reap
        if not _processed_writable():
            return
        new_speaker = False
        for _name, entries in resp:
            for entry_id, fields in entries:
                last = entry_id
                payload = json.loads(fields.get("payload", "{}"))
                if payload.get("type") == "session_end":
                    mutable = [seg for seg in processing_window if int(seg.get("_rewrite_passes", 0)) < 3]
                    if mutable and enabled:
                        n += 1
                        if not _run_beat(mutable, n):
                            return
                    if not _flush_mirror():  # ingest-only notes reach the mirrors too
                        return
                    if not _persist_envelope():  # final durable render source
                        return
                    # processed-notes.v1 ``view_end`` (ADR 0027): the stream is COMPLETE here — the
                    # final beat has run and every note is emitted. The db-writer flushes the durable
                    # view ON this marker (its bounded deadline covers a worker that dies without one);
                    # the live SSE closes on it. Emitted BEFORE the doc turn (30s+ of LLM the durable
                    # flush must not wait on) and regardless of ``enabled`` (baseline notes flow even
                    # with live beats off — the emit gate is proc_stream, same as _emit_proc_note's).
                    if proc_stream:
                        end_marker: dict = {"type": "view_end"}
                        if last and last != "0":
                            end_marker["cursor"] = str(last)
                        if not _guarded_xadd(proc_stream, end_marker):
                            return
                    if doc_turn is not None:  # post-meeting WRITE: author/update the kg meeting entity
                        # Redis cannot atomically serialize a workspace write. S09 still owns the
                        # authoritative worker stop + workspace/Brain purge; this acknowledgement
                        # prevents known-fenced work from starting and its output is fenced below.
                        if not _processed_writable():
                            return
                        if not _emit_guarded_turn(
                            lambda: doc_turn(cards), "meeting-doc"
                        ):
                            return
                    return  # meeting ended → reap
                for seg_index, seg in enumerate(payload.get("segments", [])):
                    sid = seg.get("segment_id") or f"{entry_id}:{seg_index}"
                    existing = window_by_id.get(sid)
                    if existing is not None:
                        # A live draft refining (or finalizing) a segment already in the window: update
                        # it IN PLACE so the next beat reads the latest text, never a duplicate line.
                        # Identity is preserved, so the rewrite-pass bookkeeping still holds.
                        existing["text"] = seg.get("text", existing.get("text"))
                        existing["completed"] = seg.get("completed", True)
                        continue
                    item = dict(seg)
                    item["segment_id"] = sid
                    item["_rewrite_passes"] = 0
                    window_by_id[sid] = item
                    buffer.append(item)
                    processing_window.append(item)
                    # Baseline 1:1 cleaned note onto the processed stream — ALWAYS present per segment,
                    # so the cleaned channel never has a gap even before/without an LLM upgrade beat.
                    base = _proc_note(item)
                    if base is not None:
                        if not _emit_proc_note(base):
                            return
                    sp = seg.get("speaker")
                    if sp and sp not in seen_speakers:
                        seen_speakers.add(sp)
                        new_speaker = True
                # Advance the per-meeting CURSOR to the last raw stream-id we've now cleaned (gap-fill
                # picks up from here on re-enable; OFF freezes it at the last processed entry).
                if not _set_cursor(
                    stream,
                    cursor_key,
                    entry_id,
                    fence_key=fence_key,
                    processing_flag_key=processing_flag_key,
                    processing_token=processing_token,
                    processing_expires_at_ms=processing_expires_at_ms,
                ):
                    return
        if enabled and buffer and (new_speaker or len(buffer) >= beat_segments):
            n += 1
            mutable = [seg for seg in processing_window if int(seg.get("_rewrite_passes", 0)) < 3]
            if mutable:
                if not _run_beat(mutable, n):
                    return
            processing_window = [seg for seg in processing_window if int(seg.get("_rewrite_passes", 0)) < 3]
            window_by_id = {seg["segment_id"]: seg for seg in processing_window}
            buffer = []


def _accumulate_card(cards: list[dict], seen_titles: set[str], card: dict) -> None:
    """Retain a surfaced card, de-duped by (case-folded) title so re-surfacing across beats doesn't
    duplicate it in the final meeting doc."""
    title = (card.get("title") or "").strip()
    if not title:
        return
    key = title.casefold()
    if key in seen_titles:
        return
    seen_titles.add(key)
    cards.append(card)


def _emit_turn(stream: _Stream, out_topic: str, turn: Callable[[], Iterator[dict]], tid: str) -> None:
    for ev in turn():
        stream.xadd(out_topic, {"event": json.dumps({**ev, "turn_id": tid})})
    stream.xadd(out_topic, {"event": json.dumps({"type": "turn-complete", "turn_id": tid})})


def _emit_beat(stream: _Stream, out_topic: str, card_turn, segments: list[dict], n: int) -> None:
    _emit_turn(stream, out_topic, lambda: card_turn(segments), f"beat{n}")


# ── post-meeting WRITE turn: distill the surfaced cards into the kg meeting entity ────────────────

_CARD_GROUP = {  # card kind → the section it lands under in the doc
    "person": "Attendees",
    "company": "Companies",
    "product": "Products",
    "topic": "Topics",
    "decision": "Decisions",
    "action": "Actions",
}

# Backward import compatibility only. Post-meeting documents are no longer prompts: the worker writes
# them deterministically below, so raw native ids/titles/cards never reach a tool-capable harness.
MEETING_DOC_PROMPT = None


def _structured_inline(value: object, *, limit: int) -> str:
    """Flatten untrusted display text into one bounded, HTML-inert markdown line."""
    text = " ".join(str(value or "").split())[:limit]
    return (
        text.replace("\\", "/")
        .replace("[", "(")
        .replace("]", ")")
        .replace("`", "'")
        .replace("*", "")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _wikilink_title(value: object) -> str:
    return (
        _structured_inline(value, limit=200)
        .replace("[", "(")
        .replace("]", ")")
        .replace("|", "-")
    )


def _read_meeting_notes(path: Path) -> list[dict]:
    notes: list[dict] = []
    if not path.exists():
        return notes
    try:
        for line in path.read_text().splitlines():
            match = _PROC_LINE_RE.match(line)
            if not match:
                continue
            rest = line[match.end():].strip()
            speaker = "Speaker"
            body = rest
            if rest.startswith("**") and ":**" in rest:
                speaker = rest[2:rest.index(":**")].strip() or "Speaker"
                body = rest[rest.index(":**") + 3:].strip()
            notes.append({"id": match.group("id"), "speaker": speaker, "text": body})
    except OSError:
        return []
    return notes


def render_meeting_document(meta: dict, notes: list[dict], cards: list[dict]) -> str:
    """Render a deterministic final document from already-structured notes/cards."""
    groups: dict[str, list[tuple[str, str]]] = {}
    summary_lines: list[str] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        heading = _CARD_GROUP.get(str(card.get("kind") or "").strip().lower())
        title = _wikilink_title(card.get("title"))
        if not heading or not title:
            continue
        body = _structured_inline(card.get("body"), limit=500)
        groups.setdefault(heading, []).append((title, body))
        if len(summary_lines) < 4:
            summary_lines.append(f"{title}: {body}" if body else title)

    if not summary_lines:
        for note in notes[:4]:
            text = _structured_inline(note.get("text"), limit=500)
            if text:
                summary_lines.append(text)
    if not summary_lines:
        summary_lines.append("No processed meeting notes were retained.")

    parts = ["---", _meeting_frontmatter_text(meta), "---", "", "## Summary", ""]
    parts.extend(line if line.endswith((".", "!", "?")) else f"{line}." for line in summary_lines)
    for heading in ("Attendees", "Companies", "Products", "Topics", "Decisions", "Actions"):
        entries = groups.get(heading)
        if not entries:
            continue
        parts.extend(["", f"## {heading}", ""])
        for title, body in entries:
            suffix = f" — {body}" if body else ""
            parts.append(f"- [[{title}]]{suffix}")
    parts.extend(["", *_meeting_transcript_body(notes)])
    return "\n".join(parts).rstrip() + "\n"


def meeting_doc_turn(
    work: Path,
    cards: list[dict],
    *,
    row_id: str | int,
    platform: str,
    date: str,
    authority_current: Callable[[], bool] | None = None,
) -> Iterator[dict]:
    """Write one row-scoped final meeting document without invoking a model or tools."""
    document, _envelope = meeting_artifact_paths(work, row_id)
    canonical = str(row_id)
    meta = {
        "type": "meeting",
        "id": canonical,
        "title": f"Meeting {canonical}",
        "meeting_id": canonical,
        "session_uid": canonical,
        "platform": str(platform),
        "date": str(date),
    }
    notes = _read_meeting_notes(document)
    writer = lambda: _atomic_write_text(document, render_meeting_document(meta, notes, cards))
    if authority_current is not None:
        if not write_artifact_if_current(work, document, writer, authority_current):
            return
    else:
        writer()
    yield {"type": "message-delta", "text": f"Updated meeting {canonical}."}
