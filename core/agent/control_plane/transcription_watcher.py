"""transcription_watcher.py — the agent's IN-PROCESS inbound watch (trigger → arm) over the live transcript.

It runs ONE daemon thread, ARM (``_run_arm``): tail ``transcription_segments`` purely as a TRIGGER to do
the jobs only the agent-api can do — key the copilot on the meetings-domain numeric ROW id per meeting,
REGISTER the live meeting, RE-ARM the copilot dispatch while the user has processing enabled (spawn-or-touch,
idempotent), and on ``session_end`` reap the copilot + connect the meeting's kg doc.

Before registration or dispatch, every numeric row is resolved to its canonical database owner through
meeting-api's secret-protected internal owner edge. Missing, unreachable, or malformed authority fails
closed; bot/caller owner hints are ignored and there is no shared production subject fallback.

P0 (cross-tenant leak fix): the transcript CARRIER + ``:on`` + ``:cursor`` + dispatch keys are the numeric
ROW id ``mid`` (unique per (user, platform, native, run)), NOT the native Meet code (which collides across
DIFFERENT users AND across ONE user's re-sends — keying transcript data by it leaked one user's transcript
to another). The native code is resolved best-effort for DISPLAY only (the kg doc/title + the ``native_id``
field); a resolution miss no longer diverges the carrier key.

It does NOT write the transcript carrier. The MEETINGS domain (meeting-api's collector) is the SINGLE
writer of the per-meeting feed ``tc:meeting:{row_id}`` and its ``session_end`` marker (P23) — the agent only
CONSUMES it. This loop is also the ONE dispatch arbiter for copilot processing (ADR 0027): /api/meeting/
process writes the desired-state flag only; the arm here resumes from the worker-advanced cursor
(``proc:meeting:{row_id}:cursor``, else 0-0) and relies on runtime.v1's idempotent create (a running
copilot is touched, never respawned). `meetings ⊥ agent` (P3): the agent re-derives nothing. ``keymap``
(numeric meeting_id → row-id routing key) is the arm thread's own state.

No extra container, no HTTP hop: it holds the Dispatcher directly.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request

from shared import units
from shared.http import open_no_redirect, read_json_bounded
from shared.meeting_retention import (
    claim_processing_if_writable,
    processing_deadline_from_token,
)

logger = logging.getLogger("agent_api.tx_watch")

SRC = "transcription_segments"           # the wire every bot publishes to (configurable upstream)
GROUP = "agent_copilot"                  # our consumer group — independent of the collector's
REARM_SEC = 30.0                         # re-touch a meeting's dispatch at most this often (keep-alive)
# Rolling TTL the arm block refreshes on the ``proc:meeting:{row}:on`` flag while segments flow —
# the flag's REAL end-of-life (the wire carries no session_end on the stop path, so the reap branch
# is belt-only). Refresh cadence == REARM_SEC, so anything ≥ a few minutes is safe; an hour also
# rides out long mid-meeting silences without silently disarming an active toggle.
PROC_FLAG_ROLLING_TTL_SEC = 3600
_BRIEF = (
    "You are the live meeting copilot. Watch the meeting transcript as it streams in and surface the "
    "people, companies, products, and projects worth tagging."
)
_PLATFORM = {"google_meet": "Google Meet", "teams": "Microsoft Teams", "zoom": "Zoom", "jitsi": "Jitsi Meet"}
_native: dict[str, tuple[str, str]] = {}  # numeric meeting_id → (native_meeting_id, platform), cached
# Only the meeting_id whose row we actually matched is cached above. A MISS is NOT cached (so it is
# retried on the next segment — the new meeting's row may not be visible in the gateway list yet),
# but we throttle the refetch per meeting_id so a quiet miss doesn't hammer the gateway every segment.
_resolve_miss_at: dict[str, float] = {}  # numeric meeting_id → last failed-resolve (monotonic)
RESOLVE_RETRY_SEC = 3.0
# The gateway/meeting-api caps `limit` at 100 (>100 → HTTP 422 Unprocessable Entity). Asking for more
# made EVERY resolve fail, so _resolve_native always returned None. Post-P0 the carrier no longer
# depends on this resolve (it keys on the row id `mid`, always present) — a miss now degrades only
# the human-readable native DISPLAY, never the transcript itself. Keep at/under the cap. (Pagination
# isn't needed: live meetings are always among the newest rows, which the gateway returns first.)
MEETINGS_LIST_LIMIT = 100

# ── P18 (ADR 0010) — fail loud & attributable: the relay's observable health ─────────────────────────
# The transcript relay used to fail SILENTLY: a stale VEXA_BOT_API_KEY made GET /meetings 401, native
# resolution failed, segments fell back to the numeric key, and the copilot's native feed stayed empty —
# logged once as "native-id resolve failed" then retried quietly forever. P18: a dependency failure is a
# TYPED fault surfaced on an OBSERVABLE channel, and "absence of an expected signal is itself a reportable
# state." `relay_health()` is that channel (read by /api/meeting/relay-health → the control panel).
_relay_health: dict = {
    "owner_resolve": {"ok": True, "kind": None, "detail": None, "at": None, "misses": 0},
    "native_resolve": {"ok": True, "kind": None, "detail": None, "at": None, "misses": 0},
    "ingest": {"ok": True, "last_segment_at": None, "segments": 0},
}
_HEALTH_LOCK = threading.Lock()


def relay_health() -> dict:
    """A cheap snapshot of the transcript relay's health (P18 observable). True == flowing."""
    with _HEALTH_LOCK:
        return {k: dict(v) for k, v in _relay_health.items()}


def _classify_http(status: int) -> str:
    if status in (401, 403):
        return "unauthorized"
    if status == 402:
        return "payment_required"
    if status == 429:
        return "rate_limited"
    if status == 422:
        return "bad_request"
    if status >= 500:
        return "unavailable"
    return "error"


def _report_fault(stage: str, kind: str, detail: str) -> None:
    """Fail LOUD + attributed (P18). Record the typed fault and log at ERROR with an ESCALATING throttle
    (scream the first couple, then keep visible without flooding every 3s)."""
    with _HEALTH_LOCK:
        h = _relay_health.setdefault(stage, {"ok": True, "kind": None, "detail": None, "at": None, "misses": 0})
        h.update(ok=False, kind=kind, detail=detail, at=time.time(), misses=int(h.get("misses", 0)) + 1)
        n = h["misses"]
    if n <= 2 or n % 30 == 0:
        logger.error("RELAY FAULT [%s] %s — %s (occurrence #%d)", stage, kind, detail, n)


def _clear_fault(stage: str) -> None:
    """Mark a stage healthy again (loud once on recovery)."""
    with _HEALTH_LOCK:
        h = _relay_health.get(stage)
        recovered = bool(h and not h.get("ok", True))
        misses = int(h.get("misses", 0)) if h else 0
        _relay_health[stage] = {"ok": True, "kind": None, "detail": None, "at": time.time(), "misses": 0}
    if recovered:
        logger.info("RELAY RECOVERED [%s] after %d failure(s)", stage, misses)


def _title(platform: str, native: str) -> str:
    return f"{_PLATFORM.get(platform, platform)} · {native}"


OWNER_RESPONSE_MAX_BYTES = 4096
DOC_LINK_RESPONSE_MAX_BYTES = 8192
MAX_MEETING_ROW_ID = 2**63 - 1


def _canonical_meeting_id(value) -> "str | None":
    raw = str(value or "")
    if not raw.isdigit() or raw.startswith("0") or len(raw) > 19:
        return None
    return raw if int(raw) <= MAX_MEETING_ROW_ID else None


def _validated_owner_record(record, expected_mid: int) -> "dict | None":
    expected = str(expected_mid)
    if (
        not isinstance(record, dict)
        or set(record) != {"meeting_id", "user_id"}
        or not isinstance(record.get("meeting_id"), str)
        or _canonical_meeting_id(record["meeting_id"]) != expected
        or not isinstance(record.get("user_id"), str)
        or _canonical_meeting_id(record["user_id"]) is None
    ):
        return None
    return {"meeting_id": expected, "user_id": record["user_id"]}


def _http_owner_lookup(meeting_api_url: str, internal_secret: str):
    """Build the trusted row-owner lookup used by the production watcher.

    The edge is deliberately narrower than the user-facing meeting read: one exact numeric row in,
    ``{meeting_id, user_id}`` out.  The internal credential never follows a redirect, responses are
    bounded before JSON parsing, and any missing/malformed/dependency state fails closed.
    """
    base = (meeting_api_url or "").rstrip("/")
    secret = internal_secret or ""

    def _lookup(meeting_id: str) -> "dict | None":
        raw_mid = _canonical_meeting_id(meeting_id)
        if not base or not secret or raw_mid is None:
            return None
        mid = int(raw_mid)
        try:
            request = urllib.request.Request(
                f"{base}/internal/meetings/{mid}/owner",
                headers={"X-Internal-Secret": secret},
            )
            with open_no_redirect(request, timeout=5) as response:
                if response.status != 200:
                    _report_fault("owner_resolve", _classify_http(response.status),
                                  f"HTTP {response.status}")
                    return None
                record = read_json_bounded(response, max_bytes=OWNER_RESPONSE_MAX_BYTES)
        except urllib.error.HTTPError as error:
            _report_fault("owner_resolve", _classify_http(error.code), f"HTTP {error.code}")
            return None
        except Exception as error:  # noqa: BLE001 — dependency/parse faults deny attribution
            _report_fault("owner_resolve", "unavailable", type(error).__name__)
            return None

        record = _validated_owner_record(record, mid)
        if record is None:
            _report_fault("owner_resolve", "bad_response", "invalid owner response")
            return None
        _clear_fault("owner_resolve")
        return record

    return _lookup


def _resolve_native(meeting_id: str) -> "tuple[str, str] | None":
    """Map the bot's NUMERIC meeting_id → its native Meet code (e.g. nba-agyz-gbe) via the gateway, so
    the wire/dispatch/feed key on ONE id per physical meeting (re-launches dedupe to one entry) — and the
    terminal can stop the bot by its native id.

    Cache discipline (the multi-meeting-collapse fix): we cache ONLY the exact meeting_id→native pair we
    matched, and we ONLY return the native for THIS meeting_id (never the first/any row in the list). A
    miss is left UNCACHED so it retries (the just-launched meeting's row can lag the gateway list by a
    beat), but throttled so a genuinely-unknown id doesn't refetch on every segment."""
    if meeting_id in _native:
        return _native[meeting_id]
    now = time.monotonic()
    if now - _resolve_miss_at.get(meeting_id, 0.0) < RESOLVE_RETRY_SEC:
        return None  # recently failed — don't refetch yet (caller keys on numeric id meanwhile)
    key = os.environ.get("VEXA_BOT_API_KEY", "")
    if not key:
        _report_fault("native_resolve", "unauthorized",
                      "VEXA_BOT_API_KEY not set — cannot resolve numeric→native meeting id")
        _resolve_miss_at[meeting_id] = now
        return None
    gw = os.environ.get("VEXA_GATEWAY_URL", "http://gateway:8000").rstrip("/")
    try:
        req = urllib.request.Request(
            gw + f"/meetings?limit={MEETINGS_LIST_LIMIT}", headers={"X-API-Key": key})
        with open_no_redirect(req, timeout=5) as resp:
            data = read_json_bounded(resp)
        items = data if isinstance(data, list) else (
            (data.get("meetings") or data.get("items") or []) if isinstance(data, dict) else None
        )
        if (not isinstance(items, list) or len(items) > MEETINGS_LIST_LIMIT
                or any(not isinstance(item, dict) for item in items)):
            raise ValueError("invalid gateway meetings response")
        resolved: dict[str, tuple[str, str]] = {}
        for mt in items:
            raw_mid = mt.get("id") or mt.get("meeting_id") or ""
            nat = mt.get("native_meeting_id") or mt.get("native_id") or mt.get("platform_specific_id")
            platform = mt.get("platform") or "google_meet"
            if (raw_mid and (
                not isinstance(raw_mid, (str, int))
                or isinstance(raw_mid, bool)
                or len(str(raw_mid)) > 128
            )) or (nat and (not isinstance(nat, str) or len(nat) > 512)) or (
                not isinstance(platform, str) or len(platform) > 64
            ):
                raise ValueError("invalid gateway meetings response")
            mid = str(raw_mid)
            if mid and nat:
                resolved[mid] = (nat, platform)
        _native.update(resolved)
    except urllib.error.HTTPError as e:
        # P18: a TYPED, ATTRIBUTED fault — not a swallowed "best-effort" miss. 401/403 almost always means
        # the bot key is stale/invalid (e.g. after a DB wipe), which is exactly the 90-minute mystery.
        kind = _classify_http(e.code)
        hint = " — VEXA_BOT_API_KEY is stale/invalid for this stack" if kind == "unauthorized" else ""
        _report_fault("native_resolve", kind, f"GET {gw}/meetings → HTTP {e.code}{hint}")
        _resolve_miss_at[meeting_id] = now
        return None
    except Exception as e:  # noqa: BLE001 — network/parse fault: still surface it, never swallow
        _report_fault("native_resolve", "unavailable",
                      f"GET {gw}/meetings failed: {type(e).__name__}: {e}")
        _resolve_miss_at[meeting_id] = now
        return None
    hit = _native.get(meeting_id)
    if hit is None:
        _resolve_miss_at[meeting_id] = now  # our id wasn't in the list yet — retry shortly (not a fault)
    else:
        _clear_fault("native_resolve")      # reachable + resolved → relay healthy again
    return hit


def _validated_doc_link_response(record, expected_mid: int) -> bool:
    if not isinstance(record, dict) or set(record) != {"meeting_id", "doc"}:
        return False
    if (
        not isinstance(record.get("meeting_id"), str)
        or _canonical_meeting_id(record["meeting_id"]) != str(expected_mid)
    ):
        return False
    doc = record.get("doc")
    if not isinstance(doc, dict) or set(doc) != {"workspace", "path", "title", "kind"}:
        return False
    workspace = doc.get("workspace")
    expected_title = f"Meeting {expected_mid}"
    return (
        isinstance(workspace, str)
        and _canonical_meeting_id(workspace) is not None
        and doc.get("title") == expected_title
        and doc.get("kind") == "meeting"
        and doc.get("path") == f"kg/entities/meeting/{expected_mid}.md"
    )


def _record_meeting_doc(meeting_id: str) -> bool:
    """Best-effort exact-row doc link over meeting-api's secret-protected internal edge.

    Meeting-api derives the owner workspace and native-id path from the locked numeric row. The
    watcher sends neither a global user API key nor a native/owner carrier. Redirects are refused,
    the response is bounded + shape-checked, and every failure is contained so session reaping
    cannot crash.
    """
    raw_mid = _canonical_meeting_id(meeting_id)
    base = os.environ.get("VEXA_MEETING_API_URL", "http://meeting-api:8080").rstrip("/")
    secret = os.environ.get("VEXA_INTERNAL_API_SECRET", "")
    if raw_mid is None or not base or not secret:
        return False
    mid = int(raw_mid)
    try:
        req = urllib.request.Request(
            f"{base}/internal/meetings/{mid}/docs",
            data=b"",
            method="POST",
            headers={"X-Internal-Secret": secret},
        )
        with open_no_redirect(req, timeout=5) as response:
            if response.status != 200:
                return False
            record = read_json_bounded(response, max_bytes=DOC_LINK_RESPONSE_MAX_BYTES)
        if not _validated_doc_link_response(record, mid):
            logger.error("connect meeting doc ref returned an invalid response for row %s", mid)
            return False
        return True
    except Exception as error:  # noqa: BLE001 — doc linking is best-effort; never crash the watcher
        logger.error("connect meeting doc ref failed for row %s: %s", mid, type(error).__name__)
        return False


def _resume_cursor(r, key: str) -> str:
    """Where the copilot resumes in ``tc:meeting:{key}``: the per-meeting cursor the worker advances
    as it cleans (``proc:meeting:{key}:cursor``), else ``0-0`` (never processed ⇒ full history).
    ADR 0027: this is the ONE resume source — arming from the stream TAIL here used to race the
    /process toggle's cursor-armed dispatch, and a tail-armed win silently skipped the backfill."""
    try:
        cursor = r.get(f"proc:meeting:{key}:cursor")
    except Exception:  # noqa: BLE001 — cursoring is best-effort; an empty cursor is still valid
        logger.exception("resume-cursor lookup failed for %s", key)
        cursor = None
    return str(cursor) if cursor else "0-0"


def start(redis_url: str, dispatcher, live, *, owner_lookup) -> threading.Thread:
    """Spawn the ARM daemon with a mandatory authoritative row-owner lookup.

    ``keymap`` and the verified row-owner cache are thread-local and cleared at ``session_end``.  There
    is intentionally no default subject: production cannot register or dispatch a meeting until its
    numeric row has been bound to the owner returned by meeting-api.
    """
    keymap: dict[str, str] = {}
    t = threading.Thread(
        target=_run_arm, args=(redis_url, dispatcher, live, owner_lookup, keymap),
        daemon=True, name="tx-watch",
    )
    t.start()
    return t


def _run_arm(redis_url: str, dispatcher, live, owner_lookup, keymap: dict) -> None:
    """Inbound watch → key on the row id, register live, re-arm copilot, reap on session_end. Does NOT
    write the transcript carrier — meeting-api's collector owns ``tc:meeting:{row_id}`` (P23/P0)."""
    import redis as redislib

    r = redislib.from_url(redis_url, decode_responses=True, socket_keepalive=True, health_check_interval=10)
    # id="$": only segments produced AFTER we start — never replay prior/ended meetings on (re)start.
    try:
        r.xgroup_create(SRC, GROUP, id="$", mkstream=True)
    except redislib.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
    # row key → (last spawn-or-touch monotonic time, opaque consent generation).  A new ON token
    # bypasses the keep-alive throttle so re-enabling does not wait up to REARM_SEC, while an old
    # worker remains permanently tied to its prior token.
    last_arm: dict[str, tuple[float, str]] = {}
    first_seen: dict[str, float] = {}   # numeric meeting_id → first segment time (resolve-grace window)
    owner_cache: dict[str, str] = {}    # exact verified row id → immutable owner subject
    logger.info("transcription watcher up — consuming %s (group=%s)", SRC, GROUP)

    while True:
        try:
            resp = r.xreadgroup(GROUP, "agent-api", {SRC: ">"}, count=50, block=5000)
        except (redislib.exceptions.TimeoutError, redislib.exceptions.ConnectionError):
            continue
        except Exception:  # noqa: BLE001 — a watcher must never die on a bad frame
            logger.exception("xreadgroup failed; retrying")
            time.sleep(1)
            continue
        for _stream, entries in resp or []:
            for msg_id, fields in entries:
                try:
                    r.xack(SRC, GROUP, msg_id)
                    _handle(
                        r,
                        dispatcher,
                        live,
                        owner_lookup,
                        json.loads(fields.get("payload") or "{}"),
                        last_arm,
                        keymap,
                        first_seen,
                        owner_cache,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception("bad transcription frame; skipping")


RESOLVE_GRACE_SEC = 6.0  # how long to wait for a native id before falling back to the numeric key


def _verified_owner(meeting_id: str, owner_lookup, owner_cache: dict[str, str]) -> "str | None":
    """Resolve and cache only an exact, minimal meeting-api owner record."""
    cached = owner_cache.get(meeting_id)
    if cached is not None:
        return cached
    try:
        record = owner_lookup(meeting_id)
    except Exception as error:  # noqa: BLE001 — owner authority uncertainty always denies processing
        _report_fault("owner_resolve", "unavailable", type(error).__name__)
        return None
    expected_mid = int(meeting_id)
    record = _validated_owner_record(record, expected_mid)
    if record is None:
        _report_fault("owner_resolve", "bad_response", "invalid owner response")
        return None
    subject = str(record["user_id"])
    owner_cache[meeting_id] = subject
    _clear_fault("owner_resolve")
    return subject


def _handle(r, dispatcher, live, owner_lookup, p, last_arm, keymap, first_seen, owner_cache) -> None:
    # P0 (cross-tenant leak fix): the TRANSCRIPT CARRIER + :on + :cursor + dispatch keys are the numeric
    # ROW id `mid` — NOT the native Meet code. The native id is NOT unique (it collides across DIFFERENT
    # users and across ONE user's re-sends of the same link), so keying transcript data by it leaked one
    # user's transcript to another and hydrated the wrong row. The bot stamps a NUMERIC meeting_id (the
    # meetings-domain row id, unique per run) on every segment, so we can key on it IMMEDIATELY — no
    # resolve-grace wait, no gateway round-trip on the hot path.
    #
    # The native code is still resolved (best-effort) but ONLY for DISPLAY: the human-readable title
    # and the `native_id` field on the live entry / meeting_ref. The doc-link edge uses the exact row
    # id and derives its native/owner server-side. A resolution
    # miss no longer diverges the carrier key (that is `mid`, always present) — it only degrades display,
    # so the P18 relay-health fault is still reported (display only) but the transcript never leaks/starves.
    mid = _canonical_meeting_id(p.get("meeting_id"))
    if mid is None:
        return
    subject = _verified_owner(mid, owner_lookup, owner_cache)
    if subject is None:
        return
    # PREFER the native id stamped on the segment by its producer (the bot knows it from its invocation).
    # The gateway lookup is only a labeled fallback for older bots that don't stamp it — and now purely a
    # DISPLAY concern (the carrier keys on `mid` regardless).
    stamped = p.get("native_meeting_id") or p.get("native_id")
    if stamped:
        resolved = (str(stamped), p.get("platform") or "google_meet")
    else:
        resolved = _resolve_native(mid)
    native, platform = resolved if resolved else (mid, p.get("platform") or "google_meet")
    if resolved is None and p.get("type") != "session_end":
        # DISPLAY-only divergence: the copilot/terminal still key transcript data on the row id `mid`
        # (correct + isolated) — only the human-readable native code/title is unavailable until the
        # gateway row surfaces. Report it (P18) but do NOT hold or fork the meeting.
        _report_fault("native_resolve", "unresolved_display",
                      f"meeting {mid}: native id not resolved yet — transcript keyed on row id "
                      f"tc:meeting:{mid} (correct); the human-readable native code/title is pending")
    # The routing key is the numeric ROW id, frozen once per meeting_id (mid is stable, so this is
    # trivially stable — kept for structural parity with the reap path below).
    key = keymap.get(mid)
    if key is None:
        key = keymap[mid] = mid
    kind = p.get("type")
    if kind == "transcription":  # P18 liveness: record that segments ARE arriving (distinct from relayed)
        with _HEALTH_LOCK:
            ing = _relay_health["ingest"]
            ing["last_segment_at"] = time.time()
            ing["segments"] = int(ing.get("segments", 0)) + 1
    out_stream = f"tc:meeting:{key}"
    if kind == "session_end":
        # The collector emits the session_end MARKER onto tc:meeting:{row_id} (P23/P0, single writer); the
        # agent only does its OWN reaping here — drop the live row (by the row-id key we registered it
        # under), clear keymap, reap the processing DESIRED STATE (the meeting is over — a stale `:on`
        # flag would re-arm a copilot for a dead meeting and litter redis; ADR 0027 makes this watcher
        # the flag's end-of-life owner), connect the kg doc (native, for display).
        live.drop(key)
        last_arm.pop(key, None)
        keymap.pop(mid, None)
        first_seen.pop(mid, None)
        owner_cache.pop(mid, None)
        try:
            r.delete(f"proc:meeting:{key}:on")
        except Exception as error:  # noqa: BLE001 — best-effort; a leftover flag only wastes a re-arm attempt
            logger.warning(
                "processing-flag reap failed (error_type=%s)",
                type(error).__name__,
            )
        logger.info("meeting %s ended → reaping copilot", key)
        # Connect this meeting's own kg doc (authored by the §4 worker on session_end) to the exact
        # row. Meeting-api derives owner/native under its row lock; no global user key participates.
        _record_meeting_doc(mid)
        return
    if kind != "transcription":
        return

    # Keep the terminal's live feed fresh on EVERY batch (a cheap dict write) so an agent-api restart
    # can't drop the meeting from the list — it reappears on the first segment. Throttle only the spawn.
    # session_uid == the ROW id `mid` too, so the copilot out-stream (unit:agent-meet-{mid}) and the
    # transcript carrier (tc:meeting:{mid}) agree — the terminal SSE reads both by the same id.
    live.add({
        "meeting_id": key, "session_uid": key, "native_id": native, "platform": platform,
        "title": _title(platform, native), "unit_id": f"agent-meet-{key}",
        # The meetings-domain ROW id (unique per meeting run). Now the ROUTING key itself — carried
        # explicitly so /api/meeting/process keys the SAME copilot dispatch by it, and the worker writes
        # proc:meeting:{row_id} which the meeting-api db-writer persists into the meeting row's data JSONB.
        "numeric_meeting_id": mid if mid.isdigit() else None,
    })
    # Processing is OPT-IN per meeting: only arm / keep-alive the copilot while the user has enabled it
    # (the terminal sets ``proc:meeting:{row_id}:on`` via /api/meeting/process — DESIRED STATE only;
    # ADR 0027 makes this loop the ONE dispatch arbiter). Default OFF → no copilot → no processing;
    # the RAW transcript still flows through the collector-owned feed above.
    now = time.monotonic()
    # The opt-in flag is ``proc:meeting:{key}:on`` — a DISTINCT key from the processed-notes stream
    # ``proc:meeting:{key}`` (a GET on that stream raises WRONGTYPE and would crash this arm loop).
    flag_key = f"proc:meeting:{key}:on"
    cursor_key = f"proc:meeting:{key}:cursor"
    try:
        desired_token = r.get(flag_key)
    except Exception as error:  # noqa: BLE001 — consent authority failure must not arm a worker
        logger.warning(
            "processing desired-state lookup failed (error_type=%s)",
            type(error).__name__,
        )
        return
    prior = last_arm.get(key)
    prior_at = prior[0] if isinstance(prior, tuple) else float(prior or 0.0)
    prior_token = prior[1] if isinstance(prior, tuple) else None
    if desired_token and (
        str(desired_token) != prior_token or now - prior_at > REARM_SEC
    ):
        desired_token = str(desired_token)
        try:
            expires_at_ms = processing_deadline_from_token(desired_token)
            claimed, cursor = claim_processing_if_writable(
                r,
                key,
                flag_key=flag_key,
                cursor_key=cursor_key,
                ttl_seconds=PROC_FLAG_ROLLING_TTL_SEC,
                token=desired_token,
                expires_at_ms=expires_at_ms,
            )
        except Exception as error:  # noqa: BLE001 — retention authority failure is fail-closed
            logger.warning(
                "processing retention claim failed (error_type=%s)",
                type(error).__name__,
            )
            return
        if claimed:
            # Close claim→dispatch against OFF/re-ON.  The worker also validates this exact token before
            # reading or writing, so a delete after this check still produces a harmless stale worker.
            try:
                confirmed_token = r.get(flag_key)
            except Exception as error:  # noqa: BLE001
                logger.warning(
                    "processing generation confirmation failed (error_type=%s)",
                    type(error).__name__,
                )
                return
            if not confirmed_token or str(confirmed_token) != desired_token:
                return
            confirmed_token = str(confirmed_token)
            last_arm[key] = (now, confirmed_token)
            _arm(
                dispatcher,
                subject,
                key,
                platform,
                transcript_start_id=cursor or "0-0",
                numeric_meeting_id=mid if mid.isdigit() else None,
                native_id=native,
                processing_token=confirmed_token,
                processing_expires_at_ms=expires_at_ms,
            )


def _arm(dispatcher, subject: str, key: str, platform: str, *, transcript_start_id: str = "0-0",
         numeric_meeting_id: str | None = None, native_id: str | None = None,
         processing_token: str | None = None,
         processing_expires_at_ms: int | None = None) -> None:
    """Spawn-or-touch the meeting's copilot (keyed agent-meet-{key}, where key is the ROW id). Idempotent
    FOR REAL since ADR 0027: runtime.v1 create touches a running workload (returns its live status) and
    only spawns one that is absent/exited — before that, every re-arm force-replaced the live container
    (the copilot-churn defect). The live-feed registration happens in _handle every batch. ``native_id``
    is carried for DISPLAY only (the worker names the kg doc/title by the human-readable native code,
    while the transcript/proc/cursor keys stay the row id)."""
    meeting_ref: dict = {
        "meeting_id": key, "session_uid": key, "platform": platform,
        "transcript_start_id": transcript_start_id,
    }
    if native_id:
        # DISPLAY only: the worker names kg/entities/meeting/{native}.md + the title by this human-readable
        # code (e.g. wfn-gzwz-kwt), never the numeric row id. An internal hint — stripped before the
        # unit.v1 check. The transcript carrier / proc / cursor keys are all the ROW id (key).
        meeting_ref["native_id"] = str(native_id)
    if numeric_meeting_id:
        # The meetings-domain row id → the worker keys its processed-notes stream by it
        # (proc:meeting:{numeric}) so a re-sent bot on the same native link never mixes/clobbers a
        # previous meeting's processed doc. An internal hint — stripped before the unit.v1 check.
        meeting_ref["numeric_meeting_id"] = str(numeric_meeting_id)
    if processing_token:
        meeting_ref["processing_token"] = str(processing_token)
    if processing_expires_at_ms is not None:
        meeting_ref["processing_expires_at_ms"] = processing_expires_at_ms
    inv = units.make_dispatch(
        subject=subject, trigger="transcription",
        start=units.entrypoint(inline=_BRIEF),
        context={"kind": "meeting", "meeting": meeting_ref},
    )
    try:
        dispatcher.dispatch(inv)  # idempotent: spawns if reaped, touches if running
    except Exception:  # noqa: BLE001
        logger.exception("dispatch failed for meeting %s", key)
