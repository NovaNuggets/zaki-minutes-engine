"""``create_app(...) -> FastAPI`` — the ONE uvicorn-able meeting-api modular monolith (P2).

This is the unified meeting-api: ONE FastAPI app composed of front-doored modules, each a
sub-package of ``meeting_api`` mounted here (the v0.12 analog of the parent ``main.py``'s flat
``app.include_router(...)`` list, but each module is an isolated brick behind a port-seam):

  * **lifecycle** — the bot lifecycle callback receiver + meeting-state FSM (lifecycle.v1):
    POST ``/bots/internal/callback/lifecycle``.
  * **bot_spawn** — POST ``/bots``: build the invocation.v1 invocation + mint the MeetingToken +
    spawn the meeting-bot over runtime.v1, eager-creating the MeetingSession on spawn.
  * **collector** — the folded-in transcript backend (collector domain):
    GET ``/transcripts/{platform}/{native_meeting_id}``, GET ``/meetings``,
    POST ``/ws/authorize-subscribe`` (+ the ``transcription_segments`` → ``tc:…:mutable`` consumer).
  * **recordings** — POST ``/internal/recordings/upload``, GET ``/recordings``,
    GET ``/recordings/{id}/master`` (chunks + master → ``meeting.data`` JSONB).
  * **obs** — ``TraceMiddleware`` (logevent.v1 trace_id threading) + the shared ``GET /health``.

webhooks + scheduling are library bricks (no HTTP surface of their own in the core path — they are
driven by the lifecycle/bot_spawn flows); they are re-exported from the package front door and wired
by the production composition root in P3. continue_meeting / max-bots / join-retry / the segment
consumer loop are P3 seams.

``create_app`` takes every collaborator as an injected port (or builds a default in-memory stack for
the app factory / tests), so the SAME app runs with real adapters in prod and in-process fakes in
the conformance harness — the conformance assertions therefore drive THIS shipped app.
"""
from __future__ import annotations

from collections.abc import Mapping
import hmac
from typing import Callable, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import bot_spawn as _bot_spawn
from . import recordings as _recordings
from .collector.app import build_router as _build_collector_router
from .collector.ports import RedisBus, TranscriptStore
from .lifecycle.machine import BotStatus, LifecycleSink, MeetingStore
from .managed_auth import validate_hub_token, validated_hmac_secret_bytes
from .obs import TraceMiddleware


def create_app(
    *,
    # collector ports
    transcript_store: Optional[TranscriptStore] = None,
    redis: Optional[RedisBus] = None,
    # bot_spawn ports
    meeting_repo: Optional["_bot_spawn.MeetingRepo"] = None,
    runtime: Optional["_bot_spawn.RuntimeClient"] = None,
    # recordings ports
    recording_repo: Optional["_recordings.RecordingRepo"] = None,
    storage: Optional["_recordings.Storage"] = None,
    # lifecycle store
    meeting_store: Optional[MeetingStore] = None,
    token_secret: Optional[str] = None,
    # Runtime workload callbacks use a dedicated service credential. Bot lifecycle and recording
    # writes use only the per-spawn MeetingToken; no platform-wide service secret enters a bot.
    runtime_callback_secret: Optional[str] = None,
    # user-stop (DELETE /bots) redis command publisher
    command_publisher: Optional["object"] = None,
    # per-user webhook delivery sink (WebhookSink) — delivers meeting.status_change on each FSM advance
    webhook_sink: Optional["object"] = None,
    # completion finalizer — awaited with the NUMERIC meeting id when the FSM lands on a TERMINAL
    # status (completed/failed). Production wires collector/db_writer.finalize_meeting: flush the
    # meeting's remaining redis segments to Postgres + persist the processed doc into meeting.data,
    # so a finished meeting's transcript is durable IMMEDIATELY. Best-effort — never fails the callback.
    transcript_finalizer: Optional["object"] = None,
    # Operator-owned transcript.finalized edge. This is deliberately independent of the user's
    # webhook URL/secret/subscriptions and stays absent unless the operator flag is enabled.
    minutes_finalized_enabled: bool = False,
    minutes_finalized_outbox: Optional["object"] = None,
    minutes_finalized_sink: Optional["object"] = None,
    # calendar-sync user edges (async callables from the composition root; None → routes 503)
    calendar_sync_now: Optional["object"] = None,
    calendar_sync_status: Optional["object"] = None,
    # Minutes' cross-spoke reference read plane.  The router is physically absent while the
    # operator flag is false; when enabled it also requires a dedicated token and per-user opt-in
    # authority.  This is intentionally separate from ordinary Vexa API-key authentication.
    zaki_read_enabled: bool = False,
    zaki_read_token: Optional[str] = None,
    zaki_read_scope: Optional["object"] = None,
    zaki_read_now: Optional[Callable] = None,
    # Launch-facing managed Minutes capture.  Like the read plane this is physically absent while
    # disabled.  Identity owns the per-user policy; Minutes owns the operator flag and Redis fence.
    minutes_capture_enabled: bool = False,
    minutes_invocation_v2_enabled: bool = False,
    minutes_settings: Optional["object"] = None,
    minutes_capture_fencer: Optional["object"] = None,
    minutes_now: Optional[Callable] = None,
    minutes_redis_url: Optional[str] = None,
    minutes_meeting_api_url: Optional[str] = None,
    # Dedicated BFF→Minutes service authentication. User/quota headers are trusted only after this
    # timing-safe gate succeeds; the value is never shared with bots or browser JavaScript.
    minutes_hub_token: Optional[str] = None,
    # Downstream complete-mediation policy. When true, ordinary POST /bots remains as a
    # deterministic denial and only the managed Minutes capture route may create workloads.
    managed_minutes_only: bool = False,
    # Cross-spoke GDPR path. It stays mounted independently of capture/read activation so turning
    # the product off can never strand already-captured data without a deletion path.
    minutes_retention_repo: Optional["object"] = None,
    minutes_retention_storage: Optional["object"] = None,
    minutes_agent_eraser: Optional["object"] = None,
    minutes_erasure_signing_key_id: Optional[str] = None,
    minutes_erasure_signing_secret: Optional[str | bytes] = None,
    minutes_erasure_verification_keys: Optional[Mapping[str, str | bytes]] = None,
    minutes_erasure_nonce_factory: Optional[Callable[[], str]] = None,
) -> FastAPI:
    """Build the unified meeting-api app from the injected ports.

    Any port left ``None`` falls back to its in-memory fake so the app factory stands up a fully
    in-process meeting-api (no DB, no redis, no MinIO, no runtime kernel) — the shape the unified
    health + conformance harnesses drive. Production wires the real adapters via each module's
    ``adapters.build_production_*`` (composition is P3; the seams are here).
    """
    app = FastAPI(title="Vexa Meeting API (v0.12)", version="0.12.0")
    # The edge: read/mint X-Trace-Id and bind it for the request (logevent.v1 trace_id).
    app.add_middleware(TraceMiddleware)

    if not isinstance(minutes_finalized_enabled, bool):
        raise ValueError("Minutes platform finalized flag must be boolean")
    platform_finalized_dependencies = (
        minutes_finalized_outbox,
        minutes_finalized_sink,
    )
    if minutes_finalized_enabled:
        if (
            transcript_finalizer is None
            or any(dependency is None for dependency in platform_finalized_dependencies)
        ):
            raise ValueError(
                "Minutes platform finalized delivery requires finalizer, outbox, and operator sink"
            )
        if (
            any(
                not callable(getattr(minutes_finalized_outbox, method, None))
                for method in ("enqueue", "process", "drain")
            )
            or not callable(getattr(minutes_finalized_sink, "deliver", None))
            or not isinstance(getattr(minutes_finalized_sink, "key_id", None), str)
        ):
            raise ValueError("Minutes platform finalized boundaries are invalid")
    elif any(dependency is not None for dependency in platform_finalized_dependencies):
        raise ValueError(
            "Minutes platform finalized dependencies require the operator flag"
        )
    app.state.minutes_finalized_enabled = minutes_finalized_enabled

    # --- shared liveness probe (gate:health): the unified process is up. No auth. The ADDITIVE
    # `capabilities` rows are the config.v1 tri-states (stt · object_storage) incl. the cached STT
    # live auth probe (ADR-0026) — existing consumers key on `status` only and keep working; the
    # rows never flip `status` (an unconfigured capability degrades a FEATURE, not the process). ---
    @app.get("/health")
    async def health():
        from .config_preflight import capability_health

        return {"status": "ok", "service": "meeting-api", "capabilities": capability_health()}

    # --- bot_spawn ports (resolved FIRST: the meeting_repo is also the lifecycle-persistence target) ---
    if meeting_repo is None:
        meeting_repo = _bot_spawn_fakes().InMemoryMeetingRepo()
    if minutes_finalized_enabled and not callable(
        getattr(meeting_repo, "list_terminal_meeting_ids", None)
    ):
        raise ValueError(
            "Minutes platform finalized recovery requires a terminal meeting read port"
        )
    if runtime is None:
        runtime = _bot_spawn_fakes().FakeRuntimeClient()

    # --- lifecycle: bot lifecycle callbacks + FSM (lifecycle.v1), PERSISTED to the meeting row ---
    sink = LifecycleSink(store=meeting_store if meeting_store is not None else MeetingStore())
    app.state.lifecycle_sink = sink
    app.state.lifecycle_store = sink.store
    app.state.webhook_sink = webhook_sink
    # The lifecycle callback publishes each persisted FSM advance to bm:meeting:{id}:status so the
    # gateway /ws (which SUBSCRIBEs that channel) forwards a ws.v1 BotStatus frame to the dashboard.
    _mount_lifecycle(
        app,
        sink,
        meeting_repo,
        webhook_sink,
        redis,
        transcript_finalizer,
        minutes_finalized_outbox if minutes_finalized_enabled else None,
        minutes_finalized_sink if minutes_finalized_enabled else None,
        token_secret,
        runtime_callback_secret,
    )

    # --- bot_spawn: POST /bots (invocation.v1 + runtime.v1) ---
    if not isinstance(managed_minutes_only, bool):
        raise ValueError("managed Minutes complete-mediation flag must be boolean")
    app.include_router(_bot_spawn.build_router(
        meeting_repo,
        runtime,
        create_enabled=not managed_minutes_only,
    ))

    # --- user-stop: DELETE /bots/{platform}/{native_meeting_id} (lifecycle/stop.py over redis) ---
    from .lifecycle.stop_router import InMemoryCommandPublisher, build_stop_router

    if command_publisher is None:
        command_publisher = InMemoryCommandPublisher()
    app.state.command_publisher = command_publisher
    # The stop router also gets the runtime client so a stop can directly tear down a still-booting bot's
    # workload (the leave command alone is fire-and-forget — a booting bot may never receive it → orphan).
    app.include_router(build_stop_router(meeting_repo, command_publisher, runtime))

    # --- Managed Minutes capture / consent withdrawal. The downstream deployment also enables
    # ``managed_minutes_only`` so legacy POST /bots cannot bypass this product boundary. ---
    if not isinstance(minutes_capture_enabled, bool):
        raise ValueError("managed Minutes operator flag must be boolean")
    erasure_dependencies = (
        minutes_retention_repo,
        minutes_retention_storage,
        minutes_agent_eraser,
        minutes_erasure_signing_key_id,
        minutes_erasure_signing_secret,
        minutes_erasure_verification_keys,
        minutes_erasure_nonce_factory,
    )
    erasure_boundary_present = any(
        dependency is not None for dependency in erasure_dependencies
    )
    erasure_boundary_configured = all(
        dependency is not None for dependency in erasure_dependencies
    )
    if erasure_boundary_present and not erasure_boundary_configured:
        raise ValueError(
            "Minutes erasure requires repo, storage, Agent eraser, and Minutes signing boundary"
        )
    managed_minutes_dependencies = (minutes_settings, minutes_capture_fencer)
    if any(dependency is not None for dependency in managed_minutes_dependencies) and any(
        dependency is None for dependency in managed_minutes_dependencies
    ):
        raise ValueError("managed Minutes requires settings authority and carrier fencer")
    managed_user_routes_enabled = (
        minutes_capture_enabled or erasure_boundary_configured
    )
    if managed_user_routes_enabled and minutes_settings is None:
        raise ValueError("managed Minutes requires settings authority and carrier fencer")
    if not isinstance(minutes_invocation_v2_enabled, bool):
        raise ValueError("managed Minutes invocation.v2 flag must be boolean")
    if minutes_capture_enabled and not minutes_invocation_v2_enabled:
        raise ValueError(
            "managed Minutes capture requires the explicit invocation.v2 runtime route"
        )
    if minutes_capture_enabled and (
        not isinstance(token_secret, str) or not token_secret or redis is None
    ):
        raise ValueError(
            "managed Minutes capture requires a MeetingToken signing key and transcript bus"
        )
    # Active capture and the complete historical-erasure boundary both retain the user control
    # edge. The separate ``managed_minutes_only`` switch merely denies legacy POST /bots; deploys
    # intentionally keep it true while Minutes is default-off, so it must never imply Hub trust or
    # require a Hub secret by itself.
    if managed_user_routes_enabled and minutes_settings is not None:
        validated_hub_token = validate_hub_token(minutes_hub_token)
        if (
            isinstance(token_secret, str)
            and token_secret
            and hmac.compare_digest(validated_hub_token, token_secret)
        ):
            raise ValueError("managed Minutes requires a dedicated Hub token")
        from datetime import datetime, timezone

        from .managed_minutes import build_router as build_managed_minutes_router

        app.include_router(build_managed_minutes_router(
            repo=meeting_repo,
            runtime=runtime,
            publisher=command_publisher,
            carrier_fencer=minutes_capture_fencer,
            settings_provider=minutes_settings,
            now=minutes_now or (lambda: datetime.now(timezone.utc)),
            token_secret=token_secret,
            redis_url=minutes_redis_url,
            meeting_api_url=minutes_meeting_api_url,
            hub_token=validated_hub_token,
            operator_enabled=minutes_capture_enabled,
        ))

    if erasure_boundary_configured:
        validated_hub_token = validate_hub_token(minutes_hub_token)
        try:
            signing_secret_bytes = validated_hmac_secret_bytes(
                minutes_erasure_signing_secret
            )
        except ValueError:
            raise ValueError("Minutes erasure signing boundary is invalid") from None
        if (
            not isinstance(minutes_erasure_signing_key_id, str)
            or not minutes_erasure_signing_key_id
            or not isinstance(minutes_erasure_verification_keys, Mapping)
            or not minutes_erasure_verification_keys
            or not callable(minutes_erasure_nonce_factory)
            or not callable(minutes_agent_eraser)
            or not callable(
                getattr(minutes_agent_eraser, "verify_durable_receipt", None)
            )
            or not callable(getattr(runtime, "scrub_workload", None))
        ):
            raise ValueError("Minutes erasure signing boundary is invalid")
        normalized_verification_keys = dict(minutes_erasure_verification_keys)
        try:
            verification_secret_bytes = {
                key_id: validated_hmac_secret_bytes(secret)
                for key_id, secret in normalized_verification_keys.items()
                if isinstance(key_id, str) and key_id
            }
        except ValueError:
            raise ValueError("Minutes erasure signing boundary is invalid") from None
        if (
            not 1 <= len(normalized_verification_keys) <= 2
            or len(verification_secret_bytes) != len(normalized_verification_keys)
            or len(set(verification_secret_bytes.values()))
            != len(verification_secret_bytes)
        ):
            raise ValueError("Minutes erasure signing boundary is invalid")
        hub_token_bytes = validated_hub_token.encode("ascii")
        if any(
            hmac.compare_digest(secret, hub_token_bytes)
            for secret in verification_secret_bytes.values()
        ):
            raise ValueError("managed Minutes requires a dedicated Hub token")
        current_verification_secret = verification_secret_bytes.get(
            minutes_erasure_signing_key_id
        )
        if current_verification_secret != signing_secret_bytes:
            raise ValueError("Minutes erasure signing boundary is invalid")
        from datetime import datetime, timezone

        from .managed_erasure import build_router as build_managed_erasure_router

        app.include_router(build_managed_erasure_router(
            repo=minutes_retention_repo,
            storage=minutes_retention_storage,
            agent_eraser=minutes_agent_eraser,
            runtime_scrubber=runtime,
            now=minutes_now or (lambda: datetime.now(timezone.utc)),
            signing_key_id=minutes_erasure_signing_key_id,
            signing_secret=minutes_erasure_signing_secret,
            verification_keys=normalized_verification_keys,
            nonce_factory=minutes_erasure_nonce_factory,
            hub_token=validated_hub_token,
        ))

    # --- collector: transcripts + meetings + ws-authorize (api.v1) ---
    if transcript_store is None:
        transcript_store = _collector_fakes().InMemoryTranscriptStore()
    app.include_router(_build_collector_router(transcript_store, redis,
                                            calendar_sync_now=calendar_sync_now,
                                            calendar_sync_status=calendar_sync_status))
    # Managed invocation.v2 bots never receive Redis. The ingress edge is physically absent while
    # capture is off; when enabled, each write is bound to the spawn-scoped MeetingToken plus the
    # authoritative session and then enters the existing collector retention lease.
    if minutes_capture_enabled:
        from .collector.bot_ingress import build_bot_ingress_router

        app.include_router(build_bot_ingress_router(
            store=transcript_store,
            redis=redis,
            meeting_repo=meeting_repo,
            token_secret=token_secret,
            carrier_fencer=minutes_capture_fencer,
        ))

    # --- Agent → Minutes cross-spoke READ only (zaki-read.v1).  Default-off means no route
    # exists at all.  Enabling without both operator-owned dependencies is a composition error,
    # not a silently permissive fallback. ---
    if not isinstance(zaki_read_enabled, bool):
        raise ValueError("zaki-read.v1 operator flag must be boolean")
    if zaki_read_enabled:
        from datetime import datetime, timezone

        from .zaki_read import build_router as build_zaki_read_router

        app.include_router(build_zaki_read_router(
            store=transcript_store,
            token=zaki_read_token,
            scope=zaki_read_scope,
            now=zaki_read_now or (lambda: datetime.now(timezone.utc)),
        ))

    # --- recordings: chunk upload + finalize → meeting.data JSONB (recording.v1) ---
    if recording_repo is None:
        recording_repo = _recordings_fakes().InMemoryRecordingRepo()
    if storage is None:
        storage = _recordings_fakes().InMemoryStorage()
    app.include_router(_recordings.build_router(recording_repo, storage, token_secret=token_secret))

    return app


# ── lifecycle mount (the receiver's callback route, on the shared app) ───────────────────────────


def _mount_lifecycle(
    app: FastAPI,
    sink: LifecycleSink,
    meeting_repo: "_bot_spawn.MeetingRepo",
    webhook_sink: "object" = None,
    redis: "object" = None,
    transcript_finalizer: "object" = None,
    minutes_finalized_outbox: "object" = None,
    minutes_finalized_sink: "object" = None,
    token_secret: Optional[str] = None,
    runtime_callback_secret: Optional[str] = None,
) -> None:
    """Register the lifecycle.v1 callback route on the unified app (the lifecycle receiver's
    ``/bots/internal/callback/lifecycle`` handler, sharing the app's TraceMiddleware).

    P3a — each FSM advance emits the sealed ``meeting.status_change`` webhook.v1 envelope and
    records the full diagnostics (``status_transition[]`` + forensics in ``rec.data``). The
    receiver is a bot callback → ``transition_source=bot_callback``. Each advance is ALSO persisted
    to the DB meeting row via ``meeting_repo`` (durable + queryable status, not only the in-process
    store). Also mounts ``POST /runtime/callback`` so the runtime kernel's workload callbacks ACK
    (no 404-retry).

    Before applying an event the callback REHYDRATES the in-memory FSM record from the DB meeting
    status, so the FSM survives a process restart (the in-process store starts empty) and a terminal
    callback reconciles against the durable status. After a persisted advance it PUBLISHES a ws.v1
    ``BotStatus`` frame to ``bm:meeting:{id}:status`` for the gateway ``/ws`` to forward to clients.
    """
    import asyncio
    import hmac
    from copy import deepcopy
    from weakref import WeakValueDictionary

    import jsonschema

    from .lifecycle.machine import IllegalTransition, TransitionSource
    from .bot_spawn.ports import MeetingStatusWrite
    from .lifecycle.receiver import (
        LifecycleBodyError,
        authorize_internal_callback,
        conforms,
        read_lifecycle_json,
        read_runtime_json,
    )
    from .bot_spawn.invocation import conforms_runtime_event, verify_meeting_token
    from .lifecycle.webhook import (
        build_status_change_envelope,
        build_typed_envelope,
    )
    from .meeting_writes import capture_is_withdrawn, minutes_transcript_is_finalizable
    from .obs import log_event
    from .public_status import public_meeting_status
    from .webhooks import clean_meeting_data

    def _iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    def _meeting_projection_from_row(row: dict) -> dict:
        """The parent's `_build_meeting_event_data` shape (webhooks.py) from a meeting row dict —
        the meeting block the typed webhooks carry (golden Envelope.meeting-completed.json).
        completion_reason/failure_stage are hoisted to top level; internal data keys stripped."""
        data = row.get("data") if isinstance(row.get("data"), dict) else {}
        return {
            "id": row.get("id"),
            "user_id": row.get("user_id"),
            "platform": row.get("platform"),
            "native_meeting_id": row.get("native_meeting_id"),
            "constructed_meeting_url": row.get("constructed_meeting_url"),
            "status": public_meeting_status(row.get("status")),
            "completion_reason": data.get("completion_reason"),
            "failure_stage": data.get("failure_stage"),
            "start_time": _iso(row.get("start_time")),
            "end_time": _iso(row.get("end_time")),
            "data": clean_meeting_data(data),
            "created_at": _iso(row.get("created_at")),
            "updated_at": _iso(row.get("updated_at")),
        }

    app.state.status_change_webhooks = []
    app.state.typed_webhooks = []
    app.state.transcript_finalized_webhooks = []
    # Serialize a single process's mutable FSM projection. This is deliberately only the first
    # layer: ``MeetingRepo.update_meeting_status`` also enforces a DB-transaction CAS across replicas.
    lifecycle_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
    minutes_finalized_backfill_before_id = None

    async def _drain_minutes_finalized():
        nonlocal minutes_finalized_backfill_before_id
        if minutes_finalized_outbox is None:
            return []
        try:
            # Reconstruct intents from the durable terminal projection before draining Redis.
            # This closes the database-commit → Redis-enqueue process-death window.  The scan is
            # bounded and reads row ids/status only: no transcript or user-webhook payload enters
            # this operator-owned path.
            terminal_ids = await meeting_repo.list_terminal_meeting_ids(
                before_id=minutes_finalized_backfill_before_id,
                limit=100,
            )
            for terminal_id in terminal_ids:
                await minutes_finalized_outbox.enqueue(terminal_id)
            if terminal_ids and len(terminal_ids) == 100:
                minutes_finalized_backfill_before_id = min(terminal_ids)
            else:
                # One complete bounded pass has reached the oldest terminal row. Start a new
                # newest-first pass next tick so a prior enqueue outage cannot strand later rows.
                minutes_finalized_backfill_before_id = None
        except Exception:
            # PostgreSQL recovery is additive to already-durable Redis work. A read outage must
            # never prevent an existing intent from reaching its finalizer/operator sink.
            log_event(
                "minutes_platform_finalized_backfill_failed",
                audience="system",
                level="warning",
                span="lifecycle.callback",
                fields={"reason": "retry_required"},
            )
        try:
            results = await minutes_finalized_outbox.drain(
                transcript_finalizer, minutes_finalized_sink
            )
        except Exception:
            log_event(
                "minutes_platform_finalized_drain_failed",
                audience="system",
                level="warning",
                span="lifecycle.callback",
                fields={"reason": "retry_required"},
            )
            return []
        for result in results:
            if result.newly_finalized and result.envelope is not None:
                app.state.transcript_finalized_webhooks.append(result.envelope)
            if not result.delivered:
                log_event(
                    "minutes_platform_finalize_pending",
                    audience="system",
                    level="warning",
                    span="lifecycle.callback",
                    fields={
                        "meeting_id": result.meeting_id,
                        "stage": result.stage,
                    },
                )
        return results

    app.state.minutes_finalized_drain = _drain_minutes_finalized

    async def _apply_lifecycle_event_unlocked(
        body: dict,
        *,
        transition_source: "TransitionSource" = TransitionSource.BOT_CALLBACK,
        force_terminal_on_destroy: bool = False,
    ) -> tuple[int, dict]:
        """Apply ONE lifecycle.v1 event to the FSM + run every side effect (persist, finalize,
        webhook deliver, ws publish, copilot reap), returning ``(status_code, content)``.

        This is the SINGLE in-process entry the FSM advance flows through — the HTTP endpoint
        ``POST /bots/internal/callback/lifecycle`` (the bot's own callback) is a thin wrapper around
        it, and the runtime-callback synthetic-terminal path calls it DIRECTLY (no HTTP self-POST).
        The prior implementation POSTed to ``http://127.0.0.1:PORT/…`` to re-enter this logic; that
        loopback round-trip was fragile (a rehydration race made the synthetic terminal 409, and
        under any harness that cannot reach the loopback it silently dropped) — the direct in-process
        call removes the network hop entirely, so the synthetic terminal advances the SAME FSM
        instance deterministically. ``force_terminal_on_destroy`` rides through to the sink so a
        runtime-confirmed destroy can force the terminal edge from a stale non-terminal state."""
        try:
            conforms(body, "LifecycleEvent")
        except jsonschema.ValidationError as e:
            log_event(
                "lifecycle_event_rejected", audience="system", level="warning",
                span="lifecycle.callback",
                fields={"reason": "schema_violation", "detail": e.message},
            )
            return (
                422,
                {"status": "error", "detail": f"lifecycle.v1 schema violation: {e.message}"},
            )
        # Reconcile the process-local projection from the durable status before EVERY advance. A
        # replica may be behind another replica; process memory is never allowed to overrule the DB.
        # The subsequent write carries this observed status as a compare-and-set expectation, closing
        # the read→write race. A failed read is retryable: applying against guesses would reintroduce
        # exactly the stale-writer bug this boundary exists to prevent.
        connection_id = body.get("connection_id")
        persisted = None
        if connection_id:
            try:
                persisted = await meeting_repo.get_status_by_session(session_uid=connection_id)
            except Exception as e:  # noqa: BLE001 — transient storage failure; caller retries
                log_event(
                    "lifecycle_rehydrate_failed",
                    audience="system",
                    level="warning",
                    span="lifecycle.callback",
                    fields={"stage": "rehydrate", "error_type": type(e).__name__},
                )
                return (
                    503,
                    {
                        "status": "error",
                        "detail": "lifecycle state was not committed; retry",
                    },
                )
            if persisted is not None:
                sink.store.reconcile_status(connection_id, persisted)
        accepted_record = deepcopy(sink.store.get(connection_id)) if connection_id else None

        def _restore_speculative_record(*, durable_status=None) -> None:
            if accepted_record is not None:
                sink.store.replace(accepted_record)
            elif connection_id:
                sink.store.discard(connection_id)
            if connection_id and durable_status is not None:
                sink.store.reconcile_status(connection_id, durable_status)

        def _accepted_durable_noop(row: dict) -> tuple[int, dict]:
            durable_data = row.get("data") if isinstance(row.get("data"), dict) else {}
            return (
                200,
                {
                    "status": "accepted",
                    "connection_id": connection_id,
                    "meeting_status": row.get("status"),
                    "completion_reason": durable_data.get("completion_reason"),
                    "failure_stage": durable_data.get("failure_stage"),
                    "transition_source": transition_source.value,
                    "status_transition": durable_data.get("status_transition", []),
                    "data": durable_data,
                },
            )
        try:
            change = sink.apply_change(
                body,
                transition_source=transition_source,
                force_terminal_on_destroy=force_terminal_on_destroy,
                force_terminal_after_stop=bool(
                    (accepted_record is not None and accepted_record.stop_requested)
                    or persisted == "stopping"
                ),
            )
        except IllegalTransition as e:
            # Consent withdrawal can terminalize the durable row while a bot callback is already in
            # flight. The FSM correctly calls that late edge illegal, but retrying it forever is both
            # pointless and hostile to teardown. Ask the row-locked write guard to distinguish the
            # privacy-specific suppression from an ordinary illegal transition; the persisted and
            # in-memory transition maps are aligned, so this probe cannot apply an illegal bot edge.
            try:
                rejected_write = await meeting_repo.update_meeting_status(
                    session_uid=e.connection_id,
                    status=e.to.value,
                    expected_status=persisted,
                    force_terminal=False,
                )
            except Exception as persist_error:  # noqa: BLE001 — retry storage failures
                log_event(
                    "lifecycle_rejection_check_failed",
                    audience="system",
                    level="warning",
                    span="lifecycle.callback",
                    fields={
                        "stage": "rejection_check",
                        "error_type": type(persist_error).__name__,
                    },
                )
                _restore_speculative_record()
                return (
                    503,
                    {
                        "status": "error",
                        "detail": "lifecycle state was not committed; retry",
                    },
                )
            if isinstance(rejected_write, MeetingStatusWrite):
                durable_row = rejected_write.row
                if (
                    rejected_write.disposition == "suppressed"
                    and (
                        e.to.value not in ("completed", "failed")
                        or rejected_write.previous_status != persisted
                    )
                ):
                    _restore_speculative_record(durable_status=durable_row.get("status"))
                    return _accepted_durable_noop(durable_row)
                if rejected_write.disposition == "conflict":
                    _restore_speculative_record(durable_status=durable_row.get("status"))
                    return (
                        503,
                        {
                            "status": "error",
                            "detail": "lifecycle state changed concurrently; retry",
                        },
                    )
            return (
                409,
                {
                    "status": "error", "detail": str(e),
                    "connection_id": e.connection_id,
                    "from": e.frm.value if e.frm is not None else None,
                    "to": e.to.value,
                },
            )
        rec = change.record
        # Build + record the status_change envelope only after persistence confirms that the advance
        # is user-visible. Besides idempotent replays, a late non-terminal callback after durable
        # consent withdrawal is suppressed: it must not resurrect an active state in HTTP, WS, or
        # webhook projections.
        envelope = None
        # Persist the FSM advance to the DB meeting row → durable + queryable (GET /meetings reflects
        # it, survives a restart), not only the in-process MeetingStore. Best-effort: a DB hiccup must
        # never fail the bot's lifecycle callback (the in-process FSM + webhook already advanced).
        # On an idempotent replay (change.no_op) the FSM did not actually advance — skip the
        # re-persist + re-deliver so a redelivered terminal does not fire a duplicate webhook /
        # publish. We still return 200 (handled below) — the redelivery is acknowledged as a no-op.
        meeting_row = None
        if rec.status is not None and not change.no_op:
            try:
                write_result = await meeting_repo.update_meeting_status(
                    session_uid=rec.connection_id,
                    status=rec.status.value,
                    completion_reason=rec.completion_reason.value if rec.completion_reason else None,
                    failure_stage=rec.failure_stage.value if rec.failure_stage else None,
                    data=rec.data if isinstance(rec.data, dict) else None,
                    expected_status=persisted,
                    force_terminal=force_terminal_on_destroy,
                )
            except Exception as e:  # noqa: BLE001 — transient storage failure; caller must retry
                log_event("lifecycle_persist_failed", audience="system", level="warning",
                          span="lifecycle.callback", fields={
                              "stage": "persist",
                              "error_type": type(e).__name__,
                          })
                _restore_speculative_record()
                return (
                    503,
                    {
                        "status": "error",
                        "detail": "lifecycle state was not committed; retry",
                    },
                )
            if write_result is None:
                log_event(
                    "lifecycle_persist_missing",
                    audience="system",
                    level="warning",
                    span="lifecycle.callback",
                    fields={"connection_id": rec.connection_id},
                )
                _restore_speculative_record()
                return (
                    503,
                    {
                        "status": "error",
                        "detail": "lifecycle state was not committed; retry",
                    },
                )
            if isinstance(write_result, MeetingStatusWrite):
                meeting_row = write_result.row
                write_disposition = write_result.disposition
            else:
                # Compatibility for a third-party MeetingRepo implementing the older dict return.
                meeting_row = write_result
                write_disposition = "applied"
            if write_disposition in ("conflict", "rejected"):
                durable_status = meeting_row.get("status") if isinstance(meeting_row, dict) else None
                _restore_speculative_record(durable_status=durable_status)
                if write_disposition == "conflict":
                    return (
                        503,
                        {
                            "status": "error",
                            "detail": "lifecycle state changed concurrently; retry",
                        },
                    )
                return (
                    409,
                    {
                        "status": "error",
                        "detail": (
                            f"Invalid durable transition: {durable_status} → {rec.status.value} "
                            f"(connection_id={rec.connection_id})"
                        ),
                        "connection_id": rec.connection_id,
                        "from": durable_status,
                        "to": rec.status.value,
                    },
                )
        else:
            write_disposition = "idempotent"
        meeting_data = (
            meeting_row.get("data")
            if isinstance(meeting_row, dict) and isinstance(meeting_row.get("data"), dict)
            else {}
        )
        meeting_capture = meeting_data.get("zaki_capture")
        teardown_was_already_confirmed = (
            isinstance(meeting_capture, dict)
            and meeting_capture.get("teardown_state") == "confirmed"
        )
        suppressed_withdrawn_advance = (
            not change.no_op
            and rec.status is not None
            and isinstance(meeting_row, dict)
            and capture_is_withdrawn(meeting_data)
            and (
                meeting_row.get("status") != rec.status.value
                or teardown_was_already_confirmed
            )
        )
        if suppressed_withdrawn_advance and accepted_record is not None:
            accepted_record.stop_requested = True
            sink.store.replace(accepted_record)
        durable_noop = write_disposition != "applied"
        visible_change = (
            not change.no_op
            and not durable_noop
            and not suppressed_withdrawn_advance
        )
        if visible_change:
            envelope = build_status_change_envelope(change)
            app.state.status_change_webhooks.append(envelope)
        # COMPLETION FINALIZATION — the moment the FSM lands on a terminal status, flush the
        # meeting's remaining live redis segments to the durable store (threshold 0: the mutable
        # tail included, no more updates are coming) and persist the processed doc into
        # meeting.data, via the injected finalizer (prod: collector/db_writer.finalize_meeting).
        # This guarantees a completed meeting's transcript is durable even if the periodic
        # db-writer never gets another tick (crash/restart right after completion). Best-effort:
        # the periodic loop retries anything this misses; never fail the bot's callback.
        transcript_finalized_envelope = None
        terminal_transition = (
            transcript_finalizer is not None
            and rec.status is not None
            and rec.status.value in ("completed", "failed")
        )
        if (
            terminal_transition
            and visible_change
            and isinstance(meeting_row, dict)
            and meeting_row.get("id") is not None
        ):
            meeting_data = (
                meeting_row.get("data")
                if isinstance(meeting_row.get("data"), dict)
                else {}
            )
            platform_event_authorized = minutes_transcript_is_finalizable(meeting_data)
            if minutes_finalized_outbox is not None and platform_event_authorized:
                try:
                    meeting_id = meeting_row["id"]
                    # Persist intent before touching transcript carriers. A process death or
                    # finalizer failure therefore leaves a restart-safe retry record.
                    await minutes_finalized_outbox.enqueue(meeting_id)
                    attempt = await minutes_finalized_outbox.process(
                        meeting_id, transcript_finalizer, minutes_finalized_sink
                    )
                    if attempt.newly_finalized and attempt.envelope is not None:
                        transcript_finalized_envelope = attempt.envelope
                        app.state.transcript_finalized_webhooks.append(
                            transcript_finalized_envelope
                        )
                    if not attempt.delivered:
                        log_event(
                            "minutes_platform_finalize_pending",
                            audience="system",
                            level="warning",
                            span="lifecycle.callback",
                            fields={
                                "meeting_id": meeting_id,
                                "stage": attempt.stage,
                            },
                        )
                except Exception:
                    log_event(
                        "minutes_platform_finalize_failed",
                        audience="system",
                        level="warning",
                        span="lifecycle.callback",
                        fields={
                            "meeting_id": meeting_row.get("id"),
                            "reason": "retry_required",
                        },
                    )
            else:
                try:
                    await transcript_finalizer(meeting_row["id"])
                except Exception as e:  # noqa: BLE001 — legacy periodic retry remains the backstop
                    log_event("transcript_finalize_failed", audience="system", level="warning",
                              span="lifecycle.callback",
                              fields={
                                  "meeting_id": meeting_row.get("id"),
                                  "stage": "finalize",
                                  "error_type": type(e).__name__,
                              })
        elif terminal_transition and minutes_finalized_outbox is not None:
            # A redelivered terminal is an FSM no-op, but it is still a retry signal for durable
            # finalization work left pending by an earlier crash/failure.
            await _drain_minutes_finalized()
        # Build the TYPED event the transition maps to (meeting.started on active,
        # meeting.completed with the post-meeting envelope on completion, bot.failed on terminal
        # failure) — additive alongside meeting.status_change, never instead of it. Built AFTER the
        # persist so the meeting block is the durable row projection (the parent's
        # _build_meeting_event_data shape) when the row is known; the FSM-record fallback otherwise.
        typed_envelope = None
        if visible_change:
            typed_envelope = build_typed_envelope(
                change,
                meeting=_meeting_projection_from_row(meeting_row)
                if isinstance(meeting_row, dict) else None,
            )
            if typed_envelope is not None:
                app.state.typed_webhooks.append(typed_envelope)
        # Deliver the sealed webhook.v1 envelopes (meeting.status_change + the typed event, if any)
        # to the user's configured endpoint (per-user config rides on meeting.data — set at spawn
        # from identity via the gateway; NO users-table read). The sink's per-user event filter
        # (webhooks/delivery.py) suppresses unsubscribed event types before any HTTP.
        # Best-effort: a delivery hiccup must never fail the bot's lifecycle callback (P3a).
        if webhook_sink is not None and isinstance(meeting_row, dict):
            data = meeting_row.get("data") if isinstance(meeting_row.get("data"), dict) else {}
            url = data.get("webhook_url")
            if url:
                for env in (envelope, typed_envelope):
                    if env is None:
                        continue
                    try:
                        await webhook_sink.deliver(
                            url, env, data.get("webhook_secret"),
                            events_config=data.get("webhook_events"),
                            label=f"meeting:{meeting_row.get('id')}",
                        )
                    except Exception as e:  # noqa: BLE001 — delivery is best-effort
                        log_event("webhook_deliver_failed", audience="system", level="warning",
                                  span="lifecycle.callback", fields={"error": str(e)})
        # Publish each persisted FSM advance to bm:meeting:{id}:status in the canonical 0.10.6 WS
        # contract shape (the source of truth; api-gateway forwards the redis payload verbatim):
        #   {type:"meeting.status", meeting:{id,platform,native_id}, payload:{status}, user_id, ts}
        # Internal lifecycle/storage keeps `needs_help`; public WS frames translate it to the
        # canonical api.v1 `needs_human_help` vocabulary at this boundary. Skipped on
        # a no-op advance (idempotent replay) / unknown session. Best-effort: never fail the callback.
        if redis is not None and visible_change and isinstance(meeting_row, dict) and rec.status is not None:
            meeting_id = meeting_row.get("id")
            if meeting_id is not None:
                import json as _json
                from datetime import datetime, timezone

                frame = {
                    "type": "meeting.status",
                    "meeting": {
                        "id": meeting_id,
                        "platform": meeting_row.get("platform"),
                        "native_id": meeting_row.get("native_meeting_id"),
                    },
                    "payload": {"status": public_meeting_status(rec.status.value)},
                    "user_id": meeting_row.get("user_id"),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
                try:
                    await redis.publish(f"bm:meeting:{meeting_id}:status", _json.dumps(frame))
                except Exception as e:  # noqa: BLE001 — publish is best-effort
                    log_event("ws_status_publish_failed", audience="system", level="warning",
                              span="lifecycle.callback", fields={"error": str(e)})
                # ALSO publish the FLAT frame to the USER-scoped channel u:{user_id}:meetings so the
                # terminal's list surface gets every bot-FSM transition over WS (superset of bm:; it
                # also carries the pre-FSM idle/scheduled states). KEEP bm: above for the open-meeting
                # tab. Best-effort: never fail the lifecycle callback.
                user_id = meeting_row.get("user_id")
                if user_id is not None:
                    user_frame = {
                        "type": "meeting.status",
                        "meeting_id": meeting_id,
                        "native": meeting_row.get("native_meeting_id"),
                        "status": public_meeting_status(rec.status.value),
                        "when": frame["ts"],
                    }
                    try:
                        await redis.publish(
                            f"u:{user_id}:meetings", _json.dumps(user_frame)
                        )
                    except Exception as e:  # noqa: BLE001 — publish is best-effort
                        log_event("user_meeting_status_publish_failed", audience="system",
                                  level="warning", span="lifecycle.callback",
                                  fields={"error": str(e)})
        # COPILOT REAP (Bug 3): the moment a meeting lands TERMINAL, emit the `session_end` marker onto
        # the meeting copilot transcript feed — the EXACT stream the meeting copilot worker
        # (agent worker/meeting.py, via VEXA_TRANSCRIPT_STREAM) blocks on. The worker reaps immediately
        # on that marker (exit 0 → container reaped), instead of sitting idle for its
        # VEXA_IDLE_TIMEOUT_SEC (default 4h) when the bot never emitted its own `session_end` — e.g. it
        # was SIGKILLed, or stopped in the waiting room (Bug 2) before it could. Idempotent: a redundant
        # session_end (the bot already sent one via the collector) just reasserts the reap. Best-effort;
        # never fails the lifecycle callback.
        #
        # KEYING (P0 fix/transcript-cross-tenant-leak, now merged): the carrier is ROW-scoped
        # `tc:meeting:{meeting_row_id}` — the numeric meetings-domain ROW id, NOT the native id (which
        # collides across tenants/rows and is never a data key post-P0). The collector
        # (collector/ingest.py `_transcript_stream`) writes its session_end on the same row key and the
        # worker tails the row key (agent dispatch.py sets VEXA_TRANSCRIPT_STREAM=tc:meeting:{row_id}),
        # so this lifecycle reap must key by the row id to land on the live stream the worker blocks on.
        if (
            redis is not None
            and visible_change
            and rec.status is not None
            and rec.status.value in ("completed", "failed")
            and isinstance(meeting_row, dict)
            and hasattr(redis, "xadd")
        ):
            meeting_row_id = meeting_row.get("id")
            native = meeting_row.get("native_meeting_id") or rec.connection_id
            if meeting_row_id is not None:
                try:
                    await redis.xadd(
                        f"tc:meeting:{meeting_row_id}",
                        {"type": "session_end", "uid": str(native or meeting_row_id)},
                    )
                    log_event(
                        "meeting_copilot_reap_signalled", audience="system", span="lifecycle.callback",
                        meeting_id=rec.connection_id,
                        fields={"meeting_row_id": meeting_row_id, "native": native,
                                "meeting_status": rec.status.value},
                    )
                except Exception as e:  # noqa: BLE001 — the worker's idle timeout is the backstop
                    log_event("meeting_copilot_reap_failed", audience="system", level="warning",
                              span="lifecycle.callback",
                              fields={"meeting_row_id": meeting_row_id, "error": str(e)})
        report_durable_projection = (
            (suppressed_withdrawn_advance or durable_noop)
            and isinstance(meeting_row, dict)
        )
        reported_status = (
            meeting_row.get("status")
            if report_durable_projection
            else (rec.status.value if rec.status else None)
        )
        reported_data = meeting_data if report_durable_projection else rec.data
        reported_transitions = (
            meeting_data.get("status_transition", [])
            if report_durable_projection
            else rec.status_transition
        )
        reported_completion_reason = (
            meeting_data.get("completion_reason")
            if report_durable_projection
            else (rec.completion_reason.value if rec.completion_reason else None)
        )
        reported_failure_stage = (
            meeting_data.get("failure_stage")
            if report_durable_projection
            else (rec.failure_stage.value if rec.failure_stage else None)
        )
        log_event(
            "meeting_lifecycle_advanced", audience="user", span="lifecycle.callback",
            meeting_id=rec.connection_id,
            fields={"meeting_status": reported_status},
        )
        return (
            200,
            {
                "status": "accepted",
                "connection_id": rec.connection_id,
                "meeting_status": reported_status,
                "completion_reason": reported_completion_reason,
                "failure_stage": reported_failure_stage,
                "transition_source": change.transition_source.value,
                "status_transition": reported_transitions,
                "data": reported_data,
            },
        )

    async def _apply_lifecycle_event(
        body: dict,
        *,
        transition_source: "TransitionSource" = TransitionSource.BOT_CALLBACK,
        force_terminal_on_destroy: bool = False,
    ) -> tuple[int, dict]:
        """Serialize the mutable local FSM for one connection; DB CAS handles other replicas."""
        connection_id = body.get("connection_id") if isinstance(body, dict) else None
        if not isinstance(connection_id, str):
            return await _apply_lifecycle_event_unlocked(
                body,
                transition_source=transition_source,
                force_terminal_on_destroy=force_terminal_on_destroy,
            )
        lock = lifecycle_locks.get(connection_id)
        if lock is None:
            lock = asyncio.Lock()
            lifecycle_locks[connection_id] = lock
        async with lock:
            return await _apply_lifecycle_event_unlocked(
                body,
                transition_source=transition_source,
                force_terminal_on_destroy=force_terminal_on_destroy,
            )

    # Expose the in-process entry so the runtime-callback synthetic-terminal path can advance the FSM
    # DIRECTLY (no HTTP self-POST to 127.0.0.1:PORT). Same instance, same store, same side effects.
    app.state.apply_lifecycle_event = _apply_lifecycle_event

    @app.post("/bots/internal/callback/lifecycle")
    async def lifecycle_callback(request: Request) -> JSONResponse:
        claims = None
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer ") and authorization.count(" ") == 1:
            bearer = authorization[len("Bearer "):]
            try:
                claims = verify_meeting_token(
                    bearer,
                    purpose="lifecycle",
                    secret=token_secret,
                )
            except ValueError:
                return JSONResponse(
                    status_code=403,
                    content={"status": "error", "detail": "forbidden"},
                )
        else:
            # Only the explicit two-key in-process development escape can omit a MeetingToken.
            # X-Internal-Secret is never consulted by this production route.
            if token_secret is not None:
                return JSONResponse(
                    status_code=403,
                    content={"status": "error", "detail": "forbidden"},
                )
            denial = authorize_internal_callback(request, None)
            if denial is not None:
                return denial
        try:
            body = await read_lifecycle_json(request)
        except LifecycleBodyError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"status": "error", "detail": exc.detail},
            )
        if claims is not None:
            connection_id = body.get("connection_id") if isinstance(body, dict) else None
            if not isinstance(connection_id, str) or not hmac.compare_digest(
                claims["session_uid"], connection_id
            ):
                return JSONResponse(
                    status_code=403,
                    content={"status": "error", "detail": "forbidden"},
                )
            try:
                authoritative_meeting_id = await meeting_repo.get_meeting_id_by_session(
                    session_uid=connection_id
                )
            except Exception:
                return JSONResponse(
                    status_code=503,
                    content={
                        "status": "error",
                        "detail": "lifecycle authorization unavailable; retry",
                    },
                )
            if authoritative_meeting_id != claims["meeting_id"]:
                return JSONResponse(
                    status_code=403,
                    content={"status": "error", "detail": "forbidden"},
                )
        status_code, content = await _apply_lifecycle_event(
            body, transition_source=TransitionSource.BOT_CALLBACK
        )
        return JSONResponse(status_code=status_code, content=content)

    @app.post("/runtime/callback")
    async def runtime_callback(request: Request) -> JSONResponse:
        """ACK the runtime kernel's workload-level callback (state/terminal events). The bot's own
        ``lifecycle.v1`` callback is the meeting-status source of truth for a STARTED bot; this route
        ALSO consumes a runtime-confirmed TERMINAL workload state as evidence the run is over, driving a
        synthetic terminal through the SAME in-process lifecycle logic (no HTTP self-POST):

          * PRE-ACTIVE meeting → ``failed`` (CC5): the bot never started/reported and never will, so the
            meeting would otherwise hang ``requested``/``joining`` forever.
          * WAS-ACTIVE meeting (``stopping``/``active``/``needs_help``) → ``completed``: the bot reached
            the meeting but its workload is now runtime-confirmed gone WITHOUT its own terminal callback
            (e.g. SIGKILLed at teardown before it could POST ``completed``, or killed in the waiting room
            on a stop). Without this the meeting stays ``stopping`` and the stop-reconcile sweep re-DELETEs
            (now 404) every 15s FOREVER — the reaper loop. The confirmed destroy IS the terminal evidence
            (#50's principle: real evidence, not a bare 404) → complete it and stop the loop.

        THE FIX (live 409): the synthetic terminal is applied by calling the in-process lifecycle entry
        (``app.state.apply_lifecycle_event``) DIRECTLY with ``transition_source=RUNTIME_DESTROY`` and
        ``force_terminal_on_destroy=True`` — NOT an httpx POST to ``127.0.0.1:PORT``. The old self-POST
        409'd whenever the in-process FSM record was a stale non-terminal state the DB had already moved
        past (e.g. store still ``joining`` while the DB user-stop set ``stopping`` — ``joining →
        completed`` is illegal for a bot-driven edge). The direct in-process call advances the SAME FSM
        instance, and the runtime-destroy source forces the terminal edge on real teardown evidence, so
        the meeting reaches terminal, the reaper stops, and the copilot ``session_end`` reap fires."""
        if not isinstance(runtime_callback_secret, str) or not runtime_callback_secret:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "detail": "runtime callback authentication unavailable",
                },
            )
        provided = request.headers.get("X-Runtime-Callback-Secret", "")
        if not hmac.compare_digest(provided, runtime_callback_secret):
            return JSONResponse(
                status_code=403,
                content={"status": "error", "detail": "forbidden"},
            )
        try:
            body = await read_runtime_json(request)
            conforms_runtime_event(body)
        except LifecycleBodyError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"status": "error", "detail": exc.detail},
            )
        except (jsonschema.ValidationError, TypeError) as exc:
            detail = (
                exc.message
                if isinstance(exc, jsonschema.ValidationError)
                else "body must be an object"
            )
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "detail": f"runtime.v1 schema violation: {detail}",
                },
            )
        workload_id = body.get("workloadId") or body.get("workload_id")
        state = body.get("state")
        log_event(
            "runtime_callback", audience="system", span="runtime.callback",
            fields={"workload_id": workload_id, "state": state},
        )
        # Consume a runtime-confirmed TERMINAL workload as evidence (pre-active → failed / was-active →
        # completed). Drive it through the SAME in-process lifecycle logic (FSM/persist/webhook/ws/reap
        # all fire identically) — best-effort; a non-terminal state or an already-terminal meeting is a
        # no-op. Imported lazily to keep the prod import path lean.
        try:
            import logging as _logging

            from .lifecycle.machine import TransitionSource as _TS
            from .lifecycle.reconcile import synthesize_terminal_for_dead_workload

            async def _drive_terminal(event: dict):
                # In-process — no network hop. The runtime-destroy source forces the terminal edge past
                # a stale non-terminal FSM record; returns the HTTP-equivalent status code for the log.
                status_code, _content = await _apply_lifecycle_event(
                    event,
                    transition_source=_TS.RUNTIME_DESTROY,
                    force_terminal_on_destroy=True,
                )
                return status_code

            await synthesize_terminal_for_dead_workload(
                meeting_repo, workload_id, state, _drive_terminal,
                log=_logging.getLogger("meeting_api.runtime.callback"),
                raise_on_transient=True,
            )
        except Exception as e:  # noqa: BLE001 — any lost terminal work must be redelivered
            log_event("runtime_callback_terminal_error", audience="system", level="warning",
                      span="runtime.callback", fields={
                          "stage": "terminal_synthesis",
                          "error_type": type(e).__name__,
                      })
            return JSONResponse(
                status_code=503,
                content={
                    "status": "error",
                    "detail": "runtime callback processing failed; retry",
                },
            )
        return JSONResponse(status_code=200, content={"status": "accepted"})


# ── lazy fake imports (keep the default in-memory stack off the prod import path) ────────────────


def _bot_spawn_fakes():
    from .bot_spawn import fakes

    return fakes


def _collector_fakes():
    from .collector import fakes

    return fakes


def _recordings_fakes():
    from .recordings import fakes

    return fakes
