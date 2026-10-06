"""Error sink (L-0990) — CANONICAL COPY, vendored VERBATIM by every adopted first-party service
as ``<pkg>/error_sink.py`` (the same discipline as ``config_preflight.py``) and wired into the
service's production entrypoint::

    from . import error_sink

    app = create_app(...)
    if error_sink.init("<service>"):
        app.add_middleware(error_sink.ErrorSinkMiddleware)

Behaviour:

  * each unhandled exception and each 5xx the service answers is reported as ONE event to
    ``SENTRY_DSN`` (GlitchTip speaks the Sentry protocol), tagged with ``SENTRY_ENVIRONMENT``
    and ``SENTRY_RELEASE``;
  * with ``SENTRY_DSN`` unset or blank ``init()`` returns ``False`` BEFORE importing
    ``sentry_sdk`` — no init, no middleware, no network, no behaviour change;
  * privacy (L-0989 — Minutes holds meeting audio, transcripts and summaries): the event
    carries no request (no body, headers, cookies, query string, raw path), no user, no
    extra, no breadcrumbs and no frame locals. ``send_default_pii=False``; ``before_send``
    scrubs; breadcrumbs and tracing stay off (integrations disabled — a logging breadcrumb
    carries the log message, a span carries the raw url). An exception title keeps the error
    TYPE plus an ALL-CAPS code when the thrown error carries one (``ENOENT``-style constants,
    never content) — the free-text message is dropped, because free text is where a user's
    words leak.
"""
from __future__ import annotations

import errno as _errno
import logging
import os
import re

log = logging.getLogger("error_sink")

# Probe paths the pods answer before warmup (k8s startup/readiness/liveness): never an event.
_PROBE_PATHS = frozenset({"/health", "/healthz", "/livez", "/readyz"})

# ENOENT, SQLITE_BUSY, ERR_INVALID_ARG_TYPE: constants, never content.
_CODE_RE = re.compile(r"[A-Z][A-Z0-9_]{1,63}")


def init(service: str, transport=None) -> bool:
    """Initialize the SDK from env; return True when the sink is live.

    ``transport`` exists for the pins' fake-DSN capture transport; production leaves it None
    (the SDK builds the HTTP transport from the DSN).
    """
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        return False
    import sentry_sdk

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("SENTRY_ENVIRONMENT") or None,
        release=os.getenv("SENTRY_RELEASE") or None,
        send_default_pii=False,
        include_local_variables=False,
        traces_sample_rate=0.0,
        profiles_sample_rate=0.0,
        default_integrations=False,
        auto_enabling_integrations=False,
        integrations=[],
        before_send=scrub_event,
        transport=transport,
    )
    sentry_sdk.set_tag("service", service)
    log.info("error sink initialized for %s", service)
    return True


def _error_code(exc) -> str | None:
    """An ALL-CAPS constant the thrown error carries (``err.code``, ``OSError.errno``) or None."""
    if exc is None:
        return None
    code = getattr(exc, "code", None)
    if isinstance(code, str) and _CODE_RE.fullmatch(code):
        return code
    if isinstance(exc, OSError) and exc.errno is not None:
        return _errno.errorcode.get(exc.errno)
    return None


def scrub_event(event: dict, hint: dict) -> dict:
    """``before_send`` — the last word before an event leaves the process (L-0989).

    Drops every field that can carry request data, identity or user text. Exception titles
    keep the type, plus the code when the thrown error's is an ALL-CAPS constant.
    """
    for key in ("request", "user", "extra", "breadcrumbs", "transaction"):
        event.pop(key, None)
    values = (event.get("exception") or {}).get("values") or []
    for value in values:
        value.pop("value", None)
        for frame in (value.get("stacktrace") or {}).get("frames") or []:
            frame.pop("vars", None)
    code = _error_code(hint.get("original_exception"))
    if code and values:
        values[-1]["value"] = code
    return event


def _route_template(scope: dict) -> str:
    """The matched route's TEMPLATE (``/meetings/{meeting_id}``), never the raw path."""
    path = getattr(scope.get("route"), "path", None)
    return path if isinstance(path, str) and path else "(no route)"


class ErrorSinkMiddleware:
    """Pure ASGI middleware: ONE event per unhandled exception or per 5xx this process answers.

    Sits inside Starlette's ServerErrorMiddleware, so an unhandled exception is captured at the
    ``except`` and the 500 it becomes is written outside our wrapped ``send`` — exactly one
    report per failure either way.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") in _PROBE_PATHS:
            await self.app(scope, receive, send)
            return
        reported = False

        async def send_wrapped(message):
            nonlocal reported
            if (
                not reported
                and message["type"] == "http.response.start"
                and message["status"] >= 500
            ):
                reported = True
                _report_5xx(scope, message["status"])
            await send(message)

        try:
            await self.app(scope, receive, send_wrapped)
        except Exception:
            if not reported:
                reported = True
                _report_exception()
            raise


def _report_exception() -> None:
    import sentry_sdk

    sentry_sdk.capture_exception()


def _report_5xx(scope: dict, status: int) -> None:
    import sentry_sdk

    sentry_sdk.capture_message(
        f"HTTP {status} {scope.get('method', '?')} {_route_template(scope)}", level="error"
    )
