"""config_test.py — on-demand credential tests behind Settings → Models "Test" buttons.

The two silent-failure modes this surface exists to catch (both bit the owner on 2026-07-09):
  * subscription mode: the mounted ``~/.claude/.credentials.json`` is a STALE export (macOS
    Keychain holds the live token; the file expires ~8-12h) → every turn dies with
    "401 Invalid authentication credentials" and nothing in the UI says why.
  * transcription: a Settings-level override silently outranks ``.env`` (user > global > env)
    and a zero-balance external token 402s every segment while the bot logs stay in docker.

Tests are HONEST about depth: a custom endpoint gets a real 1-token completion; the
subscription file gets an existence + expiry check (the CLI lives only in worker images, so a
live inference test would need a dispatch — the expiry check catches 100% of the observed
failures); the transcription backend gets a real authenticated ``/balance`` probe.

Pure functions over injected fetchers — the routes in api.py are thin wrappers.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Callable, Optional

# The compose-mounted subscription credential file (same :ro mount the runtime probes; the
# docker backend mounts the same host path into workers at /root/.claude/.credentials.json).
CREDS_PATH = "/var/lib/vexa/host-claude-credentials"

# The macOS remedy, verbatim — the error message must carry the fix (fail loud AND helpful).
KEYCHAIN_REFRESH = ('security find-generic-password -s "Claude Code-credentials" -w '
                    "> ~/.claude/.credentials.json")

_TIMEOUT = 8.0
MAX_HTTP_RESPONSE_BYTES = 1024 * 1024

# (status, body_text) — injectable for tests; None body on network failure.
HttpPost = Callable[[str, dict, dict], tuple[int, str]]
HttpGet = Callable[[str, dict], tuple[int, str]]


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep authenticated probes bound to the exact operator-configured endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirectHandler())


def open_no_redirect(request: urllib.request.Request, *, timeout: float):
    """Open one authenticated request without forwarding its headers across origins."""

    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


def _read_bounded_response(response) -> str:
    """Read one untrusted probe response with a hard byte ceiling before decoding.

    ``Content-Length`` provides a fast rejection when present; the ``limit + 1`` read also
    bounds chunked/missing-length responses. The local error never includes upstream bytes.
    """
    declared = response.headers.get("Content-Length")
    if declared is not None:
        try:
            declared_length = int(declared)
        except (TypeError, ValueError):
            # A malformed length cannot be trusted for a fast decision; the bounded read below
            # remains authoritative.
            declared_length = None
        if declared_length is not None and declared_length > MAX_HTTP_RESPONSE_BYTES:
            raise ValueError("upstream response exceeds limit")
    body = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    if len(body) > MAX_HTTP_RESPONSE_BYTES:
        raise ValueError("upstream response exceeds limit")
    return body.decode("utf-8", "replace")


def _post(url: str, payload: dict, headers: dict) -> tuple[int, str]:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json", **headers},
                                 method="POST")
    try:
        # Model keys and the completion payload are authorized for this configured origin only.
        with open_no_redirect(req, timeout=_TIMEOUT) as r:
            return r.status, _read_bounded_response(r)
    except urllib.error.HTTPError as e:
        return e.code, _read_bounded_response(e)


def _get(url: str, headers: dict) -> tuple[int, str]:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        # urllib follows redirects by default and copies custom headers, which can leak
        # an STT token to an arbitrary redirect target. Refuse instead.
        with open_no_redirect(req, timeout=_TIMEOUT) as r:
            return r.status, _read_bounded_response(r)
    except urllib.error.HTTPError as e:
        return e.code, _read_bounded_response(e)


def _result(ok: bool, summary: str, **extra) -> dict:
    return {"ok": ok, "summary": summary, **extra}


def managed_models_status(config: dict, env: Optional[dict] = None) -> dict:
    """Non-secret status for an inherited operator model tier. Never opens a credential file or
    endpoint: a normal user may learn configured/not-configured, but not origin, expiry, or account."""
    env = env if env is not None else dict(os.environ)
    configured = bool(
        (config.get("mode") or config.get("base_url"))
        or env.get("ANTHROPIC_AUTH_TOKEN")
        or env.get("ANTHROPIC_API_KEY")
        or env.get("VEXA_LLM_API_KEY")
        or env.get("HOST_CLAUDE_CREDENTIALS")
        or env.get("CLAUDE_CODE_OAUTH_TOKEN")
    )
    return _result(
        configured,
        "Deployment model credentials are configured and operator-managed; live credential "
        "details are not exposed here."
        if configured else "Deployment model credentials are operator-managed but not configured.",
        source="operator",
        managed=True,
        mode=(config.get("mode") or "deployment"),
    )


def managed_transcription_status(url: str, token: str) -> dict:
    """Non-secret status for an inherited operator STT tier; deliberately performs no request."""
    configured = bool((url or "").strip() and (token or "").strip())
    return _result(
        configured,
        "Deployment transcription is configured and operator-managed; live account and balance "
        "details are not exposed here."
        if configured else "Deployment transcription is operator-managed but not fully configured.",
        source="operator",
        managed=True,
    )


# ── models ────────────────────────────────────────────────────────────────────────────────────

def test_subscription_credentials(creds_path: str = CREDS_PATH, *, now: Optional[float] = None) -> dict:
    """The mounted credentials file: present → parseable → unexpired. Expiry IS the recurring
    local failure (stale Keychain export), so the failure message ships the exact remedy."""
    if not os.path.isfile(creds_path):
        # docker turns a MISSING host path into an empty dir — same failure, same message.
        return _result(False, "No subscription credentials mounted "
                              "(HOST_CLAUDE_CREDENTIALS unset, or the host file is missing) — "
                              "all setup options: https://docs.vexa.ai/configuration")
    try:
        with open(creds_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return _result(False, "Credentials file is unreadable or not JSON — re-export it: "
                              + KEYCHAIN_REFRESH)
    oauth = data.get("claudeAiOauth") if isinstance(data, dict) else None
    expires_ms = (oauth or data or {}).get("expiresAt") if isinstance(oauth or data, dict) else None
    if not isinstance(expires_ms, (int, float)):
        return _result(False, "Credentials file has no expiresAt — not a Claude Code "
                              "credential export? Re-export it: " + KEYCHAIN_REFRESH)
    left_h = (expires_ms / 1000.0 - (now if now is not None else time.time())) / 3600.0
    if left_h <= 0:
        return _result(False, "Subscription token EXPIRED (stale Keychain export — the known "
                              "macOS gotcha). Refresh with: " + KEYCHAIN_REFRESH,
                       expired=True)
    return _result(True, f"Subscription credentials valid — token expires in {left_h:.1f} h. "
                         "(File check; inference itself runs in workers.)",
                   expires_in_hours=round(left_h, 1))


def test_custom_endpoint(base_url: str, api_key: str, model: str = "",
                         post: HttpPost = _post) -> dict:
    """A REAL 1-token completion against the configured endpoint. Anthropic-style first
    (``/v1/messages``), OpenAI-compat fallback (``/v1/chat/completions``) on 404/405 — the two
    dialects the dispatch overlay brokers (ANTHROPIC_* vs VEXA_LLM_*)."""
    base = base_url.rstrip("/")
    if not base:
        return _result(False, "Custom mode but no Base URL set.")
    model = model or "claude-haiku-4-5-20251001"
    auth = {"anthropic-version": "2023-06-01"}
    if api_key:
        auth.update({"x-api-key": api_key, "Authorization": f"Bearer {api_key}"})
    try:
        status, body = post(f"{base}/v1/messages",
                            {"model": model, "max_tokens": 1,
                             "messages": [{"role": "user", "content": "ping"}]}, auth)
        if status in (404, 405):  # not an anthropic dialect — try openai-compat
            status, body = post(f"{base}/v1/chat/completions",
                                {"model": model, "max_tokens": 1,
                                 "messages": [{"role": "user", "content": "ping"}]}, auth)
    except Exception:  # DNS, refused, TLS, timeout — keep exception text out of browser/log output
        return _result(False, "Endpoint unreachable.")
    if status in (401, 403):
        return _result(False, f"Authentication FAILED at {base} (HTTP {status}) — bad or "
                              "expired API key.", status=status)
    if 200 <= status < 300:
        return _result(True, f"Live completion OK against {base} (model {model}).",
                       status=status)
    # Never reflect an untrusted gateway body: some providers echo request auth on errors.
    return _result(False, f"Endpoint answered HTTP {status}.", status=status)


def run_models_test(config: dict, env: Optional[dict] = None,
                    creds_path: str = CREDS_PATH, post: HttpPost = _post) -> dict:
    """The EFFECTIVE model credential test — same resolution the dispatch overlay applies
    (Settings user > global config already collapsed by admin-api; env is the floor)."""
    env = env if env is not None else dict(os.environ)
    mode = (config.get("mode") or "").strip()
    if config.get("blocked"):
        return _result(
            False,
            "Personal model configuration is blocked by operator policy; choose an approved "
            "custom endpoint or Deployment default.",
            mode=mode or "custom",
            config={k: v for k, v in config.items() if k in ("mode", "model", "meeting_model") and v},
        )
    config_base_url = (config.get("base_url") or "").strip()
    config_api_key = (config.get("api_key") or "").strip()
    settings_custom = mode == "custom" or bool(config_base_url)
    if settings_custom:
        # Settings selects a complete credential owner tier. Missing fields stay missing; never
        # fill a user/platform URL with deployment secrets.
        base_url, api_key = config_base_url, config_api_key
    else:
        base_url = (env.get("ANTHROPIC_BASE_URL", "") or env.get("VEXA_LLM_BASE_URL", "")).strip()
        api_key = (env.get("ANTHROPIC_AUTH_TOKEN", "") or env.get("ANTHROPIC_API_KEY", "")
                   or env.get("VEXA_LLM_API_KEY", "")).strip()
    if settings_custom or (not mode and base_url and api_key):
        out = test_custom_endpoint(base_url, api_key, (config.get("model") or "").strip(),
                                   post=post)
        out["mode"] = "custom"
    else:
        out = test_subscription_credentials(creds_path)
        out["mode"] = "subscription"
    # Non-secret provenance so the UI can say WHAT was tested.
    out["config"] = {k: v for k, v in config.items() if k in ("mode", "model", "meeting_model",
                                                              "base_url") and v}
    return out


# ── transcription ─────────────────────────────────────────────────────────────────────────────

def run_transcription_test(url: str, token: str, source: str, get: HttpGet = _get) -> dict:
    """A real authenticated probe of the effective STT backend: GET ``{base}/balance`` with the
    token. Grades the answers the 2026-07-09 402 saga taught us to distinguish."""
    base = (url or "").strip().rstrip("/")
    if not base:
        return _result(False, "No transcription backend configured at any level "
                              "(user, global, or deployment env).", source=source)
    # Bots post to {url}/v1/audio/transcriptions — strip the path for the account probe.
    for suffix in ("/v1/audio/transcriptions", "/v1/audio", "/v1"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    if not token:
        return _result(False, f"Backend {base} configured but NO token set ({source}).",
                       source=source)
    try:
        status, body = get(f"{base}/balance", {"X-API-Key": token})
    except Exception:
        return _result(False, "Backend unreachable.", source=source)
    if status in (401, 403):
        return _result(False, f"Token REJECTED by {base} (HTTP {status}).", source=source,
                       status=status)
    if status == 404:
        # Not a vexa transcription gateway (no /balance) — reachable is all we can attest.
        return _result(True, f"Backend {base} reachable; no /balance endpoint, so the token "
                             "was not verified (non-Vexa gateway?).", source=source,
                       status=status, unverified=True)
    if status != 200:
        return _result(False, f"Backend answered HTTP {status}.", source=source, status=status)
    try:
        acct = json.loads(body)
    except ValueError:
        acct = {}
    email = acct.get("email") or "?"
    balance = acct.get("balance_minutes")
    if email == "internal@vexa.ai":
        return _result(True, f"OK — internal service token at {base} (billing-exempt).",
                       source=source, account=email)
    if isinstance(balance, (int, float)) and balance <= 0:
        return _result(False, f"Token valid ({email}) but balance is {balance:g} minutes — "
                              "every segment will fail 402 payment_required. Top up or switch "
                              "the token.", source=source, account=email, balance=balance)
    return _result(True, f"OK — {email}, {balance:g} minutes remaining." if isinstance(
        balance, (int, float)) else f"OK — {email}.", source=source, account=email,
        balance=balance)
