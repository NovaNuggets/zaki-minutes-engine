# webhooks — outbound delivery, system + per-client (O-MTG-2)

Outbound webhook delivery behind a **`WebhookSink`** port. Derived from the parent
`services/meeting-api/meeting_api/{webhook_delivery.py, webhook_url.py, webhook_retry_worker.py,
webhooks.py}`, reimplemented clean. The wire shape is sealed in `meetings/contracts/webhook.v1`.

## What it does
- **Envelope + HMAC** (`delivery.py`) — `build_envelope` = the `{event_id, event_type, api_version,
  created_at, data}` shape; `build_headers` signs `X-Webhook-Signature: sha256=<hmac(ts.payload)>`
  with the `X-Webhook-Timestamp` it used (replay window); `verify_signature` is the symmetric
  verifier a receiver runs (recompute HMAC over `ts.payload`, constant-time compare).
- **SSRF guard** (`ssrf.py`) — `validate_webhook_url` rejects localhost / loopback / link-local
  (incl. `169.254.169.254` cloud-metadata) / private CIDRs / internal Docker hostnames / non-http
  schemes, and resolves DNS names to catch rebinding. `resolver=` is injectable for offline evals.
- **Event filter** (`delivery.py`) — `is_event_enabled`: per-client subscribers only receive the
  events in their `webhook_events` map (default: `meeting.completed`). Suppressed before any HTTP.
- **Scopes** — `WebhookSink.deliver(..., scope=)`: `per-client` applies the filter; `system`
  (billing/analytics) bypasses it.
- **Retry** (`retry.py`) — a `RetryQueue` over a Redis list (`webhook:retry_queue`); a 5xx/429/
  transport-error enqueues; `drain_retry_queue` is one worker sweep (exponential `BACKOFF_SCHEDULE`
  = 1m·5m·30m·2h, 24h max-age). Direct and retry POSTs first acquire a content-free,
  meeting-scoped Redis delivery claim behind the permanent erasure cancel fence. Erasure installs
  that fence, purges queued/DLQ payloads, and waits for already-claimed transports to release;
  therefore no delivery can begin or finish after erasure reports completion. The eval drives the
  retry clock forward — no real sleeps.
- **Minutes platform finalization** (`platform_finalized.py`) — a separate default-off,
  operator-owned `minutes-finalized.v1` sink plus Redis outbox. It never enters the user's
  `webhook.v1` delivery path. Finalization intent is durable before the transcript
  finalizer runs; finalizer/delivery failure remains pending across callback replay and process
  restart. The drain performs a bounded, newest-first terminal-row backfill before processing the
  Redis queue, closing the crash window between the terminal PostgreSQL commit and Redis enqueue;
  repeated startup scans and lifecycle replays are harmless because a successful delivery leaves
  only a compact content-free completion tombstone. Redis contains no transcript, user-webhook
  payload, URL, secret, or signing-key binding. Each attempt signs with the current process-local
  operator key, so a pending envelope survives safe key rotation. The URL and signing secret stay
  in the process-local sink, and this path never uses `RetryQueue` (whose per-user compatibility
  entries include their own user webhook secret).

The HTTP transport is **injected** (`transport(url, body, headers) -> resp`), so the eval supplies a
fake in-memory receiver — no httpx, no network, no live receiver.

## Evals
`tests/test_webhook_signing.py` · `test_webhook_delivery.py` · `test_webhook_ssrf.py` ·
`test_minutes_platform_finalized.py`. Ride
`gate:python`. `webhook.v1` remains byte-for-byte isolated from the platform event;
`minutes-finalized.v1` envelope/header goldens conform via `gate:schema`.
