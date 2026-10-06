- **First-party services report errors to a Sentry-protocol sink (L-0990).** `admin-api`,
  `meeting-api` and `runtime` now report each unhandled exception and each 5xx they answer as one
  event to `SENTRY_DSN` (GlitchTip speaks the Sentry protocol), tagged with `SENTRY_ENVIRONMENT`
  and `SENTRY_RELEASE`. With `SENTRY_DSN` unset or blank the sink is fully inert — no SDK init, no
  middleware, no network. Events are scrubbed before send per L-0989: no request data, identity,
  breadcrumbs, frame locals or free-text messages; exception titles keep the type plus an ALL-CAPS
  code only. Chart wiring of `SENTRY_*` and egress to the sink host are deployment follow-ups.
