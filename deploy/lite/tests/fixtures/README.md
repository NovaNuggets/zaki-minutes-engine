# Lite deployment test fixtures

This directory contains executable probes used only by the Lite static and
container permission tests. They are not copied into production images.

- `lite-entrypoint-probe.sh` verifies non-Minutes root-owned credential projection and environment
  scrubbing at the entrypoint-to-supervisor boundary.
- `bot-boundary-probe/` verifies the per-meeting bot user, capability,
  filesystem, secret, PulseAudio, and sibling-process isolation boundaries.

Fixtures use synthetic values and must not contain deployable credentials.
